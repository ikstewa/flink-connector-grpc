//
// Copyright 2024 Ian Stewart
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
package org.apache.flink.connector.grpc;

import io.grpc.StatusRuntimeException;
import java.io.Serializable;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import javax.annotation.Nullable;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.connector.Projection;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.format.EncodingFormat;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.LookupTableSource;
import org.apache.flink.table.connector.source.abilities.SupportsLimitPushDown;
import org.apache.flink.table.connector.source.abilities.SupportsProjectionPushDown;
import org.apache.flink.table.connector.source.abilities.SupportsReadingMetadata;
import org.apache.flink.table.connector.source.lookup.AsyncLookupFunctionProvider;
import org.apache.flink.table.connector.source.lookup.PartialCachingAsyncLookupProvider;
import org.apache.flink.table.connector.source.lookup.cache.LookupCache;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.utils.JoinedRowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.utils.LogicalTypeChecks;

class GrpcLookupTableSource
    implements LookupTableSource,
        SupportsProjectionPushDown,
        SupportsLimitPushDown,
        SupportsReadingMetadata {

  private final GrpcServiceOptions grpcConfig;
  private DataType physicalRowDataType;
  private final EncodingFormat<SerializationSchema<RowData>> requestFormat;
  private final DecodingFormat<DeserializationSchema<RowData>> responseFormat;
  @Nullable private final LookupCache cache;
  private List<GrpcMetadataField> metadataFields;

  GrpcLookupTableSource(
      GrpcServiceOptions grpcConfig,
      DataType physicalRowDataType,
      EncodingFormat<SerializationSchema<RowData>> requestFormat,
      DecodingFormat<DeserializationSchema<RowData>> responseFormat,
      @Nullable LookupCache cache) {
    this.grpcConfig = grpcConfig;
    this.physicalRowDataType = physicalRowDataType;
    this.metadataFields = List.of();
    this.requestFormat = requestFormat;
    this.responseFormat = responseFormat;
    this.cache = cache;
  }

  @Override
  public DynamicTableSource copy() {
    return new GrpcLookupTableSource(
        this.grpcConfig,
        this.physicalRowDataType,
        this.requestFormat,
        this.responseFormat,
        this.cache);
  }

  @Override
  public String asSummaryString() {
    return "GRPC Lookup Table Source";
  }

  @Override
  public LookupRuntimeProvider getLookupRuntimeProvider(LookupContext lookupContext) {
    final var rowDef =
        extractRequestResponseTypes(this.physicalRowDataType, lookupContext.getKeys());

    final var lookupFunc =
        new GrpcLookupFunction(
            this.grpcConfig,
            new ResponseHandler(rowDef.combiner(), this.metadataFields),
            this.requestFormat.createRuntimeEncoder(null, rowDef.requestRow()),
            this.responseFormat.createRuntimeDecoder(lookupContext, rowDef.responseRow()));

    if (cache != null) {
      return PartialCachingAsyncLookupProvider.of(lookupFunc, cache);
    } else {
      return AsyncLookupFunctionProvider.of(lookupFunc);
    }
  }

  private static class ResponseHandler implements GrpcResponseHandler<RowData, RowData, RowData> {

    private final BiFunction<RowData, RowData, RowData> combiner;
    private final List<GrpcMetadataField> metaFields;

    public ResponseHandler(
        BiFunction<RowData, RowData, RowData> combiner, List<GrpcMetadataField> metaFields) {
      this.combiner = combiner;
      this.metaFields = metaFields;
    }

    @Override
    public RowData handle(RowData request, RowData response, StatusRuntimeException err) {
      // TODO: Add config for supressing status codes and throw others
      return new JoinedRowData(combiner.apply(request, response), createMetadataFields(err));
    }

    private RowData createMetadataFields(@Nullable StatusRuntimeException error) {
      final var row = new GenericRowData(metaFields.size());
      for (var i = 0; i < metaFields.size(); i++) {
        final var metaVal =
            switch (metaFields.get(i)) {
              case STATUS_CODE -> error == null ? 0 : error.getStatus().getCode().value();
            };
        row.setField(i, metaVal);
      }
      return row;
    }
  }

  /**
   * Given a DataType with only the physical columns and the lookup join keys, determine which
   * fields are mapped to the request type and the response type.
   *
   * <p>Note: Overlapping names take precendence from request to ensure join keys always match
   */
  private static RowDefinition extractRequestResponseTypes(DataType physicalRow, int[][] keys) {
    final List<String> fieldNames = LogicalTypeChecks.getFieldNames(physicalRow.getLogicalType());
    final List<DataType> fieldTypes = physicalRow.getChildren();

    final Set<Integer> keyIdx = new HashSet<>();
    for (int[] key : keys) {
      for (int keyIndex : key) {
        keyIdx.add(keyIndex);
      }
    }

    final List<Integer> reqRowIdx =
        IntStream.range(0, fieldNames.size()).filter(i -> keyIdx.contains(i)).boxed().toList();
    final List<Integer> respRowIdx =
        IntStream.range(0, fieldNames.size()).filter(i -> !keyIdx.contains(i)).boxed().toList();

    final DataType reqRowType =
        DataTypes.ROW(
            reqRowIdx.stream()
                .map(i -> DataTypes.FIELD(fieldNames.get(i), fieldTypes.get(i)))
                .toList());
    final DataType respRowType =
        DataTypes.ROW(
            respRowIdx.stream()
                .map(i -> DataTypes.FIELD(fieldNames.get(i), fieldTypes.get(i)))
                .toList());

    final ResultRowBuilder combiner =
        (reqRow, respRow) -> {
          GenericRowData row = new GenericRowData(fieldNames.size());
          if (respRow != null) {
            for (var i = 0; i < respRowIdx.size(); i++) {
              row.setField(respRowIdx.get(i), ((GenericRowData) respRow).getField(i));
            }
          }
          for (var i = 0; i < reqRowIdx.size(); i++) {
            row.setField(reqRowIdx.get(i), ((GenericRowData) reqRow).getField(i));
          }
          return row;
        };
    return new RowDefinition(reqRowType, respRowType, combiner);
  }

  private interface ResultRowBuilder extends BiFunction<RowData, RowData, RowData>, Serializable {}

  private record RowDefinition(
      DataType requestRow, DataType responseRow, ResultRowBuilder combiner) {}

  @Override
  public void applyProjection(int[][] projectedFields, DataType producedDataType) {
    this.physicalRowDataType = Projection.of(projectedFields).project(physicalRowDataType);
  }

  @Override
  public boolean supportsNestedProjection() {
    return true;
  }

  @Override
  public void applyLimit(long limit) {}

  @Override
  public Map<String, DataType> listReadableMetadata() {
    return Stream.of(GrpcMetadataField.values())
        .collect(Collectors.toMap(md -> md.key, md -> md.dataType));
  }

  @Override
  public void applyReadableMetadata(List<String> metadataKeys, DataType producedDataType) {
    this.metadataFields = metadataKeys.stream().map(GrpcMetadataField::fromKey).toList();
  }
}
