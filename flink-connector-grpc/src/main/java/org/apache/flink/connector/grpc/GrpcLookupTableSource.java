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

import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nullable;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.connector.grpc.handler.GrpcResponseHandler;
import org.apache.flink.connector.grpc.util.Projections;
import org.apache.flink.table.connector.Projection;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.format.EncodingFormat;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.LookupTableSource;
import org.apache.flink.table.connector.source.abilities.SupportsReadingMetadata;
import org.apache.flink.table.connector.source.lookup.AsyncLookupFunctionProvider;
import org.apache.flink.table.connector.source.lookup.LookupFunctionProvider;
import org.apache.flink.table.connector.source.lookup.PartialCachingAsyncLookupProvider;
import org.apache.flink.table.connector.source.lookup.PartialCachingLookupProvider;
import org.apache.flink.table.connector.source.lookup.cache.LookupCache;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.utils.JoinedRowData;
import org.apache.flink.table.data.utils.ProjectedRowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.util.FlinkRuntimeException;

class GrpcLookupTableSource implements LookupTableSource, SupportsReadingMetadata {

  private final GrpcServiceOptions grpcConfig;
  private final EncodingFormat<SerializationSchema<RowData>> requestFormat;
  private final DecodingFormat<DeserializationSchema<RowData>> responseFormat;
  @Nullable private final LookupCache cache;
  private DataType physicalRowDataType;
  private List<GrpcMetadataField> metadataFields;
  private final boolean async;

  GrpcLookupTableSource(
      GrpcServiceOptions grpcConfig,
      DataType physicalRowDataType,
      EncodingFormat<SerializationSchema<RowData>> requestFormat,
      DecodingFormat<DeserializationSchema<RowData>> responseFormat,
      @Nullable LookupCache cache,
      boolean async) {
    this.grpcConfig = grpcConfig;
    this.physicalRowDataType = physicalRowDataType;
    this.metadataFields = List.of();
    this.requestFormat = requestFormat;
    this.responseFormat = responseFormat;
    this.cache = cache;
    this.async = async;
  }

  @Override
  public DynamicTableSource copy() {
    return new GrpcLookupTableSource(
        this.grpcConfig,
        this.physicalRowDataType,
        this.requestFormat,
        this.responseFormat,
        this.cache,
        this.async);
  }

  @Override
  public String asSummaryString() {
    return "GRPC Lookup Table Source";
  }

  @Override
  @SuppressWarnings("unchecked")
  public LookupRuntimeProvider getLookupRuntimeProvider(LookupContext lookupContext) {
    // Create projections from physical data type to request/response/metadata types
    final int[][] reqProjection =
        Projections.trim(
                Projection.of(lookupContext.getKeys()),
                DataType.getFieldCount(this.physicalRowDataType)
                    - this.metadataFields.size()) // exclude extra metadata fields
            .toNestedIndexes();
    final int[][] respProjection =
        Projections.trim(
                Projection.of(reqProjection).complement(this.physicalRowDataType),
                DataType.getFieldCount(this.physicalRowDataType) - this.metadataFields.size())
            .toNestedIndexes();
    final int[][] metadataProjection =
        Projections.concat(reqProjection, respProjection)
            .complement(this.physicalRowDataType)
            .toNestedIndexes();
    // Projection from `new JoinedRowData(req, resp, metadata)` -> `physicalRowDataType`
    final int[][] resultProjection =
        Projections.concat(reqProjection, respProjection, metadataProjection).toNestedIndexes();

    // Create (de)serializers for GRPC request/response
    final SerializationSchema<RowData> requestSchemaEncoder =
        this.requestFormat.createRuntimeEncoder(
            null, Projection.of(reqProjection).project(this.physicalRowDataType));
    final DeserializationSchema<RowData> responseSchemaDecoder =
        this.responseFormat.createRuntimeDecoder(
            lookupContext, Projection.of(respProjection).project(this.physicalRowDataType));

    final var metadataParser = new GrpcMetadataParser(this.metadataFields);
    final var errorCodes = Set.copyOf(this.grpcConfig.errorStatusCodes());
    final GrpcResponseHandler<RowData, RowData, RowData> responseHandler =
        (reqRow, respRow, error) -> {
          if (error != null && errorCodes.contains(error.getStatus().getCode().value())) {
            // Throw a flink exception as StatusRuntimeException is not serializeable
            throw new FlinkRuntimeException(
                "GRPC request failed after retries: " + error.getMessage());
          }
          final var resultRow =
              new JoinedRowData(
                  new JoinedRowData(
                      reqRow,
                      respRow != null ? respRow : new GenericRowData(respProjection.length)),
                  metadataParser.toRow(error));
          return ProjectedRowData.from(resultProjection).replaceRow(resultRow);
        };

    final var requestHandler =
        (Function<RowData, RowData> & Serializable)
            req -> {
              // When queries filter by metadata such as stats_code = 0, the value will show up as a
              // column in the request row. Trim the request so it includes only the lookup columns.
              // We will extract out the metadata columns from the response instead.
              if (req.getArity() == reqProjection.length) {
                return req;
              } else {
                final var target = new GenericRowData(reqProjection.length);
                for (var i = 0; i < target.getArity(); i++) {
                  target.setField(i, ((GenericRowData) req).getField(i));
                }
                return target;
              }
            };

    if (async) {
      final var asyncLookupFunc =
          new AsyncGrpcLookupFunction(
              this.grpcConfig,
              requestHandler,
              responseHandler,
              requestSchemaEncoder,
              responseSchemaDecoder);
      if (cache != null) {
        return PartialCachingAsyncLookupProvider.of(asyncLookupFunc, cache);
      } else {
        return AsyncLookupFunctionProvider.of(asyncLookupFunc);
      }
    } else {
      final var lookupFunc =
          new GrpcLookupFunction(
              this.grpcConfig,
              requestHandler,
              responseHandler,
              requestSchemaEncoder,
              responseSchemaDecoder);
      if (cache != null) {
        return PartialCachingLookupProvider.of(lookupFunc, cache);
      } else {
        return LookupFunctionProvider.of(lookupFunc);
      }
    }
  }

  @Override
  public Map<String, DataType> listReadableMetadata() {
    return Stream.of(GrpcMetadataField.values())
        .collect(Collectors.toMap(md -> md.key, md -> md.dataType));
  }

  @Override
  public void applyReadableMetadata(List<String> metadataKeys, DataType producedDataType) {
    this.metadataFields = metadataKeys.stream().map(GrpcMetadataField::fromKey).toList();
    this.physicalRowDataType = producedDataType;
  }
}
