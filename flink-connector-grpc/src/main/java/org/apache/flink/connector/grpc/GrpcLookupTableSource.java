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

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nullable;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.connector.grpc.handler.CombinedRowResponseHandler;
import org.apache.flink.connector.grpc.handler.GrpcResponseHandler;
import org.apache.flink.connector.grpc.handler.JoinedResponseHandler;
import org.apache.flink.connector.grpc.handler.MetadataResponseHandler;
import org.apache.flink.table.connector.Projection;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.format.EncodingFormat;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.LookupTableSource;
import org.apache.flink.table.connector.source.abilities.SupportsProjectionPushDown;
import org.apache.flink.table.connector.source.abilities.SupportsReadingMetadata;
import org.apache.flink.table.connector.source.lookup.AsyncLookupFunctionProvider;
import org.apache.flink.table.connector.source.lookup.PartialCachingAsyncLookupProvider;
import org.apache.flink.table.connector.source.lookup.cache.LookupCache;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;

class GrpcLookupTableSource
    implements LookupTableSource, SupportsProjectionPushDown, SupportsReadingMetadata {

  private final GrpcServiceOptions grpcConfig;
  private final EncodingFormat<SerializationSchema<RowData>> requestFormat;
  private final DecodingFormat<DeserializationSchema<RowData>> responseFormat;
  @Nullable private final LookupCache cache;
  private DataType physicalRowDataType;
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

    // Create projections from physical data type to to request/response types
    final var reqProjection = Projection.of(lookupContext.getKeys());
    final var respProjection = Projection.all(this.physicalRowDataType).difference(reqProjection);

    // Create (de)serializers for GRPC request/response
    final SerializationSchema<RowData> requestSchemaEncoder =
        this.requestFormat.createRuntimeEncoder(
            null, reqProjection.project(this.physicalRowDataType));
    final DeserializationSchema<RowData> responseSchemaDecoder =
        this.responseFormat.createRuntimeDecoder(
            lookupContext, respProjection.project(this.physicalRowDataType));

    // Create the response handler to compose the final produced row
    final GrpcResponseHandler<RowData, RowData, RowData> responseHandler =
        new JoinedResponseHandler<>(
            CombinedRowResponseHandler.fromProjection(
                reqProjection, respProjection, this.physicalRowDataType),
            new MetadataResponseHandler(this.metadataFields));

    final var lookupFunc =
        new GrpcLookupFunction(
            this.grpcConfig, responseHandler, requestSchemaEncoder, responseSchemaDecoder);

    if (cache != null) {
      return PartialCachingAsyncLookupProvider.of(lookupFunc, cache);
    } else {
      return AsyncLookupFunctionProvider.of(lookupFunc);
    }
  }

  @Override
  public void applyProjection(int[][] projectedFields, DataType producedDataType) {
    this.physicalRowDataType = Projection.of(projectedFields).project(physicalRowDataType);
  }

  @Override
  public boolean supportsNestedProjection() {
    return true;
  }

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
