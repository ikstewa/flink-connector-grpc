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
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.connector.grpc.handler.GrpcResponseHandler;
import org.apache.flink.connector.grpc.service.GrpcServiceClient;
import org.apache.flink.connector.grpc.service.SharedGrpcServiceClient;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.functions.AsyncLookupFunction;
import org.apache.flink.table.functions.FunctionContext;

public class GrpcLookupFunction extends AsyncLookupFunction {

  private final GrpcServiceOptions grpcConfig;
  private final SerializationSchema<RowData> requestSchema;
  private final DeserializationSchema<RowData> responseSchema;
  private final GrpcResponseHandler<RowData, RowData, RowData> responseHandler;

  private transient GrpcServiceClient grpcClient;
  private transient AtomicInteger grpcCallCounter;
  private transient AtomicInteger grpcErrorCounter;

  public GrpcLookupFunction(
      GrpcServiceOptions grpcConfig,
      GrpcResponseHandler<RowData, RowData, RowData> responseHandler,
      SerializationSchema<RowData> requestSchema,
      DeserializationSchema<RowData> responseSchema) {
    this.grpcConfig = grpcConfig;
    this.requestSchema = requestSchema;
    this.responseSchema = responseSchema;
    this.responseHandler = responseHandler;
  }

  @Override
  public void open(FunctionContext context) throws Exception {
    super.open(context);
    this.requestSchema.open(null);
    this.responseSchema.open(null);

    this.grpcClient =
        SharedGrpcServiceClient.getOrCreate(
            this.grpcConfig, this.requestSchema, this.responseSchema);

    this.grpcCallCounter = new AtomicInteger(0);
    this.grpcErrorCounter = new AtomicInteger(0);
    context
        .getMetricGroup()
        .gauge("grpc-table-lookup-call-counter", () -> grpcCallCounter.intValue());
    context
        .getMetricGroup()
        .gauge("grpc-table-lookup-call-error", () -> grpcErrorCounter.intValue());
  }

  @Override
  public void close() throws Exception {
    this.grpcClient.close();
  }

  @Override
  public CompletableFuture<Collection<RowData>> asyncLookup(RowData keyRow) {
    this.grpcCallCounter.incrementAndGet();

    final var fut = this.grpcClient.asyncCall(keyRow);
    return fut.handle(
        (r, err) -> {
          if (err != null) {
            this.grpcErrorCounter.incrementAndGet();
          }
          if (err != null && !(err instanceof StatusRuntimeException)) {
            throw new CompletionException(err);
          } else {
            return List.of(this.responseHandler.handle(keyRow, r, (StatusRuntimeException) err));
          }
        });
  }
}
