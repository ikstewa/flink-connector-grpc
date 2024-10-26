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

import com.google.common.io.ByteStreams;
import com.google.gson.Gson;
import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.MethodDescriptor;
import io.grpc.MethodDescriptor.MethodType;
import io.grpc.stub.ClientCalls;
import io.grpc.stub.StreamObserver;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.functions.AsyncLookupFunction;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class GrpcLookupFunction extends AsyncLookupFunction {

  private static final Logger LOG = LogManager.getLogger(GrpcLookupFunction.class);
  private static final String RETRY_SERVICE_CONFIG =
      """
        {
          "methodConfig": [
            {
              "name": [ { } ],
              "retryPolicy": {
                "maxAttempts": %s,
                "initialBackoff": "0.5s",
                "maxBackoff": "30s",
                "backoffMultiplier": 2,
                "retryableStatusCodes": [
                  "UNAVAILABLE",
                  "INTERNAL",
                  "PERMISSION_DENIED"
                ]
              }
            }
          ]
        }""";

  private final GrpcServiceOptions grpcConfig;
  private final SerializationSchema<RowData> requestSchema;
  private final DeserializationSchema<RowData> responseSchema;
  private final ResultRowBuilder resultBuilder;

  private transient Channel channel;
  private transient MethodDescriptor<RowData, RowData> grpcService;
  private transient AtomicInteger grpcCallCounter;
  private transient AtomicInteger grpcErrorCounter;

  public GrpcLookupFunction(
      GrpcServiceOptions grpcConfig,
      ResultRowBuilder resultBuilder,
      SerializationSchema<RowData> requestSchema,
      DeserializationSchema<RowData> responseSchema) {
    this.grpcConfig = grpcConfig;
    this.requestSchema = requestSchema;
    this.responseSchema = responseSchema;
    this.resultBuilder = resultBuilder;
  }

  @Override
  @SuppressWarnings("unchecked")
  public void open(FunctionContext context) throws Exception {
    super.open(context);
    this.requestSchema.open(null);
    this.responseSchema.open(null);

    var channelBuilder =
        ManagedChannelBuilder.forAddress(this.grpcConfig.url(), this.grpcConfig.port());
    if (this.grpcConfig.maxRetryTimes() > 0) {
      final var serviceConfig =
          String.format(RETRY_SERVICE_CONFIG, this.grpcConfig.maxRetryTimes());
      channelBuilder =
          channelBuilder
              .defaultServiceConfig(new Gson().fromJson(serviceConfig, Map.class))
              .enableRetry()
              .maxRetryAttempts(this.grpcConfig.maxRetryTimes());
    } else {
      LOG.warn("'lookup.max-retries' set to 0. Disabling retries..");
      channelBuilder = channelBuilder.disableRetry();
    }
    if (this.grpcConfig.usePlainText()) {
      channelBuilder = channelBuilder.usePlaintext();
    }
    this.channel = channelBuilder.build();

    // MethodDescriptor is not serializable so re-build on open
    final var marshaller = new RequestMarshaller(this.requestSchema, this.responseSchema);
    this.grpcService =
        io.grpc.MethodDescriptor.<RowData, RowData>newBuilder()
            .setType(MethodType.UNARY)
            .setFullMethodName(this.grpcConfig.serviceMethodName())
            .setSampledToLocalTracing(true)
            .setRequestMarshaller(marshaller)
            .setResponseMarshaller(marshaller)
            .build();

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
  public CompletableFuture<Collection<RowData>> asyncLookup(RowData keyRow) {
    this.grpcCallCounter.incrementAndGet();
    final var fut = new CompletableFuture<RowData>();
    ClientCalls.asyncUnaryCall(
        channel.newCall(this.grpcService, CallOptions.DEFAULT), keyRow, new UnaryHandler<>(fut));
    return fut.thenApply(r -> List.of(this.resultBuilder.apply(keyRow, r)));
  }

  /** Internal GRPC marshaller which uses a SerializationSchema to convert RowData to bytes. */
  private static final class RequestMarshaller implements MethodDescriptor.Marshaller<RowData> {

    private final SerializationSchema<RowData> serializer;
    private final DeserializationSchema<RowData> deserializer;

    public RequestMarshaller(
        SerializationSchema<RowData> ser, DeserializationSchema<RowData> deser) {
      this.serializer = ser;
      this.deserializer = deser;
    }

    @Override
    public RowData parse(InputStream stream) {
      try {
        return this.deserializer.deserialize(ByteStreams.toByteArray(stream));
      } catch (IOException e) {
        throw new RuntimeException("Error reading grpc response bytes", e);
      }
    }

    @Override
    public InputStream stream(RowData row) {
      return new ByteArrayInputStream(this.serializer.serialize(row));
    }
  }

  /** StreamObserver for GRPC to convert to a CompletableFuture. */
  public class UnaryHandler<T> implements StreamObserver<T> {
    private T result;
    private final CompletableFuture<T> future;

    public UnaryHandler(CompletableFuture<T> f) {
      this.future = f;
    }

    @Override
    public void onNext(T value) {
      result = value;
    }

    @Override
    public void onError(Throwable t) {
      grpcErrorCounter.incrementAndGet();
      future.completeExceptionally(t);
    }

    @Override
    public void onCompleted() {
      if (result != null) {
        future.complete(result);
      } else {
        future.completeExceptionally(new Error("Failed to complete properly"));
      }
    }
  }
}
