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
import com.google.rpc.Code;
import io.grpc.CallOptions;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.MethodDescriptor;
import io.grpc.MethodDescriptor.MethodType;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.ClientCalls;
import io.grpc.stub.StreamObserver;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.connector.grpc.handler.GrpcResponseHandler;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.functions.AsyncLookupFunction;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class GrpcLookupFunction extends AsyncLookupFunction {

  private static final Gson GSON = new Gson();
  private static final Logger LOG = LogManager.getLogger(GrpcLookupFunction.class);

  private final GrpcServiceOptions grpcConfig;
  private final SerializationSchema<RowData> requestSchema;
  private final DeserializationSchema<RowData> responseSchema;
  private final GrpcResponseHandler<RowData, RowData, RowData> responseHandler;

  private transient ManagedChannel channel;
  private transient MethodDescriptor<RowData, RowData> grpcService;
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

    this.channel = buildChannel(this.grpcConfig);

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

  private static ManagedChannel buildChannel(GrpcServiceOptions grpcConfig) {
    var channelBuilder = ManagedChannelBuilder.forAddress(grpcConfig.url(), grpcConfig.port());
    if (grpcConfig.maxRetryTimes() > 0) {
      channelBuilder =
          channelBuilder
              .defaultServiceConfig(buildServiceConfig(grpcConfig))
              .enableRetry()
              .maxRetryAttempts(grpcConfig.maxRetryTimes());
    } else {
      LOG.warn("'lookup.max-retries' set to 0. Disabling retries..");
      channelBuilder = channelBuilder.disableRetry();
    }
    if (grpcConfig.usePlainText()) {
      channelBuilder = channelBuilder.usePlaintext();
    }
    channelBuilder = channelBuilder.maxInboundMessageSize(Integer.MAX_VALUE);
    return channelBuilder.build();
  }

  @SuppressWarnings("unchecked")
  private static Map<String, ?> buildServiceConfig(GrpcServiceOptions grpcConfig) {
    final var serviceConfig =
        String.format(
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
                    "retryableStatusCodes": %s
                  }
                }
              ]
            }""",
            grpcConfig.maxRetryTimes(),
            GSON.toJson(grpcConfig.retryStatusCodes().stream().map(Code::forNumber).toList()));
    return GSON.fromJson(serviceConfig, Map.class);
  }

  @Override
  public void close() throws Exception {
    this.channel.shutdown();
    this.channel.awaitTermination(10L, TimeUnit.SECONDS);
  }

  @Override
  public CompletableFuture<Collection<RowData>> asyncLookup(RowData keyRow) {
    this.grpcCallCounter.incrementAndGet();
    final var fut = new CompletableFuture<RowData>();
    ClientCalls.asyncUnaryCall(
        channel.newCall(this.grpcService, CallOptions.DEFAULT), keyRow, new UnaryHandler<>(fut));
    return fut.handle(
        (r, err) -> {
          if (err != null && !(err instanceof StatusRuntimeException)) {
            throw new CompletionException(err);
          } else {
            return List.of(this.responseHandler.handle(keyRow, r, (StatusRuntimeException) err));
          }
        });
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
  private class UnaryHandler<T> implements StreamObserver<T> {
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
