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
package org.apache.flink.connector.grpc.service;

import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.google.common.io.ByteStreams;
import com.google.gson.Gson;
import com.google.rpc.Code;
import io.grpc.CallOptions;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.MethodDescriptor;
import io.grpc.MethodDescriptor.MethodType;
import io.grpc.stub.ClientCalls;
import io.grpc.stub.StreamObserver;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.connector.grpc.GrpcServiceOptions;
import org.apache.flink.table.data.RowData;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class GrpcServiceClient implements Closeable {

  private static final Gson GSON = new Gson();
  private static final Logger LOG = LogManager.getLogger(GrpcServiceClient.class);

  private static final LoadingCache<CacheKey, GrpcServiceClient> CLIENT_CACHE =
      Caffeine.newBuilder().maximumSize(100).build(GrpcServiceClient::new);

  // TODO: Add cache for re-using channels

  public static GrpcServiceClient getOrCreate(
      GrpcServiceOptions config,
      SerializationSchema<RowData> requestSchema,
      DeserializationSchema<RowData> responseSchema) {
    return CLIENT_CACHE.get(new CacheKey(config, requestSchema, responseSchema));
  }

  private final ManagedChannel channel;
  private final MethodDescriptor<RowData, RowData> grpcMethodDesc;

  private GrpcServiceClient(CacheKey key) {
    this(key.config(), key.requestSchema(), key.responseSchema());
  }

  private GrpcServiceClient(
      GrpcServiceOptions config,
      SerializationSchema<RowData> requestSchema,
      DeserializationSchema<RowData> responseSchema) {
    this.channel = buildChannel(config);
    // MethodDescriptor is not serializable so re-build on open
    final var marshaller = new RequestMarshaller(requestSchema, responseSchema);
    this.grpcMethodDesc =
        io.grpc.MethodDescriptor.<RowData, RowData>newBuilder()
            .setType(MethodType.UNARY)
            .setFullMethodName(config.serviceMethodName())
            .setSampledToLocalTracing(true)
            .setRequestMarshaller(marshaller)
            .setResponseMarshaller(marshaller)
            .build();
  }

  public CompletableFuture<RowData> asyncCall(RowData req) {
    final var fut = new CompletableFuture<RowData>();
    ClientCalls.asyncUnaryCall(
        channel.newCall(this.grpcMethodDesc, CallOptions.DEFAULT), req, new UnaryHandler<>(fut));
    return fut;
  }

  @Override
  public void close() throws IOException {
    try {
      // FIXME: TODO: Need ref counters for closing!
      if (!this.channel.isShutdown()) {
        this.channel.shutdown();
        this.channel.awaitTermination(10L, TimeUnit.SECONDS);
      }
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
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
  private static class UnaryHandler<T> implements StreamObserver<T> {
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

  private record CacheKey(
      GrpcServiceOptions config,
      SerializationSchema<RowData> requestSchema,
      DeserializationSchema<RowData> responseSchema)
      implements Serializable {

    @Override
    public boolean equals(Object obj) {
      if (obj instanceof CacheKey other) {
        // H4k!? : PbRowDataSerializationSchema does not implement equals. Use serialization to
        // compare.
        return Arrays.equals(toBytes(this), toBytes(other));
      } else {
        return false;
      }
    }

    @Override
    public int hashCode() {
      return Arrays.hashCode(toBytes(this));
    }

    private static byte[] toBytes(Object o) {
      ByteArrayOutputStream bos = new ByteArrayOutputStream();
      try (ObjectOutputStream oos = new ObjectOutputStream(bos)) {
        oos.writeObject(o);
        oos.flush();
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
      return bos.toByteArray();
    }
  }
}
