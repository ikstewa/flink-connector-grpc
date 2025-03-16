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

import com.google.common.io.ByteStreams;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.gson.Gson;
import com.google.rpc.Code;
import io.grpc.CallOptions;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.MethodDescriptor;
import io.grpc.MethodDescriptor.MethodType;
import io.grpc.stub.ClientCalls;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.connector.grpc.GrpcServiceOptions;
import org.apache.flink.table.data.RowData;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

class GrpcServiceClientImpl implements GrpcServiceClient {

  private static final Gson GSON = new Gson();
  private static final Logger LOG = LogManager.getLogger(GrpcServiceClient.class);

  private final ManagedChannel channel;
  private final MethodDescriptor<RowData, RowData> grpcMethodDesc;

  GrpcServiceClientImpl(
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

  public ListenableFuture<RowData> asyncCall(RowData req, @Nullable Executor executor) {
    LOG.debug("Sending async GRPC request with row: {}", req);
    var callOptions = CallOptions.DEFAULT;
    if (executor != null) {
      callOptions = callOptions.withExecutor(executor);
    }
    return ClientCalls.futureUnaryCall(channel.newCall(this.grpcMethodDesc, callOptions), req);
  }

  @Override
  public void close() throws IOException {
    try {
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
      final var serviceConfig = buildServiceConfig(grpcConfig);
      LOG.info("Using GRPC ServiceConfig: {}", serviceConfig);
      channelBuilder =
          channelBuilder
              .defaultServiceConfig(serviceConfig)
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
    // Use a direct executor by default. Set the executor on the client call options to override
    channelBuilder = channelBuilder.directExecutor();
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
}
