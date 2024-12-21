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
package io.mock.grpc;

import com.dashjoin.jsonata.Functions;
import com.dashjoin.jsonata.Jsonata;
import com.dashjoin.jsonata.json.Json;
import com.google.common.base.Preconditions;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import com.google.protobuf.util.JsonFormat;
import io.grpc.MethodDescriptor;
import io.grpc.MethodDescriptor.Marshaller;
import io.grpc.MethodDescriptor.MethodType;
import io.grpc.MethodDescriptor.ReflectableMarshaller;
import io.grpc.ServerServiceDefinition;
import io.grpc.Status;
import io.grpc.stub.ServerCalls.UnaryMethod;
import io.grpc.stub.StreamObserver;
import io.mock.grpc.MockServer.JsonataErrorResponse;
import io.mock.grpc.MockServer.JsonataResponse;
import io.mock.grpc.MockServer.Service;
import java.io.InputStream;
import java.util.List;
import java.util.Objects;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

class JsonataRpcService {
  private static final Logger LOG = LogManager.getLogger(MockJsonRpcServer.class);

  private final ServerServiceDefinition serviceDef;

  public JsonataRpcService(Service config) {
    this.serviceDef = buildServiceDefinition(Objects.requireNonNull(config));
  }

  public ServerServiceDefinition serviceDefinition() {
    return this.serviceDef;
  }

  private static <ReqT, RespT> ServerServiceDefinition buildServiceDefinition(
      MockServer.Service serviceConfig) {

    final var service = loadMethodDescriptor(serviceConfig.methodDescriptorSource);

    // Reubild the MethodDescriptor with wrapping JSON Marshaller
    final MethodDescriptor<String, String> jsonMethodDescriptor =
        io.grpc.MethodDescriptor.<String, String>newBuilder()
            .setType(MethodType.UNARY)
            .setFullMethodName(service.getFullMethodName())
            .setSampledToLocalTracing(true)
            .setRequestMarshaller(new JsonWrappingMarshaller(service.getRequestMarshaller()))
            .setResponseMarshaller(new JsonWrappingMarshaller(service.getResponseMarshaller()))
            .build();

    final io.grpc.ServiceDescriptor serviceDescriptor =
        io.grpc.ServiceDescriptor.newBuilder(service.getFullMethodName().split("/")[0])
            // .setSchemaDescriptor(new GreeterFileDescriptorSupplier())
            .addMethod(jsonMethodDescriptor)
            .build();

    final var sDef =
        io.grpc.ServerServiceDefinition.builder(serviceDescriptor)
            .addMethod(
                jsonMethodDescriptor,
                io.grpc.stub.ServerCalls.asyncUnaryCall(
                    new JSONataRequestHandler(serviceConfig.requests)))
            .build();

    return sDef;
  }

  /** Request handler leveraging JSONata for matching and transformation. */
  private static class JSONataRequestHandler implements UnaryMethod<String, String> {

    private final List<JSONataRequest> jsonataRequets;

    public JSONataRequestHandler(List<? extends MockServer.MockRequest> requests) {
      this.jsonataRequets =
          requests.stream()
              .<JSONataRequest>map(
                  r -> {
                    if (r instanceof JsonataResponse rj) {
                      return new JSONataResponse(
                          rj.requestExpression,
                          Jsonata.jsonata(rj.requestExpression),
                          rj.responseExpression,
                          Jsonata.jsonata(rj.responseExpression));
                    } else if (r instanceof JsonataErrorResponse er) {
                      return new JSONataErrorResponse(
                          er.requestExpression,
                          Jsonata.jsonata(er.requestExpression),
                          er.responseStatusCode,
                          er.responseMessage);
                    } else {
                      throw new IllegalArgumentException(
                          "Unimplemented request type: " + r.getClass());
                    }
                  })
              .toList();
    }

    @Override
    public void invoke(String request, StreamObserver<String> responseObserver) {

      LOG.debug("Processing request: '{}'", request);

      final var requestJson = Json.parseJson(request);

      for (var r : this.jsonataRequets) {
        final var jResult = r.request().evaluate(requestJson);
        LOG.debug("JSONata: query: '{}' result: '{}'", r.requestString(), jResult);

        if (Boolean.TRUE.equals(jResult)) {
          LOG.info("Found matching request: '{}'", r.request());

          if (r instanceof JSONataResponse jr) {
            final var response = Functions.string(jr.response().evaluate(requestJson), false);
            LOG.debug("Generated response: '{}'", response);

            responseObserver.onNext(response);
            responseObserver.onCompleted();
            return;
          } else if (r instanceof JSONataErrorResponse jer) {
            responseObserver.onError(
                Status.fromCodeValue(jer.responseStatusCode)
                    .augmentDescription(jer.responseMessage)
                    .asException());
            return;
          } else {
            throw new RuntimeException("Unexepcted code path");
          }
        }
      }

      LOG.info("No matching request for for: '{}'", request);

      responseObserver.onError(Status.NOT_FOUND.asException());
    }

    private static record JSONataResponse(
        String requestString, Jsonata request, String responseString, Jsonata response)
        implements JSONataRequest {}

    private static record JSONataErrorResponse(
        String requestString, Jsonata request, int responseStatusCode, String responseMessage)
        implements JSONataRequest {}

    private static sealed interface JSONataRequest {
      String requestString();

      Jsonata request();
    }
  }

  /** Wrapping Marshaller which transforms GRPC type into JSON */
  private static class JsonWrappingMarshaller implements Marshaller<String> {

    private final ReflectableMarshaller<Message> protoMarsh;

    @SuppressWarnings("unchecked")
    public JsonWrappingMarshaller(Marshaller<?> protoMarsh) {
      Preconditions.checkArgument(protoMarsh instanceof ReflectableMarshaller);
      this.protoMarsh = (ReflectableMarshaller<Message>) protoMarsh;
    }

    public InputStream stream(String value) {
      try {
        Message defaultInstance =
            (Message) protoMarsh.getMessageClass().getMethod("getDefaultInstance").invoke(null);

        final var builder = defaultInstance.toBuilder();

        JsonFormat.parser().merge(value, builder);

        final Message msg = (Message) builder.build();
        return protoMarsh.stream(msg);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }

    public String parse(InputStream stream) {
      final Message msg = protoMarsh.parse(stream);
      try {
        return JsonFormat.printer().preservingProtoFieldNames().print(msg);
      } catch (InvalidProtocolBufferException e) {
        throw new RuntimeException(e);
      }
    }
  }

  @SuppressWarnings("unchecked")
  private static <ReqT, RespT> io.grpc.MethodDescriptor<ReqT, RespT> loadMethodDescriptor(
      String method) {
    try {
      final var clazzName = method.split("#")[0];
      final var methodName = method.split("#")[1];

      Class<?> grpcClass =
          Class.forName(clazzName, true, Thread.currentThread().getContextClassLoader());
      return (io.grpc.MethodDescriptor<ReqT, RespT>) grpcClass.getMethod(methodName).invoke(null);
    } catch (Exception e) {
      throw new IllegalArgumentException(
          String.format("Failed to load method descriptor from %s", method), e);
    }
  }
}
