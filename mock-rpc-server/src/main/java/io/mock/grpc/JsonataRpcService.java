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
import com.google.protobuf.DescriptorProtos.FileDescriptorSet;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import io.grpc.MethodDescriptor;
import io.grpc.MethodDescriptor.Marshaller;
import io.grpc.MethodDescriptor.MethodType;
import io.grpc.ServerServiceDefinition;
import io.grpc.Status;
import io.grpc.protobuf.ProtoUtils;
import io.grpc.stub.ServerCalls.UnaryMethod;
import io.grpc.stub.StreamObserver;
import io.mock.grpc.MockServer.JsonataErrorResponse;
import io.mock.grpc.MockServer.JsonataResponse;
import io.mock.grpc.MockServer.Service;
import java.io.InputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

class JsonataRpcService {
  private static final Logger LOG = LogManager.getLogger(MockJsonRpcServer.class);

  private final ServerServiceDefinition serviceDef;

  public JsonataRpcService(Service config, Map<String, Descriptors.ServiceDescriptor> services) {
    this.serviceDef = buildServiceDefinition(Objects.requireNonNull(config), services);
  }

  public ServerServiceDefinition serviceDefinition() {
    return this.serviceDef;
  }

  static Map<String, Descriptors.ServiceDescriptor> linkServices(FileDescriptorSet set) {
    final Map<String, Descriptors.FileDescriptor> built = new HashMap<>();
    final Map<String, Descriptors.ServiceDescriptor> services = new HashMap<>();
    // protoc writes each file after the files it imports.
    for (var proto : set.getFileList()) {
      final var deps =
          proto.getDependencyList().stream()
              .map(
                  dep ->
                      Objects.requireNonNull(
                          built.get(dep),
                          () ->
                              proto.getName()
                                  + " imports "
                                  + dep
                                  + ", which is missing from the descriptor set"))
              .toArray(Descriptors.FileDescriptor[]::new);
      try {
        final var file = Descriptors.FileDescriptor.buildFrom(proto, deps);
        built.put(proto.getName(), file);
        file.getServices().forEach(s -> services.put(s.getFullName(), s));
      } catch (Descriptors.DescriptorValidationException e) {
        throw new IllegalArgumentException(e);
      }
    }
    return services;
  }

  private static Descriptors.MethodDescriptor findMethod(
      Map<String, Descriptors.ServiceDescriptor> services, String fullMethodName) {
    final var service = services.get(MethodDescriptor.extractFullServiceName(fullMethodName));
    final var method =
        service == null
            ? null
            : service.findMethodByName(MethodDescriptor.extractBareMethodName(fullMethodName));
    if (method == null) {
      throw new IllegalArgumentException("Method not found in descriptor set: " + fullMethodName);
    }
    return method;
  }

  private static <ReqT, RespT> ServerServiceDefinition buildServiceDefinition(
      MockServer.Service serviceConfig, Map<String, Descriptors.ServiceDescriptor> services) {

    final var method = findMethod(services, serviceConfig.methodDescriptorSource);

    // Reubild the MethodDescriptor with wrapping JSON Marshaller
    final MethodDescriptor<String, String> jsonMethodDescriptor =
        io.grpc.MethodDescriptor.<String, String>newBuilder()
            .setType(MethodType.UNARY)
            .setFullMethodName(
                MethodDescriptor.generateFullMethodName(
                    method.getService().getFullName(), method.getName()))
            .setSampledToLocalTracing(true)
            .setRequestMarshaller(
                new JsonWrappingMarshaller(
                    DynamicMessage.getDefaultInstance(method.getInputType())))
            .setResponseMarshaller(
                new JsonWrappingMarshaller(
                    DynamicMessage.getDefaultInstance(method.getOutputType())))
            .build();

    final io.grpc.ServiceDescriptor serviceDescriptor =
        io.grpc.ServiceDescriptor.newBuilder(method.getService().getFullName())
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

    private final DynamicMessage prototype;
    private final Marshaller<DynamicMessage> protoMarsh;

    public JsonWrappingMarshaller(DynamicMessage prototype) {
      this.prototype = prototype;
      this.protoMarsh = ProtoUtils.marshaller(prototype);
    }

    public InputStream stream(String value) {
      try {
        final var builder = prototype.newBuilderForType();
        JsonFormat.parser().merge(value, builder);
        return protoMarsh.stream(builder.build());
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }

    public String parse(InputStream stream) {
      final var msg = protoMarsh.parse(stream);
      try {
        return JsonFormat.printer().preservingProtoFieldNames().print(msg);
      } catch (InvalidProtocolBufferException e) {
        throw new RuntimeException(e);
      }
    }
  }
}
