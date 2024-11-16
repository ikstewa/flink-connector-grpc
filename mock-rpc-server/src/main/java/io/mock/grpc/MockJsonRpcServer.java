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

import io.grpc.Grpc;
import io.grpc.InsecureServerCredentials;
import io.grpc.Metadata;
import io.grpc.Server;
import io.grpc.ServerCall;
import io.grpc.ServerCall.Listener;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import java.io.IOException;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.pkl.config.java.ConfigEvaluator;
import org.pkl.core.ModuleSource;

/** Test server used for mocking gRPC requests using json payloads. */
public class MockJsonRpcServer {
  private static final Logger LOG = LogManager.getLogger(MockJsonRpcServer.class);

  private final MockServer config;
  private final Server server;

  public MockJsonRpcServer(MockServer config) {
    this.config = Objects.requireNonNull(config);
    this.server = buildServer(config);
  }

  private static Server buildServer(MockServer config) {
    final var serverBldr =
        Grpc.newServerBuilderForPort(config.port, InsecureServerCredentials.create())
            /* This method call adds the Interceptor to enable compressed server responses for all RPCs */
            .intercept(
                new ServerInterceptor() {
                  @Override
                  public <ReqT, RespT> Listener<ReqT> interceptCall(
                      ServerCall<ReqT, RespT> call,
                      Metadata headers,
                      ServerCallHandler<ReqT, RespT> next) {
                    call.setCompression("gzip");
                    return next.startCall(call, headers);
                  }
                });
    config.services.stream()
        .map(JsonataRpcService::new)
        .map(JsonataRpcService::serviceDefinition)
        .forEach(serverBldr::addService);
    return serverBldr.build();
  }

  MockJsonRpcServer start() throws IOException {
    server.start();

    LOG.info("Server started, listening on " + this.config.port);
    Runtime.getRuntime()
        .addShutdownHook(
            new Thread() {
              @Override
              public void run() {
                // Use stderr here since the logger may have been reset by its JVM shutdown hook.
                System.err.println("*** shutting down gRPC server since JVM is shutting down");
                try {
                  MockJsonRpcServer.this.stop();
                } catch (InterruptedException e) {
                  e.printStackTrace(System.err);
                }
                System.err.println("*** server shut down");
              }
            });
    return this;
  }

  void stop() throws InterruptedException {
    if (server != null) {
      server.shutdown().awaitTermination(30, TimeUnit.SECONDS);
    }
  }

  private void blockUntilShutdown() throws InterruptedException {
    if (server != null) {
      server.awaitTermination();
    }
  }

  public static void main(String[] args) throws IOException, InterruptedException {
    if (args.length != 1) {
      LOG.error("Expected one input param of pkl config file.");
      System.exit(1);
    } else {

      final MockServer serverConfig;
      try (var evaluator = ConfigEvaluator.preconfigured()) {
        serverConfig = evaluator.evaluate(ModuleSource.uri(args[0])).as(MockServer.class);
      }

      final MockJsonRpcServer server = new MockJsonRpcServer(serverConfig);
      server.start();
      server.blockUntilShutdown();
    }
  }
}
