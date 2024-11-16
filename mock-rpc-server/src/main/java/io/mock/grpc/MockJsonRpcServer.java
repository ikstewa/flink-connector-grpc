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

import java.io.File;
import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nullable;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.pkl.config.java.ConfigEvaluator;
import org.pkl.core.ModuleSource;

import io.grpc.Grpc;
import io.grpc.InsecureServerCredentials;
import io.grpc.Metadata;
import io.grpc.Server;
import io.grpc.ServerCall;
import io.grpc.ServerCall.Listener;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;

/** Test server used for mocking gRPC requests using json payloads. */
public class MockJsonRpcServer {
  private static final Logger LOG = LogManager.getLogger(MockJsonRpcServer.class);

  private MockServer config;
  private Server server;
  private final CountDownLatch shutdownSignal;

  public MockJsonRpcServer() {
    this.shutdownSignal = new CountDownLatch(1);
    Runtime.getRuntime()
        .addShutdownHook(
            new Thread() {
              @Override
              public void run() {
                // Use stderr here since the logger may have been reset by its JVM shutdown hook.
                System.err.println("*** shutting down gRPC server since JVM is shutting down");
                MockJsonRpcServer.this.shutdown();
                System.err.println("*** server shut down");
              }
            });
  }

  void start(@Nullable String configFile) throws IOException {
    final MockServer serverConfig;
    if (configFile != null) {
      try (var evaluator = ConfigEvaluator.preconfigured()) {
        serverConfig = evaluator.evaluate(ModuleSource.file(configFile)).as(MockServer.class);
      }
    } else {
      serverConfig = null;
    }
    this.start(serverConfig);
  }

  synchronized void start(@Nullable MockServer targetConfig) throws IOException {
    if (server != null) {
      if (config.equals(targetConfig)) {
        LOG.info("Config file unchanged. Skipping restart...");
        return;
      }
      LOG.info("Shutting down Server...");
      try {
        server.shutdown().awaitTermination(30, TimeUnit.SECONDS);
      } catch (InterruptedException e) {
        LOG.error("Interrupted shutting down server", e);
      }
    }

    if (targetConfig != null) {
      try {
        config = targetConfig;
        server = buildServer(targetConfig);
        server.start();
        LOG.info(
            "Started server on port '{}' for services '{}'",
            config.port,
            config.services.stream().map(s -> s.name).toList());
      } catch (RuntimeException e) {
        LOG.error("Failed starting server", e);
        this.shutdown();
      }
    } else {
      config = null;
      server = null;
    }
  }

  synchronized void shutdown() {
    try {
      if (server != null) {
        server.shutdown().awaitTermination(30, TimeUnit.SECONDS);
      }
      this.shutdownSignal.countDown();
    } catch (InterruptedException e) {
      LOG.error("Caught exception closing Server", e);
    }
  }

  void blockUntilShutdown() throws InterruptedException {
    this.shutdownSignal.await();
    LOG.info("Server terminated");
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

  public static void main(String[] args) throws IOException, InterruptedException {

    if (args.length != 1) {
      LOG.error("Expected one input param of pkl config file or directory.");
      System.exit(1);
    }
    final var configPath = new File(args[0]).toPath();

    final var server = new MockJsonRpcServer();
    final var config = new ConfigWatcher(server, configPath);
    config.start();
    server.blockUntilShutdown();
    System.exit(1);
  }
}
