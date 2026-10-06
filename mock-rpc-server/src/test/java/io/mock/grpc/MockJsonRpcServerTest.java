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

import com.google.common.truth.Truth;
import com.google.protobuf.DescriptorProtos.FileDescriptorSet;
import io.grpc.Grpc;
import io.grpc.InsecureChannelCredentials;
import io.grpc.ManagedChannel;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.examples.helloworld.GreeterGrpc;
import io.grpc.examples.helloworld.HelloRequest;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.pkl.config.java.ConfigEvaluator;
import org.pkl.core.ModuleSource;

class MockJsonRpcServerTest {

  private static FileDescriptorSet descriptors;

  private MockJsonRpcServer server;
  private ManagedChannel clientChannel;

  @BeforeAll
  static void loadDescriptors() throws IOException {
    descriptors =
        FileDescriptorSet.parseFrom(
            Files.readAllBytes(Path.of(System.getProperty("test.descriptor.set"))));
  }

  @AfterEach
  void shutdown() throws InterruptedException {
    if (this.server != null) {
      this.server.shutdown();
    }
    if (this.clientChannel != null) {
      this.clientChannel.shutdownNow();
    }
  }

  @Test
  @DisplayName("No requests configured returns not found")
  void test_no_requests() throws IOException {
    final var config =
        """
      amends "modulepath:/test_config.pkl"

      services {
        [[name == "SayHello"]]
        {
          requests = new {}
        }
      }
    """;

    final var client = GreeterGrpc.newBlockingStub(startServer(ModuleSource.text(config)));
    var e =
        Assertions.assertThrows(
            StatusRuntimeException.class,
            () -> client.sayHello(HelloRequest.newBuilder().setName("Name not found").build()));
    Truth.assertThat(e.getStatus()).isEqualTo(Status.NOT_FOUND);
  }

  @Test
  @DisplayName("Error Response returns status exception")
  void test_error_response() throws IOException {
    final var config =
        """
      amends "modulepath:/test_config.pkl"

      services {
        [[name == "SayHello"]]
        {
          requests = new {
            new JsonataErrorResponse {
              requestExpression = "true"
              responseStatusCode = 3
              responseMessage = "unexpected request shape"
            }
          }
        }
      }
    """;

    final var client = GreeterGrpc.newBlockingStub(startServer(ModuleSource.text(config)));
    var e =
        Assertions.assertThrows(
            StatusRuntimeException.class,
            () -> client.sayHello(HelloRequest.newBuilder().setName("Name not found").build()));
    Truth.assertThat(e.getStatus().getCode()).isEqualTo(Status.INVALID_ARGUMENT.getCode());
    Truth.assertThat(e.getMessage()).isEqualTo("INVALID_ARGUMENT: unexpected request shape");
  }

  @Test
  @DisplayName("Can return static response")
  void test_static_response() throws IOException {
    final var config =
        """
      amends "modulepath:/test_config.pkl"

      services {
        [[name == "SayHello"]]
        {
          requests = new {
            new JsonataResponse {
              requestExpression = "true"
              responseExpression = \"""
                {
                  "message": "Hello stranger..."
                }
              \"""
            }
          }
        }
      }
    """;

    final var client = GreeterGrpc.newBlockingStub(startServer(ModuleSource.text(config)));

    final var response = client.sayHello(HelloRequest.newBuilder().setName("Random guy").build());

    Truth.assertThat(response.getMessage()).isEqualTo("Hello stranger...");
  }

  @Test
  @DisplayName("Can restart server")
  void test_server_restart() throws IOException {
    var config =
        """
      amends "modulepath:/test_config.pkl"

      services {
        [[name == "SayHello"]]
        {
          requests = new {
            new JsonataResponse {
              requestExpression = "true"
              responseExpression = \"""
                {
                  "message": "Hello stranger..."
                }
              \"""
            }
          }
        }
      }
    """;

    final var client = GreeterGrpc.newBlockingStub(startServer(ModuleSource.text(config)));

    final var response = client.sayHello(HelloRequest.newBuilder().setName("Random guy").build());

    Truth.assertThat(response.getMessage()).isEqualTo("Hello stranger...");

    this.server.start(parseConfig(ModuleSource.text(config)), descriptors);
    config =
        """
      amends "modulepath:/test_config.pkl"

      services {
        [[name == "SayHello"]]
        {
          requests = new {
            new JsonataResponse {
              requestExpression = "true"
              responseExpression = \"""
                {
                  "message": "I know you..."
                }
              \"""
            }
          }
        }
      }
    """;
    this.server.start(parseConfig(ModuleSource.text(config)), descriptors);
    final var response2 = client.sayHello(HelloRequest.newBuilder().setName("Random guy").build());
    Truth.assertThat(response2.getMessage()).isEqualTo("I know you...");
  }

  @Test
  @DisplayName("Can apply jsonata expressions")
  void test_jsonata() throws IOException {
    final var config =
        """
      amends "modulepath:/test_config.pkl"

      services {
        [[name == "SayHello"]]
        {
          requests = new {
            new JsonataResponse {
              requestExpression = "name='John'"
              responseExpression = \"""
                {
                  "message": "Hello " & name & "! Nice to meet you!"
                }
              \"""
            }
            new JsonataResponse {
              requestExpression = "true"
              responseExpression = \"""
                {
                  "message": "Hello stranger..."
                }
              \"""
            }
          }
        }
      }
    """;

    final var client = GreeterGrpc.newBlockingStub(startServer(ModuleSource.text(config)));

    var response = client.sayHello(HelloRequest.newBuilder().setName("Fred").build());
    Truth.assertThat(response.getMessage()).isEqualTo("Hello stranger...");
    response = client.sayHello(HelloRequest.newBuilder().setName("John").build());
    Truth.assertThat(response.getMessage()).isEqualTo("Hello John! Nice to meet you!");
  }

  @Test
  @DisplayName("Config file resolves its method from descriptor_set.desc beside it")
  void test_config_file_with_descriptor_set(@TempDir Path dir) throws Exception {
    final var pkl = dir.resolve("config.pkl");
    Files.writeString(pkl, "amends \"modulepath:/test_config.pkl\"");
    Files.write(dir.resolve("descriptor_set.desc"), descriptors.toByteArray());
    Files.writeString(dir.resolve("notes.txt"), "unrelated");

    this.server = new MockJsonRpcServer();
    new ConfigWatcher(this.server, pkl).start();
    this.clientChannel =
        Grpc.newChannelBuilder(
                "localhost:" + parseConfig(ModuleSource.file(pkl.toFile())).port,
                InsecureChannelCredentials.create())
            .build();

    final var client = GreeterGrpc.newBlockingStub(this.clientChannel);
    final var e =
        Assertions.assertThrows(
            StatusRuntimeException.class,
            () -> client.sayHello(HelloRequest.newBuilder().setName("Random guy").build()));
    Truth.assertThat(e.getStatus()).isEqualTo(Status.NOT_FOUND);
  }

  @Test
  @DisplayName("Method missing from the descriptor set is rejected")
  void test_unknown_method() {
    final var service =
        parseConfig(ModuleSource.text("amends \"modulepath:/test_config.pkl\""))
            .services
            .get(0)
            .withMethodDescriptorSource("helloworld.Greeter/NoSuchMethod");
    final var linked = JsonataRpcService.linkServices(descriptors);

    final var e =
        Assertions.assertThrows(
            IllegalArgumentException.class, () -> new JsonataRpcService(service, linked));
    Truth.assertThat(e).hasMessageThat().contains("helloworld.Greeter/NoSuchMethod");
  }

  @Test
  @DisplayName("Config file without a descriptor set beside it fails")
  void test_config_file_without_descriptor_set(@TempDir Path dir) throws Exception {
    final var pkl = dir.resolve("config.pkl");
    Files.writeString(pkl, "amends \"modulepath:/test_config.pkl\"");

    Assertions.assertThrows(
        IOException.class, () -> new ConfigWatcher(new MockJsonRpcServer(), pkl).start());
  }

  ManagedChannel startServer(ModuleSource cfgSource) {
    final MockServer serverConfig = parseConfig(cfgSource);

    try {
      this.server = new MockJsonRpcServer();
      this.server.start(serverConfig, descriptors);
    } catch (Exception e) {
      throw new RuntimeException("Failed to start server", e);
    }

    this.clientChannel =
        Grpc.newChannelBuilder(
                "localhost:" + serverConfig.port, InsecureChannelCredentials.create())
            .build();
    return this.clientChannel;
  }

  MockServer parseConfig(ModuleSource cfgSource) {
    try (var evaluator = ConfigEvaluator.preconfigured()) {
      return evaluator.evaluate(cfgSource).as(MockServer.class);
    }
  }
}
