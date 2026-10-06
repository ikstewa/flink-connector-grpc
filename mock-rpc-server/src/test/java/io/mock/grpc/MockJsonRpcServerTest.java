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
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.pkl.config.java.ConfigEvaluator;
import org.pkl.core.ModuleSource;

class MockJsonRpcServerTest {

  private MockJsonRpcServer server;
  private ManagedChannel clientChannel;

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

    this.server.start(serverConfig(ModuleSource.text(config)));
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
    this.server.start(serverConfig(ModuleSource.text(config)));
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
  @DisplayName("Replacing only the descriptor set restarts the server")
  void test_descriptor_set_restart() throws IOException {
    final var mock =
        parseConfig(
            ModuleSource.text(
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
                  "greeting": "Renamed"
                }
              \"""
            }
          }
        }
      }
    """));
    final var original = loadDescriptorSet();

    final var client = GreeterGrpc.newBlockingStub(startServer(new ServerConfig(mock, original)));
    Assertions.assertThrows(
        StatusRuntimeException.class,
        () -> client.sayHello(HelloRequest.newBuilder().setName("Random guy").build()));

    // Field 1 keeps its wire number, so the generated client still reads it as 'message'.
    final var renamed = original.toBuilder();
    renamed
        .getFileBuilder(0)
        .getMessageTypeBuilderList()
        .forEach(
            m -> {
              if (m.getName().equals("HelloReply")) {
                m.getFieldBuilder(0).setName("greeting").clearJsonName();
              }
            });
    this.server.start(new ServerConfig(mock, renamed.build()));

    final var response = client.sayHello(HelloRequest.newBuilder().setName("Random guy").build());
    Truth.assertThat(response.getMessage()).isEqualTo("Renamed");
  }

  @Test
  @DisplayName("Descriptor set with a missing import fails naming both files")
  void test_missing_import() throws IOException {
    final var set = loadDescriptorSet();
    final var broken = set.toBuilder();
    broken.getFileBuilder(0).addDependency("missing/dep.proto");

    final var e =
        Assertions.assertThrows(
            IllegalArgumentException.class, () -> DescriptorSetSchema.link(broken.build()));
    Truth.assertThat(e).hasMessageThat().contains(set.getFile(0).getName());
    Truth.assertThat(e).hasMessageThat().contains("missing/dep.proto");
  }

  @ParameterizedTest
  @CsvSource({
    "helloworld.Missing/SayHello, Unknown service",
    "helloworld.Greeter/Missing, Unknown method",
    "helloworld.Greeter/SayHelloStream, streaming",
  })
  @DisplayName("Rejects methods that cannot be mocked")
  void test_rejected_methods(String method, String expectedError) throws IOException {
    final var schema = DescriptorSetSchema.link(loadDescriptorSet());

    final var e =
        Assertions.assertThrows(
            IllegalArgumentException.class, () -> schema.findUnaryMethod(method));
    Truth.assertThat(e).hasMessageThat().contains(expectedError);
  }

  @Test
  @DisplayName("Config files are found among unrelated files and staged writes")
  void test_find_config_files(@TempDir Path dir) throws IOException {
    Files.writeString(dir.resolve("config.pkl"), "");
    Files.write(dir.resolve("descriptor_set.desc"), loadDescriptorSet().toByteArray());
    Files.writeString(dir.resolve("notes.txt"), "");
    Files.writeString(dir.resolve("descriptor_set.desc.tmp"), "");

    final var files = ConfigWatcher.findConfigFiles(dir);

    Truth.assertThat(files.pkl()).isEqualTo(dir.resolve("config.pkl"));
    Truth.assertThat(files.desc()).isEqualTo(dir.resolve("descriptor_set.desc"));
  }

  @Test
  @DisplayName("Direct config file starts using the descriptor set beside it")
  void test_direct_config_file(@TempDir Path dir) throws Exception {
    final var pkl = dir.resolve("config.pkl");
    Files.writeString(
        pkl,
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
                  "message": "Hello direct..."
                }
              \"""
            }
          }
        }
      }
    """);
    Files.write(dir.resolve("descriptor_set.desc"), loadDescriptorSet().toByteArray());
    Files.writeString(dir.resolve("notes.txt"), "");

    this.server = new MockJsonRpcServer();
    new ConfigWatcher(this.server, pkl).start();
    this.clientChannel =
        Grpc.newChannelBuilder(
                "localhost:" + parseConfig(ModuleSource.file(pkl.toFile())).port,
                InsecureChannelCredentials.create())
            .build();

    final var response =
        GreeterGrpc.newBlockingStub(this.clientChannel)
            .sayHello(HelloRequest.newBuilder().setName("Random guy").build());
    Truth.assertThat(response.getMessage()).isEqualTo("Hello direct...");
  }

  ManagedChannel startServer(ModuleSource cfgSource) {
    try {
      return startServer(new ServerConfig(parseConfig(cfgSource), loadDescriptorSet()));
    } catch (IOException e) {
      throw new RuntimeException("Failed to load descriptor set", e);
    }
  }

  ManagedChannel startServer(ServerConfig serverConfig) {
    try {
      this.server = new MockJsonRpcServer();
      this.server.start(serverConfig);
    } catch (Exception e) {
      throw new RuntimeException("Failed to start server", e);
    }

    this.clientChannel =
        Grpc.newChannelBuilder(
                "localhost:" + serverConfig.mock().port, InsecureChannelCredentials.create())
            .build();
    return this.clientChannel;
  }

  MockServer parseConfig(ModuleSource cfgSource) {
    try (var evaluator = ConfigEvaluator.preconfigured()) {
      return evaluator.evaluate(cfgSource).as(MockServer.class);
    }
  }

  ServerConfig serverConfig(ModuleSource cfgSource) throws IOException {
    return new ServerConfig(parseConfig(cfgSource), loadDescriptorSet());
  }

  static FileDescriptorSet loadDescriptorSet() throws IOException {
    return FileDescriptorSet.parseFrom(
        Files.readAllBytes(Path.of(System.getProperty("test.descriptor.set"))));
  }
}
