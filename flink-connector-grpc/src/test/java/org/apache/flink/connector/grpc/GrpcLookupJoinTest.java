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

import com.google.common.collect.ImmutableList;
import com.google.common.truth.Truth;
import io.grpc.Grpc;
import io.grpc.InsecureServerCredentials;
import io.grpc.Server;
import io.grpc.examples.helloworld.GreeterGrpc;
import io.grpc.examples.helloworld.HelloReply;
import io.grpc.examples.helloworld.HelloRequest;
import io.grpc.stub.StreamObserver;
import java.io.IOException;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class GrpcLookupJoinTest {

  static MiniClusterWithClientResource flinkCluster;
  static Server grpcServer;

  @BeforeAll
  static void setupCluster() throws IOException {
    flinkCluster =
        new MiniClusterWithClientResource(
            new MiniClusterResourceConfiguration.Builder()
                .setNumberSlotsPerTaskManager(2)
                .setNumberTaskManagers(1)
                .build());

    // Start GRPC greeter service
    grpcServer =
        Grpc.newServerBuilderForPort(50051, InsecureServerCredentials.create())
            .addService(new GreeterImpl())
            .build()
            .start();
  }

  @AfterAll
  static void teardownCluster() throws IOException {
    grpcServer.shutdownNow();
  }

  @Test
  @DisplayName("Can perform lookup join")
  void testLookupJoin() {
    final EnvironmentSettings settings = EnvironmentSettings.newInstance().inBatchMode().build();
    final TableEnvironment env = TableEnvironment.create(settings);

    env.executeSql(
        """
      CREATE TABLE input_data (
        name STRING,
        proc_time AS PROCTIME()
      ) WITH (
        'connector' = 'datagen',
        'number-of-rows' = '5',
        'fields.name.kind' = 'sequence',
        'fields.name.start' = '1',
        'fields.name.end' = '10'
      );""");
    env.executeSql(
        """
      CREATE TABLE Greeter (
        message STRING,
        name STRING
      ) WITH (
        'connector' = 'grpc-lookup',
        'host' = 'localhost',
        'port' = '50051',
        'use-plain-text' = 'true',
        'grpc-method-desc' = 'io.grpc.examples.helloworld.GreeterGrpc#getSayHelloMethod'
      );""");

    final var sql =
        """
        SELECT
          E.name as name,
          G.message as message
        FROM input_data AS E
          JOIN Greeter FOR SYSTEM_TIME AS OF E.proc_time AS G
            ON E.name = G.name;""";

    final var results = ImmutableList.copyOf(env.executeSql(sql).collect());

    Truth.assertThat(results)
        .containsExactly(
            Row.of("1", "Hello 1"),
            Row.of("2", "Hello 2"),
            Row.of("3", "Hello 3"),
            Row.of("4", "Hello 4"),
            Row.of("5", "Hello 5"));
  }

  @Test
  @DisplayName("Advanced configuration")
  void testAdvancedConfiguration() {
    final EnvironmentSettings settings = EnvironmentSettings.newInstance().inBatchMode().build();
    final TableEnvironment env = TableEnvironment.create(settings);

    env.executeSql(
        """
      CREATE TABLE input_data (
        name STRING,
        proc_time AS PROCTIME()
      ) WITH (
        'connector' = 'datagen',
        'number-of-rows' = '5',
        'fields.name.kind' = 'sequence',
        'fields.name.start' = '1',
        'fields.name.end' = '10'
      );""");
    env.executeSql(
        """
      CREATE TABLE Greeter (
        message STRING,
        name STRING
      ) WITH (
        'connector' = 'grpc-lookup',
        'host' = 'localhost',
        'port' = '50051',
        'use-plain-text' = 'true',
        'lookup.max-retries' = '10',
        'lookup.cache' = 'PARTIAL',
        'lookup.partial-cache.expire-after-write' = '1h',
        'lookup.partial-cache.max-rows' = '1000',
        'grpc-method-name' = 'helloworld.Greeter/SayHello',
        'request.format' = 'protobuf',
        'request.protobuf.message-class-name' = 'io.grpc.examples.helloworld.HelloRequest',
        'response.format' = 'protobuf',
        'response.protobuf.message-class-name' = 'io.grpc.examples.helloworld.HelloReply'
      );""");

    final var sql =
        """
        SELECT
          E.name as name,
          G.message as message
        FROM input_data AS E
          JOIN Greeter FOR SYSTEM_TIME AS OF E.proc_time AS G
            ON E.name = G.name;""";

    final var results = ImmutableList.copyOf(env.executeSql(sql).collect());

    Truth.assertThat(results)
        .containsExactly(
            Row.of("1", "Hello 1"),
            Row.of("2", "Hello 2"),
            Row.of("3", "Hello 3"),
            Row.of("4", "Hello 4"),
            Row.of("5", "Hello 5"));
  }

  static class GreeterImpl extends GreeterGrpc.GreeterImplBase {

    @Override
    public void sayHello(HelloRequest req, StreamObserver<HelloReply> responseObserver) {
      HelloReply reply = HelloReply.newBuilder().setMessage("Hello " + req.getName()).build();
      responseObserver.onNext(reply);
      responseObserver.onCompleted();
    }
  }
}
