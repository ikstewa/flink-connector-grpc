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

import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.truth.Truth;
import io.grpc.Grpc;
import io.grpc.InsecureServerCredentials;
import io.grpc.Metadata;
import io.grpc.Server;
import io.grpc.Status;
import io.grpc.examples.helloworld.GreeterGrpc;
import io.grpc.examples.helloworld.HelloReply;
import io.grpc.examples.helloworld.HelloRequest;
import io.grpc.stub.StreamObserver;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.apache.flink.types.Row;
import org.apache.flink.util.SerializedThrowable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class GrpcLookupJoinTest {

  static MiniClusterWithClientResource flinkCluster;
  private Server grpcServer;
  private AtomicInteger grpcRequestCounter;

  @BeforeAll
  static void setupCluster() throws IOException {
    flinkCluster =
        new MiniClusterWithClientResource(
            new MiniClusterResourceConfiguration.Builder()
                .setNumberSlotsPerTaskManager(2)
                .setNumberTaskManagers(1)
                .build());
  }

  @BeforeEach
  void setupGrpcService() throws IOException {
    this.grpcRequestCounter = new AtomicInteger();
    // Start GRPC greeter service
    grpcServer =
        Grpc.newServerBuilderForPort(50051, InsecureServerCredentials.create())
            .addService(new GreeterImpl(this.grpcRequestCounter))
            .build()
            .start();
  }

  @AfterEach
  void shtudownGrpcService() throws IOException {
    grpcServer.shutdownNow();
    grpcServer = null;
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
    Truth.assertThat(this.grpcRequestCounter.get()).isEqualTo(5);
  }

  @Test
  @DisplayName("Advanced configuration")
  void testAdvancedConfiguration() {
    final EnvironmentSettings settings = EnvironmentSettings.newInstance().inBatchMode().build();
    final TableEnvironment env = TableEnvironment.create(settings);

    env.executeSql(
        """
      CREATE TABLE input_data (
        name STRING NOT NULL,
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
    Truth.assertThat(this.grpcRequestCounter.get()).isEqualTo(5);
  }

  @Test
  @DisplayName("Supports metadata")
  void testMetadata() {
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
        grpc_status_code INT METADATA FROM 'status-code',
        grpc_status_desc STRING METADATA FROM 'status-description',
        message STRING,
        grpc_status_trailers MAP<STRING NOT NULL, STRING> METADATA FROM 'status-trailers',
        name STRING,
        grpc_status_trailers_bin MAP<STRING NOT NULL, STRING> METADATA FROM 'status-trailers-bin'
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
          E.name AS name,
          G.message AS message,
          G.grpc_status_code,
          G.grpc_status_desc,
          G.grpc_status_trailers,
          G.grpc_status_trailers_bin
        FROM input_data AS E
          JOIN Greeter FOR SYSTEM_TIME AS OF E.proc_time AS G
            ON E.name = G.name;""";

    final var results = ImmutableList.copyOf(env.executeSql(sql).collect());

    Truth.assertThat(results)
        .containsExactly(
            Row.of("1", "Hello 1", 0, null, Map.of(), Map.of()),
            Row.of("2", "Hello 2", 0, null, Map.of(), Map.of()),
            Row.of("3", "Hello 3", 0, null, Map.of(), Map.of()),
            Row.of("4", "Hello 4", 0, null, Map.of(), Map.of()),
            Row.of("5", "Hello 5", 0, null, Map.of(), Map.of()));
    Truth.assertThat(this.grpcRequestCounter.get()).isEqualTo(5);
  }

  @Test
  @DisplayName("Supports metadata on failure")
  void testMetadataOnFailure() {
    final EnvironmentSettings settings = EnvironmentSettings.newInstance().inBatchMode().build();
    final TableEnvironment env = TableEnvironment.create(settings);

    env.executeSql(
        """
      CREATE TABLE Greeter (
        message STRING,
        grpc_status_code INT METADATA FROM 'status-code',
        grpc_status_desc STRING METADATA FROM 'status-description',
        grpc_status_trailers MAP<STRING NOT NULL, STRING> METADATA FROM 'status-trailers',
        grpc_status_trailers_bin MAP<STRING NOT NULL, VARBINARY> METADATA FROM 'status-trailers-bin',
        name STRING
      ) WITH (
        'connector' = 'grpc-lookup',
        'host' = 'localhost',
        'port' = '50051',
        'use-plain-text' = 'true',
        'grpc-method-desc' = 'io.grpc.examples.helloworld.GreeterGrpc#getSayHelloMethod',
        'lookup.max-retries' = '0'
      );""");

    final var sql =
        """
        SELECT
          E.name AS name,
          G.message AS message,
          G.grpc_status_code,
          G.grpc_status_desc,
          G.grpc_status_trailers,
          G.grpc_status_trailers_bin
        FROM (
          SELECT
              'FAIL_ME' AS name,
              PROCTIME() AS proc_time) E
          JOIN Greeter FOR SYSTEM_TIME AS OF E.proc_time AS G
            ON E.name = G.name;""";

    final var results = ImmutableList.copyOf(env.executeSql(sql).collect());

    Truth.assertThat(results)
        .containsExactly(
            Row.of(
                "FAIL_ME",
                null,
                14,
                "I WAS TOLD TO FAIL",
                Map.of(
                    "failure-info", "my-failure-reason",
                    "content-type", "application/grpc"),
                Map.of("failure-data-bin", "my-failure-reason".getBytes())));
    Truth.assertThat(this.grpcRequestCounter.get()).isEqualTo(1);
  }

  @Test
  @DisplayName("Handles not null on failure")
  void testNullableFailure() {
    final EnvironmentSettings settings = EnvironmentSettings.newInstance().inBatchMode().build();
    final TableEnvironment env = TableEnvironment.create(settings);

    env.executeSql(
        """
      CREATE TABLE Greeter (
        name STRING NOT NULL,
        grpc_status_code INT METADATA FROM 'status-code',
        message STRING NOT NULL
      ) WITH (
        'connector' = 'grpc-lookup',
        'host' = 'localhost',
        'port' = '50051',
        'use-plain-text' = 'true',
        'grpc-method-desc' = 'io.grpc.examples.helloworld.GreeterGrpc#getSayHelloMethod',
        'lookup.max-retries' = '0'
      );""");

    env.executeSql(
        """
        CREATE TEMPORARY VIEW grpc
        AS (
          SELECT G.*
          FROM (
            SELECT 'FAIL_ME' AS name, PROCTIME() AS proc_time
            UNION SELECT 'Fred' AS name, PROCTIME() AS proc_time
          ) E
          JOIN Greeter FOR SYSTEM_TIME AS OF E.proc_time AS G
            ON E.name = G.name
        )
      """);

    final var successMsgs =
        env.sqlQuery(
            """
        SELECT
          name AS name,
          message AS message,
          'SUCCESS' AS status
        FROM grpc
        WHERE grpc_status_code = 0;""");

    final var failedMsgs =
        env.sqlQuery(
            """
        SELECT
          name AS name,
          message AS message,
          'FAILED' AS status
        FROM grpc
        WHERE grpc_status_code <> 0;""");

    final var result = failedMsgs.unionAll(successMsgs);

    final var failedResults = ImmutableList.copyOf(result.execute().collect());

    Truth.assertThat(failedResults)
        .containsExactly(Row.of("FAIL_ME", "", "FAILED"), Row.of("Fred", "Hello Fred", "SUCCESS"));
    Truth.assertThat(this.grpcRequestCounter.get())
        .isEqualTo(4); // FIXME: This needs de-duplication
  }

  @Test
  @DisplayName("Handles not null fields")
  void testNotNull() {
    final EnvironmentSettings settings = EnvironmentSettings.newInstance().inBatchMode().build();
    final TableEnvironment env = TableEnvironment.create(settings);

    env.executeSql(
        """
      CREATE TABLE Greeter (
        grpc_status_code INT METADATA FROM 'status-code',
        message STRING NOT NULL,
        name STRING NOT NULL
      ) WITH (
        'connector' = 'grpc-lookup',
        'host' = 'localhost',
        'port' = '50051',
        'use-plain-text' = 'true',
        'grpc-method-desc' = 'io.grpc.examples.helloworld.GreeterGrpc#getSayHelloMethod',
        'lookup.max-retries' = '0'
      );""");

    final var sql =
        """
        SELECT
          E.name AS name,
          G.message AS message
        FROM (
          SELECT 'FAIL_ME' AS name, PROCTIME() AS proc_time
          UNION SELECT 'Fred' AS name, PROCTIME() AS proc_time
        ) E
        JOIN Greeter FOR SYSTEM_TIME AS OF E.proc_time AS G
          ON E.name = G.name
        WHERE G.grpc_status_code = 0;""";

    final var successResults = ImmutableList.copyOf(env.executeSql(sql).collect());

    Truth.assertThat(successResults).containsExactly(Row.of("Fred", "Hello Fred"));
    Truth.assertThat(this.grpcRequestCounter.get()).isEqualTo(2);
  }

  @Test
  @DisplayName("Retries non-error status codes")
  void testRetries() {
    final EnvironmentSettings settings = EnvironmentSettings.newInstance().inBatchMode().build();
    final TableEnvironment env = TableEnvironment.create(settings);

    env.executeSql(
        """
      CREATE TABLE Greeter (
        grpc_status_code INT METADATA FROM 'status-code',
        message STRING NOT NULL,
        name STRING NOT NULL
      ) WITH (
        'connector' = 'grpc-lookup',
        'host' = 'localhost',
        'port' = '50051',
        'use-plain-text' = 'true',
        'grpc-method-desc' = 'io.grpc.examples.helloworld.GreeterGrpc#getSayHelloMethod',
        'grpc-retry-codes' = '1;14',
        'grpc-error-codes' = '16',
        'lookup.max-retries' = '4'
      );""");

    final var sql =
        """
        SELECT
          E.name,
          G.grpc_status_code
        FROM (
          SELECT 'FAIL_ME' AS name, PROCTIME() AS proc_time
        ) E
        JOIN Greeter FOR SYSTEM_TIME AS OF E.proc_time AS G
          ON E.name = G.name""";

    final var successResults = ImmutableList.copyOf(env.executeSql(sql).collect());

    Truth.assertThat(successResults).containsExactly(Row.of("FAIL_ME", 14));
    Truth.assertThat(this.grpcRequestCounter.get()).isEqualTo(4);
  }

  @Test
  @DisplayName("Retries error status codes")
  void testRetriesErrors() {
    final EnvironmentSettings settings = EnvironmentSettings.newInstance().inBatchMode().build();
    final TableEnvironment env = TableEnvironment.create(settings);

    env.executeSql(
        """
      CREATE TABLE Greeter (
        grpc_status_code INT METADATA FROM 'status-code',
        message STRING NOT NULL,
        name STRING NOT NULL
      ) WITH (
        'connector' = 'grpc-lookup',
        'host' = 'localhost',
        'port' = '50051',
        'use-plain-text' = 'true',
        'grpc-method-desc' = 'io.grpc.examples.helloworld.GreeterGrpc#getSayHelloMethod',
        'grpc-retry-codes' = '1;14',
        'grpc-error-codes' = '4;14',
        'lookup.max-retries' = '4'
      );""");

    final var sql =
        """
        SELECT
          E.name,
          G.grpc_status_code
        FROM (
          SELECT 'FAIL_ME' AS name, PROCTIME() AS proc_time
        ) E
        JOIN Greeter FOR SYSTEM_TIME AS OF E.proc_time AS G
          ON E.name = G.name""";

    final Exception error =
        Assertions.assertThrows(Exception.class, () -> env.executeSql(sql).await());
    Truth.assertThat(Throwables.getRootCause(error).toString())
        .isEqualTo(
            "io.grpc.StatusRuntimeException: io.grpc.StatusRuntimeException: UNAVAILABLE: I WAS TOLD TO FAIL");

    Truth.assertThat(this.grpcRequestCounter.get()).isEqualTo(4);
  }

  private Throwable getRootCause(Throwable err) {
    final var root = Throwables.getRootCause(err);
    if (err instanceof org.apache.flink.util.SerializedThrowable) {
      return getRootCause(SerializedThrowable.get(err, ClassLoader.getSystemClassLoader()));
    }
    return root;
  }

  @Test
  @DisplayName("Fails when both grpc-method config is defined")
  void testGrpcMethod() {
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
        'grpc-method-desc' = 'io.grpc.examples.helloworld.GreeterGrpc#getSayHelloMethod',
        'grpc-method-name' = 'helloworld.Greeter/SayHello'
      );""");

    final var sql =
        """
        SELECT
          E.name as name,
          G.message as message
        FROM input_data AS E
          JOIN Greeter FOR SYSTEM_TIME AS OF E.proc_time AS G
            ON E.name = G.name;""";

    final Exception error =
        Assertions.assertThrows(ValidationException.class, () -> env.executeSql(sql));
    Truth.assertThat(error.getCause().getMessage())
        .isEqualTo("Required only one of 'grpc-method-name' or 'grpc-method-desc'");
  }

  @Test
  @DisplayName("Fails when both grpc-method-name format missing")
  void testGrpcMethodNameFormat() {
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
        'grpc-method-name' = 'helloworld.Greeter/SayHello'
      );""");

    final var sql =
        """
        SELECT
          E.name as name,
          G.message as message
        FROM input_data AS E
          JOIN Greeter FOR SYSTEM_TIME AS OF E.proc_time AS G
            ON E.name = G.name;""";

    final Exception error =
        Assertions.assertThrows(ValidationException.class, () -> env.executeSql(sql));
    Truth.assertThat(error.getCause().getMessage())
        .isEqualTo(
            "Config options 'request.format' and 'response.format' are required, if 'grpc-method-name' is configured.");
  }

  static class GreeterImpl extends GreeterGrpc.GreeterImplBase {

    private AtomicInteger requestCounter;

    public GreeterImpl(AtomicInteger requestCounter) {
      this.requestCounter = requestCounter;
    }

    @Override
    public void sayHello(HelloRequest req, StreamObserver<HelloReply> responseObserver) {
      this.requestCounter.incrementAndGet();

      if (req.getName().equals("FAIL_ME")) {
        final var metadata = new Metadata();
        metadata.put(
            Metadata.Key.of("failure-info", io.grpc.Metadata.ASCII_STRING_MARSHALLER),
            "my-failure-reason");
        metadata.put(
            Metadata.Key.of("failure-data-bin", io.grpc.Metadata.BINARY_BYTE_MARSHALLER),
            "my-failure-reason".getBytes());
        responseObserver.onError(
            Status.UNAVAILABLE
                .augmentDescription("I WAS TOLD TO FAIL")
                .asRuntimeException(metadata));
      } else {
        HelloReply reply = HelloReply.newBuilder().setMessage("Hello " + req.getName()).build();
        responseObserver.onNext(reply);
        responseObserver.onCompleted();
      }
    }
  }
}
