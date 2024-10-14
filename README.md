# flink-connector-grpc
gRPC Connector for Apache Flink.

[![Maven Central](https://img.shields.io/maven-central/v/io.github.ikstewa/flink-connector-grpc)](https://central.sonatype.com/artifact/io.github.ikstewa/flink-connector-grpc)

A gRPC connector for the Flink Table API that allows pulling data from gRPC services.

The primary use case for the gRPC connector is to be used in a [Lookup Join](https://nightlies.apache.org/flink/flink-docs-release-1.20/docs/dev/table/sql/queries/joins/#lookup-join) for hydrating data streams.

## TODO

* Sink - Add Table Sink support to egress via gRPC
* Auth - Currently assuming internal private networks
* Remove `{request,response}.protobuf.message-class-name` - load from method

## Example Usage

The following steps will setup a sql-client REPL for demonstrating the Lookup Join:

1. Compile the test GRPC server:
```shell
./gradlew jibDockerBuild
```
1. Build the flink runtime dependencies:
```shell
./gradlew shadowJar
```
1. Start the services:
```shell
docker compose -f example/docker-compose.yaml run --rm --name sql-client sql-client
```

### gRPC TableLookup Source

Flink SQL table definition:

Data Source Table
```roomsql
CREATE TABLE NamedEvents (
  name STRING,
  num INT,
  proc_time AS PROCTIME()
) WITH (
  'connector' = 'datagen',
  'rows-per-second' = '1',
  'fields.num.kind' = 'sequence',
  'fields.num.start' = '1',
  'fields.num.end' = '99999'
);
```

Enrichment Lookup Table
```roomsql
CREATE TABLE Greeter (
  message STRING,
  name STRING
) WITH (
  'connector' = 'grpc-lookup',
  'host' = 'grpc-server',
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
);
```

Flink SQL Lookup Join from _NamedEvents_ to _Greeter_:

```roomsql
SELECT
  *
FROM NamedEvents AS E
  JOIN Greeter FOR SYSTEM_TIME AS OF E.proc_time AS G
    ON E.name = G.name;
```

The columns specified as the `PIMARY KEY` and used for the JOIN `ON` condition will be converted to the configured proto `request` object.
