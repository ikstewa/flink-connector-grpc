plugins {
    id("org.gradle.toolchains.foojay-resolver-convention") version "1.0.0"
}
rootProject.name = "flink-connector-grpc"

include("flink-connector-grpc")
include("example")
include("mock-rpc-server")
