// plugins {
//     // Apply the foojay-resolver plugin to allow automatic download of JDKs
//     id("org.gradle.toolchains.foojay-resolver-convention") version "0.8.0"
// }
rootProject.name = "flink-connector-grpc"

include("flink-connector-grpc")

include("example")

include("mock-rpc-server")
