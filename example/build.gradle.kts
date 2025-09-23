
plugins {
    application
    id("com.google.protobuf") version "0.9.4"
    id("com.google.cloud.tools.jib") version "3.4.5"
    id("com.gradleup.shadow") version "8.3.6"
}

java {
    toolchain {
        languageVersion = JavaLanguageVersion.of(17)
    }
}

application {
    mainClass = "io.mock.grpc.MockJsonRpcServer"
}
jib.to.image = "example-grpc-server"
jib.container.mainClass = application.mainClass.get()


repositories {
    mavenCentral()
}

// Gradle configuration for loading flink libs for docker build
val flinkLib by configurations.creating
val flinkVersion: String by rootProject.extra
dependencies {
    implementation(platform("org.apache.logging.log4j:log4j-bom:2.25.2"))

    implementation(project(":mock-rpc-server"))
    implementation("io.grpc:grpc-protobuf")
    implementation("io.grpc:grpc-services")
    implementation("javax.annotation:javax.annotation-api:1.3.2")

    runtimeOnly("org.apache.logging.log4j:log4j-core")

    flinkLib(project(":flink-connector-grpc"))
    flinkLib("org.apache.logging.log4j:log4j-core")
    flinkLib("org.apache.flink:flink-protobuf:$flinkVersion")
}

protobuf {
    protoc {
        artifact = "com.google.protobuf:protoc:3.25.6"
    }
    plugins {
        create("grpc") {
            artifact = "io.grpc:protoc-gen-grpc-java:1.71.0"
        }
    }
    generateProtoTasks {
        ofSourceSet("main").forEach {
            it.plugins {
                create("grpc") { }
            }
        }
    }
}

tasks.named<com.github.jengelman.gradle.plugins.shadow.tasks.ShadowJar>("shadowJar") {
  mergeServiceFiles()
  configurations = listOf(flinkLib)
  archiveVersion.set("")
}
