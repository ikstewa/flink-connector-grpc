
plugins {
    `java-library`
    application
    jacoco
    id("com.diffplug.spotless") version "6.25.0"
    id("com.google.protobuf") version "0.9.4"
    id("com.google.cloud.tools.jib") version "3.4.3"
    id("com.gradleup.shadow") version "8.3.3"
}

java {
    toolchain {
        languageVersion = JavaLanguageVersion.of(17)
    }
}

application {
    mainClass = "org.apache.examples.helloworld.HelloWorldServer"
}
jib.to.image = "example-grpc-server"

repositories {
    mavenCentral()
}

// Gradle configuration for loading flink libs for docker build
val flinkLib by configurations.creating
val flinkVersion: String by rootProject.extra
dependencies {
    api(platform("org.apache.logging.log4j:log4j-bom:2.24.1"))
    api(platform("io.grpc:grpc-bom:1.68.0"))

    implementation("javax.annotation:javax.annotation-api:1.3.2")
    implementation("io.grpc:grpc-protobuf")
    implementation("io.grpc:grpc-services")
    implementation("io.grpc:grpc-netty-shaded")

    flinkLib(project(":flink-connector-grpc"))
    flinkLib("org.apache.logging.log4j:log4j-core")
    flinkLib("org.apache.flink:flink-protobuf:$flinkVersion")
}

protobuf {
    protoc {
        artifact = "com.google.protobuf:protoc:4.28.2"
    }
    plugins {
        create("grpc") {
            artifact = "io.grpc:protoc-gen-grpc-java:1.68.0"
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
}
