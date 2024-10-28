
plugins {
    `java-library`
    application
    jacoco
    id("com.diffplug.spotless") version "6.25.0"
    id("com.google.protobuf") version "0.9.4"
    id("com.google.cloud.tools.jib") version "3.4.4"
    id("com.gradleup.shadow") version "8.3.3"
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
    api(platform("org.apache.logging.log4j:log4j-bom:2.24.1"))
    api(platform("io.grpc:grpc-bom:1.68.0"))

    implementation(project(":mock-rpc-server"))

    runtimeOnly("org.apache.logging.log4j:log4j-core")

    flinkLib(project(":flink-connector-grpc"))
    flinkLib("org.apache.logging.log4j:log4j-core")
    flinkLib("org.apache.flink:flink-protobuf:$flinkVersion")
}

tasks.named<Test>("test") {
    useJUnitPlatform()
    testLogging {
        events("passed")
    }
}

spotless {
    // generic formatting for miscellaneous files
    format("misc") {
        target("*.gradle.kts", "*.gradle", "*.md", ".gitignore")

        trimTrailingWhitespace()
        indentWithSpaces()
        endWithNewline()
    }

    // chose the Google java formatter, version 1.9
    java {
        targetExclude("**/build/generated/**")
        importOrder()
        removeUnusedImports()
        googleJavaFormat()
    }
}

protobuf {
    protoc {
        artifact = "com.google.protobuf:protoc:3.25.3"
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
  archiveVersion.set("")
}
