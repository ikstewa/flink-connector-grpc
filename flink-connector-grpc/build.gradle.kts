plugins {
    `java-library`
    jacoco
    `maven-publish`
    signing
    id("com.diffplug.spotless") version "7.0.2"
    id("com.google.protobuf") version "0.9.4"
}

java {
    toolchain {
        languageVersion = JavaLanguageVersion.of(17)
    }
    withJavadocJar()
    withSourcesJar()
}

repositories {
    mavenCentral()
}

val flinkVersion: String by rootProject.extra
dependencies {
    api(platform("org.apache.logging.log4j:log4j-bom:2.25.0"))
    api(platform("io.grpc:grpc-bom:1.71.0"))

    implementation("com.google.code.findbugs:jsr305:3.0.2")


    implementation("org.apache.logging.log4j:log4j-api")
    implementation("javax.annotation:javax.annotation-api:1.3.2")

    implementation("io.grpc:grpc-protobuf")
    implementation("io.grpc:grpc-services")
    implementation("io.grpc:grpc-netty-shaded")
    implementation("com.google.code.gson:gson:2.12.1")
    implementation("com.google.auto.service:auto-service-annotations:1.1.1")
    implementation("org.apache.flink:flink-protobuf:$flinkVersion")
    implementation("com.github.ben-manes.caffeine:caffeine:3.2.0")

    compileOnly("org.apache.flink:flink-table-api-java:$flinkVersion")

    annotationProcessor("com.google.auto.service:auto-service:1.1.1")

    testImplementation("org.apache.logging.log4j:log4j-core")
    testImplementation("org.apache.logging.log4j:log4j-slf4j-impl")
    testImplementation("org.apache.logging.log4j:log4j-slf4j2-impl")
    testImplementation("org.apache.logging.log4j:log4j-jcl")
    testImplementation("org.apache.logging.log4j:log4j-jpl")
    testImplementation("org.apache.logging.log4j:log4j-jul")

    testImplementation("org.apache.flink:flink-core:$flinkVersion")
    testImplementation("org.apache.flink:flink-table-common:$flinkVersion")
    testImplementation("org.apache.flink:flink-table-test-utils:$flinkVersion")
    testImplementation("org.apache.flink:flink-test-utils:$flinkVersion")
    testImplementation("org.apache.flink:flink-protobuf:$flinkVersion")

    testImplementation("com.google.truth.extensions:truth-java8-extension:1.4.4")
    testImplementation("com.google.truth:truth:1.4.4")

}

testing {
    suites {
        val test by getting(JvmTestSuite::class) {
            useJUnitJupiter()
            targets {
                all {
                    testTask.configure {
                      testLogging {
                       showStandardStreams = true
                      }
                    }
                }
            }
        }
    }
}
tasks.test {
    finalizedBy(tasks.jacocoTestReport) // report is always generated after tests run
    // systemProperty("log4j2.configurationFile", "log4j2-test.xml")
    // systemProperty("sun.io.serialization.extendedDebugInfo", "true")
}
tasks.jacocoTestReport {
    dependsOn(tasks.test) // tests are required to run before generating the report
    reports {
        xml.required.set(true)
        csv.required.set(true)
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

        // and apply a license header
        licenseHeaderFile(rootProject.file("HEADER"))
    }
}

publishing {
    publications {
        create<MavenPublication>("mavenJava") {
            from(components["java"])
            pom {
                name.set("Flink GRPC Connector")
                description.set("Flink connectors for GRPC services.")
                url.set("https://github.com/ikstewa/flink-connector-grpc/")
                licenses {
                    license {
                        name.set("The Apache License, Version 2.0")
                        url.set("http://www.apache.org/licenses/LICENSE-2.0.txt")
                    }
                }
                developers {
                    developer {
                        id.set("ikstewa")
                        name.set("Ian Stewart")
                        url.set("https://github.com/ikstewa/")
                    }
                }
                scm {
                    url.set("https://github.com/ikstewa/flink-connector-grpc/")
                    connection.set("scm:git:git://github.com/ikstewa/flink-connector-grpc/")
                    developerConnection.set("scm:git:ssh://github.com/ikstewa/flink-connector-grpc/")
                }
            }
        }
    }
}

signing {
    sign(publishing.publications["mavenJava"])
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
        ofSourceSet("test").forEach {
            it.plugins {
                create("grpc") { }
            }
        }
    }
}
