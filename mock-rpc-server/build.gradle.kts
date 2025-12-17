import org.gradle.api.tasks.testing.logging.TestExceptionFormat
import org.gradle.api.tasks.testing.logging.TestLogEvent

plugins {
    `java-library`
    jacoco
    `maven-publish`
    signing
    id("com.google.protobuf") version "0.9.6"
    id("org.pkl-lang") version ("0.26.2")
}

java {
    toolchain { languageVersion = JavaLanguageVersion.of(17) }
    withJavadocJar()
    withSourcesJar()
}

repositories { mavenCentral() }

dependencies {
    api(platform("org.apache.logging.log4j:log4j-bom:2.25.3"))
    api(platform("io.grpc:grpc-bom:1.77.0"))
    api(platform("com.google.protobuf:protobuf-bom:3.25.8"))

    implementation("org.pkl-lang:pkl-config-java:0.30.0")

    implementation("org.apache.logging.log4j:log4j-api")

    implementation("io.grpc:grpc-protobuf")
    implementation("io.grpc:grpc-services")
    implementation("io.grpc:grpc-netty-shaded")
    implementation("com.google.protobuf:protobuf-java-util")

    implementation("com.dashjoin:jsonata:0.9.9")

    testImplementation("org.apache.logging.log4j:log4j-core")
    testImplementation("javax.annotation:javax.annotation-api:1.3.2")
    testImplementation("com.google.truth:truth:1.4.5")
}

testing {
    suites {
        val test by
            getting(JvmTestSuite::class) {
                useJUnitJupiter()
                targets {
                    all {
                        testTask.configure {
                            testLogging {
                                showStandardStreams = true
                                showExceptions = true
                                showCauses = true
                                showStackTraces = true
                                exceptionFormat = TestExceptionFormat.FULL
                                showStandardStreams = false
                                events =
                                    setOf(
                                        TestLogEvent.STANDARD_OUT,
                                        TestLogEvent.STANDARD_ERROR,
                                        TestLogEvent.STARTED,
                                        TestLogEvent.PASSED,
                                        TestLogEvent.SKIPPED,
                                        TestLogEvent.FAILED,
                                    )
                            }
                        }
                    }
                }
            }
    }
}

tasks.test {
    finalizedBy(tasks.jacocoTestReport) // report is always generated after tests run
}

tasks.jacocoTestReport {
    dependsOn(tasks.test) // tests are required to run before generating the report
    reports {
        xml.required.set(true)
        csv.required.set(true)
    }
}

spotless {
    java {
        targetExclude("**/build/generated/**")
        importOrder()
        removeUnusedImports()
        googleJavaFormat()

        // and apply a license header
        licenseHeaderFile(rootProject.file("HEADER"))
    }
}

pkl {
    javaCodeGenerators {
        register("configClasses") {
            sourceModules.set(files("src/main/resources/MockServer.pkl"))
            generateJavadoc.set(true)
        }
    }
}

publishing {
    publications {
        create<MavenPublication>("mavenJava") {
            from(components["java"])
            pom {
                name.set("Mock GRPC Server")
                description.set("GRPC Server used for mocking multiple rpc services.")
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

signing { sign(publishing.publications["mavenJava"]) }

protobuf {
    protoc { artifact = "com.google.protobuf:protoc:3.25.8" }
    plugins { create("grpc") { artifact = "io.grpc:protoc-gen-grpc-java:1.77.0" } }
    generateProtoTasks { ofSourceSet("test").forEach { it.plugins { create("grpc") {} } } }
}
