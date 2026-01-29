// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

subprojects {
    // Common properties for all staros subprojects
    ext {
        set("grpc-java.version", "3.16.1")
        set("metrics.version", "1.0.0")
    }

    tasks.withType<JavaCompile> {
        sourceCompatibility = "1.8"
        targetCompatibility = "1.8"
    }

    tasks.withType<ProcessResources> {
        duplicatesStrategy = DuplicatesStrategy.INCLUDE
    }

    // Apply common dependencies
    dependencies {
        // Log4j dependencies - common to all subprojects
        constraints {
            implementation("org.apache.logging.log4j:log4j-api:${project.ext["log4j.version"]}")
            implementation("org.apache.logging.log4j:log4j-core:${project.ext["log4j.version"]}")
            implementation("org.apache.logging.log4j:log4j-slf4j-impl:${project.ext["log4j.version"]}")

            // gRPC dependencies
            implementation("io.grpc:grpc-netty-shaded:${project.ext["grpc.version"]}")
            implementation("io.grpc:grpc-protobuf:${project.ext["grpc.version"]}")
            implementation("io.grpc:grpc-stub:${project.ext["grpc.version"]}")

            // Protobuf
            implementation("com.google.protobuf:protobuf-java:${project.ext["protobuf-java.version"]}")
            implementation("com.google.protobuf:protobuf-java-util:3.21.1")

            // Commons
            implementation("org.apache.commons:commons-lang3:3.11")
            implementation("commons-io:commons-io:2.11.0")

            // Tomcat annotations
            implementation("org.apache.tomcat:tomcat-annotations-api:${project.ext["tomcat.version"]}")

            // Gson
            implementation("com.google.code.gson:gson:2.8.9")

            // Guava
            implementation("com.google.guava:guava:30.1.1-jre")

            // Prometheus metrics
            implementation("io.prometheus:prometheus-metrics-core:1.0.0")
            implementation("io.prometheus:prometheus-metrics-exporter-httpserver:1.0.0")

            // Test dependencies
            testImplementation("junit:junit:4.13.2")
            testImplementation("com.github.hazendaz.jmockit:jmockit:1.49.4")
            testImplementation("org.awaitility:awaitility:4.2.0")
        }
    }

    tasks.withType<Test> {
        // Add JMockit Java agent to JVM arguments for all staros subprojects
        doFirst {
            jvmArgs(
                "-javaagent:${configurations.testCompileClasspath.get().find { it.name.contains("jmockit") }?.absolutePath}"
            )
        }
    }
}
