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

import com.google.protobuf.gradle.id

plugins {
    id("com.google.protobuf") version "0.9.4"
}

group = "com.starrocks"
version = "4.1.0-SNAPSHOT"

dependencies {
    // gRPC dependencies
    implementation("io.grpc:grpc-protobuf:${project.ext["grpc.version"]}")
    implementation("io.grpc:grpc-stub:${project.ext["grpc.version"]}")
    runtimeOnly("io.grpc:grpc-netty-shaded:${project.ext["grpc.version"]}")
    compileOnly("org.apache.tomcat:tomcat-annotations-api:${project.ext["tomcat.version"]}")

    // Protobuf
    implementation("com.google.protobuf:protobuf-java:${project.ext["protobuf-java.version"]}")

    // Logging
    implementation("org.apache.logging.log4j:log4j-api:${project.ext["log4j.version"]}")
    implementation("org.apache.logging.log4j:log4j-core:${project.ext["log4j.version"]}")
    implementation("org.apache.logging.log4j:log4j-slf4j-impl:${project.ext["log4j.version"]}")

    // Prometheus metrics
    implementation("io.prometheus:prometheus-metrics-core:1.0.0")
    implementation("io.prometheus:prometheus-metrics-exporter-httpserver:1.0.0")

    // Test dependencies
    testImplementation("junit:junit:4.13.2")
    testImplementation("org.apache.commons:commons-lang3:3.11")
    testImplementation("commons-io:commons-io:2.11.0")
    testImplementation("com.github.hazendaz.jmockit:jmockit:1.49.4")
    testImplementation("com.google.code.gson:gson:2.8.9")
}

protobuf {
    protoc {
        artifact = "com.google.protobuf:protoc:3.16.1"
    }
    plugins {
        id("grpc") {
            artifact = "io.grpc:protoc-gen-grpc-java:1.44.1"
        }
    }
    generateProtoTasks {
        all().forEach { task ->
            task.plugins {
                id("grpc") {}
            }
        }
    }
}

sourceSets {
    main {
        proto {
            srcDir("src/main/proto")
        }
    }
    test {
        proto {
            srcDir("src/test/proto")
        }
    }
}

tasks.withType<JavaCompile> {
    sourceCompatibility = "1.8"
    targetCompatibility = "1.8"
}

tasks.withType<ProcessResources> {
    duplicatesStrategy = DuplicatesStrategy.INCLUDE
}
