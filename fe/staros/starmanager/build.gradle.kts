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

group = "com.starrocks"
version = "4.1.0-SNAPSHOT"

dependencies {
    // Internal dependencies
    implementation(project(":staros:starcommon"))

    // Logging
    implementation("org.apache.logging.log4j:log4j-api:${project.ext["log4j.version"]}")
    implementation("org.apache.logging.log4j:log4j-core:${project.ext["log4j.version"]}")
    implementation("org.apache.logging.log4j:log4j-slf4j-impl:${project.ext["log4j.version"]}")

    // Prometheus metrics
    implementation("io.prometheus:prometheus-metrics-core:1.0.0")
    implementation("io.prometheus:prometheus-metrics-exporter-httpserver:1.0.0")

    // Netty
    implementation("io.netty:netty-all:4.1.61.Final")

    // Jackson
    implementation("com.fasterxml.jackson.core:jackson-databind:2.13.4.2")

    // AWS SDK
    implementation("software.amazon.awssdk:s3")
    implementation("software.amazon.awssdk:dynamodb")
    implementation("software.amazon.awssdk:sts")

    // Google Cloud Storage
    implementation("com.google.cloud:google-cloud-storage:2.43.2")

    // Azure Storage
    implementation("com.azure:azure-storage-blob")
    implementation("com.azure:azure-storage-blob-batch")
    implementation("com.azure:azure-storage-file-datalake")
    implementation("com.azure:azure-identity")

    // RocksDB
    implementation("org.rocksdb:rocksdbjni:6.22.1")

    // Commons
    implementation("org.apache.commons:commons-lang3:3.11")
    implementation("commons-io:commons-io:2.11.0")

    // Guava
    implementation("com.google.guava:guava:32.1.1-jre")

    // Test dependencies
    testImplementation("junit:junit:4.13.2")
    testImplementation("com.github.hazendaz.jmockit:jmockit:1.49.4")
    testImplementation("org.awaitility:awaitility:4.2.0")
}

tasks.withType<JavaCompile> {
    sourceCompatibility = "1.8"
    targetCompatibility = "1.8"
}
