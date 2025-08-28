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

plugins {
    java
    id("com.github.johnrengelman.shadow") version "8.1.1"
}

version = "1.0.0"

java {
    sourceCompatibility = JavaVersion.VERSION_1_8
    targetCompatibility = JavaVersion.VERSION_1_8
}

dependencies {
    implementation(project(":fe-utils"))
    compileOnly("io.trino.hive:hive-apache")
    compileOnly("org.apache.hadoop:hadoop-client")
}

tasks.withType<JavaCompile> {
    options.release.set(8)
}


// Replace the jar task with shadowJar configuration
tasks.named<com.github.jengelman.gradle.plugins.shadow.tasks.ShadowJar>("shadowJar") {
    // Minimize JAR
    minimize {
        exclude(dependency("${project.group}:fe-utils:${project.version}"))
    }

    // Relocate packages
    relocate("org.roaringbitmap", "shade.starrocks.org.roaringbitmap")

    // Filter artifacts and exclude signatures
    exclude("META-INF/*.SF", "META-INF/*.DSA", "META-INF/*.RSA")
    exclude("org/apache/logging/log4j/**")

    mergeServiceFiles()
    archiveClassifier.set("")
}

// Make the shadowJar task run when the build task runs
tasks.build {
    dependsOn(tasks.shadowJar)
}

// Equivalent to Maven Surefire plugin
tasks.test {
    val failIfNoSpecifiedTests: String by project
    val failIfNoSpecifiedTestsValue = project.findProperty("failIfNoSpecifiedTests") ?: "false"
    systemProperty("failIfNoSpecifiedTests", failIfNoSpecifiedTestsValue)
}
