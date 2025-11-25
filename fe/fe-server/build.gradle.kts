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
    `maven-publish`
}

dependencies {
    implementation(project(":fe-core"))

    implementation("com.google.guava:guava")
    implementation("commons-cli:commons-cli")

    testImplementation("org.junit.jupiter:junit-jupiter")
}

tasks.jar {
    archiveBaseName.set("starrocks-fe")
    manifest {
        attributes(
            "Main-Class" to "com.starrocks.StarRocksFE"
        )
    }
}

tasks.register<Copy>("copyDependencies") {
    from(configurations.runtimeClasspath)
    into("${layout.buildDirectory.get()}/lib")
}

tasks.build {
    dependsOn("copyDependencies")
}