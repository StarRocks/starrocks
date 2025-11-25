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
    antlr
}

java {
    sourceCompatibility = JavaVersion.VERSION_17
    targetCompatibility = JavaVersion.VERSION_17
}

configurations.configureEach {
    resolutionStrategy.force("org.antlr:antlr4-runtime:${project.ext["antlr.version"]}")
}
dependencies {
    antlr("org.antlr:antlr4:${project.ext["antlr.version"]}")

    implementation("org.antlr:antlr4-runtime")
}

// Configure ANTLR plugin
tasks.generateGrammarSource {
    maxHeapSize = "512m"

    val grammarDir = file("src/main/antlr/com/starrocks/grammar")

    arguments = listOf(
        "-visitor",
        "-package", "com.starrocks.sql.parser",
        "-lib", grammarDir.absolutePath
    )
}

// Add source generation tasks to the build process
tasks.compileJava {
    dependsOn("generateGrammarSource")
}
