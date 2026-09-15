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

// The .g4 files sit under src/main/antlr/com/starrocks/grammar, and ANTLR must be pointed at
// that directory as its source root instead of the plugin's default src/main/antlr. This is the
// same root antlr4-maven-plugin uses via <sourceDirectory> in pom.xml, and the two builds must
// agree.
//
// Why the deeper root is required, not just tidier: ANTLR mirrors each grammar's path *relative
// to the source root* into the output directory, but resolves a `tokenVocab` reference only in
// -lib or in the *base* output directory (TokenVocabParser#getImportedVocabFile). Rooted at
// src/main/antlr, SearchDslLexer.tokens is written to <out>/com/starrocks/grammar/ while
// SearchDslParser.g4's `options { tokenVocab=SearchDslLexer; }` is looked up in <out>/, so
// generation fails with "cannot find tokens file". A flat root puts both in the same place.
// It also keeps `import StarRocksLex;` in StarRocks.g4 resolvable via Tool.inputDirectory.
val grammarDir = file("src/main/antlr/com/starrocks/grammar")

sourceSets {
    main {
        antlr {
            setSrcDirs(listOf(grammarDir))
        }
    }
}

// Configure ANTLR plugin
tasks.generateGrammarSource {
    maxHeapSize = "512m"

    arguments = listOf(
        "-visitor",
        "-package", "com.starrocks.sql.parser",
        // Resolves `import StarRocksLex;` in StarRocks.g4; dropping it breaks that import.
        "-lib", grammarDir.absolutePath
    )
}

// Add source generation tasks to the build process
tasks.compileJava {
    dependsOn("generateGrammarSource")
}
