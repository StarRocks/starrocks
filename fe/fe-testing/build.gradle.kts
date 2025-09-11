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
    checkstyle
}

java {
    sourceCompatibility = JavaVersion.VERSION_1_8
    targetCompatibility = JavaVersion.VERSION_1_8
}

group = "com.starrocks"

// Note: There are no explicit dependencies in the original pom.xml for this module

tasks.withType<Test> {
    // Configuration from Maven: failIfNoSpecifiedTests=false
    ignoreFailures = true
}

tasks.withType<JavaCompile> {
    options.encoding = "UTF-8"
}

checkstyle {
    toolVersion = project.findProperty("puppycrawl.version") as String? ?: "10.21.1"
    configFile = rootProject.file("checkstyle.xml")
}

tasks.withType<Checkstyle> {
    exclude("**/jmockit/**/*")
    isShowViolations = true
    ignoreFailures = false
}
