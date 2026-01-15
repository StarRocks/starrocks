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
}

java {
    sourceCompatibility = JavaVersion.VERSION_1_8
    targetCompatibility = JavaVersion.VERSION_1_8
}

group = "com.starrocks"

dependencies {
    implementation("com.google.guava:guava")
    implementation("org.roaringbitmap:RoaringBitmap")

    testImplementation("org.junit.jupiter:junit-jupiter")
    testImplementation("com.github.hazendaz.jmockit:jmockit")
}

tasks.withType<Test> {
    // Configure JMockit agent for tests
    jvmArgs("-javaagent:${repositories.mavenLocal().url.path}/com/github/hazendaz/jmockit/jmockit/1.49.4/jmockit-1.49.4.jar")

    // Set for parallel test execution as in the Maven config
    maxParallelForks = providers.gradleProperty("fe_ut_parallel").map { it.toInt() }.getOrElse(1)

    // Equivalent to reuseForks=false in Maven
    forkEvery = 1
}

tasks.withType<JavaCompile> {
    options.encoding = "UTF-8"
}
