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
    sourceCompatibility = JavaVersion.VERSION_17
    targetCompatibility = JavaVersion.VERSION_17
}

group = "com.starrocks"

dependencies {
    implementation("com.google.guava:guava")
    implementation("com.google.code.gson:gson")
    implementation("org.apache.commons:commons-lang3")
    implementation("org.apache.logging.log4j:log4j-api")
    testImplementation("org.junit.jupiter:junit-jupiter")
}

tasks.withType<Test> {
    useJUnitPlatform()
    
    // Configure JMockit agent for tests
    jvmArgs("-javaagent:${repositories.mavenLocal().url.path}/com/github/hazendaz/jmockit/jmockit/1.49.4/jmockit-1.49.4.jar")

    // Set for parallel test execution similar to Maven config
    maxParallelForks = providers.gradleProperty("fe_ut_parallel").map { it.toInt() }.getOrElse(1)

    // Equivalent to reuseForks=false in Maven
    forkEvery = 1
}

tasks.withType<JavaCompile> {
    options.encoding = "UTF-8"
}

// Checkstyle configuration to match Maven behavior
checkstyle {
    toolVersion = "10.21.1"  // puppycrawl.version from parent pom
    configFile = rootProject.file("checkstyle.xml")
}

// Configure Checkstyle tasks to match Maven behavior
tasks.withType<Checkstyle>().configureEach {
    exclude("**/sql/parser/gen/**")
    ignoreFailures = false  // Match Maven behavior: failsOnError=true
    // Avoid circular dependency: Checkstyle should not depend on compiled classes
    classpath = files()
}
