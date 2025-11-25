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

// Property equivalent to fe_ut_parallel in Maven
val feUtParallel = project.findProperty("fe_ut_parallel") ?: "1"

dependencies {
    // StarRocks modules
    implementation(project(":fe-testing"))
    implementation(project(":fe-utils"))

    // Regular dependencies
    implementation("com.google.guava:guava")
    implementation("com.google.code.gson:gson")
    implementation("io.netty:netty-handler")
    implementation("org.roaringbitmap:RoaringBitmap")

    // Provided scope dependencies - equivalent to compileOnly in Gradle
    compileOnly("commons-codec:commons-codec")
    compileOnly("org.apache.commons:commons-lang3")
    compileOnly("org.apache.spark:spark-core_2.12")
    compileOnly("org.apache.spark:spark-sql_2.12")
    compileOnly("org.apache.spark:spark-catalyst_2.12")
    compileOnly("org.apache.hadoop:hadoop-common") {
        exclude(group = "io.netty")
    }
    compileOnly("org.apache.parquet:parquet-column")
    compileOnly("org.apache.parquet:parquet-hadoop")
    compileOnly("org.apache.parquet:parquet-common")
    compileOnly("commons-collections:commons-collections")
    compileOnly("org.scala-lang:scala-library")
    compileOnly("com.esotericsoftware:kryo-shaded")
    compileOnly("org.apache.logging.log4j:log4j-slf4j-impl")

    // Test dependencies
    testImplementation("org.junit.jupiter:junit-jupiter")
    testImplementation("com.github.hazendaz.jmockit:jmockit")
    testImplementation("org.apache.spark:spark-sql_2.12")
}

tasks.withType<Test> {
    // Configure JMockit agent for tests
    jvmArgs("-javaagent:${repositories.mavenLocal().url.path}/com/github/hazendaz/jmockit/jmockit/1.49.4/jmockit-1.49.4.jar")

    // Set for parallel test execution as in the Maven config
    maxParallelForks = (feUtParallel as String).toInt()

    // Equivalent to reuseForks=false in Maven
    forkEvery = 1
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

// Equivalent to Maven Assembly plugin to create a jar with dependencies
tasks.register<Jar>("jarWithDependencies") {
    archiveClassifier.set("with-dependencies")
    duplicatesStrategy = DuplicatesStrategy.EXCLUDE

    manifest {
        attributes["Main-Class"] = "com.starrocks.load.loadv2.etl.SparkEtlJob"
    }

    from(sourceSets.main.get().output)

    dependsOn(configurations.runtimeClasspath)
    from({
        configurations.runtimeClasspath.get()
            .filter { it.name.endsWith("jar") }
            .map { zipTree(it) }
    })
}

// Make the jarWithDependencies task run as part of the build
tasks.build {
    dependsOn("jarWithDependencies")
}

// Set the final JAR name
tasks.jar {
    archiveBaseName.set("spark-dpp")
    archiveVersion.set(project.version.toString())
}
