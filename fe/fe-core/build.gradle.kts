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

import com.baidu.jprotobuf.plugin.PrecompileTask
import org.gradle.api.tasks.testing.logging.TestExceptionFormat
import org.gradle.api.tasks.testing.logging.TestLogEvent

plugins {
    java
    id("com.baidu.jprotobuf") version "1.2.1"
}

java {
    sourceCompatibility = JavaVersion.VERSION_17
    targetCompatibility = JavaVersion.VERSION_17
    sourceSets {
        main {
            java {
                srcDir("src/main/java")
                srcDir("build/generated-sources/proto")
                srcDir("build/generated-sources/thrift")
                srcDir("build/generated-sources/genscript")
            }
        }
        test {
            java {
                srcDir("src/test/java")
            }
            resources {
                srcDir("src/test/resources")
            }
        }
    }
}

dependencies {
    // Internal project dependencies
    implementation(project(":fe-grammar"))
    implementation(project(":fe-parser"))
    implementation(project(":fe-spi"))
    implementation(project(":fe-testing"))
    implementation(project(":fe-utils"))
    implementation(project(":plugin:hive-udf"))
    implementation(project(":plugin:spark-dpp"))

    // dependency sync start
    implementation("com.aliyun.datalake:metastore-client-hive3") {
        exclude(group = "com.aliyun", module = "tea")
        exclude(group = "com.aliyun", module = "tea-openapi")
        exclude(group = "com.aliyun", module = "tea-util")
        exclude(group = "com.aliyun", module = "datalake20200710")
    }
    implementation("com.aliyun.odps:odps-sdk-core") {
        exclude(group = "org.codehaus.jackson", module = "jackson-mapper-asl")
        exclude(group = "org.ini4j", module = "ini4j")
        exclude(group = "org.antlr", module = "antlr4")
    }
    implementation("com.aliyun.odps:odps-sdk-table-api") {
        exclude(group = "org.antlr", module = "antlr4")
    }
    implementation("com.azure:azure-identity")
    implementation("com.azure:azure-storage-blob")
    compileOnly("com.baidu:jprotobuf-precompile-plugin") {
        exclude(group = "org.apache.maven", module = "maven-core")
        exclude(group = "org.codehaus.plexus", module = "plexus-utils")
        exclude(group = "junit", module = "junit")
    }
    implementation("com.baidu:jprotobuf-rpc-common")
    implementation("com.baidu:jprotobuf-rpc-core") {
        exclude(group = "com.baidu", module = "jprotobuf")
        exclude(group = "junit", module = "junit")
    }
    implementation("com.clickhouse:clickhouse-jdbc")
    implementation("com.fasterxml.jackson.core:jackson-annotations")
    implementation("com.fasterxml.jackson.core:jackson-core")
    implementation("com.fasterxml.jackson.core:jackson-databind")
    implementation("com.fasterxml.jackson.dataformat:jackson-dataformat-yaml")
    implementation("com.fasterxml.jackson.module:jackson-module-jaxb-annotations")
    implementation("com.fasterxml.uuid:java-uuid-generator")
    implementation("com.github.ben-manes.caffeine:caffeine")
    testImplementation("com.github.hazendaz.jmockit:jmockit")
    implementation("com.github.oshi:oshi-core")
    implementation("com.github.seancfoley:ipaddress")
    implementation("com.google.cloud.bigdataoss:gcs-connector")
    implementation("com.google.code.gson:gson")
    implementation("com.google.guava:guava")
    implementation("com.google.protobuf:protobuf-java")
    implementation("com.google.protobuf:protobuf-java-util")
    implementation("com.microsoft.sqlserver:mssql-jdbc")
    testImplementation("com.mockrunner:mockrunner-jdbc") {
        exclude(group = "xerces", module = "xercesImpl")
        exclude(group = "junit", module = "junit")
    }
    implementation("com.opencsv:opencsv")
    implementation("com.oracle.database.jdbc:ojdbc10")
    implementation("com.oracle.database.nls:orai18n")
    implementation("com.qcloud.cos:hadoop-cos")
    implementation("com.qcloud:chdfs_hadoop_plugin_network")
    implementation("com.squareup.okhttp3:okhttp")
    implementation("com.squareup.okio:okio")
    implementation("com.starrocks:spark-dpp")
    implementation("com.starrocks:starclient")
    implementation("com.starrocks:starmanager")
    implementation("com.starrocks:starrocks-bdb-je") {
        exclude(group = "org.checkerframework", module = "checker-qual")
    }
    implementation("com.sun.activation:javax.activation")
    implementation("com.zaxxer:HikariCP:${project.ext["hikaricp.version"]}")
    implementation("commons-cli:commons-cli")
    implementation("commons-codec:commons-codec")
    implementation("commons-io:commons-io")
    implementation("commons-lang:commons-lang")
    implementation("commons-validator:commons-validator") {
        exclude(group = "commons-collections", module = "commons-collections")
    }
    implementation("de.jflex:jflex")
    implementation("io.airlift:concurrent")
    implementation("io.airlift:security")
    implementation("io.delta:delta-kernel-api")
    implementation("io.delta:delta-kernel-defaults") {
        exclude(group = "org.apache.hadoop", module = "hadoop-client-api")
        exclude(group = "org.apache.hadoop", module = "hadoop-client-runtime")
    }
    implementation("io.grpc:grpc-api")
    implementation("io.grpc:grpc-core")
    implementation("io.grpc:grpc-netty-shaded")
    implementation("io.grpc:grpc-protobuf")
    implementation("io.grpc:grpc-stub")
    implementation("io.netty:netty-all")
    implementation("io.opentelemetry:opentelemetry-api")
    implementation("io.opentelemetry:opentelemetry-exporter-jaeger")
    implementation("io.opentelemetry:opentelemetry-exporter-otlp")
    implementation("io.opentelemetry:opentelemetry-sdk")
    implementation("io.trino.hive:hive-apache") {
        exclude(group = "org.apache.parquet", module = "*")
        exclude(group = "org.apache.avro", module = "*")
    }
    implementation("io.trino:trino-parser:385")
    implementation("it.unimi.dsi:fastutil")
    implementation("javax.annotation:javax.annotation-api")
    implementation("javax.validation:validation-api")
    implementation("net.openhft:zero-allocation-hashing:0.16")
    implementation("org.apache.arrow:arrow-jdbc")
    implementation("org.apache.arrow:arrow-memory-netty")
    implementation("org.apache.arrow:arrow-vector")
    implementation("org.apache.arrow:flight-core")
    implementation("org.apache.arrow:flight-sql")
    testImplementation("org.apache.arrow:flight-sql-jdbc-driver")
    testImplementation("org.apache.commons:commons-dbcp2")
    implementation("org.apache.commons:commons-lang3")
    implementation("org.apache.commons:commons-pool2")
    implementation("org.apache.groovy:groovy-groovysh")
    implementation("org.apache.hadoop:hadoop-aliyun") {
        exclude(group = "org.jdom", module = "jdom2")
        exclude(group = "org.ini4j", module = "ini4j")
    }
    implementation("org.apache.hadoop:hadoop-aws") {
        exclude(group = "software.amazon.awssdk", module = "bundle")
        exclude(group = "org.apache.hadoop", module = "hadoop-common")
    }
    implementation("org.apache.hadoop:hadoop-azure")
    implementation("org.apache.hadoop:hadoop-azure-datalake")
    implementation("org.apache.hadoop:hadoop-client") {
        exclude(group = "org.slf4j", module = "slf4j-reload4j")
        exclude(group = "ch.qos.reload4j", module = "reload4j")
    }
    implementation("org.apache.hadoop:hadoop-client-api")
    implementation("org.apache.hadoop:hadoop-client-runtime") {
        exclude(group = "dnsjava", module = "dnsjava")
        exclude(group = "org.apache.avro", module = "avro")
    }
    implementation("org.apache.hadoop:hadoop-common") {
        exclude(group = "org.apache.zookeeper", module = "zookeeper")
        exclude(group = "org.slf4j", module = "slf4j-reload4j")
        exclude(group = "ch.qos.reload4j", module = "reload4j")
        exclude(group = "javax.ws.rs", module = "jsr311-api")
    }
    implementation("org.apache.hadoop:hadoop-hdfs")
    implementation("org.apache.httpcomponents.client5:httpclient5")
    implementation("org.apache.hudi:hudi-common") {
        exclude(group = "io.netty", module = "*")
        exclude(group = "org.glassfish", module = "javax.el")
        exclude(group = "org.apache.zookeeper", module = "zookeeper")
    }
    implementation("org.apache.hudi:hudi-hadoop-mr") {
        exclude(group = "org.glassfish", module = "javax.el")
    }
    implementation("org.apache.hudi:hudi-io")
    implementation("org.apache.iceberg:iceberg-api") {
        exclude(group = "org.apache.parquet", module = "parquet-format-structures")
    }
    implementation("org.apache.iceberg:iceberg-aws")
    implementation("org.apache.iceberg:iceberg-bundled-guava")
    implementation("org.apache.iceberg:iceberg-common")
    implementation("org.apache.iceberg:iceberg-core")
    implementation("org.apache.iceberg:iceberg-hive-metastore")
    implementation("org.apache.ivy:ivy")
    implementation("org.apache.kudu:kudu-client") {
        exclude(group = "io.netty", module = "netty-handler")
    }
    implementation("org.apache.logging.log4j:log4j-1.2-api")
    implementation("org.apache.logging.log4j:log4j-api")
    implementation("org.apache.logging.log4j:log4j-core")
    implementation("org.apache.logging.log4j:log4j-layout-template-json")
    implementation("org.apache.logging.log4j:log4j-slf4j-impl")
    implementation("org.apache.paimon:paimon-bundle")
    implementation("org.apache.paimon:paimon-oss")
    implementation("org.apache.paimon:paimon-s3")
    implementation("org.apache.parquet:parquet-avro")
    implementation("org.apache.parquet:parquet-column")
    implementation("org.apache.parquet:parquet-common")
    implementation("org.apache.parquet:parquet-hadoop")
    implementation("org.apache.ranger:ranger-plugins-common") {
        exclude(group = "org.elasticsearch", module = "*")
        exclude(group = "org.elasticsearch.client", module = "*")
        exclude(group = "com.nimbusds", module = "nimbus-jose-jwt")
        exclude(group = "com.sun.jersey", module = "jersey-bundle")
    }
    compileOnly("org.apache.spark:spark-catalyst_2.12")
    implementation("org.apache.spark:spark-core_2.12") {
        exclude(group = "org.slf4j", module = "slf4j-log4j12")
        exclude(group = "org.apache.zookeeper", module = "zookeeper")
        exclude(group = "org.apache.logging.log4j", module = "log4j-slf4j2-impl")
        exclude(group = "org.apache.hadoop", module = "hadoop-client")
        exclude(group = "org.apache.hadoop", module = "hadoop-client-api")
        exclude(group = "org.apache.ivy", module = "ivy")
        exclude(group = "log4j", module = "log4j")
        exclude(group = "com.clearspring.analytics", module = "stream")
        exclude(group = "org.apache.hadoop", module = "hadoop-client-runtime")
        exclude(group = "org.apache.commons", module = "commons-compress")
        exclude(group = "com.google.protobuf", module = "protobuf-java")
        exclude(group = "org.eclipse.jetty", module = "jetty-server")
        exclude(group = "org.eclipse.jetty", module = "jetty-util")
        exclude(group = "org.eclipse.jetty", module = "jetty-io")
        exclude(group = "org.eclipse.jetty", module = "jetty-servlet")
        exclude(group = "org.eclipse.jetty", module = "jetty-client")
        exclude(group = "org.eclipse.jetty", module = "jetty-security")
    }
    implementation("org.apache.spark:spark-launcher_2.12")
    compileOnly("org.apache.spark:spark-sql_2.12")
    implementation("org.apache.thrift:libthrift") {
        exclude(group = "org.apache.tomcat.embed", module = "tomcat-embed-core")
        exclude(group = "org.apache.tomcat", module = "tomcat-annotations-api")
    }
    implementation("org.apache.velocity:velocity-engine-core")
    testImplementation("org.assertj:assertj-core:3.24.2")
    testImplementation("org.awaitility:awaitility:4.2.0")
    implementation("org.jboss.byteman:byteman")
    implementation("org.jboss.xnio:xnio-nio")
    implementation("org.jdom:jdom2")
    implementation("org.json:json")
    testImplementation("org.junit.jupiter:junit-jupiter")
    implementation("org.mariadb.jdbc:mariadb-java-client")
    testImplementation("org.mockito:mockito-inline:4.11.0")
    testImplementation("org.openjdk.jmh:jmh-core:1.37")
    testImplementation("org.openjdk.jmh:jmh-generator-annprocess:1.37")
    implementation("org.owasp.encoder:encoder")
    implementation("org.postgresql:postgresql")
    implementation("org.quartz-scheduler:quartz:2.5.0")
    implementation("org.roaringbitmap:RoaringBitmap") {
        exclude(group = "org.apache.zookeeper", module = "zookeeper")
    }
    implementation("org.slf4j:slf4j-api")
    implementation("org.threeten:threeten-extra:1.7.2")
    implementation("org.xerial.snappy:snappy-java")
    implementation("software.amazon.awssdk:bundle")
    implementation("tools.profiler:async-profiler")
    implementation("com.github.vertical-blank:sql-formatter:2.0.4")
    // dependency sync end

    // extra dependencies pom.xml does not have
    implementation("com.starrocks:jprotobuf-starrocks:${project.ext["jprotobuf-starrocks.version"]}")
    implementation("org.apache.groovy:groovy:4.0.9")
    testImplementation("org.apache.spark:spark-sql_2.12")
    implementation("software.amazon.awssdk:s3-transfer-manager")
    implementation("net.openhft:zero-allocation-hashing:0.16")
}

// Custom task for Protocol Buffer generation
tasks.register<Task>("generateProtoSources") {
    description = "Generates Java source files from Protocol Buffer definitions"
    group = "build"

    // Create a special configuration for the protobuf compiler rather than using runtime classpath
    val protoGenClasspath = configurations.create("protoGenClasspath")
    dependencies {
        protoGenClasspath("com.starrocks:jprotobuf-starrocks:${project.ext["jprotobuf-starrocks.version"]}:jar-with-dependencies")
    }

    val protoDir = file("../../gensrc/proto")
    val outputDir = layout.buildDirectory.get().dir("generated-sources/proto").asFile

    // List of proto files to process
    val protoFiles = listOf(
        "lake_types.proto",
        "internal_service.proto",
        "types.proto",
        "tablet_schema.proto",
        "lake_service.proto",
        "encryption.proto"
    )

    // Declare inputs (proto files)
    inputs.files(protoFiles.map { file("$protoDir/$it") })

    // Declare output directory
    outputs.dir(outputDir)

    doFirst {
        mkdir(outputDir)

        // Process each proto file individually
        protoFiles.forEach { protoFile ->
            logger.info("Processing proto file: $protoFile")
            project.javaexec {
                classpath = protoGenClasspath
                mainClass.set("com.baidu.bjf.remoting.protobuf.command.Main")
                args = listOf(
                    "--java_out=$outputDir",
                    "$protoDir/$protoFile"
                )
            }
        }
    }
}

// Custom task for Thrift generation
tasks.register<Task>("generateThriftSources") {
    description = "Generates Java source files from Thrift definitions"
    group = "build"

    // Create a special configuration for the thrift compiler rather than using runtime classpath
    val thriftGenClasspath = configurations.create("thriftGenClasspath")
    dependencies {
        thriftGenClasspath("io.github.decster:thrift-java-maven-plugin:0.1.3")
    }

    val protoDir = file("../../gensrc/thrift")
    val outputDir = layout.buildDirectory.get().dir("generated-sources/thrift").asFile

    // List of proto files to process
    val protoFiles = fileTree(protoDir) {
        include("*.thrift")
        exclude("parquet.thrift")
    }.files

    // Declare inputs (proto files)
    inputs.files(protoFiles)

    // Declare output directory
    outputs.dir(outputDir)

    doFirst {
        mkdir(outputDir)
        // Process each proto file individually
        project.javaexec {
            classpath = thriftGenClasspath
            mainClass.set("io.github.decster.ThriftCompiler")
            // Build arguments list with the output directory and all thrift files
            val allArgs = mutableListOf("-o", "$outputDir")
            protoFiles.forEach { file ->
                allArgs.add(file.absolutePath)
            }
            args = allArgs
        }
    }
}


tasks.register<Task>("generateByScripts") {
    description = "Generates java code by scripts"
    group = "build"

    val outputDir = layout.buildDirectory.get().dir("generated-sources/genscript").asFile

    outputs.dir(outputDir)

    doFirst {
        mkdir(outputDir)

        // First Python script - build version generation
        project.exec {
            commandLine(
                "python3",
                "${project.rootProject.projectDir}/../build-support/gen_build_version.py",
                "--java", outputDir.toString()
            )
        }

        // Second Python script - function generation
        project.exec {
            commandLine(
                "python3",
                "${project.rootProject.projectDir}/../gensrc/script/gen_functions.py",
                "--cpp", outputDir.toString(),
                "--java", outputDir.toString()
            )
        }
    }
}

// Add source generation tasks to the build process
tasks.compileJava {
    dependsOn("generateThriftSources", "generateProtoSources", "generateByScripts")
    // Add explicit dependency on hive-udf shadowJar task
    dependsOn(":plugin:hive-udf:shadowJar")
}

tasks.named<PrecompileTask>("jprotobuf_precompile") {
    filterClassPackage = "com.starrocks.proto;com.starrocks.rpc;com.starrocks.server"
    generateProtoFile = "true"
}

tasks.named<ProcessResources>("processTestResources") {
    duplicatesStrategy = DuplicatesStrategy.EXCLUDE
}

// Configure test task
tasks.test {
    useJUnitPlatform()
    maxParallelForks = (project.findProperty("fe_ut_parallel") as String? ?: "16").toInt()

    // Don't reuse JVM processes for tests
    forkEvery = 1

    maxHeapSize = "4096m"

    testLogging {
        // Events to log, like you have
        events = setOf(
            TestLogEvent.PASSED,
            TestLogEvent.SKIPPED,
            TestLogEvent.FAILED
        )

        // Show the standard output and error streams of the test JVM(s)
        showStandardStreams = false

        // Configure how exceptions are displayed
        exceptionFormat = TestExceptionFormat.SHORT // Or FULL
        showStackTraces = false
        showCauses = false // Show underlying causes for exceptions
    }

    systemProperty("starrocks.home", project.ext["starrocks.home"] as String)

    // Add JMockit Java agent to JVM arguments
    jvmArgs(
        "-Djdk.attach.allowAttachSelf",
        "-Duser.timezone=Asia/Shanghai",
        "-javaagent:${configurations.testCompileClasspath.get().find { it.name.contains("jmockit") }?.absolutePath}"
    )

    // Use independent class loading (equivalent to useSystemClassLoader=false)
    systemProperty("java.security.manager", "allow")

    exclude {
        it.name.contains("QueryDumpRegressionTest") || it.name.contains("QueryDumpCaseRewriter")
    }
}


// Configure JAR task
tasks.jar {
    //dependsOn("jprotobuf_precompile")
    manifest {
        attributes(
            "Main-Class" to "com.starrocks.StarRocksFE",
            "Implementation-Version" to project.version
        )
    }
}
