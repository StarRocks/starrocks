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

allprojects {
    group = "com.starrocks"
    version = "3.4.0"

    repositories {
        mavenCentral()
        maven { url = uri("https://repository.cloudera.com/repository/public/") }
        maven { url = uri("https://repository.cloudera.com/repository/cloudera-repos/") }
        maven { url = uri("https://mirror.iscas.ac.cn/kunpeng/maven/") }
    }
}

subprojects {
    apply {
        plugin("java")
        plugin("maven-publish")
    }

    // Common properties from pom.xml
    ext {
        set("starrocks.home", "${rootDir}/../")
        // var sync start
        set("antlr.version", "4.9.3")
        set("arrow.version", "18.0.0")
        set("async-profiler.version", "4.0")
        set("avro.version", "1.12.0")
        set("aws-v2-sdk.version", "2.29.52")
        set("azure.version", "1.2.34")
        set("byteman.version", "4.0.24")
        set("commons-beanutils.version", "1.11.0")
        set("delta-kernel.version", "4.0.0rc1")
        set("dlf-metastore-client.version", "0.2.14")
        set("dnsjava.version", "3.6.3")
        set("fastutil.version", "8.5.15")
        set("gcs.connector.version", "hadoop3-2.2.26")
        set("grpc.version", "1.63.0")
        set("hadoop.version", "3.4.1")
        set("hbase.version", "2.6.2")
        set("hikaricp.version", "3.4.5")
        set("hive-apache.version", "3.1.2-22")
        set("hudi.version", "1.0.2")
        set("iceberg.version", "1.10.0")
        set("io.netty.version", "4.1.128.Final")
        set("jackson.version", "2.15.2")
        set("jetty.version", "9.4.57.v20241219")
        set("jprotobuf-starrocks.version", "1.0.0")
        set("kafka-clients.version", "3.4.0")
        set("kudu.version", "1.17.1")
        set("log4j.version", "2.19.0")
        set("nimbusds.version", "9.37.2")
        set("odps.version", "0.48.7-public")
        set("paimon.version", "1.2.0")
        set("parquet.version", "1.15.2")
        set("protobuf-java.version", "3.25.5")
        set("puppycrawl.version", "10.21.1")
        set("spark.version", "3.5.5")
        set("staros.version", "3.5-rc3")
        set("tomcat.version", "8.5.70")
        // var sync end
    }

    dependencies {
        implementation(platform("com.azure:azure-sdk-bom:${project.ext["azure.version"]}"))
        implementation(platform("io.opentelemetry:opentelemetry-bom:1.14.0"))
        implementation(platform("software.amazon.awssdk:bom:${project.ext["aws-v2-sdk.version"]}"))

        constraints {
            // dependency sync start
            implementation("com.aliyun.datalake:metastore-client-hive3:${project.ext["dlf-metastore-client.version"]}")
            implementation("com.aliyun.odps:odps-sdk-core:${project.ext["odps.version"]}")
            implementation("com.aliyun.odps:odps-sdk-table-api:${project.ext["odps.version"]}")
            implementation("com.aliyun:datalake20200710:2.0.12")
            implementation("com.baidu:jprotobuf-precompile-plugin:2.2.12")
            implementation("com.baidu:jprotobuf-rpc-common:1.9")
            implementation("com.baidu:jprotobuf-rpc-core:4.2.1")
            implementation("com.clickhouse:clickhouse-jdbc:0.4.6")
            implementation("com.esotericsoftware:kryo-shaded:4.0.2")
            implementation("com.fasterxml.jackson.core:jackson-annotations:${project.ext["jackson.version"]}")
            implementation("com.fasterxml.jackson.core:jackson-core:${project.ext["jackson.version"]}")
            implementation("com.fasterxml.jackson.core:jackson-databind:${project.ext["jackson.version"]}")
            implementation("com.fasterxml.jackson.dataformat:jackson-dataformat-yaml:${project.ext["jackson.version"]}")
            implementation("com.fasterxml.jackson.module:jackson-module-jaxb-annotations:${project.ext["jackson.version"]}")
            implementation("com.fasterxml.uuid:java-uuid-generator:5.1.0")
            implementation("com.github.ben-manes.caffeine:caffeine:2.9.3")
            implementation("com.github.hazendaz.jmockit:jmockit:1.49.4")
            implementation("com.github.oshi:oshi-core:6.2.1")
            implementation("com.github.seancfoley:ipaddress:5.4.2")
            implementation("com.google.cloud.bigdataoss:gcs-connector:${project.ext["gcs.connector.version"]}")
            implementation("com.google.code.gson:gson:2.8.9")
            implementation("com.google.guava:guava:32.0.1-jre")
            implementation("com.google.protobuf:protobuf-java:${project.ext["protobuf-java.version"]}")
            implementation("com.google.protobuf:protobuf-java-util:${project.ext["protobuf-java.version"]}")
            implementation("com.microsoft.sqlserver:mssql-jdbc:12.4.2.jre11")
            implementation("com.mockrunner:mockrunner-jdbc:1.0.1")
            implementation("com.nimbusds:nimbus-jose-jwt:${project.ext["nimbusds.version"]}")
            implementation("com.opencsv:opencsv:5.7.1")
            implementation("com.oracle.database.jdbc:ojdbc10:19.18.0.0")
            implementation("com.oracle.database.nls:orai18n:19.18.0.0")
            implementation("com.qcloud.cos:hadoop-cos:3.3.0-8.3.2")
            implementation("com.qcloud:chdfs_hadoop_plugin_network:3.2")
            implementation("com.squareup.okhttp3:okhttp:4.10.0")
            implementation("com.squareup.okio:okio:3.4.0")
            implementation("com.starrocks:fe-common:1.0.0")
            implementation("com.starrocks:hive-udf:1.0.0")
            implementation("com.starrocks:jprotobuf-starrocks:${project.ext["jprotobuf-starrocks.version"]}")
            implementation("com.starrocks:plugin-common:1.0.0")
            implementation("com.starrocks:spark-dpp:1.0.0")
            implementation("com.starrocks:starclient:${project.ext["staros.version"]}")
            implementation("com.starrocks:starmanager:${project.ext["staros.version"]}")
            implementation("com.starrocks:starrocks-bdb-je:18.3.20")
            implementation("com.sun.activation:javax.activation:1.2.0")
            implementation("commons-beanutils:commons-beanutils:${project.ext["commons-beanutils.version"]}")
            implementation("commons-cli:commons-cli:1.4")
            implementation("commons-codec:commons-codec:1.13")
            implementation("commons-collections:commons-collections:3.2.2")
            implementation("commons-io:commons-io:2.16.1")
            implementation("commons-lang:commons-lang:2.4")
            implementation("commons-validator:commons-validator:1.7")
            implementation("de.jflex:jflex:1.4.3")
            implementation("dnsjava:dnsjava:${project.ext["dnsjava.version"]}")
            implementation("io.airlift:aircompressor:0.27")
            implementation("io.airlift:concurrent:202")
            implementation("io.airlift:security:202")
            implementation("io.delta:delta-kernel-api:${project.ext["delta-kernel.version"]}")
            implementation("io.delta:delta-kernel-defaults:${project.ext["delta-kernel.version"]}")
            implementation("io.grpc:grpc-api:${project.ext["grpc.version"]}")
            implementation("io.grpc:grpc-core:${project.ext["grpc.version"]}")
            implementation("io.grpc:grpc-netty-shaded:${project.ext["grpc.version"]}")
            implementation("io.grpc:grpc-protobuf:${project.ext["grpc.version"]}")
            implementation("io.grpc:grpc-stub:${project.ext["grpc.version"]}")
            implementation("io.netty:netty-all:${project.ext["io.netty.version"]}")
            implementation("io.netty:netty-handler:${project.ext["io.netty.version"]}")
            implementation("io.trino.hive:hive-apache:${project.ext["hive-apache.version"]}")
            implementation("it.unimi.dsi:fastutil:${project.ext["fastutil.version"]}")
            implementation("javax.annotation:javax.annotation-api:1.3.2")
            implementation("javax.validation:validation-api:1.1.0.Final")
            implementation("javax.xml.ws:jaxws-api:2.3.0")
            implementation("net.sourceforge.czt.dev:java-cup:0.11-a-czt02-cdh")
            implementation("org.antlr:antlr4-runtime:${project.ext["antlr.version"]}")
            implementation("org.apache.arrow:arrow-jdbc:${project.ext["arrow.version"]}")
            implementation("org.apache.arrow:arrow-memory-netty:${project.ext["arrow.version"]}")
            implementation("org.apache.arrow:arrow-vector:${project.ext["arrow.version"]}")
            implementation("org.apache.arrow:flight-core:${project.ext["arrow.version"]}")
            implementation("org.apache.arrow:flight-sql:${project.ext["arrow.version"]}")
            implementation("org.apache.arrow:flight-sql-jdbc-driver:${project.ext["arrow.version"]}")
            implementation("org.apache.avro:avro:${project.ext["avro.version"]}")
            implementation("org.apache.commons:commons-dbcp2:2.9.0")
            implementation("org.apache.commons:commons-lang3:3.9")
            implementation("org.apache.commons:commons-pool2:2.3")
            implementation("org.apache.groovy:groovy-groovysh:4.0.9")
            implementation("org.apache.hadoop:hadoop-aliyun:${project.ext["hadoop.version"]}")
            implementation("org.apache.hadoop:hadoop-aws:${project.ext["hadoop.version"]}")
            implementation("org.apache.hadoop:hadoop-azure:${project.ext["hadoop.version"]}")
            implementation("org.apache.hadoop:hadoop-azure-datalake:${project.ext["hadoop.version"]}")
            implementation("org.apache.hadoop:hadoop-client:${project.ext["hadoop.version"]}")
            implementation("org.apache.hadoop:hadoop-client-api:${project.ext["hadoop.version"]}")
            implementation("org.apache.hadoop:hadoop-client-runtime:${project.ext["hadoop.version"]}")
            implementation("org.apache.hadoop:hadoop-common:${project.ext["hadoop.version"]}")
            implementation("org.apache.hadoop:hadoop-hdfs:${project.ext["hadoop.version"]}")
            implementation("org.apache.hbase:hbase-client:${project.ext["hbase.version"]}")
            implementation("org.apache.hbase:hbase-server:${project.ext["hbase.version"]}")
            implementation("org.apache.httpcomponents.client5:httpclient5:5.4.3")
            implementation("org.apache.hudi:hudi-common:${project.ext["hudi.version"]}")
            implementation("org.apache.hudi:hudi-hadoop-mr:${project.ext["hudi.version"]}")
            implementation("org.apache.hudi:hudi-io:${project.ext["hudi.version"]}")
            implementation("org.apache.iceberg:iceberg-api:${project.ext["iceberg.version"]}")
            implementation("org.apache.iceberg:iceberg-aws:${project.ext["iceberg.version"]}")
            implementation("org.apache.iceberg:iceberg-bundled-guava:${project.ext["iceberg.version"]}")
            implementation("org.apache.iceberg:iceberg-common:${project.ext["iceberg.version"]}")
            implementation("org.apache.iceberg:iceberg-core:${project.ext["iceberg.version"]}")
            implementation("org.apache.iceberg:iceberg-hive-metastore:${project.ext["iceberg.version"]}")
            implementation("org.apache.ivy:ivy:2.5.2")
            implementation("org.apache.kafka:kafka-clients:${project.ext["kafka-clients.version"]}")
            implementation("org.apache.kudu:kudu-client:${project.ext["kudu.version"]}")
            implementation("org.apache.logging.log4j:log4j-1.2-api:${project.ext["log4j.version"]}")
            implementation("org.apache.logging.log4j:log4j-api:${project.ext["log4j.version"]}")
            implementation("org.apache.logging.log4j:log4j-core:${project.ext["log4j.version"]}")
            implementation("org.apache.logging.log4j:log4j-layout-template-json:${project.ext["log4j.version"]}")
            implementation("org.apache.logging.log4j:log4j-slf4j-impl:${project.ext["log4j.version"]}")
            implementation("org.apache.paimon:paimon-bundle:${project.ext["paimon.version"]}")
            implementation("org.apache.paimon:paimon-oss:${project.ext["paimon.version"]}")
            implementation("org.apache.paimon:paimon-s3:${project.ext["paimon.version"]}")
            implementation("org.apache.parquet:parquet-avro:${project.ext["parquet.version"]}")
            implementation("org.apache.parquet:parquet-column:${project.ext["parquet.version"]}")
            implementation("org.apache.parquet:parquet-common:${project.ext["parquet.version"]}")
            implementation("org.apache.parquet:parquet-hadoop:${project.ext["parquet.version"]}")
            implementation("org.apache.ranger:ranger-plugins-common:2.5.0")
            implementation("org.apache.spark:spark-catalyst_2.12:${project.ext["spark.version"]}")
            implementation("org.apache.spark:spark-core_2.12:${project.ext["spark.version"]}")
            implementation("org.apache.spark:spark-launcher_2.12:${project.ext["spark.version"]}")
            implementation("org.apache.spark:spark-sql_2.12:${project.ext["spark.version"]}")
            implementation("org.apache.thrift:libthrift:0.20.0")
            implementation("org.apache.velocity:velocity-engine-core:2.4.1")
            implementation("org.eclipse.jetty:jetty-client:${project.ext["jetty.version"]}")
            implementation("org.eclipse.jetty:jetty-io:${project.ext["jetty.version"]}")
            implementation("org.eclipse.jetty:jetty-security:${project.ext["jetty.version"]}")
            implementation("org.eclipse.jetty:jetty-server:${project.ext["jetty.version"]}")
            implementation("org.eclipse.jetty:jetty-servlet:${project.ext["jetty.version"]}")
            implementation("org.eclipse.jetty:jetty-util:${project.ext["jetty.version"]}")
            implementation("org.eclipse.jetty:jetty-util-ajax:${project.ext["jetty.version"]}")
            implementation("org.eclipse.jetty:jetty-webapp:${project.ext["jetty.version"]}")
            implementation("org.jboss.byteman:byteman:${project.ext["byteman.version"]}")
            implementation("org.jboss.xnio:xnio-nio:3.8.16.Final")
            implementation("org.jdom:jdom2:2.0.6.1")
            implementation("org.json:json:20231013")
            implementation("org.junit.jupiter:junit-jupiter:5.8.2")
            implementation("org.mariadb.jdbc:mariadb-java-client:3.3.2")
            implementation("org.owasp.encoder:encoder:1.3.1")
            implementation("org.postgresql:postgresql:42.4.4")
            implementation("org.roaringbitmap:RoaringBitmap:0.8.13")
            implementation("org.scala-lang:scala-library:2.12.10")
            implementation("org.slf4j:slf4j-api:1.7.30")
            implementation("org.xerial.snappy:snappy-java:1.1.10.5")
            implementation("software.amazon.awssdk:bundle:${project.ext["aws-v2-sdk.version"]}")
            implementation("tools.profiler:async-profiler:${project.ext["async-profiler.version"]}")
            // dependency sync end
        }
    }

    tasks.withType<JavaCompile> {
        options.encoding = "UTF-8"
    }

    publishing {
        publications {
            create<MavenPublication>("maven") {
                from(components["java"])
            }
        }
    }
}
