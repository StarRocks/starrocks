---
displayed_sidebar: docs
---

# Releases of StarRocks Connector for Flink

## Notifications

**User guide:**

- [Load data into StarRocks using Flink connector](../loading/Flink-connector-starrocks.md)
- [Read data from StarRocks using Flink connector](../unloading/Flink_connector.md)

**Source codes:** [starrocks-connector-for-apache-flink](https://github.com/StarRocks/starrocks-connector-for-apache-flink)

**Naming format of the JAR file:**

- Flink 1.15 and later: `flink-connector-starrocks-${connector_version}_flink-${flink_version}.jar`
- Prior to Flink 1.15: `flink-connector-starrocks-${connector_version}_flink-${flink_version}_${scala_version}.jar`

**Methods to obtain the JAR file:**

- Directly download the the Flink connector JAR file from the [Maven Central Repository](https://repo1.maven.org/maven2/com/starrocks).
- Add the Flink connector as a dependency in your Maven project's `pom.xml` file and download it. For specific instructions, see [user guide](../loading/Flink-connector-starrocks.md#obtain-flink-connector).
- Compile the source codes into Flink connector JAR file. For specific instructions, see [user guide](../loading/Flink-connector-starrocks.md#obtain-flink-connector).

**Version requirements:**

| Connector | Flink                    | StarRocks     | Java | Scala     |
| --------- | ------------------------ | ------------- | ---- | --------- |
| 1.2.9 | 1.15,1.16,1.17,1.18 | 2.1 and later| 8 | 2.11,2.12 |
| 1.2.8     | 1.13,1.14,1.15,1.16,1.17 | 2.1 and later | 8    | 2.11,2.12 |
| 1.2.7     | 1.11,1.12,1.13,1.14,1.15 | 2.1 and later | 8    | 2.11,2.12 |

> **NOTICE**
>
> In general, the latest version of the Flink connector only maintains compatibility with the three most recent versions of Flink.

## Release notes

### 1.2

#### 1.2.9

This release includes some features and bug fixes. The notable change is that the Flink connector is integrated with [Flink CDC 3.0](https://ververica.github.io/flink-cdc-connectors/master/content/overview/cdc-pipeline.html) to easily build a streaming ELT pipeline from CDC sources (such as MySQL and Kafka) to StarRocks. You can see [Flink CDC Synchronization](../loading/Flink-connector-starrocks.md#synchronize-data-with-flink-cdc-30-with-schema-change-supported) for details.

**Features**

- Implement catalog to support Flink CDC 3.0. [#295](https://github.com/StarRocks/starrocks-connector-for-apache-flink/pull/295)
- Implement new sink API in [FLP-191](https://cwiki.apache.org/confluence/display/FLINK/FLIP-191%3A+Extend+unified+Sink+interface+to+support+small+file+compaction) to support Flink CDC 3.0. [#301](https://github.com/StarRocks/starrocks-connector-for-apache-flink/pull/301)
- Support Flink 1.18. [#305](https://github.com/StarRocks/starrocks-connector-for-apache-flink/pull/305)

**Bug Fixes**

- Fix misleading thread name and log. [#290](https://github.com/StarRocks/starrocks-connector-for-apache-flink/pull/290)
- Fix wrong stream-load-sdk configurations used for writing to multiple tables. [#298](https://github.com/StarRocks/starrocks-connector-for-apache-flink/pull/298)

#### 1.2.8

This release includes some improvements and bug fixes. The notable changes are as follows:

- Support Flink 1.16 and 1.17.
- Recommend to set `sink.label-prefix` when the sink is configured to guarantee the exactly-once semantics. For the specific instructions, see [Exactly Once](../loading/Flink-connector-starrocks.md#exactly-once).

**Improvements**

- Support to configure whether to use Stream Load transaction interface to guarantee at-least-once. [#228](https://github.com/StarRocks/starrocks-connector-for-apache-flink/pull/228)
- Add retry metrics for sink V1. [#229](https://github.com/StarRocks/starrocks-connector-for-apache-flink/pull/229)
- No need to getLabelState when EXISTING_JOB_STATUS is FINISHED. [#231](https://github.com/StarRocks/starrocks-connector-for-apache-flink/pull/231)
- Remove useless stack trace log for sink V1. [#232](https://github.com/StarRocks/starrocks-connector-for-apache-flink/pull/232)
- [Refactor] Move StarRocksSinkManagerV2 to stream-load-sdk. [#233](https://github.com/StarRocks/starrocks-connector-for-apache-flink/pull/233)
- Automatically detect partial updates according to a Flink table's schema instead of the `sink.properties.columns` parameter explicitly specified by users. [#235](https://github.com/StarRocks/starrocks-connector-for-apache-flink/pull/235)
- [Refactor] Move probeTransactionStreamLoad to stream-load-sdk. [#240](https://github.com/StarRocks/starrocks-connector-for-apache-flink/pull/240)
- Add git-commit-id-plugin for stream-load-sdk. [#242](https://github.com/StarRocks/starrocks-connector-for-apache-flink/pull/242)
- Use info log for DefaultStreamLoader#close. [#243](https://github.com/StarRocks/starrocks-connector-for-apache-flink/pull/243)
- Support to generate stream-load-sdk JAR file without dependencies. [#245](https://github.com/StarRocks/starrocks-connector-for-apache-flink/pull/245)
- Replace fastjson with jackson in stream-load-sdk. [#247](https://github.com/StarRocks/starrocks-connector-for-apache-flink/pull/247)
- Support to process update_before record. [#250](https://github.com/StarRocks/starrocks-connector-for-apache-flink/pull/250)
- Add the Apache license into files. [#251](https://github.com/StarRocks/starrocks-connector-for-apache-flink/pull/251)
- Support to get the exception in stream-load-sdk. [#252](https://github.com/StarRocks/starrocks-connector-for-apache-flink/pull/252)
- Enable `strip_outer_array` and `ignore_json_size` by default. [#259](https://github.com/StarRocks/starrocks-connector-for-apache-flink/pull/259)
- Try to cleanup lingering transactions when a Flink job restores and the sink semantics is exactly-once. [#271](https://github.com/StarRocks/starrocks-connector-for-apache-flink/pull/271)
- Return the first exception after the retrying fails. [#279](https://github.com/StarRocks/starrocks-connector-for-apache-flink/pull/279)

**Bug Fixes**

- Fix typos in StarRocksStreamLoadVisitor. [#230](https://github.com/StarRocks/starrocks-connector-for-apache-flink/pull/230)
- Fix the fastjson classloader leak. [#260](https://github.com/StarRocks/starrocks-connector-for-apache-flink/pull/260)

**Tests**

- Add the test framework for loading from Kafka to StarRocks. [#249](https://github.com/StarRocks/starrocks-connector-for-apache-flink/pull/249)

**Doc**

- Refactor the docs. [#262](https://github.com/StarRocks/starrocks-connector-for-apache-flink/pull/262)
- Improve the doc for the sink. [#268](https://github.com/StarRocks/starrocks-connector-for-apache-flink/pull/268) [#275](https://github.com/StarRocks/starrocks-connector-for-apache-flink/pull/275)
- Add examples of DataStream API for the sink. [#253](https://github.com/StarRocks/starrocks-connector-for-apache-flink/pull/253)
