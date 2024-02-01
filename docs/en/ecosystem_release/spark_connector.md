---
displayed_sidebar: "English"
---

# Releases of StarRocks Connector for Spark

## Notifications

**User guide:**

- [Load data into StarRocks using Spark connector](../loading/Spark-connector-starrocks.md)
- [Read data from StarRocks using Spark connector](../unloading/Spark_connector.md)

**Source codes**: [starrocks-connector-for-apache-spark](https://github.com/StarRocks/starrocks-connector-for-apache-spark)

**Naming format of the JAR file**: `starrocks-spark-connector-${spark_version}_${scala_version}-${connector_version}.jar`

**Methods to obtain the JAR file:**

- Directly download the Spark connector JAR file from the [Maven Central Repository](https://repo1.maven.org/maven2/com/starrocks).
- Add the Spark connector as a dependency in your Maven project's `pom.xml` file and download it. For specific instructions, see [user guide](../loading/Spark-connector-starrocks.md#obtain-spark-connector).
- Compile the source codes into Spark connector JAR file. For specific instructions, see [user guide](../loading/Spark-connector-starrocks.md#obtain-spark-connector).

**Version requirements:**

| Spark connector | Spark            | StarRocks     | Java | Scala |
| --------------- | ---------------- | ------------- | ---- | ----- |
| 1.1.1           | 3.2, 3.3, or 3.4 | 2.5 and later | 8    | 2.12  |
| 1.1.0           | 3.2, 3.3, or 3.4 | 2.5 and later | 8    | 2.12  |

## Release notes

### 1.1

#### 1.1.1

This release mainly includes some features and improvements for loading data to StarRocks.

> **NOTICE**
>
> Take note of the some changes when you upgrade the Spark connector to this version. For details, see [Upgrade Spark connector](../loading/Spark-connector-starrocks.md#upgrade-from-version-110-to-111).

**Features**

- The sink supports retrying. [#61](https://github.com/StarRocks/starrocks-connector-for-apache-spark/pull/61)
- Support to load data to BITMAP and HLL columns. [#67](https://github.com/StarRocks/starrocks-connector-for-apache-spark/pull/67)
- Support to load ARRAY-type data. [#74](https://github.com/StarRocks/starrocks-connector-for-apache-spark/pull/74)
- Support to flush according to the number of buffered rows. [#78](https://github.com/StarRocks/starrocks-connector-for-apache-spark/pull/78)

**Improvements**

- Remove useless dependency, and make the Spark connector JAR file lightweight. [#55](https://github.com/StarRocks/starrocks-connector-for-apache-spark/pull/55) [#57](https://github.com/StarRocks/starrocks-connector-for-apache-spark/pull/57)
- Replace fastjson with jackson. [#58](https://github.com/StarRocks/starrocks-connector-for-apache-spark/pull/58)
- Add the missing Apache license header. [#60](https://github.com/StarRocks/starrocks-connector-for-apache-spark/pull/60)
- Do not package the MySQL JDBC driver in the Spark connector JAR file. [#63](https://github.com/StarRocks/starrocks-connector-for-apache-spark/pull/63)
- Support to configure timezone parameter and become compatible with Spark Java8 API datetime. [#64](https://github.com/StarRocks/starrocks-connector-for-apache-spark/pull/64)
- Optimize row-string converter to reduce CPU costs. [#68](https://github.com/StarRocks/starrocks-connector-for-apache-spark/pull/68)
- The `starrocks.fe.http.url` parameter supports to add a http scheme. [#71](https://github.com/StarRocks/starrocks-connector-for-apache-spark/pull/71)
- The interface BatchWrite#useCommitCoordinator is implemented to run on DataBricks 13.1 [#79](https://github.com/StarRocks/starrocks-connector-for-apache-spark/pull/79)
- Add the hint of checking the privileges and parameters in the error log. [#81](https://github.com/StarRocks/starrocks-connector-for-apache-spark/pull/81)

**Bug fixes**

- Parse escape characters in the CSV related parameters  `column_seperator` and `row_delimiter`. [#85](https://github.com/StarRocks/starrocks-connector-for-apache-spark/pull/85)

**Doc**

- Refactor the docs. [#66](https://github.com/StarRocks/starrocks-connector-for-apache-spark/pull/66)
- Add examples of load data to BITMAP and HLL columns. [#70](https://github.com/StarRocks/starrocks-connector-for-apache-spark/pull/70)
- Add examples of Spark applications written in Python. [#72](https://github.com/StarRocks/starrocks-connector-for-apache-spark/pull/72)
- Add examples of loading ARRAY-type data. [#75](https://github.com/StarRocks/starrocks-connector-for-apache-spark/pull/75)
- Add examples for performing partial updates and conditional updates on Primary Key tables. [#80](https://github.com/StarRocks/starrocks-connector-for-apache-spark/pull/80)

**1.1.0**

**Features**

- Support to load data into StarRocks.

### 1.0

**Features**

- Support to unload data from StarRocks.
