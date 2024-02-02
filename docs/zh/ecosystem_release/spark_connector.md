---
displayed_sidebar: "Chinese"
---

# Spark connector 版本发布

## 发布说明

**使用文档：**

- [使用 Spark connector 导入数据至 StarRocks](../loading/Spark-connector-starrocks.md)
- [使用 Spark connector 从 StarRocks 读取数据](../unloading/Spark_connector.md)

**源码下载地址：**[starrocks-connector-for-apache-spark](https://github.com/StarRocks/starrocks-connector-for-apache-spark)

**JAR 包命名规则：**`starrocks-spark-connector-${spark_version}_${scala_version}-${connector_version}.jar`

**JAR 包获取方式:**

您可以通过以下方式获取 Spark connector 的 JAR 包：

- 从 [Maven Central Repository](https://repo1.maven.org/maven2/com/starrocks) 直接下载编译好的 JAR 包。
- 在 Maven 项目的 pom 文件添加 Spark connector 为依赖项，作为依赖下载。具体方式，参见[使用文档](../loading/Spark-connector-starrocks.md)。
- 使用源码手动编译成 JAR 包。具体方式，参见[使用文档](../loading/Spark-connector-starrocks.md)。

**版本要求：**

| Connector | Spark         | StarRocks  | Java | Scala |
| --------- | ------------- | ---------- | ---- | ----- |
| 1.1.1     | 3.2, 3.3, 3.4 | 2.5 及以上 | 8    | 2.12  |
| 1.1.0     | 3.2, 3.3, 3.4 | 2.5 及以上 | 8    | 2.12  |

## 发布记录

### 1.1

### 1.1.2

**新增特性**

- 支持 Spark 的版本为 3.5。[#89](https://github.com/StarRocks/starrocks-connector-for-apache-spark/pull/89)
- 通过 Spark SQL 读取 StarRocks 时支持 `starrocks.filter.query `参数。[#92](https://github.com/StarRocks/starrocks-connector-for-apache-spark/pull/92)
- 支持读取 StarRocks 中 JSON 类型的列。[#100](https://github.com/StarRocks/starrocks-connector-for-apache-spark/pull/100)

**功能优化**

- 优化报错信息。读取 StarRocks 时如果在 `starrocks.columns` 指定 StarRocks 表中不存在的列，则报错信息会明确提示不存在的列名。[#97](https://github.com/StarRocks/starrocks-connector-for-apache-spark/pull/97)
- Spark connector 通过 HTTP 向 StarRocks 的 FE 请求查询计划时出现异常的时候，FE 会把异常信息通过 HTTP 本身的 status 和 entity 返回给 Spark connector。[#98](https://github.com/StarRocks/starrocks-connector-for-apache-spark/pull/98)

**测试**

新增单元测试，验证 spark connector 读取数据时获取不到输出列列名的问题是否已经成功修复。[#99](https://github.com/StarRocks/starrocks-connector-for-apache-spark/pull/99)

#### 1.1.1

本版本发布主要包括如下新增特性和功能优化，涉及导入数据到 StarRocks。

> **注意**
>
> 升级至本版本，涉及行为变更。详细信息，参见[升级 Spark connector](../loading/Spark-connector-starrocks.md#升级-spark-connector)。

**新增特性**

- Sink 支持重试。[#61](https://github.com/StarRocks/starrocks-connector-for-apache-spark/pull/61)
- 支持将数据导入至 BITMAP 或者 HLL 类型的列。[#67](https://github.com/StarRocks/starrocks-connector-for-apache-spark/pull/67)
- 支持导入 ARRAY 类型的数据。[#74](https://github.com/StarRocks/starrocks-connector-for-apache-spark/pull/74)
- 支持根据缓冲数据行数 flush 数据。[#78](https://github.com/StarRocks/starrocks-connector-for-apache-spark/pull/78)

**功能优化**

- 移除无用的依赖项，使 Spark connector JAR 文件更轻量。[#55](https://github.com/StarRocks/starrocks-connector-for-apache-spark/pull/55) [#57](https://github.com/StarRocks/starrocks-connector-for-apache-spark/pull/57)
- 用 jackson 替换 fastjson。[#58](https://github.com/StarRocks/starrocks-connector-for-apache-spark/pull/58)
- 添加缺失的 Apache license header。[#60](https://github.com/StarRocks/starrocks-connector-for-apache-spark/pull/60)
- Spark connector JAR 文件中不再打包 MySQL JDBC 驱动程序。[#63](https://github.com/StarRocks/starrocks-connector-for-apache-spark/pull/63)
- 兼容 Spark Java 8 API datetime。[#64](https://github.com/StarRocks/starrocks-connector-for-apache-spark/pull/64)
- 优化 row-string 转换器以减少 CPU 成本。[#68](https://github.com/StarRocks/starrocks-connector-for-apache-spark/pull/68)
- 支持在 starrocks.fe.http.url 中添加 HTTP scheme。[#71](https://github.com/StarRocks/starrocks-connector-for-apache-spark/pull/71)
- 实现接口 BatchWrite#useCommitCoordinator 以支持 DataBricks v13.1。 [#79](https://github.com/StarRocks/starrocks-connector-for-apache-spark/pull/79)
- 在错误日志中添加权限和参数检查提示。[#81](https://github.com/StarRocks/starrocks-connector-for-apache-spark/pull/81)

**问题修复**

- 解析 CSV 相关参数 `column_seperator` 和`row_delimiter` 中的转义字符。[#85](https://github.com/StarRocks/starrocks-connector-for-apache-spark/pull/85)

**文档**

- 重构文档。[#66](https://github.com/StarRocks/starrocks-connector-for-apache-spark/pull/66)
- 新增示例说明如何导入至 BITMAP 和 HLL 类型的列。[#70](https://github.com/StarRocks/starrocks-connector-for-apache-spark/pull/70)
- 新增 Python 编写的 Spark 应用程序示例。[#72](https://github.com/StarRocks/starrocks-connector-for-apache-spark/pull/72)
- 新增导入 ARRAY 类型数据的示例。[#75](https://github.com/StarRocks/starrocks-connector-for-apache-spark/pull/75)
- 新增示例说明如何实现主键表的部分更新和条件更新。[#80](https://github.com/StarRocks/starrocks-connector-for-apache-spark/pull/80)

#### 1.1.0

**新增特性**

- 支持导入数据到 StarRocks。

### 1.0

#### 1.0.0

**新增特性**

- 支持从 StarRocks 读取数据。
