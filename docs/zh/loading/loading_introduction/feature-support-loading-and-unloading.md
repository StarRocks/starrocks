---
displayed_sidebar: docs
sidebar_label: "能力边界"
---

# 功能边界：数据导入导出

本文介绍了 StarRocks 导入导出的能力边界以及所涉及功能的支持版本。

## 文件格式

### 导入文件格式

<table align="center">
    <tr>
        <th rowspan="2"></th>
        <th rowspan="2">数据源</th>
        <th colspan="7">文件格式</th>
    </tr>
    <tr>
        <th>CSV</th>
        <th>JSON [3]</th>
        <th>Parquet</th>
        <th>ORC</th>
        <th>Avro</th>
        <th>ProtoBuf</th>
        <th>Thrift</th>
    </tr>
    <tr>
        <td>Stream Load</td>
        <td>本地文件系统、应用及 Connector</td>
        <td>Yes</td>
        <td>Yes</td>
        <td>/</td>
        <td>/</td>
        <td colspan="3">/</td>
    </tr>
    <tr>
        <td>INSERT from FILES</td>
        <td rowspan="2">HDFS, S3, OSS, Azure, GCS</td>
        <td>Yes (v3.3+)</td>
        <td>/</td>
        <td>Yes (v3.1+)</td>
        <td>Yes (v3.1+)</td>
        <td colspan="3">/</td>
    </tr>
    <tr>
        <td>Broker Load</td>
        <td>Yes</td>
        <td>Yes (v3.2.3+)</td>
        <td>Yes</td>
        <td>Yes</td>
        <td colspan="3">/</td>
    </tr>
    <tr>
        <td>Routine Load</td>
        <td>Kafka</td>
        <td>Yes</td>
        <td>Yes</td>
        <td>/</td>
        <td>/</td>
        <td>Yes (v3.0+) [1]</td>
        <td>/</td>
        <td>/</td>
    </tr>
    <tr>
        <td>Spark Load</td>
        <td></td>
        <td>Yes</td>
        <td>/</td>
        <td>Yes</td>
        <td>Yes</td>
        <td colspan="3">/</td>
    </tr>
    <tr>
        <td>Connectors</td>
        <td>Flink, Spark</td>
        <td>Yes</td>
        <td>Yes</td>
        <td>/</td>
        <td>/</td>
        <td colspan="3">/</td>
    </tr>
    <tr>
        <td>Kafka Connector [2]</td>
        <td>Kafka</td>
        <td colspan="2">Yes (v3.0+)</td>
        <td>/</td>
        <td>/</td>
        <td colspan="2">Yes (v3.0+)</td>
        <td>/</td>
    </tr>
    <tr>
        <td>PIPE [4]</td>
        <td colspan="8">同 INSERT from FILES</td>
    </tr>
</table>

:::note

[1], [2]：需要依赖 Schema Registry。

[3]：JSON 支持多种 CDC 格式。有关 StarRocks 支持的 JSON CDC 格式的详细信息，参见 [JSON CDC 格式](#json-cdc-格式)。

[4]：PIPE 当前只支持 INSERT from FILES。

:::

#### JSON CDC 格式

<table align="center">
    <tr>
        <th></th>
        <th>Stream Load</th>
        <th>Routine Load</th>
        <th>Broker Load</th>
        <th>INSERT from FILES</th>
        <th>Kafka Connector</th>
    </tr>
    <tr>
        <td>Debezium</td>
        <td>将在 v3.4 版本中支持</td>
        <td>将在 v3.4 版本中支持</td>
        <td>/</td>
        <td>/</td>
        <td>Yes (v3.0+) [1]</td>
    </tr>
    <tr>
        <td>Canal</td>
        <td colspan="5" rowspan="2">/</td>
    </tr>
    <tr>
        <td>Maxwell</td>
    </tr>
</table>

:::note

[1]：在将 Debezium CDC 格式数据导入到 StarRocks 主键表时，必须配置 `transforms` 参数。

:::

### 导出文件格式

<table align="center">
    <tr>
        <th rowspan="2"></th>
        <th colspan="2">导出目标</th>
        <th colspan="4">文件格式</th>
    </tr>
    <tr>
        <th>表格式</th>
        <th>远端存储</th>
        <th>CSV</th>
        <th>JSON</th>
        <th>Parquet</th>
        <th>ORC</th>
    </tr>
    <tr>
        <td>INSERT INTO FILES</td>
        <td>-</td>
        <td>HDFS, S3, OSS, Azure, GCS</td>
        <td>Yes (v3.3+)</td>
        <td>/</td>
        <td>Yes (v3.2+)</td>
        <td>Yes (v3.3+)</td>
    </tr>
    <tr>
        <td rowspan="3">INSERT INTO Catalog</td>
        <td>Hive</td>
        <td>HDFS, S3, OSS, Azure, GCS</td>
        <td>Yes (v3.3+)</td>
        <td>/</td>
        <td>Yes (v3.2+)</td>
        <td>Yes (v3.3+)</td>
    </tr>
    <tr>
        <td>Iceberg</td>
        <td>HDFS, S3, OSS, Azure, GCS</td>
        <td>/</td>
        <td>/</td>
        <td>Yes (v3.2+)</td>
        <td>/</td>
    </tr>
    <tr>
        <td>Hudi/Delta</td>
        <td></td>
        <td colspan="4">/</td>
    </tr>
    <tr>
        <td>EXPORT</td>
        <td>-</td>
        <td>HDFS, S3, OSS, Azure, GCS</td>
        <td>Yes [1]</td>
        <td>/</td>
        <td>/</td>
        <td>/</td>
    </tr>
    <tr>
        <td>PIPE</td>
        <td colspan="6">/ [2]</td>
    </tr>
</table>

:::note

[1]：支持配置 Broker 进程。

[2]：目前，不支持使用 PIPE 导出数据。

:::

## 文件格式相关参数

### 导入文件格式相关参数

<table align="center">
    <tr>
        <th rowspan="2">文件格式</th>
        <th rowspan="2">参数</th>
        <th colspan="5">导入方式</th>
    </tr>
    <tr>
        <th>Stream Load</th>
        <th>INSERT from FILES</th>
        <th>Broker Load</th>
        <th>Routine Load</th>
        <th>Spark Load</th>
    </tr>
    <tr>
        <td rowspan="6">CSV</td>
        <td>column_separator</td>
        <td>Yes</td>
        <td rowspan="6">Yes (v3.3+)</td>
        <td colspan="3">Yes [1]</td>
    </tr>
    <tr>
        <td>row_delimiter</td>
        <td>Yes</td>
        <td>Yes [2] (v3.1+)</td>
        <td>Yes [3] (v2.2+)</td>
        <td>/</td>
    </tr>
    <tr>
        <td>enclose</td>
        <td rowspan="4">Yes (v3.0+)</td>
        <td rowspan="4">Yes (v3.0+)</td>
        <td rowspan="2">Yes (v3.0+)</td>
        <td rowspan="4">/</td>
    </tr>
    <tr>
        <td>escape</td>
    </tr>
    <tr>
        <td>skip_header</td>
        <td>/</td>
    </tr>
    <tr>
        <td>trim_space</td>
        <td>Yes (v3.0+)</td>
    </tr>
    <tr>
        <td rowspan="4">JSON</td>
        <td>jsonpaths</td>
        <td rowspan="4">Yes</td>
        <td rowspan="4">/</td>
        <td rowspan="4">Yes (v3.2.3+)</td>
        <td rowspan="3">Yes</td>
        <td rowspan="4">/</td>
    </tr>
    <tr>
        <td>strip_outer_array</td>
    </tr>
    <tr>
        <td>json_root</td>
    </tr>
    <tr>
        <td>ignore_json_size</td>
        <td>/</td>
    </tr>
</table>

:::note

[1]：对应的参数是 COLUMNS TERMINATED BY。

[2]：对应的参数是 ROWS TERMINATED BY。

[3]：对应的参数是 ROWS TERMINATED BY。

:::

### 导出文件格式相关参数

<table align="center">
    <tr>
        <th rowspan="2">文件格式</th>
        <th rowspan="2">参数</th>
        <th colspan="2">导出方式</th>
    </tr>
    <tr>
        <th>INSERT INTO FILES</th>
        <th>EXPORT</th>
    </tr>
    <tr>
        <td rowspan="2">CSV</td>
        <td>column_separator</td>
        <td rowspan="2">Yes (v3.3+)</td>
        <td rowspan="2">Yes</td>
    </tr>
    <tr>
        <td>line_delimiter [1]</td>
    </tr>
</table>

:::note

[1]：数据导入中对应的参数是 `row_delimiter`.

:::

## 压缩格式

### 导入压缩格式

<table align="center">
    <tr>
        <th rowspan="2">文件格式</th>
        <th rowspan="2">压缩格式</th>
        <th colspan="5">导入方式</th>
    </tr>
    <tr>
        <th>Stream Load</th>
        <th>Broker Load</th>
        <th>INSERT from FILES</th>
        <th>Routine Load</th>
        <th>Spark Load</th>
    </tr>
    <tr>
        <td>CSV</td>
        <td rowspan="2">
            <ul>
                <li>defalte</li>
                <li>bzip2</li>
                <li>gzip</li>
                <li>lz4_frame</li>
                <li>zstd</li>
            </ul>
        </td>
        <td>Yes [1]</td>
        <td>Yes [2]</td>
        <td>/</td>
        <td>/</td>
        <td>/</td>
    </tr>
    <tr>
        <td>JSON</td>
        <td>Yes (v3.2.7+) [3]</td>
        <td>/</td>
        <td>-</td>
        <td>/</td>
        <td>-</td>
    </tr>
    <tr>
        <td>Parquet</td>
        <td rowspan="2">
            <ul>
                <li>gzip</li>
                <li>lz4</li>
                <li>snappy</li>
                <li>zlib</li>
                <li>zstd</li>
            </ul>
        </td>
        <td rowspan="2">-</td>
        <td rowspan="2" colspan="2">Yes [4]</td>
        <td rowspan="2">/</td>
        <td rowspan="2">Yes [4]</td>
    </tr>
    <tr>
        <td>ORC</td>
    </tr>
</table>

:::note

[1]：目前，仅在使用 Stream Load 导入 CSV 文件时，支持通过 `format=gzip` 的方式指定压缩格式。除 gzip 外，还支持 deflate 和 bzip2 格式。

[2]：Broker Load 不支持通过 `format` 参数指定 CSV 文件的压缩格式。Broker Load 通过文件的后缀识别压缩格式。gzip 压缩文件的后缀是 `.gz`，zstd 压缩文件的后缀是 `.zst`。此外，Broker Load 也不支持其他格式相关参数，如 `trim_space` 和 `enclose`。

[3]：支持通过配置 `compression = gzip` 的方式指定压缩格式。

[4]：由 Arrow Library 提供支持。无需配置压缩参数。

:::

### 导出压缩格式

<table align="center">
    <tr>
        <th rowspan="3">文件格式</th>
        <th rowspan="3">压缩格式</th>
        <th colspan="5">导出方式</th>
    </tr>
    <tr>
        <th rowspan="2">INSERT INTO FILES</th>
        <th colspan="3">INSERT INTO Catalog</th>
        <th rowspan="2">EXPORT</th>
    </tr>
    <tr>
        <td>Hive</td>
        <td>Iceberg</td>
        <td>Hudi/Delta</td>
    </tr>
    <tr>
        <td>CSV</td>
        <td>
            <ul>
                <li>defalte</li>
                <li>bzip2</li>
                <li>gzip</li>
                <li>lz4_frame</li>
                <li>zstd</li>
            </ul>
        </td>
        <td>/</td>
        <td>/</td>
        <td>/</td>
        <td>/</td>
        <td>/</td>
    </tr>
    <tr>
        <td>JSON</td>
        <td>-</td>
        <td>-</td>
        <td>-</td>
        <td>-</td>
        <td>-</td>
        <td>-</td>
    </tr>
    <tr>
        <td>Parquet</td>
        <td rowspan="2">
            <ul>
                <li>gzip</li>
                <li>lz4</li>
                <li>snappy</li>
                <li>zstd</li>
            </ul>
        </td>
        <td rowspan="2">Yes (v3.2+)</td>
        <td rowspan="2">Yes (v3.2+)</td>
        <td rowspan="2">Yes (v3.2+)</td>
        <td rowspan="2">/</td>
        <td rowspan="2">-</td>
    </tr>
    <tr>
        <td>ORC</td>
    </tr>
</table>

## 认证

### 导入认证

<table align="center">
    <tr>
        <th rowspan="2">认证功能</th>
        <th colspan="5">导入方式</th>
    </tr>
    <tr>
        <th>Stream Load</th>
        <th>INSERT from FILES</th>
        <th>Broker Load</th>
        <th>Routine Load</th>
        <th>External Catalog</th>
    </tr>
    <tr>
        <td>单 Kerberos</td>
        <td>-</td>
        <td>Yes (v3.1+)</td>
        <td>Yes [1] (versions earlier than v2.5)</td>
        <td>Yes [2] (v3.1.4+)</td>
        <td>Yes</td>
    </tr>
    <tr>
        <td>Kerberos 自动续签</td>
        <td>-</td>
        <td rowspan="2" colspan="3">/</td>
        <td rowspan="2">Yes (v3.1.10+/v3.2.1+)</td>
    </tr>
    <tr>
        <td>单 KDC 多 Kerberos</td>
        <td>-</td>
    </tr>
    <tr>
        <td>用户名密码认证 (Access Key pair, IAM Role)</td>
        <td>-</td>
        <td colspan="2">Yes (HDFS 和兼容 S3 的对象存储)</td>
        <td>Yes [3]</td>
        <td>Yes</td>
    </tr>
</table>

:::note

[1]：对于HDFS，StarRocks 支持简单认证和 Kerberos 认证。

[2]：当安全协议设置为 `sasl_plaintext` 或 `sasl_ssl` 时，支持 SASL 和 GSSAPI（Kerberos）认证。

[3]：当安全协议设置为 `sasl_plaintext` 或 `sasl_ssl` 时，支持 SASL 和 PLAIN 认证。

:::

### 导出认证

|             | INSERT INTO FILES  | EXPORT  |
| :---------- | :----------------: | :-----: |
| 单 Kerberos | /                 | /      |

## 导入相关其他参数或功能

<table align="center">
    <tr>
        <th rowspan="2">参数和功能</th>
        <th colspan="8">导入方式</th>
    </tr>
    <tr>
        <th>Stream Load</th>
        <th>INSERT from FILES</th>
        <th>INSERT from SELECT/VALUES</th>
        <th>Broker Load</th>
        <th>PIPE</th>
        <th>Routine Load</th>
        <th>Spark Load</th>
    </tr>
    <tr>
        <td>partial_update</td>
        <td>Yes (v3.0+)</td>
        <td colspan="2">Yes [1] (v3.3+)</td>
        <td>Yes (v3.0+)</td>
        <td>-</td>
        <td>Yes (v3.0+)</td>
        <td>/</td>
    </tr>
    <tr>
        <td>partial_update_mode</td>
        <td>Yes (v3.1+)</td>
        <td colspan="2">/</td>
        <td>Yes (v3.1+)</td>
        <td>-</td>
        <td>/</td>
        <td>/</td>
    </tr>
    <tr>
        <td>COLUMNS FROM PATH</td>
        <td>-</td>
        <td>Yes (v3.2+)</td>
        <td>-</td>
        <td>Yes</td>
        <td>-</td>
        <td>-</td>
        <td>Yes</td>
    </tr>
    <tr>
        <td>timezone 或 Session 变量 time_zone [2]</td>
        <td>Yes [3]</td>
        <td>Yes [4]</td>
        <td>Yes [4]</td>
        <td>Yes [4]</td>
        <td>/</td>
        <td>Yes [4]</td>
        <td>/</td>
    </tr>
    <tr>
        <td>时间精度 - Microsecond</td>
        <td>Yes</td>
        <td>Yes</td>
        <td>Yes</td>
        <td>Yes (v3.1.11+/v3.2.6+)</td>
        <td>/</td>
        <td>Yes</td>
        <td>Yes</td>
    </tr>
</table>

:::note

[1]：从 v3.3 版本开始，StarRocks 支持在 INSERT INTO 操作中通过指定 Column List 进行部分更新。

[2]：通过参数或 Session 变量设置时区会影响 `strftime()`、`alignment_timestamp()` 和 `from_unixtime()` 等函数返回的结果。

[3]：仅支持参数 `timezone`。

[4]：仅支持 Session 变量 `time_zone`。

:::

## 导出相关其他参数或功能

<table align="center">
    <tr>
        <th>参数和功能</th>
        <th>INSERT INTO FILES</th>
        <th>EXPORT</th>
    </tr>
    <tr>
        <td>target_max_file_size</td>
        <td rowspan="3">Yes (v3.2+)</td>
        <td rowspan="4">/</td>
    </tr>
    <tr>
        <td>single</td>
    </tr>
    <tr>
        <td>Partitioned_by</td>
    </tr>
    <tr>
        <td>Session variable time_zone</td>
        <td>/</td>
    </tr>
    <tr>
        <td>Time accuracy - Microsecond</td>
        <td>/</td>
        <td>/</td>
    </tr>
</table>

