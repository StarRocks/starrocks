---
displayed_sidebar: docs
sidebar_label: "Feature Support"
---

# Feature Support: Data Loading and Unloading

This document outlines the features of various data loading and unloading methods supported by StarRocks.

## File format

### Loading file formats

<table align="center">
    <tr>
        <th rowspan="2"></th>
        <th rowspan="2">Data Source</th>
        <th colspan="7">File Format</th>
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
        <td>Local file systems, applications, connectors</td>
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
        <td colspan="8">Consistent with INSERT from FILES</td>
    </tr>
</table>

:::note

[1], [2]\: Schema Registry is required.

[3]\: JSON supports a variety of CDC formats. For details about the JSON CDC formats supported by StarRocks, see [JSON CDC format](#json-cdc-formats).

[4]\: Currently, only INSERT from FILES is supported for loading with PIPE.

:::

#### JSON CDC formats

<table align="center">
    <tr>
        <th></th>
        <th>Stream Load</th>
        <th>Routine Load</th>
        <th>Broker Load</th>
        <th>INSERT from FILES</th>
        <th>Kafka Connector [1]</th>
    </tr>
    <tr>
        <td>Debezium</td>
        <td>/</td>
        <td>/</td>
        <td>/</td>
        <td>/</td>
        <td>Yes (v3.0+)</td>
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

[1]\: You must configure the `transforms` parameter while loading Debezium CDC format data into Primary Key tables in StarRocks.

:::

### Unloading file formats

<table align="center">
    <tr>
        <th rowspan="2"></th>
        <th colspan="2">Target</th>
        <th colspan="4">File format</th>
    </tr>
    <tr>
        <th>Table format</th>
        <th>Remote storage</th>
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

[1]\: Configuring Broker process is supported.

[2]\: Currently, unloading data using PIPE is not supported.

:::

## File format-related parameters

### Loading file format-related parameters

<table align="center">
    <tr>
        <th rowspan="2">File format</th>
        <th rowspan="2">Parameter</th>
        <th colspan="5">Loading method</th>
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

[1]\: The corresponding parameter is `COLUMNS TERMINATED BY`.

[2]\: The corresponding parameter is `ROWS TERMINATED BY`.

[3]\: The corresponding parameter is `ROWS TERMINATED BY`.

:::

### Unloading file format-related parameters

<table align="center">
    <tr>
        <th rowspan="2">File format</th>
        <th rowspan="2">Parameter</th>
        <th colspan="2">Unloading method</th>
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

[1]\: The corresponding parameter in data loading is `row_delimiter`.

:::

## Compression formats

### Loading compression formats

<table align="center">
    <tr>
        <th rowspan="2">File format</th>
        <th rowspan="2">Compression format</th>
        <th colspan="5">Loading method</th>
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

[1]\: Currently, only when loading CSV files with Stream Load can you specify the compression format by using `format=gzip`, indicating gzip-compressed CSV files. `deflate` and `bzip2` formats are also supported.

[2]\: Broker Load does not support specifying the compression format of CSV files by using the parameter `format`. Broker Load identifies the compression format by using the suffix of the file. The suffix of gzip-compressed files is `.gz`, and that of the zstd-compressed files is `.zst`. Besides, other `format`-related parameters, such as `trim_space` and `enclose`, are also not supported.

[3]\: Supports specifying the compression format by using `compression = gzip`.

[4]\: Supported by Arrow Library. You do not need to configure the `compression` parameter.

:::

### Unloading compression formats

<table align="center">
    <tr>
        <th rowspan="3">File format</th>
        <th rowspan="3">Compression format</th>
        <th colspan="5">Unloading method</th>
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

## Credentials

### Loading - Authentication

<table align="center">
    <tr>
        <th rowspan="2">Authentication</th>
        <th colspan="5">Loading method</th>
    </tr>
    <tr>
        <th>Stream Load</th>
        <th>INSERT from FILES</th>
        <th>Broker Load</th>
        <th>Routine Load</th>
        <th>External Catalog</th>
    </tr>
    <tr>
        <td>Single Kerberos</td>
        <td>-</td>
        <td>Yes (v3.1+)</td>
        <td>Yes [1] (versions earlier than v2.5)</td>
        <td>Yes [2] (v3.1.4+)</td>
        <td>Yes</td>
    </tr>
    <tr>
        <td>Kerberos Ticket Granting Ticket (TGT)</td>
        <td>-</td>
        <td rowspan="2" colspan="3">/</td>
        <td rowspan="2">Yes (v3.1.10+/v3.2.1+)</td>
    </tr>
    <tr>
        <td>Single KDC Multiple Kerberos</td>
        <td>-</td>
    </tr>
    <tr>
        <td>Basic access authentications (Access Key pair, IAM Role)</td>
        <td>-</td>
        <td colspan="2">Yes (HDFS and S3-compatible object storage)</td>
        <td>Yes [3]</td>
        <td>Yes</td>
    </tr>
</table>

:::note

[1]\: For HDFS, StarRocks supports both simple authentication and Kerberos authentication.

[2]\: When the security protocol is set to `sasl_plaintext` or `sasl_ssl`, both SASL and GSSAPI (Kerberos) authentications are supported.

[3]\: When the security protocol is set to `sasl_plaintext` or `sasl_ssl`, both SASL and PLAIN authentications are supported.

:::

### Unloading - Authentication

|                 | INSERT INTO FILES | EXPORT |
| :-------------- | :----------------: | :-----: |
| Single Kerberos | /                 | /      |

## Loading - Other parameters and features

<table align="center">
    <tr>
        <th rowspan="2">Parameter and feature</th>
        <th colspan="8">Loading method</th>
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
        <td>timezone or session variable time_zone [2]</td>
        <td>Yes [3]</td>
        <td>Yes [4]</td>
        <td>Yes [4]</td>
        <td>Yes [4]</td>
        <td>/</td>
        <td>Yes [4]</td>
        <td>/</td>
    </tr>
    <tr>
        <td>Time accuracy - Microsecond</td>
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

[1]\: From v3.3 onwards, StarRocks supports Partial Updates in Row mode for INSERT INTO by specifying the column list.

[2]\: Setting the time zone by the parameter or the session variable will affect the results returned by functions such as strftime(), alignment_timestamp(), and from_unixtime().

[3]\: Only the parameter `timezone` is supported.

[4]\: Only the session variable `time_zone` is supported.

:::

## Unloading - Other parameters and features

<table align="center">
    <tr>
        <th>Parameter and feature</th>
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
