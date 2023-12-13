---
displayed_sidebar: "English"
---

# BINARY/VARBINARY

## Description

BINARY(M)

VARBINARY(M)

Since v3.0, StarRocks supports the BINARY/VARBINARY data type, which is used to store binary data. The maximum supported length is the same as VARCHAR (1~1048576). The unit is byte. If `M` is not specified, 1048576 is used by default. Binary data types contain byte strings while character data types contain character strings.

BINARY is an alias of VARBINARY. The usage is the same as VARBINARY.

## Limits and usage notes

- VARBINARY columns are supported in Duplicate Key, Primary Key, and Unique Key tables. They are not supported in Aggregate tables.

- VARBINARY columns cannot be used as partition keys, bucketing keys, or dimension columns of Duplicate Key, Primary Key, and Unique Key tables. They cannot be used in ORDER BY, GROUP BY, and JOIN clauses.

- BINARY(M)/VARBINARY(M) are not right-padded in the case of unaligned length.

## Examples

### Create a column of VARBINARY type

When creating a table, use the keyword `VARBINARY` to specify column `j` as a VARBINARY column.

```SQL
CREATE TABLE `test_binary` (
    `id` INT(11) NOT NULL COMMENT "",
    `j`  VARBINARY NULL COMMENT ""
) ENGINE=OLAP
DUPLICATE KEY(`id`)
COMMENT "OLAP"
DISTRIBUTED BY HASH(`id`)
PROPERTIES (
    "replication_num" = "3",
    "storage_format" = "DEFAULT"
);

mysql> DESC test_binary;
+-------+-----------+------+-------+---------+-------+
| Field | Type      | Null | Key   | Default | Extra |
+-------+-----------+------+-------+---------+-------+
| id    | int       | NO   | true  | NULL    |       |
| j     | varbinary | YES  | false | NULL    |       |
+-------+-----------+------+-------+---------+-------+
2 rows in set (0.01 sec)

```

### Load data and store it as BINARY type

StarRocks supports the following ways to load data and store it as BINARY type.

- Method 1: Use INSERT INTO to write data to a constant column of BINARY type (such as column `j`), where the constant column is prefixed with `x''`.

    ```SQL
    INSERT INTO test_binary (id, j) VALUES (1, x'abab');
    INSERT INTO test_binary (id, j) VALUES (2, x'baba');
    INSERT INTO test_binary (id, j) VALUES (3, x'010102');
    INSERT INTO test_binary (id, j) VALUES (4, x'0000'); 
    ```

- Method 2: Use the [to_binary](../../sql-functions/binary-functions/to_binary.md) function to convert VARCHAR data to binary data.

    ```SQL
    INSERT INTO test_binary select 5, to_binary('abab', 'hex');
    INSERT INTO test_binary select 6, to_binary('abab', 'base64');
    INSERT INTO test_binary select 7, to_binary('abab', 'utf8');
    ```

- Method 3: Use Broker Load to load a Parquet or ORC file and store the file as BINARY data. For more information, see [Broker Load](../data-manipulation/BROKER_LOAD.md).

  - For Parquet files, convert `parquet::Type::type::BYTE_ARRAY` to `TYPE_VARBINARY` directly.
  - For ORC files, convert `orc::BINARY` to `TYPE_VARBINARY` directly.

- Method 4: Use Stream Load to load a CSV file and store the file as `BINARY` data. For more information, see [Load CSV data](../../../loading/StreamLoad.md#load-csv-data).
  - CSV file uses the hex format for binary data. Please ensure the input binary value is a valid hex value.
  - `BINARY` type is only supported in CSV file. JSON file does not support `BINARY` type.

  For example, `t1` is a table with a VARBINARY column `b`.

    ```sql
    CREATE TABLE `t1` (
    `k` int(11) NOT NULL COMMENT "",
    `v` int(11) NOT NULL COMMENT "",
    `b` varbinary
    ) ENGINE = OLAP
    DUPLICATE KEY(`k`)
    PARTITION BY RANGE(`v`) (
    PARTITION p1 VALUES [("-2147483648"), ("0")),
    PARTITION p2 VALUES [("0"), ("10")),
    PARTITION p3 VALUES [("10"), ("20")))
    DISTRIBUTED BY HASH(`k`)
    PROPERTIES ("replication_num" = "1");

    -- csv file
    -- cat temp_data
    0,0,ab

    -- Load CSV file using Stream Load.
    curl --location-trusted -u <username>:<password> -T temp_data -XPUT -H column_separator:, -H label:xx http://172.17.0.1:8131/api/test_mv/t1/_stream_load

    -- Query the loaded data.
    mysql> select * from t1;
    +------+------+------------+
    | k    | v    | xx         |
    +------+------+------------+
    |    0 |    0 | 0xAB       |
    +------+------+------------+
    1 rows in set (0.11 sec)
    ```

### Query and process BINARY data

StarRocks supports querying and processing BINARY data, and supports the use of BINARY functions and operators. This example uses table `test_binary`.

Note: If you add the `--binary-as-hex` option When you access StarRocks from your MySQL client, binary data will be displayed using hex notation.

```Plain Text
mysql> select * from test_binary;
+------+------------+
| id   | j          |
+------+------------+
|    1 | 0xABAB     |
|    2 | 0xBABA     |
|    3 | 0x010102   |
|    4 | 0x0000     |
|    5 | 0xABAB     |
|    6 | 0xABAB     |
|    7 | 0x61626162 |
+------+------------+
7 rows in set (0.08 sec)
```

Example 1: View binary data using the [hex](../../sql-functions/string-functions/hex.md) function.

```plain
mysql> select id, hex(j) from test_binary;
+------+----------+
| id   | hex(j)   |
+------+----------+
|    1 | ABAB     |
|    2 | BABA     |
|    3 | 010102   |
|    4 | 0000     |
|    5 | ABAB     |
|    6 | ABAB     |
|    7 | 61626162 |
+------+----------+
7 rows in set (0.02 sec)
```

Example 2: View binary data using the [to_base64](../../sql-functions/crytographic-functions/to_base64.md) function.

```plain
mysql> select id, to_base64(j) from test_binary;
+------+--------------+
| id   | to_base64(j) |
+------+--------------+
|    1 | q6s=         |
|    2 | uro=         |
|    3 | AQEC         |
|    4 | AAA=         |
|    5 | q6s=         |
|    6 | q6s=         |
|    7 | YWJhYg==     |
+------+--------------+
7 rows in set (0.01 sec)
```

Example 3: View binary data using the [from_binary](../../sql-functions/binary-functions/from_binary.md) function.

```plain
mysql> select id, from_binary(j, 'hex') from test_binary;
+------+-----------------------+
| id   | from_binary(j, 'hex') |
+------+-----------------------+
|    1 | ABAB                  |
|    2 | BABA                  |
|    3 | 010102                |
|    4 | 0000                  |
|    5 | ABAB                  |
|    6 | ABAB                  |
|    7 | 61626162              |
+------+-----------------------+
7 rows in set (0.01 sec)
```
