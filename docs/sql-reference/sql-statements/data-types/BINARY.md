# BINARY/VARBINARY


## Description

BINARY(M)

VARBINARY(M)

1. Since StarRocks version 3.0, StarRocks supports `BINARY`/`VARBINARY`, the maximum supported length is the same as `VARCHAR` type, and the value range of `M` is 1~1048576.
2. `BINARY` is just an alias of `VARBINARY`, the usage is exactly the same as `VARBINARY`;
3. `BINARY(M)`/`VARBINARY(M)` will not pad the unaligned length;

## Example

### Create a column of type BINARY

When creating a table, use the keyword `BINARY` to specify column `j` as BINARY type.

```SQL
CREATE TABLE `test_binary` (
    `id` INT(11) NOT NULL COMMENT "",
    `j`  VARBINARY NULL COMMENT ""
) ENGINE=OLAP
DUPLICATE KEY(`id`)
COMMENT "OLAP"
DISTRIBUTED BY HASH(`id`) BUCKETS 1
PROPERTIES (
    "replication_num" = "1",
    "in_memory" = "false",
    "storage_format" = "DEFAULT"
);

mysql> desc test_binary;
+-------+-----------+------+-------+---------+-------+
| Field | Type      | Null | Key   | Default | Extra |
+-------+-----------+------+-------+---------+-------+
| id    | int       | NO   | true  | NULL    |       |
| j     | varbinary | YES  | false | NULL    |       |
+-------+-----------+------+-------+---------+-------+
2 rows in set (0.01 sec)

```

### Import data and store it as BINARY type

StarRocks supports the following three ways to import data and store it as BINARY type.

- Method 1: Use `INSERT INTO` to write data to a constant column of BINARY type (such as column `j`), where the constant column is prefixed with `x'``.

```SQL
INSERT INTO test_binary (id, j) VALUES (1, x'abab');
INSERT INTO test_binary (id, j) VALUES (2, x'baba');
INSERT INTO test_binary (id, j) VALUES (3, x'010102');
INSERT INTO test_binary (id, j) VALUES (4, x'0000'); 
```


- Method 2: Use the `TO_BINARY` function to convert VARCHAR type data to BINARY type.

```SQL
INSERT INTO test_binary select 5, to_binary('abab', 'hex');
INSERT INTO test_binary select 6, to_binary('abab', 'base64');
INSERT INTO test_binary select 7, to_binary('abab', 'utf8');
```

- Method 3: Use Broker Load to load a Parquet/ORC file and store the file as BINARY data. For more information, see [Broker Load](../../../loading/BrokerLoad.md).

    - For Parquet file, convert `parquet::Type::type::BYTE_ARRAY` to `TYPE_VARBINARY` directly;
    - For Orc file, convert `orc::BINARY` to `TYPE_VARBINARY` directly.

- Method 4:  Use Stream Load to load a CSV file and store the file as `BINARY` data. For more information, see [Load CSV data](../../../loading/StreamLoad.md#load-csv-data).
    - CSV file use hex format for `BINARY` type as the input.  Please ensure the input binary type input with a valid hex.
    - `BINARY` type is only supported for CSV file for now, json file doesn't suport `BINARY` type yet.


eg, `t1` is a table with `BINARY` column.
```
CREATE TABLE `t1` (
  `k` int(11) NOT NULL COMMENT "",
  `v` int(11) NOT NULL COMMENT "",
  `b` varbinary
)engine=olap
DUPLICATE KEY(`k`)
PARTITION BY RANGE(`v`) (
PARTITION p1 VALUES [("-2147483648"), ("0")),
PARTITION p2 VALUES [("0"), ("10")),
PARTITION p3 VALUES [("10"), ("20")))
DISTRIBUTED BY HASH(`k`) BUCKETS 1
PROPERTIES ("replication_num" = "1");

-- csv file
-- cat temp_data
0,0,ab

-- Load csv by stream
curl --location-trusted -u root: -T temp_data -XPUT -H column_separator:, -H label:xx http://172.17.0.1:8131/api/test_mv/t1/_stream_load

-- query the load result
mysql> select * from t1;
+------+------+------------+
| k    | v    | xx         |
+------+------+------------+
|    0 |    0 | 0xAB       |
+------+------+------------+
1 rows in set (0.11 sec)
```
### Query and process BINARY type data

StarRocks supports querying and processing `BINARY` type data, and supports the use of `BINARY` functions and operators. This example is illustrated with the table test_binary.

Note: When the mysql client adds `--binary-as-hex`, it will display the `BINARY` type in the result in `hex` by default;


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

Example 1: View binary type data through the `hex` function
```
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

Example 2: View binary type data through the `to_base64` function
```
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

Example 3: View binary type data through the `from_binary` function
```
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


## Limitations and Notes

- Currently, the maximum data length of the `BINARY`/`VARBINARY` type is the same as that of the string type.
- Columns of `BINARY`/`VARBINARY` type are not currently supported as partition keys, bucket keys, and dimension columns (DUPLICATE KEY, PRIMARY KEY, UNIQUE KEY), and are not supported for JOIN, GROUP BY, and ORDER BY clauses.
- `BINARY` is just an alias of `VARBINARY`, the usage is exactly the same as `VARBINARY`, `BINARY(M)`/`VARBINARY(M)` will not fill the unaligned length;