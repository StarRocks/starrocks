# BINARY/VARBINARY


## Description

BINARY(M)

VARBINARY(M)

1. Since StarRocks version 3.0, StarRocks supports `BINARY`/`VARBINARY`, the maximum supported length is the same as `VARCHAR` type, and the value range of `M` is 1~1048576.
2. `BINARY` is just an alias of `VARBINARY`, the usage is exactly the same as `VARBINARY`;
3. `BINARY(M)`/`VARBINARY(M)` will not pad the unaligned length;

## 示例

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


## 限制和注意事项

- 当前 BINARY/VARBINARY 类型的数据最大长度和字符串类型相同。
- 支持 BINARY/VARBINARY 类型的列存在于明细模型、主键模型、更新模型的表中，但不支持存在于聚合模型的表中。
- 暂不支持 BINARY/VARBINARY 类型的列作为分区键、分桶键、维度列（DUPLICATE KEY、PRIMARY KEY、UNIQUE KEY），并且不支持用于 JOIN、GROUP BY、ORDER BY 子句。
- BINARY只是VARBINARY的别名，用法同VARBINARY完全相同，BINARY(M)/VARBINARY(M) 不会对没有对齐的长度做补齐操作；
