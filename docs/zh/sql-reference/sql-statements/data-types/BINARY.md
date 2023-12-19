---
displayed_sidebar: "Chinese"
---

# BINARY/VARBINARY

## 描述

BINARY(M)

VARBINARY(M)

自 3.0 版本起，StarRocks 支持 BINARY/VARBINARY 数据类型，用于存储二进制数据，单位为字节。

支持的最大长度与 VARCHAR 类型相同，`M` 的取值范围为 1~1048576。如果未指定 `M`，默认为最大值 1048576。

BINARY 是 VARBINARY 的别名，用法与 VARBINARY 相同。

## 限制和注意事项

- 支持在明细模型、主键模型、更新模型表中创建 VARBINARY 类型的列，但不支持在聚合模型表中创建 VARBINARY 类型的列。
- 暂不支持 BINARY/VARBINARY 类型的列作为明细模型、主键模型、更新模型表的分区键、分桶键、维度列，并且不支持用于 JOIN、GROUP BY、ORDER BY 子句。
- BINARY(M)/VARBINARY(M) 不会对没有对齐的长度做补齐操作。

## 示例

### 创建 BINARY 类型的列

建表时，通过关键字 `VARBINARY`，指定列 `j` 为 VARBINARY 类型。

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

### 导入数据并存储为 BINARY 类型

StarRocks 支持如下方式导入数据并存储为 BINARY 类型。

- 方式一：通过 INSERT INTO 将数据写入至 BINARY 类型的常量列（例如列 `j`），其中常量列以 `x''` 作为前缀。

```SQL
INSERT INTO test_binary (id, j) VALUES (1, x'abab');
INSERT INTO test_binary (id, j) VALUES (2, x'baba');
INSERT INTO test_binary (id, j) VALUES (3, x'010102');
INSERT INTO test_binary (id, j) VALUES (4, x'0000');
```

- 方式二：通过 TO_BINARY() 函数将 VARCHAR 类型数据转换为 BINARY 类型。

```SQL
INSERT INTO test_binary select 5, to_binary('abab', 'hex');
INSERT INTO test_binary select 6, to_binary('abab', 'base64');
INSERT INTO test_binary select 7, to_binary('abab', 'utf8');
```

### 查询和处理 BINARY 类型的数据

StarRocks 支持查询和处理 BINARY 类型的数据，并且支持使用 BINARY 函数和运算符。本示例以表 `test_binary` 表进行说明。

> 注意：如果在启动 MySQL client 时添加了 `--binary-as-hex` 选项，会默认以 `hex` 的格式展示结果中的 BINARY 数据。

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

示例一：通过 [hex](../../sql-functions/string-functions/hex.md) 函数查看 BINARY 类型数据。

```Plain Text
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
