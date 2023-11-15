# multi_distinct_sum

## 功能

返回 `expr` 中去除重复值后的总和，功能等同于 sum(distinct expr)。

## 语法

```Haskell
multi_distinct_sum(expr)
```

## 参数说明

`epxr`: 用于指定参与运算的列。列值可以为 TINYINT，SMALLINT，INT，LARGEINT, FLOAT，DOUBLE，或 DECIMAL 类型。

## 返回值说明

列值和返回值类型的映射关系如下：

- BOOLEAN -> BIGINT
- TINYINT -> BIGINT
- SMALLINT -> BIGINT
- INT -> BIGINT
- BIGINT -> BIGINT
- FLOAT -> DOUBLE
- DOUBLE -> DOUBLE
- LARGEINT -> LARGEINT
- DECIMALV2 -> DECIMALV2
- DECIMAL32 -> DECIMAL128
- DECIMAL64 -> DECIMAL128
- DECIMAL128 -> DECIMAL128

## 示例

```plain text
-- 创建一张表，该表只有一个 int 域。
CREATE TABLE tabl
(k0 BIGINT NOT NULL) ENGINE=OLAP
DUPLICATE KEY(`k0`)
COMMENT "OLAP"
DISTRIBUTED BY HASH(`k0`)
PROPERTIES(
    "replication_num" = "3",
    "storage_format" = "DEFAULT"
);
Query OK, 0 rows affected (0.01 sec)

-- 插入值 0,1,1,1,2,2。
MySQL > INSERT INTO tabl VALUES ('0'), ('1'), ('1'), ('1'), ('2'), ('2');
Query OK, 6 rows affected (0.15 sec)

-- k0 的值去除重复后为 0,1,2, 将其相加得到 3。
MySQL > select multi_distinct_sum(k0) from tabl;
+------------------------+
| multi_distinct_sum(k0) |
+------------------------+
|                      3 |
+------------------------+
1 row in set (0.03 sec)
```
