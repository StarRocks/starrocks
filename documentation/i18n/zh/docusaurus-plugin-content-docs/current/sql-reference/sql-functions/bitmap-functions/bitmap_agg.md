---
displayed_sidebar: "Chinese"
---

# bitmap_agg

## 功能

将一列中的多行非 NULL 数值合并成一行 BITMAP 值，即多行转一行。

## 语法

```Haskell
BITMAP_AGG(col)
```

## 参数

`col`: 待合并数值的列。支持的数据类型为 BOOLEAN、TINYINT、SMALLINT、INT、BIGINT、LARGEINT。也支持可以转化为以上类型的 VARCHAR。

## 返回值说明

返回 BITMAP 类型的值。

## 使用说明

如果某个值小于 0 或者大于 18446744073709551615，该值会被忽略，不会合并到 Bitmap 中（见示例三）。

## 示例

建表并导入数据。

```sql
mysql> CREATE TABLE t1_test (
    c1 int,
    c2 boolean,
    c3 tinyint,
    c4 int,
    c5 bigint,
    c6 largeint
    )
DUPLICATE KEY(c1)
DISTRIBUTED BY HASH(c1);

INSERT INTO t1_test VALUES
    (1, true, 11, 111, 1111, 11111),
    (2, false, 22, 222, 2222, 22222),
    (3, true, 33, 333, 3333, 33333),
    (4, null, null, null, null, null),
    (5, -1, -11, -111, -1111, -11111),
    (6, null, null, null, null, "36893488147419103232");
```

查询表中数据。

```PlainText
select * from t1_test order by c1;
+------+------+------+------+-------+----------------------+
| c1   | c2   | c3   | c4   | c5    | c6                   |
+------+------+------+------+-------+----------------------+
|    1 |    1 |   11 |  111 |  1111 | 11111                |
|    2 |    0 |   22 |  222 |  2222 | 22222                |
|    3 |    1 |   33 |  333 |  3333 | 33333                |
|    4 | NULL | NULL | NULL |  NULL | NULL                 |
|    5 |    1 |  -11 | -111 | -1111 | -11111               |
|    6 | NULL | NULL | NULL |  NULL | 36893488147419103232 |
+------+------+------+------+-------+----------------------+
```

示例一：将 `c1` 列中的数值合并成 Bitmap。

```PlainText
mysql> select bitmap_to_string(bitmap_agg(c1)) from t1_test;
+----------------------------------+
| bitmap_to_string(bitmap_agg(c1)) |
+----------------------------------+
| 1,2,3,4,5,6                      |
+----------------------------------+
```

示例二：将 `c2` 列中的数值合并成 Bitmap，忽略 NULL 值。

```PlainText
mysql> select BITMAP_TO_STRING(BITMAP_AGG(c2)) from t1_test;
+----------------------------------+
| bitmap_to_string(bitmap_agg(c2)) |
+----------------------------------+
| 0,1                              |
+----------------------------------+
```

示例三：将 `c6` 列中的数值合并成 Bitmap，忽略 NULL 值和超过范围的后两行值。

```PlainText
mysql> select bitmap_to_string(bitmap_agg(c6)) from t1_test;
+----------------------------------+
| bitmap_to_string(bitmap_agg(c6)) |
+----------------------------------+
| 11111,22222,33333                |
+----------------------------------+
```

## Keywords

BITMAP_AGG, BITMAP
