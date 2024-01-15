---
displayed_sidebar: "Chinese"
---

# bitmap_union_int

## 功能

聚合函数，计算 TINYINT，SMALLINT 和 INT 类型的列中不同值的个数，返回值和 COUNT(DISTINCT expr) 相同。

## 语法

```Haskell
BITMAP_UNION_INT(expr)
```

## 参数说明

- `expr`：列表达式，支持 TINYINT，SMALLINT 和 INT 类型的列。

## 返回值说明

返回值的数据类型为 BIGINT。

## 示例

```Plaintext
mysql> select bitmap_union_int(k1) from tbl1;
+------------------------+
| bitmap_union_int(`k1`) |
+------------------------+
|                      2 |
+------------------------+
```
