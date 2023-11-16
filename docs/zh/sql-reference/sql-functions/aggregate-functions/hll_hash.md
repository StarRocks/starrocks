---
displayed_sidebar: "Chinese"
---

# hll_hash

## 功能

将一个数值转换为 HLL 类型。通常用于导入中，将源数据中的数值映射到 Starrocks 表中的 HLL 列类型。

## 语法

```Haskell
HLL_HASH(column_name)
```

## 参数说明

`column_name`: 生成的新的 HLL 列名。

## 返回值说明

返回 HLL 类型的值。

## 示例

```plain text
mysql> select hll_cardinality(hll_hash("a"));
+--------------------------------+
| hll_cardinality(hll_hash('a')) |
+--------------------------------+
|                              1 |
+--------------------------------+
```
