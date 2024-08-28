---
displayed_sidebar: docs
---

# regexp_split



根据正则表达式 `pattern` 对 `str` 字符串进行拆分，保留最多 `max_split` 个元素，拆分后的所有字符串将以 `ARRAY<VARCHAR>` 的格式返回。

## 语法

```Haskell
regexp_split(str, pattern[, max_split])
```

## 参数说明

`str`: 需要拆分的字符串，支持的数据类型为 `VARCHAR`。

`pattern`: 拆分字符串使用到的正则表达式，支持的数据类型为 `VARCHAR`。

`max_split`: 可选，拆分后最多保留的元素数量，支持的数据类型为 `INT`。

## 返回值说明

返回值的数据类型为 `ARRAY<VARCHAR>`。

## 示例

```Plain Text
mysql> select regexp_split('StarRocks', '');
+---------------------------------------+
| regexp_split('StarRocks', '')         |
+---------------------------------------+
| ["S","t","a","r","R","o","c","k","s"] |
+---------------------------------------+

mysql> select regexp_split('StarRocks', '[SR]');
+-----------------------------------+
| regexp_split('StarRocks', '[SR]') |
+-----------------------------------+
| ["","tar","ocks"]                 |
+-----------------------------------+

mysql> select regexp_split('StarRocks', '[SR]', 1);
+--------------------------------------+
| regexp_split('StarRocks', '[SR]', 1) |
+--------------------------------------+
| ["StarRocks"]                        |
+--------------------------------------+

mysql> select regexp_split('StarRocks', '[SR]', 2);
+--------------------------------------+
| regexp_split('StarRocks', '[SR]', 1) |
+--------------------------------------+
| ["","tarRocks"]                      |
+--------------------------------------+
```
