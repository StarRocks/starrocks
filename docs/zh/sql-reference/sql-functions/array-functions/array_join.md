---
displayed_sidebar: "Chinese"
---

# array_join

## 功能

将数组中的所有元素连接生成一个字符串。

## 语法

```Haskell
ARRAY_JOIN(array, sep[, null_replace_str])
```

## 参数说明

* `array`：需要连接的数组。支持的数据类型为 ARRAY。
* `sep`：分隔符。支持的数据类型为 VARCHAR。
* `null_replace_str`：替换 NULL 的字符串。支持的数据类型为 VARCHAR。

## 返回值说明

返回的数据类型为 VARCHAR。

## 注意事项

* `array` 只支持一维数组。
* `array` 不支持 Decimal 类型。
* 如果参数 `sep` 为 NULL，返回值为 NULL。
* 如果没有传 `null_replace_str` 参数，NULL 会被丢弃。
* 如果参数 `null_replace_str` 为 NULL，返回值为 NULL。

## 示例

丢弃数据组的 `NULL`，以 `_` 作为分隔符，连接数组中的元素。

```Plain Text
mysql> select array_join([1, 3, 5, null], '_');
+-------------------------------+
| array_join([1,3,5,NULL], '_') |
+-------------------------------+
| 1_3_5                         |
+-------------------------------+
```

将数据组中的 `NULL` 替换为字符串 `NULL`，以 `_` 作为分隔符，连接数组中的元素。

```Plain Text
mysql> select array_join([1, 3, 5, null], '_', 'NULL');
+---------------------------------------+
| array_join([1,3,5,NULL], '_', 'NULL') |
+---------------------------------------+
| 1_3_5_NULL                            |
+---------------------------------------+
```
