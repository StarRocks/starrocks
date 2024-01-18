---
displayed_sidebar: "Chinese"
---

# murmur_hash3_32

## 功能

返回输入字符串的 32 位 murmur3 hash 值。

## 语法

```Haskell
MURMUR_HASH3_32(input, ...)
```

## 参数说明

`input`: 支持的数据类型为 VARCHAR。

## 返回值说明

返回值的数据类型为 INT。

## 示例

```Plain Text
MySQL > select murmur_hash3_32(null);
+-----------------------+
| murmur_hash3_32(NULL) |
+-----------------------+
|                  NULL |
+-----------------------+

MySQL > select murmur_hash3_32("hello");
+--------------------------+
| murmur_hash3_32('hello') |
+--------------------------+
|               1321743225 |
+--------------------------+

MySQL > select murmur_hash3_32("hello", "world");
+-----------------------------------+
| murmur_hash3_32('hello', 'world') |
+-----------------------------------+
|                         984713481 |
+-----------------------------------+
```
