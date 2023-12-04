---
displayed_sidebar: "Chinese"
---

# uncompress

## 功能

支持将压缩后的数据根据不同压缩方法解压缩为原始数据的函数。

## 语法

```Haskell
VARCHAR uncompress(BINARY input[, VARCHAR method])
```

## 参数说明

`input`: 待解压的数据，支持的数据类型为 BINARY。

`method`: 原始数据使用的压缩方法，支持 SNAPPY、ZLIB、ZSTD、LZ4 四种压缩方法，大小写不敏感，若空则默认为 ZLIB。

## 示例

```Plain Text
mysql> SELECT uncompress(to_binary('789C2B49AD280100046701C6'));
+-----------------------------------------------------------+
| uncompress(to_binary('789C2B49AD280100046701C6'), 'ZLIB') |
+-----------------------------------------------------------+
| text                                                      |
+-----------------------------------------------------------+

mysql> select uncompress(compress("text"));
+----------------------------------------------+
| uncompress(compress('text', 'ZLIB'), 'ZLIB') |
+----------------------------------------------+
| text                                         |
+----------------------------------------------+

mysql> SELECT uncompress(compress("text", "SNAPPY"), 'snappy');
+--------------------------------------------------+
| uncompress(compress('text', 'SNAPPY'), 'snappy') |
+--------------------------------------------------+
| text                                             |
+--------------------------------------------------+

mysql> SELECT uncompress(compress("text", "LZ4"), 'lz4');
+--------------------------------------------+
| uncompress(compress('text', 'LZ4'), 'lz4') |
+--------------------------------------------+
| text                                       |
+--------------------------------------------+
```

## 相关函数

- [compress](./compress.md)
