---
displayed_sidebar: "Chinese"
---

# md5sum

## 功能

计算多个输入参数的 MD5 128-bit 校验和 (checksum)，以 32 字符的十六进制字符串表示。该函数可接收多个参数，与 md5() 函数相比，文件检查的效率更高。如果传入单个参数，md5sum 和 md5 计算结果相同。

md5sum 算法一般用于检查文件的完整性，防止文件被篡改。

## 语法

```Haskell
md5sum(expr,...);
```

## 参数说明

`expr`: 支持的数据类型为 VARCHAR。

## 返回值说明

返回值的数据类型为 VARCHAR。

## 示例

```Plain Text
mysql> select md5sum("starrocks");
+----------------------------------+
| md5sum('starrocks')              |
+----------------------------------+
| f75523a916caf65f1ad487a9f8017f75 |
+----------------------------------+
1 row in set (0.01 sec)

mysql> select md5sum("starrocks","star");
+----------------------------------+
| md5sum('starrocks', 'star')      |
+----------------------------------+
| 7af4bfe35b8df2786ad133c57cb2aed8 |
+----------------------------------+
1 row in set (0.01 sec)

mysql> select md5sum(null);
+----------------------------------+
| md5sum(NULL)                     |
+----------------------------------+
| d41d8cd98f00b204e9800998ecf8427e |
+----------------------------------+
1 row in set (0.01 sec)
```
