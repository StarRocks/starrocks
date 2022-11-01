# md5

## 功能

使用 MD5 加密算法将给定字符串进行加密，输出一个 128-bit 的校验和 (checksum)。校验和以 32 字符的十六进制字符串表示。

MD5 信息摘要算法 (MD5 Message-Digest Algorithm)，是一种广泛使用的密码散列函数，用于确保信息传输完整一致。

## 语法

```Haskell
md5(expr)
```

## 参数说明

`expr`: 待计算的字符串，支持的数据类型为 VARCHAR。

## 返回值说明

返回值的数据类型为 VARCHAR。如果输入 为 NULL，返回结果为 NULL。

## 示例

```Plain Text
mysql> select md5('abc');
+----------------------------------+
| md5('abc')                       |
+----------------------------------+
| 900150983cd24fb0d6963f7d28e17f72 |
+----------------------------------+
1 row in set (0.01 sec)

mysql> select md5(null);
+-----------+
| md5(NULL) |
+-----------+
| NULL      |
+-----------+
1 row in set (0.00 sec)
```
