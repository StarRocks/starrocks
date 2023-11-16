---
displayed_sidebar: "Chinese"
---

# md5

## 功能

计算给定字符串的 MD5 128-bit 校验和

## 语法

```Haskell
md5(expr)
```

## 参数说明

`expr`: 需要被计算的字符串, 支持的数据类型为 VARCHAR

## 返回值说明

校验和以 32 个十六进制数字组成的字符串表示, 如果 expr 为 NULL, 返回结果为 NULL, 否则返回的数据类型为 VARCHAR

## 示例

```Plain Text
mysql> select md5('abc');
+----------------------------------+
| md5('abc')                       |
+----------------------------------+
| 900150983cd24fb0d6963f7d28e17f72 |
+----------------------------------+
1 row in set (0.01 sec)
```

## 关键词

MD5, ENCRYPTION
