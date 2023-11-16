---
displayed_sidebar: "Chinese"
---

# md5

## 功能

计算给定字符串的MD5 128-bit校验和

## 语法

```Haskell
VARCHAR md5(VARCHAR expr)
```

## 参数说明

* expr: 需要被计算的字符串

## 返回值说明

校验和以32个十六进制数字组成的字符串表示，如果expr为NULL，返回结果为NULL

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
