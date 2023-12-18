---
displayed_sidebar: "Chinese"
---

# unhex

## 功能

将输入的参数 `str` 中的两个字符为一组转化为16进制后的字符，然后拼接成字符串输出。

## 语法

```Haskell
UNHEX(str);
```

## 参数说明

`str`: 支持的数据类型为 VARCHAR，长度为0或为奇数，或者包含`[0-9]`、`[a-z]`、`[A-Z]`之外的字符则返回空串。

## 返回值说明

返回值的数据类型为 VARCHAR。

## 示例

```Plain Text
mysql> select unhex('3132');
+---------------+
| unhex('3132') |
+---------------+
| 12            |
+---------------+
1 row in set (0.00 sec)

mysql> select unhex('4142@');
+----------------+
| unhex('4142@') |
+----------------+
|                |
+----------------+
1 row in set (0.01 sec)
```
