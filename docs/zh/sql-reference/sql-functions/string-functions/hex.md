---
displayed_sidebar: "Chinese"
---

# hex

## 功能

若参数 `x` 是数字，则返回十六进制值的字符串表示形式；若参数 `x` 是字符串，则将每个字符转化为两个十六进制的字符，将转化后的所有字符拼接为字符串输出。

## 语法

```Haskell
HEX(x);
```

## 参数说明

`x`: 支持的数据类型为 BIGINT、VARCHAR、VARBINARY (v3.0 及以后)。

## 返回值说明

返回值的数据类型为 VARCHAR。

## 示例

```Plain Text
mysql> select hex(3);
+--------+
| hex(3) |
+--------+
| 3      |
+--------+
1 row in set (0.00 sec)

mysql> select hex('3');
+----------+
| hex('3') |
+----------+
| 33       |
+----------+
1 row in set (0.00 sec)

-- 输入值为 VARBINARY 类型。
mysql> select hex(x'abab');
+-------------+
| hex('ABAB') |
+-------------+
| ABAB        |
+-------------+
```
