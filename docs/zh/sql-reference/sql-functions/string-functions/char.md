---
displayed_sidebar: docs
---

# char

## 功能

根据输入的 ASCII 值返回对应的字符。

## 语法

```Haskell
char(x);
```

## 参数说明

`x`: 支持的数据类型为 INT。

## 返回值说明

返回值的数据类型为 VARCHAR。

## 示例

```Plain Text
mysql> SELECT CHAR(77);
+----------+
| char(77) |
+----------+
| M        |
+----------+
1 row in set (0.00 sec)
```
