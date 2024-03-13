---
displayed_sidebar: "Chinese"
---

# sign

## 功能

返回参数 `x` 的符号。

`x` 是负数、0、正数时，分别返回 -1、0、1。

## 语法

```Haskell
SIGN(x);
```

## 参数说明

`x`: 支持的数据类型为 DOUBLE。

## 返回值说明

返回值的数据类型为 FLOAT。

## 示例

```Plain Text
mysql> select sign(3.14159);
+---------------+
| sign(3.14159) |
+---------------+
|             1 |
+---------------+
1 row in set (0.02 sec)
```
