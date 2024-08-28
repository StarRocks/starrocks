---
displayed_sidebar: docs
---

# log10, dlog10

## 功能

返回以 10 为底数的 `x` 的对数。

## 语法

```Haskell
LOG10(x);
```

## 参数说明

`x`: 要计算对数的数值，必填。支持的数据类型为 DOUBLE。`x` 必须大于 0，否则返回 NULL。

## 返回值说明

返回值的数据类型为 DOUBLE。

## 示例

```Plain Text
mysql> select log10(100);
+------------+
| log10(100) |
+------------+
|          2 |
+------------+
1 row in set (0.01 sec)
```
