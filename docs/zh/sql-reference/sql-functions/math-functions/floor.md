---
displayed_sidebar: "Chinese"
---

# floor, dfloor

## 功能

返回不大于 `x` 的最大整数值。

## 语法

```Haskell
FLOOR(x);
```

## 参数说明

`x`: 支持的数据类型为 DOUBLE。

## 返回值说明

返回值的数据类型为 BIGINT。

## 示例

```Plain Text
mysql> select floor(3.14);
+-------------+
| floor(3.14) |
+-------------+
|           3 |
+-------------+
1 row in set (0.01 sec)
```
