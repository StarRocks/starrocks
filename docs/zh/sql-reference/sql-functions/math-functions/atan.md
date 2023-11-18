---
displayed_sidebar: "Chinese"
---

# atan

## 功能

返回 `x` 的反正切值（单位为弧度），`x` 为 DOUBLE 类型的数值。

## 语法

```Haskell
ATAN(x);
```

## 参数说明

`x`: 支持的数据类型为 DOUBLE。

## 返回值说明

返回值的数据类型为 DOUBLE。如果输入值为 NULL，则返回 NULL。

## 示例

```Plain Text
mysql> select atan(2.5);
+--------------------+
| atan(2.5)          |
+--------------------+
| 1.1902899496825317 |
+--------------------+
1 row in set (0.01 sec)
```
