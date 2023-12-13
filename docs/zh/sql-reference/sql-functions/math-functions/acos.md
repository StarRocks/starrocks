---
displayed_sidebar: "Chinese"
---

# acos

## 功能

返回 `x` 的反余弦值（单位为弧度）。`x` 为 DOUBLE 类型的数值。

## 语法

```Haskell
ACOS(x);
```

## 参数说明

`x`: 支持的数据类型为 DOUBLE。取值范围[-1,1]。

## 返回值说明

返回值的数据类型为 DOUBLE。如果输入值为 NULL 或者 `x` 超出 [-1,1] 的范围，则返回 NULL。

## 示例

```Plain Text
mysql> select acos(0.25);
+-------------------+
| acos(0.25)        |
+-------------------+
| 1.318116071652818 |
+-------------------+
1 row in set (0.02 sec)
```
