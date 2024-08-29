---
displayed_sidebar: docs
---

# cos

## 功能

求参数 `x` 的余弦值。

## 语法

```Haskell
COS(x);
```

## 参数说明

`x`: 弧度值，支持的数据类型为 DOUBLE。

## 返回值说明

返回值的数据类型为 DOUBLE。

## 示例

```Plain Text
mysql> select cos(2);
+---------------------+
| cos(2)              |
+---------------------+
| -0.4161468365471424 |
+---------------------+
1 row in set (0.01 sec)
```
