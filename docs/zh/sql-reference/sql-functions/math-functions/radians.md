---
displayed_sidebar: "Chinese"
---

# radians

## 功能

将参数 `x`转为弧度，`x` 是角度。

## 语法

```Haskell
REDIANS(x);
```

## 参数说明

`x`: 支持的数据类型为 DOUBLE。

## 返回值说明

返回值的数据类型为 DOUBLE。

## 示例

```Plain Text
mysql> select radians(90);
+--------------------+
| radians(90)        |
+--------------------+
| 1.5707963267948966 |
+--------------------+
1 row in set (0.00 sec)
```
