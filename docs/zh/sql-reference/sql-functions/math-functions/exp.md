---
displayed_sidebar: docs
---

# exp, dexp

## 功能

返回 e 的 `x` 次幂，也被称为自然指数函数。

## 语法

```Haskell
EXP(x);
```

## 参数说明

`x`: 支持的数据类型为 DOUBLE。

## 返回值说明

返回值的数据类型为 DOUBLE。

## 示例

```Plain Text
mysql> select exp(3.14);
+--------------------+
| exp(3.14)          |
+--------------------+
| 23.103866858722185 |
+--------------------+
1 row in set (0.01 sec)
```
