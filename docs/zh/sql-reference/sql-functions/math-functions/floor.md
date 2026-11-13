---
displayed_sidebar: docs
<<<<<<< HEAD
=======
description: "返回不大于x的最大整数值。"
products: [oss, enterprise, cloud, anywhere]
>>>>>>> 7fbe5e8 ([Doc] Add the products frontmatter line to the docs pages (#83243))
---

# floor, dfloor



返回不大于 `x` 的最大整数值。

## 语法

```plaintext
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
