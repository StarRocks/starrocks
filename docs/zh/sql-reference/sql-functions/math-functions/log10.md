---
displayed_sidebar: docs
<<<<<<< HEAD
=======
description: "计算以10为底的数值对数。"
products: [oss, enterprise, cloud, anywhere]
>>>>>>> 7fbe5e8 ([Doc] Add the products frontmatter line to the docs pages (#83243))
---

# log10, dlog10



返回以 10 为底数的 `x` 的对数。

## 语法

```plaintext
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
