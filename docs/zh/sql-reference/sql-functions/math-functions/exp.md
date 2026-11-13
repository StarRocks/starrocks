---
displayed_sidebar: docs
<<<<<<< HEAD
=======
description: "返回e的x次幂。"
products: [oss, enterprise, cloud, anywhere]
>>>>>>> 7fbe5e8 ([Doc] Add the products frontmatter line to the docs pages (#83243))
---

# exp, dexp



返回 e 的 `x` 次幂，也被称为自然指数函数。

## 语法

```plaintext
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
