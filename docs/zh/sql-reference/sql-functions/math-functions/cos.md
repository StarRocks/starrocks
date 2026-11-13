---
displayed_sidebar: docs
<<<<<<< HEAD
=======
description: "计算参数的余弦值。"
products: [oss, enterprise, cloud, anywhere]
>>>>>>> 7fbe5e8 ([Doc] Add the products frontmatter line to the docs pages (#83243))
---

# cos



求参数 `x` 的余弦值。

## 语法

```plaintext
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
