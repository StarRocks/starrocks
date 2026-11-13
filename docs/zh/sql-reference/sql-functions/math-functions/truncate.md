---
displayed_sidebar: docs
<<<<<<< HEAD
=======
description: "将数值x四舍五入到小数点后y位。"
products: [oss, enterprise, cloud, anywhere]
>>>>>>> 7fbe5e8 ([Doc] Add the products frontmatter line to the docs pages (#83243))
---

# truncate



返回数值 `x` 保留到小数点后 `y` 位的值。

## 语法

```plaintext
TRUNCATE(x,y);
```

## 参数说明

`x`: 支持的数据类型为 DOUBLE 或 DECIMAL128。

`y`: 支持的数据类型为 INT。

## 返回值说明

返回值的数据类型和 `x` 相同。

## 示例

```Plain Text
mysql> select truncate(3.14,1);
+-------------------+
| truncate(3.14, 1) |
+-------------------+
|               3.1 |
+-------------------+
1 row in set (0.00 sec)
```
