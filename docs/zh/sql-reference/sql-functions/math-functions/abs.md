---
displayed_sidebar: docs
<<<<<<< HEAD
=======
description: "返回数值的绝对值。"
products: [oss, enterprise, cloud, anywhere]
>>>>>>> 7fbe5e8 ([Doc] Add the products frontmatter line to the docs pages (#83243))
---

# abs



返回 `x` 的绝对值。如果输入值为 NULL，则返回 NULL。

## 语法

```plaintext
ABS(x);
```

## 参数说明

`x`: 支持的数据类型为 DOUBLE、FLOAT、LARGEINT、BIGINT、INT、SMALLINT、TINYINT、DECIMALV2、DECIMAL32、DECIMAL64、DECIMAL128。

## 返回值说明

返回值的数据类型为 与 `x` 类型一致。

## 示例

```Plain Text
mysql> select abs(-1);
+---------+
| abs(-1) |
+---------+
|       1 |
+---------+
1 row in set (0.00 sec)
```

## Keywords

abs, absolute
