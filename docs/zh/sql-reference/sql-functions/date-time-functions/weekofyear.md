---
displayed_sidebar: docs
<<<<<<< HEAD
=======
description: "计算指定时间为一年中的第几周。"
products: [oss, enterprise, cloud, anywhere]
>>>>>>> 7fbe5e8 ([Doc] Add the products frontmatter line to the docs pages (#83243))
---

# weekofyear



计算指定时间为一年中的第几周。

## 语法

```plaintext
INT WEEKOFYEAR(DATETIME date)
```

## 参数说明

参数为 DATE 或者 DATETIME 类型。

## 返回值说明

 返回 INT 类型的值。

## 示例

```Plain Text
select weekofyear('2008-02-20 00:00:00');
+-----------------------------------+
| weekofyear('2008-02-20 00:00:00') |
+-----------------------------------+
|                                 8 |
+-----------------------------------+
```
