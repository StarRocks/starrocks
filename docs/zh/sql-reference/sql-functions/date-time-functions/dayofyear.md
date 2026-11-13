---
displayed_sidebar: docs
<<<<<<< HEAD
=======
description: "返回指定日期为对应年中的哪一天。"
products: [oss, enterprise, cloud, anywhere]
>>>>>>> 7fbe5e8 ([Doc] Add the products frontmatter line to the docs pages (#83243))
---

# dayofyear



计算指定日期为对应年中的哪一天。

参数为 DATE 或 DATETIME 类型。

## 语法

```plaintext
INT DAYOFYEAR(DATETIME date)
```

## 示例

```Plain Text
select dayofyear('2007-02-03 00:00:00');
+----------------------------------+
| dayofyear('2007-02-03 00:00:00') |
+----------------------------------+
|                               34 |
+----------------------------------+
```
