---
displayed_sidebar: docs
<<<<<<< HEAD
=======
description: "根据距离 0000-01-01 的天数计算对应的日期。"
products: [oss, enterprise, cloud, anywhere]
>>>>>>> 7fbe5e8 ([Doc] Add the products frontmatter line to the docs pages (#83243))
---

# from_days



通过计算当前时间距离 `0000-01-01` 的天数计算出是哪一天。

## 语法

```plaintext
DATE FROM_DAYS(INT N)
```

## 示例

```Plain Text
select from_days(730669);
+-------------------+
| from_days(730669) |
+-------------------+
| 2000-07-03        |
+-------------------+
```
