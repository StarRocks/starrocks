---
displayed_sidebar: docs
<<<<<<< HEAD
=======
description: "从DATETIME类型值中提取日期部分。"
products: [oss, enterprise, cloud, anywhere]
>>>>>>> 7fbe5e8 ([Doc] Add the products frontmatter line to the docs pages (#83243))
---

# to_date



返回 DATETIME 类型值中的日期部分。

## 语法

```plaintext
DATE TO_DATE(DATETIME datetime)
```

## 示例

```Plain Text
select to_date("2020-02-02 00:00:00");
+--------------------------------+
| to_date('2020-02-02 00:00:00') |
+--------------------------------+
| 2020-02-02                     |
+--------------------------------+
```
