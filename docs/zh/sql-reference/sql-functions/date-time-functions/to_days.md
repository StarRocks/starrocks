---
displayed_sidebar: docs
<<<<<<< HEAD
=======
description: "返回指定日期距离0000-01-01的天数。"
products: [oss, enterprise, cloud, anywhere]
>>>>>>> 7fbe5e8 ([Doc] Add the products frontmatter line to the docs pages (#83243))
---

# to_days



返回指定日期距离 `0000-01-01` 的天数。

参数必须为 DATE 或 DATETIME 类型。

## 语法

```plaintext
INT TO_DAYS(DATETIME date)
```

## 示例

```Plain Text
select to_days('2007-10-07');
+-----------------------+
| to_days('2007-10-07') |
+-----------------------+
|                733321 |
+-----------------------+
```
