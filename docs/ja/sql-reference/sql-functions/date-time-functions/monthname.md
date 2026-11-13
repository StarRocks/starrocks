---
displayed_sidebar: docs
<<<<<<< HEAD
=======
description: "指定された日付の月名を返します。"
products: [oss, enterprise, cloud, anywhere]
>>>>>>> 7fbe5e8 ([Doc] Add the products frontmatter line to the docs pages (#83243))
---

# monthname

指定された日付の月名を返します。

`date` パラメータは DATE または DATETIME 型でなければなりません。

## Syntax

```plaintext
VARCHAR MONTHNAME(date)
```

## Examples

```Plain Text
MySQL > select monthname('2008-02-03 00:00:00');
+----------------------------------+
| monthname('2008-02-03 00:00:00') |
+----------------------------------+
| February                         |
+----------------------------------+
```

## keyword

MONTHNAME, monthname