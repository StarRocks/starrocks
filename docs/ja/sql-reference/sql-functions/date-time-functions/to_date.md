---
displayed_sidebar: docs
<<<<<<< HEAD
=======
description: "DATETIME値を日付に変換します。"
products: [oss, enterprise, cloud, anywhere]
>>>>>>> 7fbe5e8 ([Doc] Add the products frontmatter line to the docs pages (#83243))
---

# to_date

DATETIME 値を日付に変換します。

## Syntax

```plaintext
DATE TO_DATE(DATETIME datetime)
```

## 例

```Plain Text
MySQL > select to_date("2020-02-02 00:00:00");
+--------------------------------+
| to_date('2020-02-02 00:00:00') |
+--------------------------------+
| 2020-02-02                     |
+--------------------------------+
```

## キーワード

TO_DATE