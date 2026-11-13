---
displayed_sidebar: docs
<<<<<<< HEAD
=======
description: "指定された日付の年の日数を返します。"
products: [oss, enterprise, cloud, anywhere]
>>>>>>> 7fbe5e8 ([Doc] Add the products frontmatter line to the docs pages (#83243))
---

# dayofyear

指定された日付の年の日数を返します。

`date` パラメータは DATE または DATETIME 型である必要があります。

## 構文

```plaintext
INT DAYOFYEAR(DATETIME|DATE date)
```

## 例

```Plain Text
MySQL > select dayofyear('2007-02-03 00:00:00');
+----------------------------------+
| dayofyear('2007-02-03 00:00:00') |
+----------------------------------+
|                               34 |
+----------------------------------+
```

## キーワード

DAYOFYEAR