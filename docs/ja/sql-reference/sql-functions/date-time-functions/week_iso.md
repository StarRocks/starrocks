---
displayed_sidebar: docs
<<<<<<< HEAD
=======
description: "指定された日付のISO標準週を1から53の範囲内の整数で返します。"
products: [oss, enterprise, cloud, anywhere]
>>>>>>> 7fbe5e8 ([Doc] Add the products frontmatter line to the docs pages (#83243))
---

# week_iso

指定された日付のISO標準週を `1` から `53` の範囲内の整数として返します。

## Syntax

```plaintext
INT WEEK_ISO(DATETIME date)
```

## Parameters

`date`: 変換したい日付。DATE または DATETIME 型である必要があります。

## Examples

次の例は、日付 `2008-02-20 00:00:00` のISO標準週を返します。

```SQL
MySQL > select week_iso ('2008-02-20 00:00:00');
+-----------------------------------+
| week_iso('2008-02-20 00:00:00')   |
+-----------------------------------+
|                                 8 |
+-----------------------------------+
```

## Keywords

WEEK_ISO