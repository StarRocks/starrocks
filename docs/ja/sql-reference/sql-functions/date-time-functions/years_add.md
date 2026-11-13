---
displayed_sidebar: docs
<<<<<<< HEAD
=======
description: "指定された年数を日付に追加します。"
products: [oss, enterprise, cloud, anywhere]
>>>>>>> 7fbe5e8 ([Doc] Add the products frontmatter line to the docs pages (#83243))
---

# years_add

## description

### Syntax

```plaintext
DATETIME YEARS_ADD(DATETIME expr1,INT expr2)
```

指定された時間間隔を日付に追加します。単位は年です。

expr1 パラメータは有効な datetime 式です。

expr2 パラメータは追加したい年数です。

## example

```Plain Text
select years_add('2010-11-30 23:50:50', 2);
+-------------------------------------+
| years_add('2010-11-30 23:50:50', 2) |
+-------------------------------------+
| 2012-11-30 23:50:50                 |
+-------------------------------------+

select years_add('2010-11-30', 2);
+----------------------------+
| years_add('2010-11-30', 2) |
+----------------------------+
| 2012-11-30 00:00:00        |
+----------------------------+
```

## keyword

YEARS_ADD,ADD