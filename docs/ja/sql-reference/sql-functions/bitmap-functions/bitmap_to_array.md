---
displayed_sidebar: docs
<<<<<<< HEAD
=======
description: "ビットマップをBIGINT配列に変換します。"
products: [oss, enterprise, cloud, anywhere]
>>>>>>> 7fbe5e8 ([Doc] Add the products frontmatter line to the docs pages (#83243))
---

# bitmap_to_array

BITMAP を BIGINT 配列に変換します。

## Syntax

```plaintext
 ARRAY<BIGINT> BITMAP_TO_ARRAY (bitmap)
```

## Parameters

`bitmap`: 変換したいビットマップ。

## Return value

BIGINT 配列を返します。

## Examples

```Plain
select bitmap_to_array(bitmap_from_string("1, 7"));
+----------------------------------------------+
| bitmap_to_array(bitmap_from_string('1, 7'))  |
+----------------------------------------------+
| [1,7]                                        |
+----------------------------------------------+

select bitmap_to_array(NULL);
+-----------------------+
| bitmap_to_array(NULL) |
+-----------------------+
| NULL                  |
+-----------------------+
```
