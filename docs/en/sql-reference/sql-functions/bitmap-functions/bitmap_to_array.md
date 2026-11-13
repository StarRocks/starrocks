---
displayed_sidebar: docs
<<<<<<< HEAD
=======
description: "Converts a BITMAP into a BIGINT array."
products: [oss, enterprise, cloud, anywhere]
>>>>>>> 7fbe5e8 ([Doc] Add the products frontmatter line to the docs pages (#83243))
---

# bitmap_to_array



Converts a BITMAP into a BIGINT array.

## Syntax

```plaintext
 ARRAY<BIGINT> BITMAP_TO_ARRAY (bitmap)
```

## Parameters

`bitmap`: the bitmap you want to convert.

## Return value

Returns a BIGINT array.

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
