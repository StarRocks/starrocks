---
displayed_sidebar: docs
---

# bitmap_contains

<<<<<<< HEAD
## Description
=======

>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))

Calculates whether the input value is in the bitmap column, and returns a Boolean value.

## Syntax

```Haskell
B00LEAN BITMAP_CONTAINS(BITMAP bitmap, BIGINT input)
```

## Examples

```Plain Text
MySQL > select bitmap_contains(to_bitmap(1),2) cnt;
+------+
| cnt  |
+------+
|    0 |
+------+

MySQL > select bitmap_contains(to_bitmap(1),1) cnt;
+------+
| cnt  |
+------+
|    1 |
+------+
```

## keyword

BITMAP_CONTAINS,BITMAP
