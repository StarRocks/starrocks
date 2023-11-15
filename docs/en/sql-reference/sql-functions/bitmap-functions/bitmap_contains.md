# bitmap_contains

## description

### Syntax

`B00LEAN BITMAP_CONTAINS(BITMAP bitmap, BIGINT input)`

Calculate whether the input value is in the bitmap column, and return a Boolean value

## example

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
