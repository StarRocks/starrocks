# bitmap_contains

## description

### Syntax

`B00LEAN BITMAP_CONTAINS(BITMAP bitmap, BIGINT input)`

计算输入值是否在Bitmap列中，返回值是Boolean值.

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
