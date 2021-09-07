# bitmap_remove

## description

### Syntax

```Haskell
bitmap_remove(BITMAP lhs, BIGINT input)
```

从 lhs 中删除 **input** 作为结果集合返回

## example

```plain text
mysql> select bitmap_to_string(**bitmap_remove**(bitmap_from_string('1, 3'), 3)) cnt;
+------+
|cnt   |
+------+
|1     |
+------+

mysql> select bitmap_to_string(**bitmap_remove**(bitmap_from_string('1,3,5'), 6)) cnt;
+------+
|cnt   |
+------+
|1,3,5 |
+------+
```

## keyword

BITMAP_REMOVE, BITMAP
