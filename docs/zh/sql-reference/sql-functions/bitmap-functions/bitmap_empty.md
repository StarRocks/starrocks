# bitmap_empty

## description

### Syntax

```Haskell
BITMAP BITMAP_EMPTY()
```

返回一个空bitmap。主要用于 insert 或 stream load 时填充默认值。例如

```bash
cat data | curl --location-trusted -u user:passwd -T - \
    -H "columns: dt,page,v1,v2=bitmap_empty()" \
    http://host:8410/api/test/testDb/_stream_load
```

## example

```Plain Text
MySQL > select bitmap_count(bitmap_empty());
+------------------------------+
| bitmap_count(bitmap_empty()) |
+------------------------------+
|                            0 |
+------------------------------+
```

## keyword

BITMAP_EMPTY,BITMAP
