# to_bitmap

## description

### Syntax

```Haskell
BITMAP TO_BITMAP(expr)
```

The input is unsigned bigint with the value ranging from 0 to 18446744073709551615, and the output is bitmap containing this element. This function is mainly used for the stream load task to import integer fields into the bitmap field of the StarRocks table. For example:

```bash
cat data | curl --location-trusted -u user:passwd -T - \
    -H "columns: dt,page,user_id, user_id=to_bitmap(user_id)" \
    http://host:8410/api/test/testDb/_stream_load
```

## example

```Plain Text
MySQL > select bitmap_count(to_bitmap(10));
+-----------------------------+
| bitmap_count(to_bitmap(10)) |
+-----------------------------+
|                           1 |
+-----------------------------+
```

## keyword

TO_BITMAP,BITMAP
