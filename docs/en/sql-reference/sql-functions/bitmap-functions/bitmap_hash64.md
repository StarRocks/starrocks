---
displayed_sidebar: docs
---

# bitmap_hash64



Calculates a 64-bit hash value for any type of input and return the bitmap containing the hash value. It is mainly used for the stream load task to import non integer fields into the bitmap field of the StarRocks table. For example:

```bash
cat data | curl --location-trusted -u user:passwd -T - \
    -H "columns: dt,page,device_id, device_id=bitmap_hash64(device_id)" \
    http://host:8410/api/test/testDb/_stream_load
```

## Syntax

```Haskell
BITMAP BITMAP_HASH64(expr)
```

## Examples

```Plain Text
MySQL > select bitmap_count(bitmap_hash64('hello'));
+--------------------------------------+
| bitmap_count(bitmap_hash64('hello')) |
+--------------------------------------+
|                                    1 |
+--------------------------------------+

select bitmap_to_string(bitmap_hash64('hello'));
+------------------------------------------+
| bitmap_to_string(bitmap_hash64('hello')) |
+------------------------------------------+
| 10760762337991515389                     |
+------------------------------------------+
```

## keyword

BITMAP_HASH64,BITMAP
