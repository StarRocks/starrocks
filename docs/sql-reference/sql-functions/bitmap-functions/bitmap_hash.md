# bitmap_hash

## description

### Syntax

```Haskell
BITMAP BITMAP_HASH(expr)
```

Calculate a 32-bit hash value for any type of input and return the bitmap containing the hash value. It is mainly used for the stream load task to import non integer fields into the bitmap field of the StarRocks table. For example:

```bash
cat data | curl --location-trusted -u user:passwd -T - \
    -H "columns: dt,page,device_id, device_id=bitmap_hash(device_id)" \
    http://host:8410/api/test/testDb/_stream_load
```

## example

```Plain Text
MySQL > select bitmap_count(bitmap_hash('hello'));
+------------------------------------+
| bitmap_count(bitmap_hash('hello')) |
+------------------------------------+
|                                  1 |
+------------------------------------+
```

## keyword

BITMAP_HASH,BITMAP
