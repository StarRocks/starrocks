---
displayed_sidebar: docs
---

# bitmap_hash64



Calculates a 64-bit hash value for any type of input and returns a bitmap containing the hash value. It is mainly used to load non-integer data into the BITMAP field of the StarRocks table via Stream Load. For example:

```bash
cat data | curl --location-trusted -u user:passwd -T - \
    -H "columns: dt,page,device_id, device_id=bitmap_hash64(device_id)" \
    http://<fe_host>:8030/api/test_db/tbl1/_stream_load
```

## Syntax

```Haskell
BITMAP BITMAP_HASH64(expr)
```

## Parameters

`expr`: The input can be of any data type.

## Return value

Returns a value of the BITMAP data type.

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
