# to_bitmap

## description

### Syntax

```Haskell
BITMAP TO_BITMAP(expr)
```

输入为取值在 0 ~ 18446744073709551615 区间的 unsigned bigint ，输出为包含该元素的bitmap。
该函数主要用于stream load任务将整型字段导入StarRocks表的bitmap字段。例如

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
