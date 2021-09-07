# bitmap_hash

## description

### Syntax

```Haskell
BITMAP BITMAP_HASH(expr)
```

对任意类型的输入计算32位的哈希值，返回包含该哈希值的bitmap。主要用于stream load任务将非整型字段导入StarRocks表的bitmap字段。例如

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
