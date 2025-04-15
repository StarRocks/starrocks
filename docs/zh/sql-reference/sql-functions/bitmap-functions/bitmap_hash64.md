---
displayed_sidebar: docs
---

# bitmap_hash64

对任意类型的输入计算 64 位的哈希值，返回包含该哈希值的 bitmap。

主要用于 stream load 导入中将非整型字段导入到 StarRocks 表中的 bitmap 字段，如下例:

```bash
cat data | curl --location-trusted -u user:passwd -T - \
    -H "columns: dt,page,device_id, device_id=bitmap_hash64(device_id)" \
    http://host:8410/api/test/testDb/_stream_load
```

## 语法

```Haskell
BITMAP_HASH64(expr)
```

## 参数说明

`expr`: 可以是任意数据类型。

## 返回值说明

返回值的数据类型为 BITMAP。

## 示例

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
