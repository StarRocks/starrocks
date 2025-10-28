---
displayed_sidebar: docs
---

# bitmap_hash64

对任意类型的输入计算 64 位的哈希值，返回包含该哈希值的 Bitmap。

主要用于通过 Stream Load 将非整型数据导入到 StarRocks 表的 BITMAP 字段中。

```bash
cat data | curl --location-trusted -u user:passwd -T - \
    -H "columns: dt,page,device_id, device_id=bitmap_hash64(device_id)" \
    http://<fe_host>:8030/api/test_db/tbl1/_stream_load
```

## 语法

```Haskell
BITMAP_HASH64(expr)
```

## 参数说明

`expr`: 输入可以是任意数据类型。

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

## 关键词

BITMAP_HASH64,BITMAP