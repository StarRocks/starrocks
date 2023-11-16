---
displayed_sidebar: "Chinese"
---

# bitmap_empty

## 功能

返回一个空 bitmap，主要用于 insert 或 stream load 时填充默认值，如下例:

```bash
cat data | curl --location-trusted -u user:passwd -T - \
    -H "columns: dt,page,v1,v2=bitmap_empty()" \
    http://host:8410/api/test/testDb/_stream_load
```

## 语法

```Haskell
BITMAP_EMPTY()
```

## 参数说明

无

## 返回值说明

返回值的数据类型为 BITMAP。

## 示例

```Plain Text
MySQL > select bitmap_count(bitmap_empty());
+------------------------------+
| bitmap_count(bitmap_empty()) |
+------------------------------+
|                            0 |
+------------------------------+
```
