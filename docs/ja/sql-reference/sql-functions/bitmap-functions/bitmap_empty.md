---
displayed_sidebar: docs
---

# bitmap_empty

## Description

空の bitmap を返します。主に挿入やストリーム ロードの際にデフォルト値を埋めるために使用されます。例えば:

```bash
cat data | curl --location-trusted -u user:passwd -T - \
    -H "columns: dt,page,v1,v2=bitmap_empty()" \
    http://host:8410/api/test/testDb/_stream_load
```

## Syntax

```Haskell
BITMAP BITMAP_EMPTY()
```

## Examples

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