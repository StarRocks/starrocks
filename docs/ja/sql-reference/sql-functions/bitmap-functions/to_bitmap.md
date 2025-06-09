---
displayed_sidebar: docs
---

# to_bitmap

## Description

入力は 0 から 18446744073709551615 の範囲の符号なし bigint で、出力はこの要素を含む bitmap です。この関数は主に、StarRocks テーブルの bitmap フィールドに整数フィールドをインポートするための stream ロード タスクに使用されます。例えば:

```bash
cat data | curl --location-trusted -u user:passwd -T - \
    -H "columns: dt,page,user_id, user_id=to_bitmap(user_id)" \
    http://host:8410/api/test/testDb/_stream_load
```

## Syntax

```Haskell
BITMAP TO_BITMAP(expr)
```

## Examples

```Plain Text
MySQL > select bitmap_count(to_bitmap(10));
+-----------------------------+
| bitmap_count(to_bitmap(10)) |
+-----------------------------+
|                           1 |
+-----------------------------+

select bitmap_to_string(to_bitmap(10));
+---------------------------------+
| bitmap_to_string(to_bitmap(10)) |
+---------------------------------+
| 10                              |
+---------------------------------+

select bitmap_to_string(to_bitmap(-5));
+---------------------------------+
| bitmap_to_string(to_bitmap(-5)) |
+---------------------------------+
| NULL                            |
+---------------------------------+

select bitmap_to_string(to_bitmap(null));
+-----------------------------------+
| bitmap_to_string(to_bitmap(NULL)) |
+-----------------------------------+
| NULL                              |
+-----------------------------------+
```

## keyword

TO_BITMAP,BITMAP