---
displayed_sidebar: docs
---

# bitmap_hash64

任意のタイプの入力に対して64ビットのハッシュ値を計算し、そのハッシュ値を含むビットマップを返します。これは主に、StarRocks テーブルのビットマップフィールドに非整数フィールドをインポートするための stream load タスクに使用されます。例えば：

```bash
cat data | curl --location-trusted -u user:passwd -T - \
    -H "columns: dt,page,device_id, device_id=bitmap_hash64(device_id)" \
    http://host:8410/api/test/testDb/_stream_load
```

## 構文

```Haskell
BITMAP BITMAP_HASH64(expr)
```

## 例

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

## キーワード

BITMAP_HASH64,BITMAP