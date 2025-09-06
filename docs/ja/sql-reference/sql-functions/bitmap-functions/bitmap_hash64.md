---
displayed_sidebar: docs
---

# bitmap_hash64

任意の入力データに対して64ビットのハッシュ値を計算し、そのハッシュ値を含むビットマップを返します。主に、Stream Load 経由で StarRocks テーブルの BITMAP フィールドに整数以外のデータをロードするために使用されます。例：

```bash
cat data | curl --location-trusted -u user:passwd -T - \
    -H "columns: dt,page,device_id, device_id=bitmap_hash64(device_id)" \
    http://<fe_host>:8030/api/test_db/tbl1/_stream_load
```

## 構文

```Haskell
BITMAP BITMAP_HASH64(expr)
```

## パラメータ

`expr`: 入力は任意のデータ型であることができます。

## 戻り値

BITMAP データ型の値を返します。

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