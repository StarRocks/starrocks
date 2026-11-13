---
displayed_sidebar: docs
<<<<<<< HEAD
=======
description: "任意のタイプの入力に対して32ビットのハッシュ値を計算し、ビットマップを返します。"
products: [oss, enterprise, cloud, anywhere]
>>>>>>> 7fbe5e8 ([Doc] Add the products frontmatter line to the docs pages (#83243))
---

# bitmap_hash

任意のタイプの入力に対して32ビットのハッシュ値を計算し、そのハッシュ値を含むビットマップを返します。これは主に、StarRocks テーブルのビットマップフィールドに非整数フィールドをインポートするための stream load タスクに使用されます。例えば：

```bash
cat data | curl --location-trusted -u user:passwd -T - \
    -H "columns: dt,page,device_id, device_id=bitmap_hash(device_id)" \
    http://host:8410/api/test/testDb/_stream_load
```

## 構文

```plaintext
BITMAP BITMAP_HASH(expr)
```

## 例

```Plain Text
MySQL > select bitmap_count(bitmap_hash('hello'));
+------------------------------------+
| bitmap_count(bitmap_hash('hello')) |
+------------------------------------+
|                                  1 |
+------------------------------------+

select bitmap_to_string(bitmap_hash('hello'));
+----------------------------------------+
| bitmap_to_string(bitmap_hash('hello')) |
+----------------------------------------+
| 1321743225                             |
+----------------------------------------+
```

## キーワード

BITMAP_HASH,BITMAP