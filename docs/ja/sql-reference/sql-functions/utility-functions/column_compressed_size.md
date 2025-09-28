---
displayed_sidebar: docs
---

# column_compressed_size

列の圧縮サイズをバイト単位で返します。この関数は `[_META_]` ヒントと組み合わせて使用し、セグメントファイルのメタデータを検査して列のストレージ効率を分析します。

## 構文

```SQL
column_compressed_size(column_name)
```

## パラメータ

- `column_name`: 圧縮サイズを取得したい列の名前。

## 戻り値

列の圧縮サイズをバイト単位で BIGINT 値として返します。

## 使用上の注意

- この関数は `[_META_]` ヒントと組み合わせて使用し、メタデータ情報にアクセスする必要があります。
- この関数は META_SCAN オペレータを使用して、基盤となるセグメントファイルのメタデータをスキャンします。
- すべてのセグメントにわたる列データの総圧縮サイズを返します。
- 複雑なデータ型（JSON、ARRAY、MAP、STRUCT）の場合、この関数はすべてのサブ列の圧縮サイズを再帰的に計算します。
- 圧縮サイズは、序数インデックス範囲によるデータページサイズの合計によって計算されます。

## 例

例 1: シンプルな列の圧縮サイズを取得します。

```sql
SELECT column_compressed_size(id) FROM users [_META_];
```

例 2: 複雑なデータ型の圧縮サイズを取得します。

```sql
-- JSON 列の圧縮サイズを取得
SELECT column_compressed_size(json_data) FROM events [_META_];

-- ARRAY 列の圧縮サイズを取得
SELECT column_compressed_size(tags) FROM products [_META_];

-- MAP 列の圧縮サイズを取得
SELECT column_compressed_size(attributes) FROM items [_META_];

-- STRUCT 列の圧縮サイズを取得
SELECT column_compressed_size(address) FROM customers [_META_];
```

例 3: 異なる列間で圧縮サイズを比較します。

```sql
SELECT 
    column_compressed_size(name) as name_compressed_size,
    column_compressed_size(description) as desc_compressed_size,
    column_compressed_size(metadata) as meta_compressed_size
FROM products [_META_];
```

例 4: 圧縮比を計算します。

```sql
SELECT 
    column_name,
    column_size(column_name) as decompressed_size,
    column_compressed_size(column_name) as compressed_size,
    ROUND((1 - column_compressed_size(column_name) / column_size(column_name)) * 100, 2) as compression_ratio_percent
FROM (
    SELECT 'name' as column_name FROM products [_META_]
    UNION ALL
    SELECT 'description' as column_name FROM products [_META_]
    UNION ALL  
    SELECT 'content' as column_name FROM products [_META_]
) t;
```

例 5: 集約関数と組み合わせて使用します。

```sql
SELECT 
    SUM(column_compressed_size(id)) as total_id_compressed_size,
    AVG(column_compressed_size(content)) as avg_content_compressed_size
FROM articles [_META_];
```

## 関連関数

- [column_size](./column_size.md): 列の非圧縮サイズを返します。
- [META_SCAN オペレータ](../../../using_starrocks/Cost_based_optimizer.md): メタデータスキャンに関する詳細情報。