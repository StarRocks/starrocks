---
displayed_sidebar: docs
---

# column_size

列の非圧縮サイズをバイト単位で返します。この関数は `[_META_]` ヒントと組み合わせて使用し、セグメントファイルのメタデータを検査して列のストレージ特性を分析します。

## 構文

```SQL
column_size(column_name)
```

## パラメータ

- `column_name`: 非圧縮サイズを取得したい列の名前。

## 戻り値

列の非圧縮サイズをバイト単位で BIGINT 値として返します。

## 使用上の注意

- この関数は `[_META_]` ヒントと組み合わせて使用し、メタデータ情報にアクセスする必要があります。
- この関数は META_SCAN オペレータを使用して、基盤となるセグメントファイルのメタデータをスキャンします。
- すべてのセグメントにわたる列データの総メモリフットプリントを返します。
- 複雑なデータ型（JSON、ARRAY、MAP、STRUCT）の場合、この関数はすべてのサブ列のサイズを再帰的に計算します。

## 例

例 1: シンプルな列の非圧縮サイズを取得します。

```sql
SELECT column_size(id) FROM users [_META_];
```

例 2: 複雑なデータ型の非圧縮サイズを取得します。

```sql
-- JSON 列のサイズを取得
SELECT column_size(json_data) FROM events [_META_];

-- ARRAY 列のサイズを取得
SELECT column_size(tags) FROM products [_META_];

-- MAP 列のサイズを取得
SELECT column_size(attributes) FROM items [_META_];

-- STRUCT 列のサイズを取得
SELECT column_size(address) FROM customers [_META_];
```

例 3: 異なる列間で列サイズを比較します。

```sql
SELECT 
    column_size(name) as name_size,
    column_size(description) as desc_size,
    column_size(metadata) as meta_size
FROM products [_META_];
```

例 4: 集約関数と組み合わせて使用します。

```sql
SELECT 
    SUM(column_size(id)) as total_id_size,
    AVG(column_size(content)) as avg_content_size
FROM articles [_META_];
```

## 関連関数

- [column_compressed_size](./column_compressed_size.md): 列の圧縮サイズを返します。
- [META_SCAN オペレータ](../../../using_starrocks/Cost_based_optimizer.md): メタデータスキャンに関する詳細情報。