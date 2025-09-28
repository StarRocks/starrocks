---
displayed_sidebar: docs
---

# column_size & column_compressed_size

これらの関数は、ストレージ分析と最適化のためにテーブル列のサイズ情報を返します。両方の関数は `[_META_]` ヒントと組み合わせて使用し、セグメントファイルのメタデータを検査します。

## column_size

列の非圧縮サイズをバイト単位で返します。

### 構文

```SQL
column_size(column_name)
```

### パラメータ

- `column_name`: 非圧縮サイズを取得したい列の名前。

### 戻り値

列の非圧縮サイズをバイト単位で BIGINT 値として返します。

## column_compressed_size

列の圧縮サイズをバイト単位で返します。

### 構文

```SQL
column_compressed_size(column_name)
```

### パラメータ

- `column_name`: 圧縮サイズを取得したい列の名前。

### 戻り値

列の圧縮サイズをバイト単位で BIGINT 値として返します。

## 使用上の注意

- 両方の関数は `[_META_]` ヒントと組み合わせて使用し、メタデータ情報にアクセスする必要があります。
- これらの関数は META_SCAN オペレータを使用して、基盤となるセグメントファイルのメタデータをスキャンします。
- 複雑なデータ型（JSON、ARRAY、MAP、STRUCT）の場合、これらの関数はすべてのサブ列のサイズを再帰的に計算します。
- `column_size` は、すべてのセグメントにわたる列データの総メモリフットプリントを返します。
- `column_compressed_size` は、序数インデックス範囲によるデータページサイズの合計によって計算される総圧縮サイズを返します。

## 例

```sql
-- 列の非圧縮サイズと圧縮サイズを取得
SELECT 
    column_size(name) as name_decompressed_size,
    column_compressed_size(name) as name_compressed_size,
    column_size(description) as desc_decompressed_size,
    column_compressed_size(description) as desc_compressed_size
FROM products [_META_];
```

## 関連関数

- [META_SCAN オペレータ](../../../using_starrocks/Cost_based_optimizer.md): メタデータスキャンに関する詳細情報。