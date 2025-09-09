---
displayed_sidebar: docs
---

# CREATE INDEX

インデックスを作成します。

次のインデックスを作成できます:
- [Bitmap index](../../../table_design/indexes/Bitmap_index.md)
- [N-Gram bloom filter index](../../../table_design/indexes/Ngram_Bloom_Filter_Index.md)
- [Full-Text inverted index](../../../table_design/indexes/inverted_index.md)
- [Vector index](../../../table_design/indexes/vector_index.md)

これらのインデックスの作成に関する詳細な手順と例については、上記の対応するチュートリアルを参照してください。

:::tip

この操作には、対象テーブルに対する ALTER 権限が必要です。この権限を付与するには、[GRANT](../account-management/GRANT.md) の指示に従ってください。

:::

## 構文

```SQL
CREATE INDEX index_name ON table_name (column_name) 
[USING { BITMAP | NGRAMBF | GIN | VECTOR } ] 
[(index_property)] 
[COMMENT '<comment>']
```

## パラメーター

| **パラメーター** | **必須**   | **説明**                                                              |
| ------------- | -------------- | ---------------------------------------------------------------------------- |
| index_name    | はい            | インデックス名。命名規則については、[System Limits](../../System_limit.md) を参照してください。 |
| table_name    | はい            | テーブルの名前。                                                       |
| column_name   | はい            | インデックスを構築する列の名前。1つの列には1つのインデックスしか持てません。既にインデックスがある列には、さらにインデックスを作成することはできません。 |
| USING         | いいえ             | 作成するインデックスのタイプ。有効な値: <ul><li>BITMAP (デフォルト)</li><li>NGRAMBF</li><li>GIN</li><li>VECTOR</li></ul> |
| index_property | いいえ            | 作成するインデックスのプロパティ。`NGRAMBF`、`GIN`、`VECTOR` の場合、対応するプロパティを指定する必要があります。詳細な手順については、対応するチュートリアルを参照してください。 |
| COMMENT       | いいえ             | インデックスに対するコメント。                                                   |

## 例

次のようにテーブル `sales_records` を作成します:

```SQL
CREATE TABLE sales_records
(
    record_id int,
    seller_id int,
    item_id int
)
DISTRIBUTED BY hash(record_id)
PROPERTIES (
    "replication_num" = "3"
);
```

`sales_records` の `item_id` 列にビットマップインデックス `index` を作成します。

```SQL
CREATE INDEX index ON sales_records (item_id) USING BITMAP COMMENT '';
```

## 関連する SQL

- [SHOW INDEX](SHOW_INDEX.md)
- [DROP INDEX](DROP_INDEX.md)