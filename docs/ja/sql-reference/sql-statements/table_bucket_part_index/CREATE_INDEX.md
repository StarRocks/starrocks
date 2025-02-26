---
displayed_sidebar: docs
---

# CREATE INDEX

## 説明

このステートメントはインデックスを作成するために使用されます。このステートメントを使用して作成できるのはビットマップインデックスのみです。ビットマップインデックスの使用上の注意点やシナリオについては、[Bitmap index](../../../table_design/indexes/Bitmap_index.md)を参照してください。

:::tip

この操作には、対象テーブルに対するALTER権限が必要です。[GRANT](../account-management/GRANT.md)の指示に従って、この権限を付与できます。

:::

## 構文

```SQL
CREATE INDEX index_name ON table_name (column_name) [USING BITMAP] [COMMENT'']
```

## パラメータ

| **パラメータ** | **必須**      | **説明**                                                                      |
| ------------- | -------------- | ----------------------------------------------------------------------------- |
| index_name    | Yes            | インデックス名。命名規則については、[System Limits](../../System_limit.md)を参照してください。 |
| table_name    | Yes            | テーブルの名前。                                                              |
| column_name   | Yes            | インデックスを作成する列の名前。1つの列には1つのビットマップインデックスしか作成できません。既にインデックスがある列には、新たにインデックスを作成することはできません。 |
| COMMENT       | No             | インデックスに対するコメント。                                                |

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

`sales_records` の `item_id` 列にインデックス `index` を作成します。

```SQL
CREATE INDEX index3 ON sales_records (item_id) USING BITMAP COMMENT '';
```

## 関連SQL

- [SHOW INDEX](SHOW_INDEX.md)
- [DROP INDEX](DROP_INDEX.md)