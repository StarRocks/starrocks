---
displayed_sidebar: docs
---

# CREATE INDEX

## 説明

このステートメントはインデックスを作成するために使用されます。

:::tip

この操作には、対象テーブルに対する ALTER 権限が必要です。この権限を付与するには、 [GRANT](../account-management/GRANT.md) の指示に従ってください。

:::

## 構文

```SQL
CREATE INDEX index_name ON table_name (column_name) [USING BITMAP] [COMMENT'']
```

## パラメータ

| **パラメータ** | **必須**     | **説明**                                                                 |
| ------------- | -------------- | ------------------------------------------------------------------------ |
| index_name    | Yes            | インデックス名。命名規則については、 [System Limits](../../System_limit.md) を参照してください。 |
| table_name    | Yes            | テーブルの名前。                                                          |
| column_name   | Yes            | インデックスを作成するカラムの名前。1つのカラムには1つのビットマップインデックスしか作成できません。既にインデックスがあるカラムには新たにインデックスを作成することはできません。 |
| COMMENT       | No             | インデックスに対するコメント。                                            |

## 例

次のように `sales_records` テーブルを作成します。

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

`sales_records` の `item_id` カラムにインデックス `index` を作成します。

```SQL
CREATE INDEX index3 ON sales_records (item_id) USING BITMAP COMMENT '';
```

## 関連する SQL

- [SHOW INDEX](SHOW_INDEX.md)
- [DROP INDEX](DROP_INDEX.md)