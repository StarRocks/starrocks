---
displayed_sidebar: docs
---

# CREATE INDEX

Creates indexes.

You can create the following indexes:
- [Bitmap index](../../../table_design/indexes/Bitmap_index.md)
- [N-Gram bloom filter index](../../../table_design/indexes/Ngram_Bloom_Filter_Index.md)
- [Full-Text inverted index](../../../table_design/indexes/inverted_index.md)
- [Vector index](../../../table_design/indexes/vector_index.md)

For detailed instructions and examples on creating these indexes, see the corresponding tutorials listed above.

:::tip

This operation requires the ALTER privilege on the target table. You can follow the instructions in [GRANT](../account-management/GRANT.md) to grant this privilege.

:::

## Syntax

```SQL
CREATE INDEX index_name ON table_name (column_name) 
[USING { BITMAP | NGRAMBF | GIN | VECTOR } ] 
[(index_property)] 
[COMMENT '<comment>']
```

## Parameter

| **Parameter** | **Required**   | **Description**                                                              |
| ------------- | -------------- | ---------------------------------------------------------------------------- |
| index_name    | Yes            | The index name. For naming conventions, see [System Limits](../../System_limit.md). |
| table_name    | Yes            | The name of the table.                                                       |
| column_name   | Yes            | The name of the column to build index on. One column can have only one index. If a column already has an index, you cannot create one more index on it. |
| USING         | No             | The type of the index to create. Valid values: <ul><li>BITMAP (Default)</li><li>NGRAMBF</li><li>GIN</li><li>VECTOR</li></ul> |
| index_property | No            | The properties of the index to create. For `NGRAMBF`, `GIN`, and `VECTOR`, you must specify the corresponding properties. For detailed instructions, see the corresponding tutorials. |
| COMMENT       | No             | The comment for the index.                                                   |

## Examples

Create a table `sales_records` as follows:

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

Create a bitmap index `index` on the `item_id` column of `sales_records`。

```SQL
CREATE INDEX index ON sales_records (item_id) USING BITMAP COMMENT '';
```

## Relevant SQLs

- [SHOW INDEX](SHOW_INDEX.md)
- [DROP INDEX](DROP_INDEX.md)
