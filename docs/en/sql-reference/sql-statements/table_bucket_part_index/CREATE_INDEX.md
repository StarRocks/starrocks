---
displayed_sidebar: docs
---

# CREATE INDEX

## Description

This statement is used to create indexes. You can use this statement to create only Bitmap indexes. For usage notes and scenarios of Bitmap indexes, see [Bitmap index](../../../table_design/indexes/Bitmap_index.md).

:::tip

This operation requires the ALTER privilege on the target table. You can follow the instructions in [GRANT](../account-management/GRANT.md) to grant this privilege.

:::

## Syntax

```SQL
CREATE INDEX index_name ON table_name (column_name) [USING BITMAP] [COMMENT'']
```

## Parameter

| **Parameter** | **Required**   | **Description**                                                              |
| ------------- | -------------- | ---------------------------------------------------------------------------- |
| index_name    | Yes            | The index name. For naming conventions, see [System Limits](../../System_limit.md). |
| table_name    | Yes            | The name of the table.                                                       |
| column_name   | Yes            | The name of the column to build index on. One column can have only one BITMAP index. If a column already has an index, you cannot create one more index on it. |
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

Create an index `index` on the `item_id` column of `sales_records`。

```SQL
CREATE INDEX index3 ON sales_records (item_id) USING BITMAP COMMENT '';
```

## Relevant SQLs

- [SHOW INDEX](SHOW_INDEX.md)
- [DROP INDEX](DROP_INDEX.md)
