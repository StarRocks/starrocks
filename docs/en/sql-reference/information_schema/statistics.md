---
displayed_sidebar: docs
---

# statistics

:::note

This view does not apply to the available features in StarRocks.

:::

`statistics` provides information about table indexes.

The following fields are provided in `statistics`:

| **Field**     | **Description**                                              |
| ------------- | ------------------------------------------------------------ |
| TABLE_CATALOG | The name of the catalog to which the table containing the index belongs. This value is always `def`. |
| TABLE_SCHEMA  | The name of the database to which the table containing the index belongs. |
| TABLE_NAME    | The name of the table containing the index.                  |
| NON_UNIQUE    | 0 if the index cannot contain duplicates, 1 if it can.       |
| INDEX_SCHEMA  | The name of the database to which the index belongs.         |
| INDEX_NAME    | The name of the index. If the index is the primary key, the name is always `PRIMARY`. |
| SEQ_IN_INDEX  | The column sequence number in the index, starting with 1.    |
| COLUMN_NAME   | The column name. See also the description for the `EXPRESSION` column. |
| COLLATION     | How the column is sorted in the index. This can have values `A` (ascending), `D` (descending), or `NULL` (not sorted). |
| CARDINALITY   | An estimate of the number of unique values in the index.     |
| SUB_PART      | The index prefix. That is, the number of indexed characters if the column is only partly indexed, `NULL` if the entire column is indexed. |
| PACKED        | Indicates how the key is packed. `NULL` if it is not.        |
| NULLABLE      | Contains `YES` if the column may contain `NULL` values and `''` if not. |
| INDEX_TYPE    | The index method used (`BTREE`, `FULLTEXT`, `HASH`, `RTREE`). |
| COMMENT       | Information about the index not described in its own column, such as `disabled` if the index is disabled. |
| INDEX_COMMENT | Any comment provided for the index with a `COMMENT` attribute when the index was created. |
