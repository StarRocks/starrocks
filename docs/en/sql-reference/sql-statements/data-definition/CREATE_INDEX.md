---
displayed_sidebar: "English"
---

# CREATE INDEX

## Description

<<<<<<< HEAD
This statement is used to create indexes.
=======
This statement is used to create indexes. You can use this statement to create only Bitmap indexes. For usage notes and scenarios of Bitmap indexes, see [Bitmap index](../../../table_design/indexes/Bitmap_index.md).
>>>>>>> fda92b9cc9 ([Doc] Fix typo in Create Index Page (#47918))

Syntax:

```sql
CREATE INDEX index_name ON table_name (column [, ...],) [USING BITMAP] [COMMENT'balabala']
```

Note:

1. Only support bitmap index in the current version.
2. Create BITMAP index only in a single column.

## Examples

1. Create bitmap index for `siteid` on `table1`.

    ```sql
    CREATE INDEX index_name ON table1 (siteid) USING BITMAP COMMENT 'balabala';
    ```
