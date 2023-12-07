---
displayed_sidebar: "English"
---

# TRUNCATE TABLE

## Description

This statement is used to truncate the specified table and partition data.

Syntax:

```sql
TRUNCATE TABLE [db.]tbl[ PARTITION(p1, p2, ...)]
```

Note:

1. This statement is used to truncate data while retaining tables or partitions.
2. Unlike DELETE, this statement can only empty the specified tables or partitions as a whole, and filtering conditions cannot be added.
3. Unlike DELETE, using this method to clear data will not affect query performance.
4. This statement directly deletes data. The deleted data cannot be recovered.
5. The table on which you perform this operation must be in the NORMAL state. For example, you cannot perform TRUNCATE TABLE on a table with SCHEMA CHANGE going on.

## Examples

1. Truncate table `tbl` under `example_db`.

    ```sql
    TRUNCATE TABLE example_db.tbl;
    ```

2. Truncate partitions `p1` and `p2` in table `tbl`.

    ```sql
    TRUNCATE TABLE tbl PARTITION(p1, p2);
    ```
