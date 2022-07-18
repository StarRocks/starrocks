# DROP DATABASE

## description

This statement is used to drop database.

Syntax:

```sql
DROP DATABASE [IF EXISTS] [FORCE] db_name;
```

Note:

1. After executing DROP DATABASE for a while, you can restore the dropped database through RECOVER statement. See RECOVER statement for more detail.
2. If DROP DATABASE FORCE is executed, the database will be deleted directly and cannot be recovered without checking whether there are unfinished activities in the database.  Generally this operation is not recommended.

## example

1. Drop database db_text.

    ```sql
    DROP DATABASE db_test;
    ```

## keyword

DROP,DATABASE
