# CANCEL RESTORE

## description

The statement is used to cancel an ongoing RESTORE task.

Syntax:

```sql
CANCEL RESTORE FROM db_name;
```

Note:

When the restore is abolished around the COMMIT or later stage, the restored tables may be inaccessible. At this point, only by performing the recovery operation again can you restore the data.

## example

1. Cancel the RESTORE task from example_db.

    ```sql
    CANCEL RESTORE FROM example_db;
    ```

## keyword

CANCEL, RESTORE
