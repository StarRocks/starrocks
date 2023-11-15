# CANCEL LOAD

## description

This statement is used to cancel the loading jobs for the specified load label. This is an asynchronous operation, which returns if the task is submitted successfully. After execution, you can use the SHOW LOAD command to check progress.

Syntax:

```sql
CANCEL LOAD
[FROM db_name]
WHERE LABEL = "load_label";
```

## example

1. Cancel the loading job labelled example_db_test_load_label on the database example_db.

    ```sql
    CANCEL LOAD
    FROM example_db
    WHERE LABEL = "example_db_test_load_label";
    ```

## keyword

CANCEL,LOAD
