# SHOW EXPORT

## Description

This statement is used to display the execution of the specified export task

Syntax:

```sql
SHOW EXPORT
[FROM db_name]
[
WHERE
[QUERYID = your_query_id]
[STATE = ["PENDING"|"EXPORTING"|"FINISHED"|"CANCELLED"]]
]
[ORDER BY ...]
[LIMIT limit]
```

Note：

```plain text
1. 1. If db_ name is not specified, use the current default db
2. If STATE is specified, the EXPORT status is matched
3. You can use ORDER BY to sort any combination of columns
4. If ILIMIT is specified, limit matching records will be displayed. Otherwise, all are displayed
```

## Examples

1. Show all export tasks of the default db

    ```sql
    SHOW EXPORT;
    ```

2. Display the export task of specifying db and query id

    ```sql
    SHOW EXPORT FROM example_db WHERE queryid = "921d8f80-7c9d-11eb-9342-acde48001122";
    ```

3. Displays the export tasks of the specified db, sorted in descending order by StartTime

    ```sql
    SHOW EXPORT FROM example_db ORDER BY StartTime DESC;
    ```

4. The export tasks of the specified db are displayed. The state is "exporting" and sorted in descending order by StartTime

    ```sql
    SHOW EXPORT FROM example_db WHERE STATE = "exporting" ORDER BY StartTime DESC;
    ```
