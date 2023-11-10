# SHOW TABLE STATUS

## Description

This statement is used to view some of the information in Table.

Syntax:

```sql
SHOW TABLE STATUS
[FROM db] [LIKE "pattern"]
```

> Note
>
> This statement is mainly compatible with MySQL syntax. At present, it only shows a few information, such as Comment.

## Examples

1. View all the information of tables under the current database.

    ```SQL
    SHOW TABLE STATUS;
    ```

2. View all the information of tables whose names contain example and who are under specified databases.

    ```SQL
    SHOW TABLE STATUS FROM db LIKE "%example%";
    ```
