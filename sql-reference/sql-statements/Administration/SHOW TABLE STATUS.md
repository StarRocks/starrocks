# SHOW TABLE STATUS

## description

This statement is used to view some of the information in Table.

Syntax:

```sql
SHOW TABLE STATUS
[FROM db] [LIKE "pattern"]
```

Note:

```PLAIN TEXT
1. 1. This statement is mainly compatible with MySQL grammar. At present, it only shows a few information, such as Comment. 
```

## example

1. View all the information of tables under the current database.

    ```SQL
    SHOW TABLE STATUS;
    ```

2. View all the information of tables whose names contain example and who are under specified databases.

    ```SQL
    SHOW TABLE STATUS FROM db LIKE "%example%";
    ```

## keyword

SHOW,TABLE,STATUS
