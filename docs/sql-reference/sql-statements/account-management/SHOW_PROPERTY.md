# SHOW PROPERTY

## Description

This statement is used to view the user's properties

Syntax:

```sql
SHOW PROPERTY [FOR user] [LIKE key]
```

## Examples

1. View the properties of jack user

    ```sql
    SHOW PROPERTY FOR 'jack'
    ```

2. View the properties related to the cluster imported by Jack user

    ```sql
    SHOW PROPERTY FOR 'jack' LIKE '%load_cluster%'
    ```
