# SHOW GRANTS

## Description

This statement is used to view user permissions.

Syntax:

```sql
SHOW [ALL] GRANTS [FOR user_identity]
```

Note:

```plain text
1. SHOW ALL GRANTS can view the privileges of all users. 
2. When user_identity is specified, the permissions of the user thus specified will be showed. Meanwhile, this user_identity must be created through CREATE USER command. 
3. When user_identity is not specified, the permissions of the current user will be showed. 
```

## Examples

1. View all users' permissions.

    ```sql
    SHOW ALL GRANTS; 
    ```

2. View specified users' permissions.

    ```sql
    SHOW GRANTS FOR jack@'%';
    ```

3. View the current user's permissions.

    ```sql
    SHOW GRANTS;
    ```
