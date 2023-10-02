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

<<<<<<< HEAD
=======
| **Field**    | **Description**                                              |
| ------------ | ------------------------------------------------------------ |
| UserIdentity | The user identity, which is displayed when you query the privileges of a user. |
| RoleName     | The role name, which is displayed when you query the privileges of a role. |
| Catalog      | The catalog name.<br />`default` is returned if the GRANT operation is performed on the StarRocks internal catalog.<br />The name of the external catalog is returned if the GRANT operation is performed on an external catalog.<br />`NULL` is returned if the operation shown in the `Grants` column is assigning roles. |
| Grants       | The specific GRANT operation.                                |

>>>>>>> 7beebf08d1 ([Doc] MDX 2 compatibility (#32008))
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
