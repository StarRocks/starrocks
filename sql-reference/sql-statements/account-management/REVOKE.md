# REVOKE

## description

REVOKE command is used to take back specified permissions of specified users or roles.

```plain text
REVOKE privilege_list ON db_name[.tbl_name] FROM user_identity [ROLE role_name]

REVOKE privilege_list ON RESOURCE resource_name FROM user_identity [ROLE role_name]
```

user_identity：

Here, the syntax of user_identity is the same as that of CREATE USER and must be the user_identity previously created through CREATE USER. The host in user_identity could be a domain name. If it is, there may be a short one-minute delay before the revocation comes into effect.

The permission of specified ROLE can also be revoked with the precondition that the executed role must already exist.

## example

1. Revoke permission to database testDb from user jack

    ```sql
    
    REVOKE SELECT_PRIV ON db1.* FROM 'jack'@'192.%';
    ```

2. Revoke usage permission to spark_resource from user jack

    ```sql
    REVOKE USAGE_PRIV ON RESOURCE 'spark_resource' FROM 'jack'@'192.%';
    ```

## keyword

REVOKE
