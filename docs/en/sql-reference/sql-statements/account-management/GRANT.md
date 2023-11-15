# GRANT

## description

The GRANT command is used to give the specified user or role a specified permission.

### Syntax

GRANT privilege_list ON db_name[.tbl_name] TO user_identity [ROLE role_name]

GRANT privilege_list ON RESOURCE resource_name TO user_identity [ROLE role_name]

permission_list is a list of permissions that need to be granted and that are separated by commas. Currently, StarRocks supports the following permissions:

```plain text
NODE_PRIV: Operational permissions of cluster nodes, including taking nodes offline and online. This permission is only granted to root users. 
ADMIN_PRIV: All permissions except NODE_PRIV.
GRANT_PRIV: The permission to alter operational permissions, including the creating and dropping users and roles, granting, revoking, setting passwords and etc. 
SELECT_PRIV: Read permissions for specified libraries or tables. 
LOAD_PRIV: Import permissions for specified libraries or tables.
ALTER_PRIV: Schema change permissions for specified libraries or tables. 
CREATE_PRIV: Creation permissions for specified libraries or tables.
DROP_PRIV: Deletion permissions for specified libraries or tables.
USAGE_PRIV: Usage permissions for specified resources. 
```

ALL and READ_WRITE in previous permissions will be transformed as SELECT_PRIV, LOAD_PRIV, ALTER_PRIV, CREATE_PRIV, DROP_PRIV; READ_ONLY will be transformed as SELECT_PRIV.

Types of permissions:

```plain text
1. Node permission: NODE_PRIV
2. Library and table permissions: SELECT_PRIV, LOAD_PRIV, ALTER_PRIV, CREATE_PRIV, DROP_PRIV
3. Resource permission: USAGE_PRIV
```

db_name[.tbl_name] supports the following three forms:

```plain text
1. *.* permission applies to all libraries and all tables in these libraries
2. db.* permission applies to all tables under specified libraries 
3. db.tbl permission applies to specified tables under specified libraries 
```

Here, the specified libraries and tables may not yet exist.

resource_name supports the following two forms:

```plain text
1. * permission applies to all resources
2. resource permission applies to specified resources
```

Here, specified resources may not yet exist.

```plain text
user_identity：
```

Here, the syntax of user_identity is the same as that of CREATE USER and must be the user_identity previously created through CREATE USER. The host in user_identity could be a domain name. If it is, there may be a short one-minute delay before the permission comes into effect.

Permissions may also be granted to specified ROLE, which will be automatically created if it does not exist.

## example

1. Grant permissions to all libraries and tables to users.

    ```sql
    GRANT SELECT_PRIV ON *.* TO 'jack'@'%';
    ```

2. Grant permission to specified libraries and tables to users.

    ```sql
    GRANT SELECT_PRIV,ALTER_PRIV,LOAD_PRIV ON db1.tbl1 TO 'jack'@'192.8.%';
    ```

3. Grant permissions to specified libraries and tables to roles.

    ```sql
    GRANT LOAD_PRIV ON db1.* TO ROLE 'my_role';
    ```

4. Grant permissions to specified libraries and tables to roles.

    ```sql
    GRANT USAGE_PRIV ON RESOURCE * TO 'jack'@'%';
    ```

5. Grant permission to specified resources to users.

    ```sql
    GRANT USAGE_PRIV ON RESOURCE 'spark_resource' TO 'jack'@'%';
    ```

6. Grant permission to specified resources to roles.

    ```sql
    GRANT USAGE_PRIV ON RESOURCE 'spark_resource' TO ROLE 'my_role';
    ```

## keyword

GRANT
