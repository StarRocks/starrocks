---
displayed_sidebar: "English"
---

# GRANT

import UserPrivilegeCase from '../../../assets/commonMarkdown/userPrivilegeCase.md'

## Description

Grants one or more privileges on specific objects to a user or a role.

Grants roles to users or other roles.

For more information about the privileges that can be granted, see [Privilege items](../../../administration/privilege_item.md).

After a GRANT operation is performed, you can run [SHOW GRANTS](./SHOW_GRANTS.md) to view detailed privilege information or run [REVOKE](REVOKE.md) to revoke a privilege or role.

Before a GRANT operation is performed, make sure that the related user or role has been created. For more information, see [CREATE USER](./CREATE_USER.md) and [CREATE ROLE](./CREATE_ROLE.md).

:::tip

- Only users with the `user_admin` role can grant any privilege to other users and roles.
- After a role is granted to a user, you must run [SET ROLE](SET_ROLE.md) to activate this role before you perform operations as this role. If you want all default roles to be activated upon login, run [ALTER USER](ALTER_USER.md) or [SET DEFAULT ROLE](SET_DEFAULT_ROLE.md). If you want all privileges in the system to be activated for all users upon login, set the global variable `SET GLOBAL activate_all_roles_on_login = TRUE;`.
- Common users can only grant privileges that have the `WITH GRANT OPTION` keyword to other users and roles.
:::

## Syntax

### Grant privileges to roles or users

#### System

```SQL
GRANT
    { CREATE RESOURCE GROUP | CREATE RESOURCE | CREATE EXTERNAL CATALOG | REPOSITORY | BLACKLIST | FILE | OPERATE } 
    ON SYSTEM
    TO { ROLE | USER} {<role_name>|<user_identity>} [ WITH GRANT OPTION ]
```

#### Resource group

```SQL
GRANT
    { ALTER | DROP | ALL [PRIVILEGES] } 
    ON { RESOURCE GROUP <resource_group_name> [, <resource_group_name >,...] ｜ ALL RESOURCE GROUPS} 
    TO { ROLE | USER} {<role_name>|<user_identity>} [ WITH GRANT OPTION ]
```

#### Resource

```SQL
GRANT
    { USAGE | ALTER | DROP | ALL [PRIVILEGES] } 
    ON { RESOURCE <resource_name> [, < resource_name >,...] ｜ ALL RESOURCES} 
    TO { ROLE | USER} {<role_name>|<user_identity>} [ WITH GRANT OPTION ]
```

#### Global UDF

```SQL
GRANT
    { USAGE | DROP | ALL [PRIVILEGES]} 
    ON { GLOBAL FUNCTION <function_name>(input_data_type) [, < function_name >(input_data_type),...]    
       | ALL GLOBAL FUNCTIONS }
    TO { ROLE | USER} {<role_name>|<user_identity>} [ WITH GRANT OPTION ]
```

Example: `GRANT usage ON GLOBAL FUNCTION a(string) to kevin;`

#### Internal catalog

```SQL
GRANT
    { USAGE | CREATE DATABASE | ALL [PRIVILEGES]} 
    ON CATALOG default_catalog
    TO { ROLE | USER} {<role_name>|<user_identity>} [ WITH GRANT OPTION ]
```

#### External catalog

```SQL
GRANT
   { USAGE | DROP | ALL [PRIVILEGES] } 
   ON { CATALOG <catalog_name> [, <catalog_name>,...] | ALL CATALOGS}
   TO { ROLE | USER} {<role_name>|<user_identity>} [ WITH GRANT OPTION ]
```

#### Database

```SQL
GRANT
    { ALTER | DROP | CREATE TABLE | CREATE VIEW | CREATE FUNCTION | CREATE MATERIALIZED VIEW | ALL [PRIVILEGES] } 
    ON { DATABASE <database_name> [, <database_name>,...] | ALL DATABASES }
    TO { ROLE | USER} {<role_name>|<user_identity>} [ WITH GRANT OPTION ]
```

* You must first run SET CATALOG before you run this command.

#### Table

```SQL
GRANT
    { ALTER | DROP | SELECT | INSERT | EXPORT | UPDATE | DELETE | ALL [PRIVILEGES]} 
    ON { TABLE <table_name> [, < table_name >,...]
       | ALL TABLES} IN 
           { { DATABASE <database_name> [, <database_name>,...] } | ALL DATABASES }
    TO { ROLE | USER} {<role_name>|<user_identity>} [ WITH GRANT OPTION ]
```

* You must first run SET CATALOG before you run this command.
* You can also use `<db_name>.<table_name>` to represent a table.
* You can grant the SELECT privilege on all tables in Internal and External Catalogs to read data from these tables.

  ```SQL
  GRANT <priv> ON TABLE <db_name>.<table_name> TO {ROLE <role_name> | USER <user_name>}
  ```

#### View

```SQL
GRANT  
    { ALTER | DROP | SELECT | ALL [PRIVILEGES]} 
    ON { VIEW <view_name> [, < view_name >,...]
       ｜ ALL VIEWS} IN 
           { { DATABASE <database_name> [, <database_name>,...] } | ALL DATABASES }
    TO { ROLE | USER} {<role_name>|<user_identity>} [ WITH GRANT OPTION ]
```

* You must first run SET CATALOG before you run this command.
* You can also use `<db_name>.<view_name>` to represent a view.

  ```SQL
  GRANT <priv> ON VIEW <db_name>.<view_name> TO {ROLE <role_name> | USER <user_name>}
  ```
  
#### Materialized view

```SQL
GRANT
    { SELECT | ALTER | REFRESH | DROP | ALL [PRIVILEGES]} 
    ON { MATERIALIZED VIEW <mv_name> [, < mv_name >,...]
       ｜ ALL MATERIALIZED VIEWS} IN 
           { { DATABASE <database_name> [, <database_name>,...] } | ALL DATABASES }
    TO { ROLE | USER} {<role_name>|<user_identity>} [ WITH GRANT OPTION ]
```

* You must first run SET CATALOG before you run this command.
* You can also use `<db_name>.<mv_name>` to represent an mv.

  ```SQL
  GRANT <priv> ON MATERIALIZED VIEW <db_name>.<mv_name> TO {ROLE <role_name> | USER <user_name>}
  ```
  
#### Function

```SQL
GRANT
    { USAGE | DROP | ALL [PRIVILEGES]} 
    ON { FUNCTION <function_name>(input_data_type) [, < function_name >(input_data_type),...]
       ｜ ALL FUNCTIONS} IN 
           { { DATABASE <database_name> [, <database_name>,...] } | ALL DATABASES }
    TO { ROLE | USER} {<role_name>|<user_identity>} [ WITH GRANT OPTION ]
```

- You must first run SET CATALOG before you run this command.
- You can also use `<db_name>.<function_name>` to represent a function.

  ```SQL
  GRANT <priv> ON FUNCTION <db_name>.<function_name> TO {ROLE <role_name> | USER <user_name>}
  ```

#### User

```SQL
GRANT IMPERSONATE
ON USER <user_identity>
TO USER <user_identity_1> [ WITH GRANT OPTION ]
```

### Grant roles to roles or users

```SQL
GRANT <role_name> [,<role_name>, ...] TO ROLE <role_name>;
GRANT <role_name> [,<role_name>, ...] TO USER <user_identity>;
```

## Examples

Example 1: Grant the privilege to read data from all tables in all databases to user `jack`.

```SQL
GRANT SELECT ON *.* TO 'jack'@'%';
```

Example 2: Grant the privilege to load data into all tables of database `db1` to role `my_role`.

```SQL
GRANT INSERT ON db1.* TO ROLE 'my_role';
```

Example 3: Grant the privileges to read, update, and load data into table `tbl1` of database `db1` to user `jack`.

```SQL
GRANT SELECT,ALTER,INSERT ON db1.tbl1 TO 'jack'@'192.8.%';
```

Example 4: Grant the privilege to use all the resources to user `jack`.

```SQL
GRANT USAGE ON RESOURCE * TO 'jack'@'%';
```

Example 5: Grant the privilege to use resource `spark_resource` to user `jack`.

```SQL
GRANT USAGE ON RESOURCE 'spark_resource' TO 'jack'@'%';
```

Example 6: Grant the privilege to use resource `spark_resource` to role `my_role`.

```SQL
GRANT USAGE ON RESOURCE 'spark_resource' TO ROLE 'my_role';
```

Example 7: Grant the privilege to read data from table `sr_member` to user `jack` and allow user `jack` to grant this privilege to other users or roles (by specifying WITH GRANT OPTION).

```SQL
GRANT SELECT ON TABLE sr_member TO USER jack@'172.10.1.10' WITH GRANT OPTION;
```

Example 8: Grant system-defined roles `db_admin`, `user_admin`, and `cluster_admin` to user `user_platform`.

```SQL
GRANT db_admin, user_admin, cluster_admin TO USER user_platform;
```

Example 9: Allow user `jack` to perform operations as user `rose`.

```SQL
GRANT IMPERSONATE ON USER 'rose'@'%' TO USER 'jack'@'%';
```

## Best practices

### Customize roles based on scenarios

<UserPrivilegeCase />


For the best practices of multi-service access control, see [Multi-service access control](../../../administration/User_privilege.md#multi-service-access-control).
