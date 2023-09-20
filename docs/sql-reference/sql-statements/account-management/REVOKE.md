# REVOKE

## Description

Revokes specific privileges or roles from a user or a role. For the privileges supported by StarRocks, see [Privileges supported by StarRocks](../../../administration/privilege_item.md).

> NOTE: Only the `user_admin` role can perform this operation.

## Syntax

### Revoke privileges

The privileges that can be revoked are object-specific. The following part describes syntax based on objects.

#### System

```SQL
REVOKE
    { CREATE RESOURCE GROUP | CREATE RESOURCE | CREATE EXTERNAL CATALOG | REPOSITORY | BLACKLIST | FILE | OPERATE } 
    ON SYSTEM
    FROM { ROLE | USER} {<role_name>|<user_identity>}
```

#### Resource group

```SQL
REVOKE
    { ALTER | DROP | ALL [PRIVILEGES] } 
    ON { RESOURCE GROUP <resourcegroup_name> [, <resourcegroup_name>,...] ｜ ALL RESOURCE GROUPS} 
    FROM { ROLE | USER} {<role_name>|<user_identity>}
```

#### Resource

```SQL
REVOKE
    { USAGE | ALTER | DROP | ALL [PRIVILEGES] } 
    ON { RESOURCE <resource_name> [, <resource_name>,...] ｜ ALL RESOURCES} 
    FROM { ROLE | USER} {<role_name>|<user_identity>}
```

#### User

```SQL
REVOKE IMPERSONATE ON USER <user_identity> FROM USER <user_identity>;
```

#### Global UDF

```SQL
REVOKE
    { USAGE | DROP | ALL [PRIVILEGES]} 
    ON { GLOBAL FUNCTION <function_name> [, <function_name>,...]    
       | ALL GLOBAL FUNCTIONS }
    FROM { ROLE | USER} {<role_name>|<user_identity>}
```

#### Internal catalog

```SQL
REVOKE 
    { USAGE | CREATE DATABASE | ALL [PRIVILEGES]} 
    ON CATALOG default_catalog
    FROM { ROLE | USER} {<role_name>|<user_identity>}
```

#### External catalog

```SQL
REVOKE  
   { USAGE | DROP | ALL [PRIVILEGES] } 
   ON { CATALOG <catalog_name> [, <catalog_name>,...] | ALL CATALOGS}
   FROM { ROLE | USER} {<role_name>|<user_identity>}
```

#### Database

```SQL
REVOKE 
    { ALTER | DROP | CREATE TABLE | CREATE VIEW | CREATE FUNCTION | CREATE MATERIALIZED VIEW | ALL [PRIVILEGES] } 
    ON { DATABASE <database_name> [, <database_name>,...] | ALL DATABASES }
    FROM { ROLE | USER} {<role_name>|<user_identity>}
```

* You must first run SET CATALOG before you run this command.

#### Table

```SQL
REVOKE  
    { ALTER | DROP | SELECT | INSERT | EXPORT | UPDATE | DELETE | ALL [PRIVILEGES]} 
    ON { TABLE <table_name> [, < table_name >,...]
       | ALL TABLES} IN 
           {  DATABASE <database_name>  | ALL DATABASES }
    FROM { ROLE | USER} {<role_name>|<user_identity>}
```

* You must first run SET CATALOG before you run this command.
* You can also use db.tbl to represent a table.

  ```SQL
  REVOKE <priv> ON TABLE db.tbl FROM {ROLE <role_name> | USER <user_identity>}
  ```

#### View

```SQL
REVOKE  
    { ALTER | DROP | SELECT | ALL [PRIVILEGES]} 
    ON { VIEW <view_name> [, < view_name >,...]
       ｜ ALL VIEWS} IN 
           {  DATABASE <database_name> | ALL DATABASES }
    FROM { ROLE | USER} {<role_name>|<user_identity>}
```

* You must first run SET CATALOG before you run this command.
* You can also use db.view to represent a view.

  ```SQL
  REVOKE <priv> ON VIEW db.view FROM {ROLE <role_name> | USER <user_identity>}
  ```

#### Materialized view

```SQL
REVOKE
    { SELECT | ALTER | REFRESH | DROP | ALL [PRIVILEGES]} 
    ON { MATERIALIZED VIEW <mv_name> [, < mv_name >,...]
       ｜ ALL MATERIALIZED VIEWS} IN 
           {  DATABASE <database_name> | ALL [DATABASES] }
    FROM { ROLE | USER} {<role_name>|<user_identity>}
```

* You must first run SET CATALOG before you run this command.
* You can also use db.mv to represent an mv.

  ```SQL
  REVOKE <priv> ON MATERIALIZED VIEW db.mv FROM {ROLE <role_name> | USER <user_identity>}
  ```

#### Function

```SQL
REVOKE
    { USAGE | DROP | ALL [PRIVILEGES]} 
    ON { FUNCTION <function_name> [, < function_name >,...]
       ｜ ALL FUNCTIONS} IN 
           {  DATABASE <database_name> | ALL DATABASES }
    FROM { ROLE | USER} {<role_name>|<user_identity>}
```

* You must first run SET CATALOG before you run this command.
* You can also use db.function to represent a function.

  ```SQL
  REVOKE <priv> ON FUNCTION db.function FROM {ROLE <role_name> | USER <user_identity>}
  ```

#### Storage volume

```SQL
REVOKE
    CREATE STORAGE VOLUME 
    ON SYSTEM
    FROM { ROLE | USER} {<role_name>|<user_identity>}

REVOKE
    { USAGE | ALTER | DROP | ALL [PRIVILEGES] } 
    ON { STORAGE VOLUME < name > [, < name >,...] ｜ ALL STORAGE VOLUME} 
    FROM { ROLE | USER} {<role_name>|<user_identity>}
```

### Revoke roles

```SQL
REVOKE <role_name> [,<role_name>, ...] FROM ROLE <role_name>
REVOKE <role_name> [,<role_name>, ...] FROM USER <user_identity>
```

## Parameters

| **Parameter**      | **Description**                                 |
| ------------------ | ----------------------------------------------- |
| role_name          | The role name.                                  |
| user_identity      | The user identity, for example, 'jack'@'192.%'. |
| resourcegroup_name | The resource group name                         |
| resource_name      | The resource name.                              |
| function_name      | The function name.                              |
| catalog_name       | The name of the external catalog.               |
| database_name      | The database name.                              |
| table_name         | The table name.                                 |
| view_name          | The view name.                                  |
| mv_name            | The name of the materialized view.              |

## Examples

### Revoke privileges

Revoke the SELECT privilege on table `sr_member` from user `jack`:

```SQL
REVOKE SELECT ON TABLE sr_member FROM USER 'jack'@'192.%'
```

Revoke the USAGE privilege on resource `spark_resource` from role `test_role`:

```SQL
REVOKE USAGE ON RESOURCE 'spark_resource' FROM ROLE 'test_role';
```

### Revoke roles

Revoke the role `example_role` from user `jack`:

```SQL
REVOKE example_role FROM 'jack'@'%';
```

Revoke the role `example_role` from role `test_role`:

```SQL
REVOKE example_role FROM ROLE 'test_role';
```

## References

[GRANT](GRANT.md)
