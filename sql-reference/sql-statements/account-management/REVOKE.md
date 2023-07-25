# REVOKE

## 功能

从用户或角色中撤销指定的权限或角色。有关 StarRocks 支持的权限项，参见 [权限项](../../../administration/privilege_item.md)。

> **注意**
>
> - 普通用户可以将自身拥有的授权中带有 WITH GRANT OPTION 关键字的权限从其他用户或角色处收回。关于 WITH GRANT OPTION，参见 [GRANT](GRANT.md)。
> - 只有拥有 `user_admin` 角色的用户才可以收回其他用户的权限。

## 语法

### 撤销指定权限

能撤销的权限因对象 (object) 而异。下面讲解不同对象可以撤销的不同权限。

```SQL
# System 相关

REVOKE
    { CREATE RESOURCE GROUP | CREATE RESOURCE | CREATE EXTERNAL CATALOG | REPOSITORY | BLACKLIST | FILE | OPERATE | ALL [PRIVILEGES]} 
    ON SYSTEM
    FROM { ROLE | USER} {<role_name>|<user_identity>}

# Resource group 相关

REVOKE
    { ALTER | DROP | ALL [PRIVILEGES] } 
    ON { RESOURCE GROUP <resourcegroup_name> [, <resourcegroup_name>,...] ｜ ALL RESOURCE GROUPS} 
    FROM { ROLE | USER} {<role_name>|<user_identity>}

# Resource 相关

REVOKE
    { USAGE | ALTER | DROP | ALL [PRIVILEGES] } 
    ON { RESOURCE <resource_name> [, <resource_name>,...] ｜ ALL RESOURCES} 
    FROM { ROLE | USER} {<role_name>|<user_identity>}

# User 相关

REVOKE IMPERSONATE ON USER <user_identity> FROM USER <user_identity>

# 全局 UDF 相关

REVOKE
    { USAGE | DROP | ALL [PRIVILEGES]} 
    ON { GLOBAL FUNCTION <function_name> [, <function_name>,...]    
       | ALL GLOBAL FUNCTIONS }
    FROM { ROLE | USER} {<role_name>|<user_identity>}

# Internal catalog 相关

REVOKE 
    { USAGE | CREATE DATABASE | ALL [PRIVILEGES]} 
    ON CATALOG default_catalog
    FROM { ROLE | USER} {<role_name>|<user_identity>}

# External catalog 相关

REVOKE  
   { USAGE | DROP | ALL [PRIVILEGES] } 
   ON { CATALOG <catalog_name> [, <catalog_name>,...] | ALL CATALOGS}
   FROM { ROLE | USER} {<role_name>|<user_identity>}

# Database 相关

REVOKE 
    { ALTER | DROP | CREATE TABLE | CREATE VIEW | CREATE FUNCTION | CREATE MATERIALIZED VIEW | ALL [PRIVILEGES] } 
    ON { DATABASE <database_name> [, <database_name>,...] | ALL DATABASES }
    FROM { ROLE | USER} {<role_name>|<user_identity>}
  
* 注意：需要执行 set catalog 之后才能使用。

# Table 相关

REVOKE  
    { ALTER | DROP | SELECT | INSERT | EXPORT | UPDATE | DELETE | ALL [PRIVILEGES]} 
    ON { TABLE <table_name> [, < table_name >,...]
       | ALL TABLES} IN 
           { DATABASE <database_name> | ALL DATABASES }
    FROM { ROLE | USER} {<role_name>|<user_identity>}

* 注意：需要执行 set catalog 之后才能使用。
* table 还可以用 db.tbl 的方式来表示。
REVOKE <priv> ON TABLE db.tbl FROM {ROLE <role_name> | USER <user_identity>}

# View 相关

REVOKE
    { ALTER | DROP | SELECT | ALL [PRIVILEGES]} 
    ON { VIEW <view_name> [, < view_name >,...]
       ｜ ALL VIEWS} IN 
           { DATABASE <database_name> | ALL DATABASES }
    FROM { ROLE | USER} {<role_name>|<user_identity>}
    
* 注意：需要执行 set catalog 之后才能使用。 
* view 还可以用 db.view 的方式来表示。
REVOKE <priv> ON VIEW db.view FROM {ROLE <role_name> | USER <user_identity>}

# Materialized view 相关

REVOKE
    { SELECT | ALTER | REFRESH | DROP | ALL [PRIVILEGES]} 
    ON { MATERIALIZED VIEW <mv_name> [, < mv_name >,...]
       ｜ ALL MATERIALIZED VIEWS} IN 
           { DATABASE <database_name> | ALL [DATABASES] }
    FROM { ROLE | USER} {<role_name>|<user_identity>}
    
* 注意：需要执行 set catalog 之后才能使用。  
* 注意：mv 还可以用 db.mv 的方式来表示。
REVOKE <priv> ON MATERIALIZED VIEW db.mv FROM {ROLE <role_name> | USER <user_identity>};

# Function 相关

REVOKE
    { USAGE | DROP | ALL [PRIVILEGES]} 
    ON { FUNCTION <function_name> [, < function_name >,...]
       ｜ ALL FUNCTIONS } IN 
           { DATABASE <database_name> | ALL DATABASES }
    FROM { ROLE | USER} {<role_name>|<user_identity>}
    
* 注意：需要执行 set catalog 之后才能使用。 
* function 还可以用 db.function 的方式来表示。
REVOKE <priv> ON FUNCTION db.function FROM {ROLE <role_name> | USER <user_identity>}
```

### 撤销指定角色

```SQL
REVOKE <role_name> [,<role_name>, ...] FROM ROLE <role_name>
REVOKE <role_name> [,<role_name>, ...] FROM USER <user_identity>
```

## 参数说明

| **参数**           | **描述**                        |
| ------------------ | ------------------------------- |
| role_name          | 角色名称                        |
| user_identity      | 用户标识，例如 'jack'@'192.%'。 |
| resourcegroup_name | 资源组名称                      |
| resource_name      | 资源名称                        |
| function_name      | 函数名称                        |
| catalog_name       | External catalog 名称           |
| database_name      | 数据库名称                      |
| table_name         | 表名                            |
| view_name          | 视图名称                        |
| mv_name            | 物化视图名称                    |

## 示例

### 撤销权限

撤销用户 `jack` 对表 `sr_member` 的 SELECT 权限。

```SQL
REVOKE SELECT ON TABLE sr_member FROM USER 'jack@'172.10.1.10'';
```

撤销角色 `test_role` 对资源 `spark_resource` 的使用权限。

```SQL
REVOKE USAGE ON RESOURCE 'spark_resource' FROM ROLE 'test_role';
```

### 撤销角色

撤销先前授予用户 `jack` 的 `example_role` 角色。

```SQL
REVOKE example_role FROM 'jack'@'%';
```

撤销先前授予角色 `test_role` 的 `example_role` 角色。

```SQL
REVOKE example_role FROM ROLE 'test_role';
```
