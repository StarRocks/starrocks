# GRANT

## 功能

该语句可用于将权限授予给角色或用户，以及将角色授予给用户或其他角色。

有关权限项的详细信息，参见 [权限项](../../../administration/privilege_item.md)。

您可以使用 [SHOW GRANTS](SHOW%20GRANTS.md) 来查看权限授予的信息。通过 [REVOKE](REVOKE.md) 来撤销权限或角色。

> **注意**
>
> - 普通用户可以将自身拥有的授权中带有 WITH GRANT OPTION 关键字的权限赋予给其他用户和角色。参见示例七。
> - 只有拥有 `user_admin` 角色的用户才可以将任意权限授予给任意用户和角色。

## 语法

### 授予权限给用户或者角色

```SQL
# System 相关

GRANT  
    { CREATE RESOURCE GROUP | CREATE RESOURCE | CREATE EXTERNAL CATALOG | REPOSITORY | BLACKLIST | FILE | OPERATE | ALL [PRIVILEGES]} 
    ON SYSTEM
    TO { ROLE | USER} {<role_name>|<user_identity>} [ WITH GRANT OPTION ]

# Resource group 相关

GRANT  
    { ALTER | DROP | ALL [PRIVILEGES] } 
    ON { RESOURCE GROUP <resource_name> [, < resource_name >,...] ｜ ALL RESOURCE GROUPS} 
    TO { ROLE | USER} {<role_name>|<user_identity>} [ WITH GRANT OPTION ]

# Resource 相关

GRANT 
    { USAGE | ALTER | DROP | ALL [PRIVILEGES] } 
    ON { RESOURCE <resource_name> [, < resource_name >,...] ｜ ALL RESOURCES} 
    TO { ROLE | USER} {<role_name>|<user_identity>} [ WITH GRANT OPTION ]

# Global UDF 相关

GRANT
    { USAGE | DROP | ALL [PRIVILEGES]} 
    ON { GLOBAL FUNCTION <function_name> [, < function_name >,...]    
       | ALL GLOBAL FUNCTIONS }
    TO { ROLE | USER} {<role_name>|<user_identity>} [ WITH GRANT OPTION ]

# Internal catalog 相关

GRANT 
    { USAGE | CREATE DATABASE | ALL [PRIVILEGES]} 
    ON CATALOG default_catalog
    TO { ROLE | USER} {<role_name>|<user_identity>} [ WITH GRANT OPTION ]

# External catalog 相关

GRANT  
   { USAGE | DROP | ALL [PRIVILEGES] } 
   ON { CATALOG <catalog_name> [, <catalog_name>,...] | ALL CATALOGS}
   TO { ROLE | USER} {<role_name>|<user_identity>} [ WITH GRANT OPTION ]

# Database 相关

GRANT 
    { ALTER | DROP | CREATE TABLE | CREATE VIEW | CREATE FUNCTION | CREATE MATERIALIZED VIEW | ALL [PRIVILEGES] } 
    ON { DATABASE <database_name> [, <database_name>,...] | ALL DATABASES }
    TO { ROLE | USER} {<role_name>|<user_identity>} [ WITH GRANT OPTION ]
  
*注意：需要执行 set catalog 之后才能使用。

# Table 相关

GRANT  
    { ALTER | DROP | SELECT | INSERT | EXPORT | UPDATE | DELETE | ALL [PRIVILEGES]} 
    ON { TABLE <table_name> [, < table_name >,...]
       | ALL TABLES } IN 
           { DATABASE <database_name> | ALL DATABASES }
    TO { ROLE | USER} {<role_name>|<user_identity>} [ WITH GRANT OPTION ]

*注意：需要执行 set catalog 之后才能使用。
注意：table还可以用db.tbl的方式来进行表示
GRANT <priv> ON TABLE db.tbl TO {ROLE <rolename> | USER <username>}

# View 相关

GRANT  
    { ALTER | DROP | SELECT | ALL [PRIVILEGES]} 
    ON { VIEW <view_name> [, < view_name >,...]
       ｜ ALL VIEWS } IN 
           { DATABASE <database_name> | ALL DATABASES }
    TO { ROLE | USER} {<role_name>|<user_identity>} [ WITH GRANT OPTION ]
    
*注意：需要执行 set catalog 之后才能使用。
注意：view 还可以用 db.view 的方式来进行表示。
GRANT <priv> ON VIEW db.view TO {ROLE <rolename> | USER <username>}

# Materialized view 相关

GRANT
    { SELECT | ALTER | REFRESH | DROP | ALL [PRIVILEGES]} 
    ON { MATERIALIZED VIEW <mv_name> [, < mv_name >,...]
       ｜ ALL MATERIALIZED VIEWS } IN 
           { DATABASE <database_name> | ALL DATABASES }
    TO { ROLE | USER} {<role_name>|<user_identity>} [ WITH GRANT OPTION ]
    
* 注意：需要执行 set catalog 之后才能使用。 
mv 还可以用 db.mv 的方式来进行表示。
GRANT <priv> ON MATERIALIZED_VIEW db.mv TO {ROLE <rolename> | USER <username>}

# Function 相关

GRANT
    { USAGE | DROP | ALL [PRIVILEGES]} 
    ON { FUNCTION <function_name> [, < function_name >,...]
       ｜ ALL FUNCTIONS } IN 
           { DATABASE <database_name> | ALL DATABASES }
    TO { ROLE | USER} {<role_name>|<user_identity>} [ WITH GRANT OPTION ]
    
*注意：需要执行 set catalog 之后才能使用。
function 还可以用 db.function 的方式来进行表示。
GRANT <priv> ON FUNCTION db.function TO {ROLE <rolename> | USER <username>}

# User 相关

GRANT IMPERSONATE ON USER <user_identity> TO USER <user_identity> [ WITH GRANT OPTION ]

# Storage volume 相关

GRANT
    CREATE STORAGE VOLUME 
    ON SYSTEM
    TO { ROLE | USER} {<role_name>|<user_identity>} [ WITH GRANT OPTION ]

GRANT  
    { USAGE | ALTER | DROP | ALL [PRIVILEGES] } 
    ON { STORAGE VOLUME < name > [, < name >,...] ｜ ALL STORAGE VOLUME} 
    TO { ROLE | USER} {<role_name>|<user_identity>} [ WITH GRANT OPTION ]
```

### 授予角色给用户或者其他角色

```SQL
GRANT <role_name> [,<role_name>, ...] TO ROLE <role_name>
GRANT <role_name> [,<role_name>, ...] TO USER <user_identity>
```

## 示例

示例一：将所有数据库及库中所有表的读取权限授予用户 `jack` 。

```SQL
GRANT SELECT ON *.* TO 'jack'@'%';
```

示例二：将数据库 `db1` 及库中所有表的导入权限授予角色 `my_role`。

```SQL
GRANT INSERT ON db1.* TO ROLE 'my_role';
```

示例三：将数据库 `db1` 和表 `tbl1` 的读取、结构变更和导入权限授予用户 `jack`。

```SQL
GRANT SELECT,ALTER,INSERT ON db1.tbl1 TO 'jack'@'192.8.%';
```

示例四：将所有资源的使用权限授予用户 `jack`。

```SQL
GRANT USAGE ON RESOURCE * TO 'jack'@'%';
```

示例五：将资源 spark_resource 的使用权限授予用户 `jack`。

```SQL
GRANT USAGE ON RESOURCE 'spark_resource' TO 'jack'@'%';
```

示例六：将资源 spark_resource 的使用权限授予角色 `my_role` 。

```SQL
GRANT USAGE ON RESOURCE 'spark_resource' TO ROLE 'my_role';
```

示例七：将表 `sr_member` 的 SELECT 权限授予给用户 `jack`，并允许 `jack` 将此权限授予其他用户或角色（通过在 SQL 中指定 WITH GRANT OPTION）：

```SQL
GRANT SELECT ON TABLE sr_member TO USER jack@'172.10.1.10' WITH GRANT OPTION;
```

示例八：将系统预置角色 `db_admin`、`user_admin` 以及 `cluster_admin` 赋予给平台运维角色。

```SQL
GRANT db_admin, user_admin, cluster_admin TO USER user_platform;
```

示例九：授予用户 `jack` 以用户 `rose` 的身份执行操作的权限。

```SQL
GRANT IMPERSONATE ON 'rose'@'%' TO 'jack'@'%';
```
