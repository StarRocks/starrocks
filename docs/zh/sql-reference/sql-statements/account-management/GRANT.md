---
displayed_sidebar: "Chinese"
---

# GRANT

import UserPrivilegeCase from '../../../assets/commonMarkdown/userPrivilegeCase.md'

## 功能

该语句用于将一个或多个权限授予给角色或用户，以及将角色授予给用户或其他角色。

有关权限项的详细信息，参见[权限项](../../../administration/privilege_item.md)。

授权后，您可以通过 [SHOW GRANTS](SHOW_GRANTS.md) 来查看权限授予的信息；通过 [REVOKE](REVOKE.md) 来撤销权限或角色。

在执行 GRANT 操作前，确保您已经在系统中创建了用户或角色。更多创建信息，参见 [CREATE USER](./CREATE_USER.md) 和 [CREATE ROLE](./CREATE_ROLE.md)。

> **注意**
>
> - 只有拥有 `user_admin` 角色的用户才可以将任意权限授予给任意用户和角色。
> - 角色被赋予给用户之后，用户需要通过 [SET ROLE](SET_ROLE.md) 手动激活角色，方可利用角色的权限。如果希望用户登录时默认激活角色，则可以通过 [ALTER USER](ALTER_USER.md) 或 [SET DEFAULT ROLE](SET_DEFAULT_ROLE.md) 为用户设置默认角色。如果希望系统内所有用户都能够在登录时默认激活所有权限，则可以设置全局变量 `SET GLOBAL activate_all_roles_on_login = TRUE;`。
> - 普通用户可以将自身拥有的授权中带有 WITH GRANT OPTION 关键字的权限赋予给其他用户和角色，参见示例七。

## 语法

### 授予权限给用户或者角色

#### System 相关

```SQL
GRANT
    { CREATE RESOURCE GROUP | CREATE RESOURCE | CREATE EXTERNAL CATALOG | REPOSITORY | BLACKLIST | FILE | OPERATE | CREATE STORAGE VOLUME } 
    ON SYSTEM
    TO { ROLE | USER} {<role_name>|<user_identity>} [ WITH GRANT OPTION ]
```

#### Resource group 相关

```SQL
GRANT
    { ALTER | DROP | ALL [PRIVILEGES] } 
    ON { RESOURCE GROUP <resource_group_name> [, <resource_group_name >,...] ｜ ALL RESOURCE GROUPS} 
    TO { ROLE | USER} {<role_name>|<user_identity>} [ WITH GRANT OPTION ]
```

#### Resource 相关

```SQL
GRANT
    { USAGE | ALTER | DROP | ALL [PRIVILEGES] } 
    ON { RESOURCE <resource_name> [, < resource_name >,...] ｜ ALL RESOURCES} 
    TO { ROLE | USER} {<role_name>|<user_identity>} [ WITH GRANT OPTION ]
```

#### Global UDF 相关

```SQL
GRANT
    { USAGE | DROP | ALL [PRIVILEGES]} 
    ON { GLOBAL FUNCTION <function_name>(input_data_type) [, < function_name>(input_data_type),...]    
       | ALL GLOBAL FUNCTIONS }
    TO { ROLE | USER} {<role_name>|<user_identity>} [ WITH GRANT OPTION ]
```

示例：`GRANT usage ON GLOBAL FUNCTION a(string) to kevin;`

#### Internal catalog 相关

```SQL
GRANT
    { USAGE | CREATE DATABASE | ALL [PRIVILEGES]} 
    ON CATALOG default_catalog
    TO { ROLE | USER} {<role_name>|<user_identity>} [ WITH GRANT OPTION ]
```

#### External catalog 相关

```SQL
GRANT
   { USAGE | DROP | ALL [PRIVILEGES] } 
   ON { CATALOG <catalog_name> [, <catalog_name>,...] | ALL CATALOGS}
   TO { ROLE | USER} {<role_name>|<user_identity>} [ WITH GRANT OPTION ]
```

#### Database 相关

```SQL
GRANT
    { ALTER | DROP | CREATE TABLE | CREATE VIEW | CREATE FUNCTION | CREATE MATERIALIZED VIEW | ALL [PRIVILEGES] } 
    ON { DATABASE <database_name> [, <database_name>,...] | ALL DATABASES }
    TO { ROLE | USER} {<role_name>|<user_identity>} [ WITH GRANT OPTION ]
```

**注意**
>
> 1. 需要执行 SET CATALOG 之后才能使用。
> 2. 对于 External Catalog 下的数据库，只有 Hive 和 Iceberg 数据库支持赋予 CREATE TABLE 权限。

#### Table 相关

```SQL
GRANT  
    { ALTER | DROP | SELECT | INSERT | EXPORT | UPDATE | DELETE | ALL [PRIVILEGES]} 
    ON { TABLE <table_name> [, < table_name >,...]
       | ALL TABLES IN 
           { { DATABASE <database_name> [,<database_name>,...] } | ALL DATABASES }}
    TO { ROLE | USER} {<role_name>|<user_identity>} [ WITH GRANT OPTION ]
```

> **注意**
>
> 1. 需要执行 SET CATALOG 之后才能使用。table 还可以用 `<db_name>.<table_name>` 的方式来进行表示。
> 2. 所有 Internal Catalog 和 External Catalog 下的表，都支持赋予 SELECT 权限。Hive 和 Iceberg catalog 下的表，还支持赋予 INSERT 权限 (从 3.1 版本起，支持赋予 Iceberg 表的 INSERT 权限；从 3.2 版本起，支持赋予 Hive 表的 INSERT 权限)。

```SQL
GRANT <priv> ON TABLE <db_name>.<table_name> TO {ROLE <role_name> | USER <user_name>}
```

#### View 相关

```SQL
GRANT  
    { ALTER | DROP | SELECT | ALL [PRIVILEGES]} 
    ON { VIEW <view_name> [, < view_name >,...]
       ｜ ALL VIEWS IN 
           { { DATABASE <database_name> [,<database_name>,...] }| ALL DATABASES }}
    TO { ROLE | USER} {<role_name>|<user_identity>} [ WITH GRANT OPTION ]
```

> **注意**
>
> 1. 需要执行 SET CATALOG 之后才能使用。view 还可以用 `<db_name>.<view_name>` 的方式来进行表示。
> 2. 对于 External Catalog，仅 Hive 表视图支持 SELECT 权限。（3.1 及以后）

```SQL
GRANT <priv> ON VIEW <db_name>.<view_name> TO {ROLE <role_name> | USER <user_name>}
```

#### Materialized view 相关

```SQL
GRANT
    { SELECT | ALTER | REFRESH | DROP | ALL [PRIVILEGES]} 
    ON { MATERIALIZED VIEW <mv_name> [, < mv_name >,...]
       ｜ ALL MATERIALIZED VIEWS IN 
           { { DATABASE <database_name> [,<database_name>,...] }| ALL DATABASES }}
    TO { ROLE | USER} {<role_name>|<user_identity>} [ WITH GRANT OPTION ]
```

*注意：需要执行 SET CATALOG 之后才能使用。物化视图还可以用 `<db_name>.<mv_name>` 的方式来进行表示。

```SQL
GRANT <priv> ON MATERIALIZED VIEW <db_name>.<mv_name> TO {ROLE <role_name> | USER <user_name>};
```

#### Function 相关

```SQL
GRANT
    { USAGE | DROP | ALL [PRIVILEGES]} 
    ON { FUNCTION <function_name>(input_data_type) [, < function_name >(input_data_type),...]
       ｜ ALL FUNCTIONS IN 
           { { DATABASE <database_name> [,<database_name>,...] }| ALL DATABASES }}
    TO { ROLE | USER } {<role_name>|<user_identity>} [ WITH GRANT OPTION ]
```

*注意：需要执行 SET CATALOG 之后才能使用。function 还可以用 `<db_name>.<function_name>` 的方式来进行表示。

```SQL
GRANT <priv> ON FUNCTION <db_name>.<function_name> TO {ROLE <role_name> | USER <user_name>}
```

#### User 相关

```SQL
GRANT IMPERSONATE ON USER <user_identity> TO USER <user_identity_1> [ WITH GRANT OPTION ]
```

#### Storage volume 相关

```SQL
GRANT
    { USAGE | ALTER | DROP | ALL [PRIVILEGES] } 
    ON { STORAGE VOLUME < name > [, < name >,...] ｜ ALL STORAGE VOLUMES} 
    TO { ROLE | USER} {<role_name>|<user_identity>} [ WITH GRANT OPTION ]
```

### 授予角色给用户或者其他角色

```SQL
GRANT <role_name> [,<role_name>, ...] TO ROLE <role_name>
GRANT <role_name> [,<role_name>, ...] TO USER <user_identity>
```

**注意：**

- 在角色被赋予给用户之后，用户需要通过 [SET ROLE](SET_ROLE.md) 手动激活角色，方可利用角色的权限。
- 如果希望用户登录时就默认激活角色，则可以通过 [ALTER USER](ALTER_USER.md) 或者 [SET DEFAULT ROLE](SET_DEFAULT_ROLE.md) 为用户设置默认角色。
- 如果希望系统内所有用户都能够在登录时默认激活所有权限，则可以设置全局变量 `SET GLOBAL activate_all_roles_on_login = TRUE;`。

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

示例五：将资源 `spark_resource` 的使用权限授予用户 `jack`。

```SQL
GRANT USAGE ON RESOURCE 'spark_resource' TO 'jack'@'%';
```

示例六：将资源 `spark_resource` 的使用权限授予角色 `my_role` 。

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

## 最佳实践 - 基于使用场景创建自定义角色

<UserPrivilegeCase />

有关多业务线权限管理的相关实践，参见 [多业务线权限管理](../../../administration/User_privilege.md#多业务线权限管理)。
