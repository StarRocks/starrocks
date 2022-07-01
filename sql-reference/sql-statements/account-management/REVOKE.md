# REVOKE

## 功能

REVOKE 语句用于撤销某用户或某角色的指定权限，该语句也可用于撤销先前授予某用户的指定角色。

## 语法

- 撤销某用户或某角色对数据库和表的指定权限。当撤销某角色的指定权限时，该角色必须存在。

```SQL
REVOKE privilege_list ON db_name[.tbl_name] FROM {user_identity | ROLE 'role_name'};
```

- 撤销某用户或某角色对资源的指定权限。当撤销某角色的指定权限时，该角色必须存在。

```SQL
REVOKE privilege_list ON RESOURCE 'resource_name' FROM {user_identity | ROLE 'role_name'};
```

- 撤销用户 `a` 以用户 `b` 的身份执行命令的权限。

```SQL
REVOKE IMPERSONATE ON user_identity_b FROM user_identity_a;
```

- 撤销先前授予某用户的指定角色。该指定角色必须存在。

```SQL
REVOKE 'role_name' FROM user_identity;
```

## 参数说明

`user_identity`：该参数由`user_name` 和 `host` 两部分组成。`user_name` 表示用户名。`host` 表示用户的主机地址，可以不指定，也可以指定为域名。如果不指定，host 默认值为 `%`，表示该用户可以从任意 host 连接 StarRocks。如果指定 `host` 为域名，权限的生效时间可能会有 1 分钟左右的延迟。`user_identity` 必须是使用 [CREATE USER](../account-management/CREATE%20USER.md) 语句创建的。

## 示例

示例一：撤销用户 `jack` 对数据库 `db1` 及库中所有表的读取权限。

```SQL
REVOKE SELECT_PRIV ON db1.* FROM 'jack'@'192.%';
```

示例二：撤销用户 `jack` 对资源 spark_resource 的使用权限。

```SQL
REVOKE USAGE_PRIV ON RESOURCE 'spark_resource' FROM 'jack'@'192.%';
```

示例三：撤销先前授予用户 `jack` 的 `my_role` 角色。

```SQL
REVOKE 'my_role' FROM 'jack'@'%';
```

示例四：撤销用户 `jack` 以用户 `rose` 的身份执行命令的权限。

```SQL
REVOKE IMPERSONATE ON 'rose'@'%' FROM 'jack'@'%';
```
