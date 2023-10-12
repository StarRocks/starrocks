# REVOKE

## 功能

您可以使用该语句进行如下操作：

- 撤销某用户或某角色的指定权限。
- 撤销先前授予某用户的指定角色。注意仅 StarRocks 2.4 及以上版本支持该功能。
- 撤销一个用户 IMPERSONATE 另一个用户的权限。注意仅 StarRocks 2.4 及以上版本支持该功能。

## 语法

- 撤销某用户或某角色对数据库和表的指定权限。当撤销某角色的指定权限时，该角色必须存在。

    ```SQL
    REVOKE privilege_list ON db_name[.tbl_name] FROM {user_identity | ROLE 'role_name'}
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

### privilege_list

撤销某用户或某角色的指定权限。如果要一次性撤销某用户或某角色多种权限，需要用逗号 (`,`) 将不同权限分隔开。支持撤销的权限如下：

- `NODE_PRIV`：集群节点操作权限，如节点上下线。
- `ADMIN_PRIV`：除 `NODE_PRIV` 以外的所有权限。
- `GRANT_PRIV`：操作权限，如创建用户和角色、删除用户和角色、授权、撤权和设置账户密码等。
- `SELECT_PRIV`：数据库和表的读取权限。
- `LOAD_PRIV`：数据库和表的导入权限。
- `ALTER_PRIV`：数据库和表的结构变更权限。
- `CREATE_PRIV`：数据库和表的创建权限。
- `DROP_PRIV`：数据库和表的删除权限。
- `USAGE_PRIV`：资源的使用权限。

### db_name [.tbl_name]

指定的数据库和表。支持以下格式：

- `*.*`：所有数据库及库中所有表。
- `db.*`：指定数据库及库中所有表。
- `db.tbl`：指定数据库下的指定表。

> 说明：当使用 `db.*` 或 `db.tbl` 格式时，指定数据库和指定表可以是不存在的数据库和表。

### resource_name

指定的资源。支持以下格式：

- `*`：所有资源。
- `resource`：指定资源。

> 说明：当使用 `resource` 格式时，指定资源可以是不存在的资源。

### user_identity

该参数由两部分组成：`user_name` 和 `host`。 `user_name` 表示用户名。`host` 表示用户的主机地址，可以不指定，也可以指定为域名。如不指定，host 默认值为 `%`，表示该用户可以从任意 host 连接 StarRocks。如指定 `host` 为域名，权限的生效时间可能会有 1 分钟左右的延迟。`user_identity` 必须是使用 [CREATE USER](../account-management/CREATE_USER.md) 语句创建的。

### role_name

角色名。

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
