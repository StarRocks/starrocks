# GRANT

## 功能

您可以使用该语句进行如下操作：

- 将指定权限授予某用户或某角色。
- 将指定角色授予某用户。注意仅 StarRocks 2.4 及以上版本支持该功能。
- 授予用户 `a` IMPERSONATE 用户 `b` 的权限。授予后，用户 `a` 即可执行 [EXECUTE AS](../account-management/EXECUTE%20AS.md) 语句以用户 `b` 的身份执行操作。注意仅 StarRocks 2.4 及以上版本支持该功能。

## 语法

- 将数据库和表的指定权限授予某用户或某角色。如果授予的角色不存在，那么系统会自动创建该角色。

    ```SQL
    GRANT privilege_list ON db_name[.tbl_name] TO {user_identity | ROLE 'role_name'}；
    ```

- 将资源的指定权限授予某用户或某角色。如果授予的角色不存在，那么系统会自动创建该角色。

    ```SQL
    GRANT privilege_list ON RESOURCE 'resource_name' TO {user_identity | ROLE 'role_name'};
    ```

- 授予用户 `a` 以用户 `b` 的身份执行操作的权限。

    ```SQL
    GRANT IMPERSONATE ON user_identity_b TO user_identity_a;
    ```

- 将指定角色的权限授予某用户。指定角色必须存在。

    ```SQL
    GRANT 'role_name' TO user_identity;
    ```

## 参数说明

### privilege_list

授予某用户或某角色的指定权限。如果要一次性授予某用户或某角色多种权限，需要用逗号 (`,`) 将不同权限分隔开。支持权限如下：

- `NODE_PRIV`：集群节点操作权限，如节点上下线。
- `ADMIN_PRIV`：除 `NODE_PRIV` 以外的所有权限。
- `GRANT_PRIV`：操作权限，如创建用户和角色、删除用户和角色、授权、撤权、和设置账户密码等。
- `SELECT_PRIV`：数据库和表的读取权限。
- `LOAD_PRIV`：数据库和表的导入权限。
- `ALTER_PRIV`：数据库和表的结构变更权限。
- `CREATE_PRIV`：数据库和表的创建权限。
- `DROP_PRIV`：数据库和表的删除权限。
- `USAGE_PRIV`：资源的使用权限。

以上部分权限可划分为三类：

- 节点权限：`NODE_PRIV`
- 库表权限：`SELECT_PRIV`、`LOAD_PRIV`、`ALTER_PRIV`、`CREATE_PRIV`、`DROP_PRIV`
- 资源权限：`USAGE_PRIV`

### db_name [.tbl_name]

指定的数据库和表。支持以下格式：

- `*.*`：所有数据库及库中所有表。
- `db.*`：指定数据库及库中所有表。
- `db.tbl`：指定数据库下的指定表。

> 说明：当使用`db.*` 或 `db.tbl` 格式时，指定数据库和指定表可以是不存在的数据库和表。

### resource_name

指定的资源。支持以下格式：

- `*`：所有资源。
- `resource`：指定资源。

> 说明：当使用`resource` 格式时，指定资源可以是不存在的资源。

### user_identity

该参数由两部分组成：`user_name` 和 `host`。 `user_name` 表示用户名。`host` 表示用户的主机地址，可以不指定，也可以指定为域名。如不指定，host 默认值为 `%`，表示该用户可以从任意 host 连接 StarRocks。如指定 `host` 为域名，权限的生效时间可能会有 1 分钟左右的延迟。`user_identity` 必须是使用 CREATE USER 语句创建的。

### role_name

角色名。

## 示例

示例一：将所有数据库及库中所有表的读取权限授予用户 `jack` 。

```SQL
GRANT SELECT_PRIV ON *.* TO 'jack'@'%';
```

示例二：将数据库 `db1` 及库中所有表的导入权限授予角色 `my_role`。

```SQL
GRANT LOAD_PRIV ON db1.* TO ROLE 'my_role';
```

示例三：将数据库 `db1` 和表 `tbl1` 的读取、结构变更和导入权限授予用户`jack`。

```SQL
GRANT SELECT_PRIV,ALTER_PRIV,LOAD_PRIV ON db1.tbl1 TO 'jack'@'192.8.%';
```

示例四：将所有资源的使用权限授予用户 `jack`。

```SQL
GRANT USAGE_PRIV ON RESOURCE * TO 'jack'@'%';
```

示例五：将资源 spark_resource 的使用权限授予用户 `jack`。

```SQL
GRANT USAGE_PRIV ON RESOURCE 'spark_resource' TO 'jack'@'%';
```

示例六：将资源 spark_resource 的使用权限授予角色 `my_role` 。

```SQL
GRANT USAGE_PRIV ON RESOURCE 'spark_resource' TO ROLE 'my_role';
```

示例七：将角色 `my_role` 授予用户 `jack`。

```SQL
GRANT 'my_role' TO 'jack'@'%';
```

示例八：授予用户 `jack` 以用户 `rose` 的身份执行操作的权限。

```SQL
GRANT IMPERSONATE ON 'rose'@'%' TO 'jack'@'%';
```
