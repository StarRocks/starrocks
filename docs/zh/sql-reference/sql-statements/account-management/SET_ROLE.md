---
displayed_sidebar: "Chinese"
---

# SET ROLE

## 功能

在当前会话下，激活当前用户拥有权限的角色。激活后，用户可以使用该角色进行相关操作。

您可以使用 `select is_role_in_session("<role_name>")` 来检查指定角色在当前会话下是否已经被激活。

该命令从 3.0 版本开始支持。

## 注意事项

- 用户只能激活自己已有权限的角色。

- 用户可以通过 [SHOW GRANTS](SHOW_GRANTS.md) 查看拥有的角色，可以通过[SELECT CURRENT_ROLE](../../sql-functions/utility-functions/current_role.md) 查看当前激活的角色。

## 语法

```SQL
-- 激活特定角色，使用这个角色进行操作。
SET ROLE <role_name>[,<role_name>,..];
-- 激活除指定角色之外，用户拥有的所有角色。
SET ROLE ALL EXCEPT <role_name>[,<role_name>,..]; 
-- 激活用户拥有的所有角色。
SET ROLE ALL;
```

## 参数说明

`role_name`: 用户拥有的角色名。

## 示例

1. 查看当前用户拥有的角色。

    ```SQL
    SHOW GRANTS;
    +--------------+---------+----------------------------------------------+
    | UserIdentity | Catalog | Grants                                       |
    +--------------+---------+----------------------------------------------+
    | 'test'@'%'   | NULL    | GRANT 'db_admin', 'user_admin' TO 'test'@'%' |
    +--------------+---------+----------------------------------------------+
    ```

2. 激活 `db_admin` 角色。

    ```plain
    SET ROLE db_admin;
    ```

3. 查看当前生效的角色。

    ```SQL
    SELECT CURRENT_ROLE();
    +--------------------+
    | CURRENT_ROLE()     |
    +--------------------+
    | db_admin           |
    +--------------------+
    ```

## 相关文档

- [CREATE ROLE](CREATE_ROLE.md): 创建角色。
- [GRANT](GRANT.md): 将角色分配给用户或其他角色。
- [ALTER USER](ALTER_USER.md): 修改角色。
- [SHOW ROLES](SHOW_ROLES.md): 查看当前系统所有角色。
- [current_role](../../sql-functions/utility-functions/current_role.md): 查看当前用户拥有的角色。
- [is_role_in_session](../../sql-functions/utility-functions/is_role_in_session.md)
- [DROP ROLE](DROP_ROLE.md): 删除角色。
