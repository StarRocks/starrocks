# SET ROLE

## 功能

在当前会话下，激活当前用户拥有权限的角色。该命令从 3.0 版本开始支持。

## 注意事项

用户只能激活自己已有权限的角色。

用户可以通过 [SHOW GRANTS](SHOW%20GRANTS.md) 查看拥有的角色，可以通过[SELECT CURRENT_ROLE](../../sql-functions/utility-functions/current_role.md) 查看当前激活的角色。

## 语法

```SQL
-- 激活特定角色
SET ROLE <role_name>[,<role_name>,..];
-- 激活除指定角色之外，用户拥有的所有角色
SET ROLE ALL EXCEPT <role_name>[,<role_name>,..]; 
-- 激活用户拥有的所有角色
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

- [CREATE ROLE](CREATE%20ROLE.md): 创建角色。
- [GRANT](GRANT.md): 将角色分配给用户或其他角色。
- [ALTER USER](ALTER%20USER.md): 修改角色。
- [SHOW ROLES](SHOW%20ROLES.md): 查看当前系统所有角色。
- [current_role](../../sql-functions/utility-functions/current_role.md): 查看当前用户拥有的角色。
- [DROP ROLE](DROP%20ROLE.md): 删除角色。
