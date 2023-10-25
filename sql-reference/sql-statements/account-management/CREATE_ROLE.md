# CREATE ROLE

## 功能

创建一个角色。角色创建后，您可以将指定权限（比如数据库和表的读取权限和资源的使用权限等）授予该角色，也可以将该角色授予某个用户。拥有该角色的用户即拥有该角色的相关权限。有关权限的详细说明，参见 [GRANT](/sql-reference/sql-statements/account-management/GRANT.md)。

> 说明：
>
> - 只有拥有`GRANT_PRIV` 或 `ADMIN_PRIV`权限的用户才可以创建角色。
> - StarRocks 的两个系统角色 admin 和 operator 均拥有`ADMIN_PRIV`权限，所以拥有这两个系统角色的用户可以创建角色。

## 语法

```SQL
CREATE ROLE <role_name>
```

## 参数说明

`role_name`：角色名称，命名要求如下：

- 必须由数字(0-9)、下划线(_)或字母(a-z或A-Z)组成，且只能以字母开头。
- 总长度不能超过 64 个字符。

> 注意：角色名称不能与 StarRocks 的两个系统角色 admin 和 operator 重复。

## 示例

创建一个角色 `analyst`。

```SQL
CREATE ROLE analyst;
```

## 更多操作

- 如要查看角色，参见 [SHOW ROLES](/sql-reference/sql-statements/account-management/SHOW_ROLES.md)。
- 如要删除角色，参见 [DROP ROLE](/sql-reference/sql-statements/account-management/DROP_ROLE.md)。
- 如要创建用户，参见 [CREATE USER](/sql-reference/sql-statements/account-management/CREATE_USER.md)。
- 如要将指定权限授予该角色或将该角色授予某个用户，参见 [GRANT](/sql-reference/sql-statements/account-management/GRANT.md)。
