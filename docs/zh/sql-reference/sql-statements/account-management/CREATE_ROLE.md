---
displayed_sidebar: "Chinese"
---

# CREATE ROLE

import UserManagementPriv from '../../../assets/commonMarkdown/userManagementPriv.md'

## 功能

创建一个角色。角色创建后，您可以将指定权限（比如数据库和表的读取权限和资源的使用权限等）授予该角色，也可以将该角色授予某个用户。拥有该角色的用户即拥有该角色的相关权限。有关权限的详细说明，参见 [GRANT](./GRANT.md)。

<UserManagementPriv />

## 语法

```SQL
CREATE ROLE <role_name>
```

## 参数说明

`role_name`：角色名称，命名要求参见[系统限制](../../../reference/System_limit.md)。

> 注意：角色名称不能与 StarRocks 的系统预置角色 `root`，`cluster_admin`，`db_admin`，`user_admin`，`public` 重复。

## 示例

创建一个角色 `analyst`。

```SQL
CREATE ROLE analyst;
```

## 更多操作

- 如要查看角色，参见 [SHOW ROLES](./SHOW_ROLES.md)。
- 如要删除角色，参见 [DROP ROLE](./DROP_ROLE.md)。
- 如要创建用户，参见 [CREATE USER](./CREATE_USER.md)。
- 如要将指定权限授予该角色或将该角色授予某个用户，参见 [GRANT](./GRANT.md)。
