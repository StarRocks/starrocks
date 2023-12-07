---
displayed_sidebar: "Chinese"
---

# DROP ROLE

## 功能

删除一个角色。如一个角色已授予某用户，那么该角色删除后，该用户仍然保留该角色拥有的相关权限。

> 说明：
>
> - 只有拥有 `GRANT_PRIV` 或 `ADMIN_PRIV` 权限的用户才可以删除角色。更多权限说明，参见 [GRANT](./GRANT.md)。
> - StarRocks 的两个系统角色 admin 和 operator 均拥有 `ADMIN_PRIV` 权限，所以拥有这两个系统角色的用户均可以删除角色。

## 语法

```SQL
DROP ROLE <role_name>
```

## 参数说明

`role_name`：要删除的角色名称。注意 StarRocks 系统角色 admin 和 operator 无法删除。

## 示例

删除角色`analyst`。

```SQL
  DROP ROLE analyst;
```
