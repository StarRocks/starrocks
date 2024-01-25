---
displayed_sidebar: "Chinese"
---

# DROP ROLE

## 功能

删除一个角色。如一个角色已授予某用户，那么该角色删除后，该用户仍然保留该角色拥有的相关权限。

:::tip

- 只有拥有 `user_admin` 角色的用户才有权限删除角色。
- StarRocks 系统预置角色不可以删除。

:::

## 语法

```SQL
DROP ROLE <role_name>
```

## 参数说明

`role_name`：要删除的角色名称。

## 示例

删除角色`analyst`。

```SQL
  DROP ROLE analyst;
```
