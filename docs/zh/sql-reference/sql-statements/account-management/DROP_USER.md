---
displayed_sidebar: "Chinese"
---

# DROP USER

## 功能

删除用户。

:::tip

只有拥有 `user_admin` 角色的用户才可以删除用户。

:::

## 语法

```sql
 DROP USER 'user_identity'

 -- 参数说明
user_identity:user@'host'
```

 删除指定的 `user identitiy`。`user identitiy` 由 `user_name` 和 `host` 两部分组成，其中 `user_name` 为用户名。`host` 标识用户端连接所在的主机地址。

## 示例

删除用户 `jack@'192.%'`。

```sql
DROP USER 'jack'@'192.%';
```
