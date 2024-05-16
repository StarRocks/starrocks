---
displayed_sidebar: "Chinese"
---

# SET PROPERTY

## 功能

设置用户属性。当前仅支持设置单个用户的最大连接数。

:::tip

- 这里的属性是指用户 (user) 的属性，而非用户标识 (user_identity) 的属性。例如，如果通过 CREATE USER 语句创建了 `'jack'@'%'` 和 `'jack'@'192.%'`，那么使用 SET PROPERTY 语句设置的是 `jack` 这个用户的属性，而不是 `'jack'@'%'` 或 `'jack'@'192.%'` 的属性。
- 只有拥有 `user_admin` 角色的用户才可以执行此操作。

:::

## 语法

```SQL
SET PROPERTY [FOR 'user'] 'max_user_connections' = 'value'
```

## 参数说明

- `For 'user'`：指定用户，可选参数。如不设置该参数，默认设置当前用户的属性。

- `'max_user_connections' = 'value'`：单个用户的最大连接数，必选参数。有效数值范围：1~10000。

## 示例

示例一：设置当前用户的最大连接数为 1000。

```SQL
SET PROPERTY 'max_user_connections' = '1000';
```

示例二：设置用户 `jack` 的最大连接数为 1000。

```SQL
SET PROPERTY FOR 'jack' 'max_user_connections' = '1000';
```

## 相关操作

- 如要创建一个用户，参见 [CREATE USER](./CREATE_USER.md)。

- 如要查看一个用户属性，参见 [SHOW PROPERTY](./SHOW_PROPERTY.md)。
