---
displayed_sidebar: docs
---


# SHOW PROPERTY

## 功能

查看特定用户的属性。

:::tip

当前用户可以查询自己的 property。但只有拥有 `user_admin` 角色的用户才可以查看其他用户的 property。

:::

## 语法

```SQL
SHOW PROPERTY [FOR 'user_name'] [LIKE '<property_name>']
```

## 参数说明

| **参数**              | **必选** | **说明**                                    |
| -------------------- | -------- | ----------------------------------------- |
| user_name            | 否       | 用户名称。如不指定，默认查看当前用户的最大连接数。 |
| property_name        | 否       | 用户属性名。                                 |

## 示例

示例一：查看当前用户的属性。

```Plain
SHOW PROPERTY;

+----------------------+-------+
| Key                  | Value |
+----------------------+-------+
| max_user_connections | 10000 |
+----------------------+-------+
```

示例二：查看用户 `jack` 的属性。

```SQL
SHOW PROPERTY FOR 'jack';
```

或

```SQL
SHOW PROPERTY FOR 'jack' LIKE 'max_user_connections';
```

返回信息如下：

```Plain
+----------------------+-------+
| Key                  | Value |
+----------------------+-------+
| max_user_connections | 100   |
+----------------------+-------+
```

## 相关操作

如要设置用户属性，请参见 [ALTER USER](./ALTER_USER.md)。
