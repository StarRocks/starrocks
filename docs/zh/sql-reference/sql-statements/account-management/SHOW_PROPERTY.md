---
displayed_sidebar: docs
---

# SHOW PROPERTY

## 功能

SHOW PROPERTY 用于显示用户的属性，包括最大连接数、默认 Catalog 和默认数据库。

:::tip

当前用户可以查询自己的 property。但只有拥有 `user_admin` 角色的用户才可以查看其他用户的 property。

:::

:::info
如需设置 `database` 或 `catalog` 等属性，请使用带 `SET PROPERTIES` 子句的 [ALTER USER](./ALTER_USER.md) 命令。
对于 `max_user_connections`，可以使用 `SET PROPERTY` 语法。
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

+----------------------+-----------------+
| Key                  | Value           |
+----------------------+-----------------+
| max_user_connections | 1024            |
| catalog              | default_catalog |
| database             |                 |
+----------------------+-----------------+
```

示例二：查看用户 jack 的属性。

```SQL
SHOW PROPERTY FOR 'jack';
```

```Plain
+----------------------+------------------+
| Key                  | Value            |
+----------------------+------------------+
| max_user_connections | 100              |
| catalog              | default_catalog  |
| database             | sales_db         |
+----------------------+------------------+
```

示例三：使用 LIKE 过滤查看特定属性。

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
