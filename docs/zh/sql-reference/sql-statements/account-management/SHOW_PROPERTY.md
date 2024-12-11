---
displayed_sidebar: docs
---


# SHOW PROPERTY

## 功能

<<<<<<< HEAD
查看单个用户的最大连接数。
=======
查看特定用户的属性。
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))

:::tip

当前用户可以查询自己的 property。但只有拥有 `user_admin` 角色的用户才可以查看其他用户的 property。

:::

## 语法

```SQL
<<<<<<< HEAD
SHOW PROPERTY [FOR 'user_name'] [LIKE 'max_user_connections']
=======
SHOW PROPERTY [FOR 'user_name'] [LIKE '<property_name>']
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
```

## 参数说明

| **参数**              | **必选** | **说明**                                    |
| -------------------- | -------- | ----------------------------------------- |
| user_name            | 否       | 用户名称。如不指定，默认查看当前用户的最大连接数。 |
<<<<<<< HEAD
| max_user_connections | 否       | 用户的最大连接数。                           |

## 示例

示例一：查看当前用户的最大连接数。
=======
| property_name        | 否       | 用户属性名。                                 |

## 示例

示例一：查看当前用户的属性。
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))

```Plain
SHOW PROPERTY;

+----------------------+-------+
| Key                  | Value |
+----------------------+-------+
| max_user_connections | 10000 |
+----------------------+-------+
```

<<<<<<< HEAD
示例二：查看用户 `jack` 的最大连接数。
=======
示例二：查看用户 `jack` 的属性。
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))

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

<<<<<<< HEAD
如要设置用户的最大连接数，请参见 [SET PROPERTY](./SET_PROPERTY.md)。
=======
如要设置用户属性，请参见 [ALTER USER](./ALTER_USER.md)。
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
