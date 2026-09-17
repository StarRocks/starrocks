---
displayed_sidebar: docs
description: "SHOW CREATE USER returns the CREATE USER statement that reproduces a user."
---

# SHOW CREATE USER

## 功能

返回可重建指定用户的 `CREATE USER` 语句，包含认证方式、默认角色、账号状态以及用户属性。

返回的是用户的当前状态。用户创建之后通过 `ALTER USER` 或 `SET PASSWORD` 所做的修改都会体现在结果中。

:::tip

每个用户都可以对自己执行该语句。查看其他用户需要 `user_admin` 角色或 SYSTEM 级的 GRANT 权限。

只有拥有 SYSTEM 级 SHOW SECRET 权限的用户才能看到密码密文，否则密文显示为 `<secret>`。查看自己的用户时同样适用。

SHOW SECRET 只是默认不给，并不能约束管理员：任何拥有 SYSTEM 级 GRANT 权限的用户（包括 `user_admin` 角色）都可以把该权限授予自己。

:::

## 语法

```SQL
SHOW CREATE USER { <user_identity> | CURRENT_USER() }
```

## 参数说明

| **参数**       | **必选** | **说明**                                                     |
| -------------- | -------- | ------------------------------------------------------------ |
| user_identity  | 否       | 待查看的用户，格式为 `'user_name'@'host'`。                  |
| CURRENT_USER() | 否       | 查看当前用户，等同于传入自己的用户标识。                     |

## 返回信息说明

```SQL
+------+-------------+
| User | Create User |
+------+-------------+
```

| **字段**    | **说明**                          |
| ----------- | --------------------------------- |
| User        | 用户标识。                        |
| Create User | 可重建该用户的 `CREATE USER` 语句。 |

只有非默认值的子句才会输出：

| 子句              | 输出条件                                                     |
| ----------------- | ------------------------------------------------------------ |
| `IDENTIFIED WITH` | 用户设置了密码，或使用了 `mysql_native_password` 以外的认证插件。 |
| `DEFAULT ROLE`    | 用户至少有一个默认角色。                                     |
| `EXPIRE_PASSWORD` | 密码被标记为已过期。                                         |
| `LOCK`            | 账号处于锁定状态。                                           |
| `PROPERTIES`      | 至少有一项用户属性不是默认值。                               |

:::note

输出的语句有三点限制：

- 带 `<secret>` 的语句不能拿去回放。`<secret>` 不是密码：对 `mysql_native_password` 会因密文非法被拒绝，而换成别的认证插件时可能被当作合法的认证串接受，建出一个谁也登录不了的账号。请改用有 SHOW SECRET 权限的用户重新查询。
- 用户属性按用户名存储，而非按用户标识存储。因此 `'jack'@'192.168.%'` 和 `'jack'@'10.1.%'` 返回的 `PROPERTIES` 子句相同。
- 如果用户命中了密码策略（通过 `PASSWORD_POLICY` 属性或集群级策略），输出的语句无法直接回放。`IDENTIFIED WITH ... AS` 给出的是密码密文，而密码策略只能校验明文密码，因此 `CREATE USER` 会报错 `Because the Password Policy is in effect, you cannot use a hashed password.`。这类用户需改用明文密码重建。

:::

## 示例

创建一个带默认角色的用户。

```SQL
CREATE USER 'jack'@'%' IDENTIFIED BY '123456' DEFAULT ROLE 'db_admin'
PROPERTIES ("max_user_connections" = "100");
```

示例一：由拥有 SHOW SECRET 权限的用户查看。

```Plain
SHOW CREATE USER 'jack'@'%'\G
*************************** 1. row ***************************
       User: 'jack'@'%'
Create User: CREATE USER 'jack'@'%'
IDENTIFIED WITH mysql_native_password AS '*6BB4837EB74329105EE4568DDA7DC67ED2CA2AD9'
DEFAULT ROLE 'db_admin'
PROPERTIES ("max_user_connections" = "100")
1 row in set (0.00 sec)
```

示例二：由没有 SHOW SECRET 权限的用户查看同一个用户。

```Plain
SHOW CREATE USER 'jack'@'%'\G
*************************** 1. row ***************************
       User: 'jack'@'%'
Create User: CREATE USER 'jack'@'%'
IDENTIFIED WITH mysql_native_password AS '<secret>'
DEFAULT ROLE 'db_admin'
PROPERTIES ("max_user_connections" = "100")
1 row in set (0.00 sec)
```

示例三：查看当前用户。没有密码、没有默认角色且属性全为默认值的用户，输出为单行语句。

```Plain
SHOW CREATE USER CURRENT_USER()\G
*************************** 1. row ***************************
       User: 'tom'@'%'
Create User: CREATE USER 'tom'@'%'
1 row in set (0.00 sec)
```

示例四：授予 SHOW SECRET 权限，使指定用户可以查看密码密文。

```SQL
GRANT SHOW SECRET ON SYSTEM TO USER 'jack'@'%';
```

## 相关文档

- [CREATE USER](CREATE_USER.md)
- [ALTER USER](ALTER_USER.md)
- [SHOW AUTHENTICATION](SHOW_AUTHENTICATION.md)
- [GRANT](GRANT.md)
