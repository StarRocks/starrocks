# SHOW USERS

## 功能

查看当前系统中的所有用户。注意这里的用户不是用户名，而是用户标识 (user identity)。更多信息，参见 [CREATE USER](CREATE_USER.md)。该命令从 3.0 版本开始支持。

您可以通过 `SHOW GRANTS FOR <user_identity>;` 来查看某个用户的权限，参见 [SHOW GRANTS](SHOW_GRANTS.md)。

> 说明：只有 `user_admin` 角色有权限执行该语句。

## 语法

```SQL
SHOW USERS
```

返回字段说明：

| **字段名称** | **描述**   |
| ------------ | ---------- |
| User         | 用户标识。 |

## 示例

查看当前系统中的所有用户。

```SQL
mysql> SHOW USERS;
+-----------------+
| User            |
+-----------------+
| 'lily'@'%'      |
| 'root'@'%'      |
| 'admin'@'%'     |
| 'jack'@'%'      |
| 'tom'@'%'       |
+-----------------+
```

## 相关文档

- [CREATE USER](CREATE_USER.md)
- [ALTER USER](ALTER_USER.md)
- [DROP USER](DROP_USER.md)
