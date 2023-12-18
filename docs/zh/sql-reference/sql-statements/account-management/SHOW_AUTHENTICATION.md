---
displayed_sidebar: "Chinese"
---

# SHOW AUTHENTICATION

## 功能

查看当前用户或当前集群所有用户的认证信息。每个用户都有权限查看自己的认证信息，但只有拥有 `user_admin` 角色或全局 GRANT 权限的用户才可以查看所有用户的认证信息或指定用户的认证信息。

## 语法

```SQL
SHOW [ALL] AUTHENTICATION [FOR USERNAME]
```

## 参数说明

| **参数** | **必选** | **说明**                                                     |
| -------- | -------- | ------------------------------------------------------------ |
| ALL      | 否       | 如不指定，则查看用户自己的认证信息。如指定，则查看当前集群所有用户的认证信息。 |
| USERNAME      | 否       | 如不指定，则查看用户自己的认证信息。如指定，则查看指定的用户的认证信息。 |

## 返回信息说明

```SQL
+---------------+----------+-------------+-------------------+
| UserIdentity  | Password | AuthPlugin  | UserForAuthPlugin |
+---------------+----------+-------------+-------------------+
```

| **字段**          | **说明**                                                     |
| ----------------- | ------------------------------------------------------------ |
| UserIdentity      | 用户标识。                                                   |
| Password          | 是否使用密码登录到 StarRocks 集群。<ul><li>`Yes`：使用密码登录。</li><li>`No`：不使用密码登录。</li></ul> |
| AuthPlugin        | 使用的认证接口，包括三种：`MYSQL_NATIVE_PASSWORD`，`AUTHENTICATION_LDAP_SIMPLE`，`AUTHENTICATION_KERBEROS`。如未使用认证接口，则返回 `NULL`。 |
| UserForAuthPlugin | 使用 LDAP 或 Kerberos 认证的用户名称。如未使用认证，则返回 `NULL`。 |

## 示例

示例一：查看当前用户的认证信息。

```Plain
SHOW AUTHENTICATION;
+--------------+----------+------------+-------------------+
| UserIdentity | Password | AuthPlugin | UserForAuthPlugin |
+--------------+----------+------------+-------------------+
| 'root'@'%'   | No       | NULL       | NULL              |
+--------------+----------+------------+-------------------+
```

示例二：查看所有用户的认证信息。

```Plain
SHOW ALL AUTHENTICATION;
+---------------+----------+-------------------------+-------------------+
| UserIdentity  | Password | AuthPlugin              | UserForAuthPlugin |
+---------------+----------+-------------------------+-------------------+
| 'root'@'%'    | Yes      | NULL                    | NULL              |
| 'chelsea'@'%' | No       | AUTHENTICATION_KERBEROS | HADOOP.COM        |
+---------------+----------+-------------------------+-------------------+
```

示例三：查看指定用户的认证信息。

```Plain
SHOW AUTHENTICATION FOR root;
+--------------+----------+------------+-------------------+
| UserIdentity | Password | AuthPlugin | UserForAuthPlugin |
+--------------+----------+------------+-------------------+
| 'root'@'%'   | Yes      | NULL       | NULL              |
+--------------+----------+------------+-------------------+
```
