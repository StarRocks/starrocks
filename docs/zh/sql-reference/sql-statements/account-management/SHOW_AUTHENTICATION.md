---
displayed_sidebar: docs
description: "SHOW AUTHENTICATION displays the authentication information of the current user or all users in the current cluster."
---

# SHOW AUTHENTICATION

## 功能

查看当前用户或当前集群所有用户的认证信息。

:::tip

每个用户都有权限查看自己的认证信息，但只有拥有 `user_admin` 角色或全局 GRANT 权限的用户才可以查看所有用户的认证信息或指定用户的认证信息。

:::

通过安全集成登录的用户不会在 StarRocks 中创建：StarRocks 只在会话中为其创建一个虚拟用户。这类用户可以用不带 `FOR` 的 `SHOW AUTHENTICATION` 查看自己的认证信息，但不能在 `FOR USERNAME` 中指定（它只接受在 StarRocks 中创建的用户），`SHOW ALL AUTHENTICATION` 也不会返回这类用户。

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
| Password          | 是否使用密码登录到 StarRocks 集群。<ul><li>`Yes`：使用密码登录。</li><li>`No`：不使用密码登录。</li></ul>通过安全集成登录的用户返回 `No`，因为 StarRocks 不保存其密码。 |
| AuthPlugin        | 使用的认证接口，包括 `MYSQL_NATIVE_PASSWORD` 和 `AUTHENTICATION_LDAP_SIMPLE`。如未使用认证接口，则返回 `NULL`。通过安全集成登录的用户返回该安全集成的名称。 |
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
+---------------+----------+----------------------------+-------------------+
| UserIdentity  | Password | AuthPlugin                 | UserForAuthPlugin |
+---------------+----------+----------------------------+-------------------+
| 'root'@'%'    | Yes      | NULL                       | NULL              |
| 'chelsea'@'%' | No       | AUTHENTICATION_LDAP_SIMPLE | NULL              |
+---------------+----------+----------------------------+-------------------+
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

示例四：当前用户通过安全集成 `ldap_si` 登录，查看其认证信息。

```Plain
SHOW AUTHENTICATION;
+------------------------+----------+------------+-------------------+
| UserIdentity           | Password | AuthPlugin | UserForAuthPlugin |
+------------------------+----------+------------+-------------------+
| 'alice'@'192.168.1.10' | No       | ldap_si    | NULL              |
+------------------------+----------+------------+-------------------+
```
