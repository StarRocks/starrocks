---
displayed_sidebar: docs
description: "SHOW AUTHENTICATION displays the authentication information of the current user or all users in the current cluster."
---

# SHOW AUTHENTICATION

SHOW AUTHENTICATION displays the authentication information of the current user or all users in the current cluster.

:::tip
All users have the privilege to view their own authentication information. Only users with the `user_admin` role can view the authentication information of all users or the authentication information of specified users.
:::

A user that logs in through a security integration is not created in StarRocks: StarRocks creates a virtual user for its session. Such a user can view its own authentication information with `SHOW AUTHENTICATION` without `FOR`. It cannot be specified in `FOR USERNAME`, which accepts only users created in StarRocks, and it is not returned by `SHOW ALL AUTHENTICATION`.

## Syntax

```SQL
SHOW [ALL] AUTHENTICATION [FOR USERNAME]
```

## Parameters

| **Parameter** | **Required** | **Description**                                              |
| ------------- | ------------ | ------------------------------------------------------------ |
| ALL           | No           | If this keyword is specified, the authentication information of all users in the current cluster is returned. If this keyword is not specified, only the authentication information of the current user is returned. |
| USERNAME      | No           | If this parameter is specified, the authentication information of a specified user can be viewed. If this parameter is not specified, only the authentication information of the current user can be viewed. |

## Output

```SQL
+---------------+----------+-------------+-------------------+
| UserIdentity  | Password | AuthPlugin  | UserForAuthPlugin |
+---------------+----------+-------------+-------------------+
```

| **Field**         | **Description**                                              |
| ----------------- | ------------------------------------------------------------ |
| UserIdentity      | The user identity.                                           |
| Password          | Whether a password is used to log in to the StarRocks cluster.<ul><li>`Yes`: A password is used.</li><li>`No`: No password is used.</li></ul>For a user that logs in through a security integration, `No` is returned, because StarRocks does not store its password. |
| AuthPlugin        | The interface that is used for authentication. Valid values: `MYSQL_NATIVE_PASSWORD` and `AUTHENTICATION_LDAP_SIMPLE`. If no interface is used, `NULL` is returned. For a user that logs in through a security integration, the name of that security integration is returned. |
| UserForAuthPlugin | The name of the user using the LDAP or Kerberos authentication. If no authentication is used, `NULL` is returned. |

## Examples

Example 1: Display the authentication information of the current user.

```Plain
SHOW AUTHENTICATION;
+--------------+----------+------------+-------------------+
| UserIdentity | Password | AuthPlugin | UserForAuthPlugin |
+--------------+----------+------------+-------------------+
| 'root'@'%'   | No       | NULL       | NULL              |
+--------------+----------+------------+-------------------+
```

Example 2: Display the authentication information of all users in the current cluster.

```Plain
SHOW ALL AUTHENTICATION;
+---------------+----------+----------------------------+-------------------+
| UserIdentity  | Password | AuthPlugin                 | UserForAuthPlugin |
+---------------+----------+----------------------------+-------------------+
| 'root'@'%'    | Yes      | NULL                       | NULL              |
| 'chelsea'@'%' | No       | AUTHENTICATION_LDAP_SIMPLE | NULL              |
+---------------+----------+----------------------------+-------------------+
```

Example 3: Display the authentication information of a specified user.

```Plain
SHOW AUTHENTICATION FOR root;
+--------------+----------+------------+-------------------+
| UserIdentity | Password | AuthPlugin | UserForAuthPlugin |
+--------------+----------+------------+-------------------+
| 'root'@'%'   | Yes      | NULL       | NULL              |
+--------------+----------+------------+-------------------+
```

Example 4: Display the authentication information of the current user, who logs in through the security integration `ldap_si`.

```Plain
SHOW AUTHENTICATION;
+------------------------+----------+------------+-------------------+
| UserIdentity           | Password | AuthPlugin | UserForAuthPlugin |
+------------------------+----------+------------+-------------------+
| 'alice'@'192.168.1.10' | No       | ldap_si    | NULL              |
+------------------------+----------+------------+-------------------+
```
