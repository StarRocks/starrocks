# SHOW AUTHENTICATION

## Description

Displays the authentication information of the current user or all users in the current cluster. Each user has permissions to view their authentication information. Only users with the global `GRANT_PRIV` permission can view the authentication information of all users or the authentication information of specified users.

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
| Password          | Whether a password is used to log in to the StarRocks cluster.<ul><li>`Yes`: A password is used.</li><li>`No`: No password is used.</li></ul> |
| AuthPlugin        | The interface that is used for authentication. Valid values: `MYSQL_NATIVE_PASSWORD`,  `AUTHENTICATION_LDAP_SIMPLE`, or `AUTHENTICATION_KERBEROS`. If no interface is used, `NULL` is returned. |
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
+---------------+----------+-------------------------+-------------------+
| UserIdentity  | Password | AuthPlugin              | UserForAuthPlugin |
+---------------+----------+-------------------------+-------------------+
| 'root'@'%'    | Yes      | NULL                    | NULL              |
| 'chelsea'@'%' | No       | AUTHENTICATION_KERBEROS | HADOOP.COM        |
+---------------+----------+-------------------------+-------------------+
```

Example 3：Display the authentication information of a specified user.

```Plain
SHOW AUTHENTICATION FOR root;
+--------------+----------+------------+-------------------+
| UserIdentity | Password | AuthPlugin | UserForAuthPlugin |
+--------------+----------+------------+-------------------+
| 'root'@'%'   | Yes      | NULL       | NULL              |
+--------------+----------+------------+-------------------+
```
