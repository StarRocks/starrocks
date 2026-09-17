---
displayed_sidebar: docs
description: "SHOW CREATE USER returns the CREATE USER statement that reproduces a user."
---

# SHOW CREATE USER

SHOW CREATE USER returns the `CREATE USER` statement that reproduces a user, including its authentication method, default roles, account state, and properties.

The statement reflects the current state of the user. Changes made by `ALTER USER` or `SET PASSWORD` after the user was created are included.

:::tip
All users can run this statement against themselves. Viewing another user requires the `user_admin` role or the GRANT privilege on SYSTEM.

The password digest is returned only to users that hold the SHOW SECRET privilege on SYSTEM. Otherwise it is masked as `<secret>`. This applies to your own user as well.

SHOW SECRET withholds the digest by default; it is not a boundary that contains an administrator. Any holder of the GRANT privilege on SYSTEM, which includes the `user_admin` role, can grant SHOW SECRET to itself.
:::

## Syntax

```SQL
SHOW CREATE USER { <user_identity> | CURRENT_USER() }
```

## Parameters

| **Parameter**  | **Required** | **Description**                                              |
| -------------- | ------------ | ------------------------------------------------------------ |
| user_identity  | No           | The user to display, in the `'user_name'@'host'` form.       |
| CURRENT_USER() | No           | Displays the current user. Equivalent to passing your own user identity. |

## Output

```SQL
+------+-------------+
| User | Create User |
+------+-------------+
```

| **Field**   | **Description**                                      |
| ----------- | ---------------------------------------------------- |
| User        | The user identity.                                   |
| Create User | The `CREATE USER` statement that reproduces the user. |

Clauses are emitted only when they differ from the default:

| Clause            | Emitted when                                                 |
| ----------------- | ------------------------------------------------------------ |
| `IDENTIFIED WITH` | The user has a password, or uses an authentication plugin other than `mysql_native_password`. |
| `DEFAULT ROLE`    | The user has at least one default role.                      |
| `EXPIRE_PASSWORD` | The password is marked as expired.                           |
| `LOCK`            | The account is locked.                                       |
| `PROPERTIES`      | At least one user property differs from its default value.   |

:::note
Three caveats apply to the emitted statement:

- A masked statement must never be replayed. `<secret>` is not a password: for `mysql_native_password` it is rejected as an invalid digest, but another authentication plugin may accept it as a literal auth string and create an account that nobody can log in to. Re-read the user with the SHOW SECRET privilege instead.
- User properties are stored per user name rather than per user identity. `'jack'@'192.168.%'` and `'jack'@'10.1.%'` therefore return the same `PROPERTIES` clause.
- If a password policy applies to the user, through either the `PASSWORD_POLICY` property or a cluster-wide policy, the emitted statement cannot be replayed as it stands. `IDENTIFIED WITH ... AS` supplies a password digest, and a password policy can only validate a plaintext password, so `CREATE USER` rejects it with `Because the Password Policy is in effect, you cannot use a hashed password.` Recreate such a user with a plaintext password instead.
:::

## Examples

Create a user and grant it a default role.

```SQL
CREATE USER 'jack'@'%' IDENTIFIED BY '123456' DEFAULT ROLE 'db_admin'
PROPERTIES ("max_user_connections" = "100");
```

Example 1: Display the user as a holder of the SHOW SECRET privilege.

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

Example 2: Display the same user without the SHOW SECRET privilege.

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

Example 3: Display the current user. A user with no password, no default role, and only default properties yields a single-line statement.

```Plain
SHOW CREATE USER CURRENT_USER()\G
*************************** 1. row ***************************
       User: 'tom'@'%'
Create User: CREATE USER 'tom'@'%'
1 row in set (0.00 sec)
```

Example 4: Grant the SHOW SECRET privilege so that a user can read password digests.

```SQL
GRANT SHOW SECRET ON SYSTEM TO USER 'jack'@'%';
```

## References

- [CREATE USER](CREATE_USER.md)
- [ALTER USER](ALTER_USER.md)
- [SHOW AUTHENTICATION](SHOW_AUTHENTICATION.md)
- [GRANT](GRANT.md)
