# EXECUTE AS

## Description

After you use the [GRANT](../account-management/GRANT.md) statement to impersonate a specific user identity to perform operations, you can use the EXECUTE AS statement to switch the execution context of the current session to this user.

## Syntax

```SQL
EXECUTE AS user WITH NO REVERT;
```

## Parameters

`user`: The user must already exist.

## Usage notes

- The current login user (who calls the EXECUTE AS statement) must be granted the IMPERSONATE permission.

- The EXECUTE AS statement must contain the WITH NO REVERT clause, which means the execution context of the current session cannot be switched back to the original login user before the current session ends.

## Examples

Switch the execution context of the current session to the user `test2`.

```SQL
MySQL [(none)]> execute as test2 with no revert;
Query OK, 0 rows affected (0.01 sec)
```

After the switch succeeds, you can run the `select current_user()` command to obtain the current user.

```SQL
MySQL [(none)]> select current_user();
+-----------------------------+
| CURRENT_USER()              |
+-----------------------------+
| 'default_cluster:test2'@'%' |
+-----------------------------+
1 row in set (0.03 sec)
```
