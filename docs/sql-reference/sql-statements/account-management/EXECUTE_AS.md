# EXECUTE AS

## Description

After you obtain the privilege to impersonate a user, you can use the EXECUTE AS statement to switch the execution context of the current session to the user.

This command is supported from v2.4.

## Syntax

```SQL
EXECUTE AS user WITH NO REVERT
```

## Parameters

`user`: The user must already exist.

## Usage notes

- The current login user (who calls the EXECUTE AS statement) must be granted the privilege to impersonate another user. For more information, see [GRANT](../account-management/GRANT.md).
- The EXECUTE AS statement must contain the WITH NO REVERT clause, which means the execution context of the current session cannot be switched back to the original login user before the current session ends.

## Examples

Switch the execution context of the current session to the user `test2`.

```SQL
EXECUTE AS test2 WITH NO REVERT;
```

After the switch succeeds, you can run the `select current_user()` command to obtain the current user.

```SQL
select current_user();
+-----------------------------+
| CURRENT_USER()              |
+-----------------------------+
| 'default_cluster:test2'@'%' |
+-----------------------------+
```
