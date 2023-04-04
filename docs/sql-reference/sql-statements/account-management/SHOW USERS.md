# SHOW USERS

## Description

Displays all users in the system. Users mentioned here are user identities, not user names. For more information about user identities, see [CREATE USER](CREATE%20USER.md). This command is supported from v3.0.

You can use `SHOW GRANTS FOR <user_identity>;` to view the privileges of a specific user. For more information, see [SHOW GRANTS](SHOW%20GRANTS.md).

> Note: Only the `user_admin` role can execute this statement.

## Syntax

```SQL
SHOW USERS
```

Return fields:

| **Field** | **Description**    |
| --------- | ------------------ |
| User      | The user identity. |

## Examples

Display all users in the system.

```SQL
mysql> SHOW USERS;
+-----------------+
| User            |
+-----------------+
| 'wybing5'@'%'   |
| 'root'@'%'      |
| 'admin'@'%'     |
| 'star'@'%'      |
| 'wybing_30'@'%' |
| 'simo'@'%'      |
| 'wybing1'@'%'   |
| 'wybing2'@'%'   |
+-----------------+
```

## References

[CREATE USER](CREATE%20USER.md), [ALTER USER](ALTER%20USER.md), [DROP USER](DROP%20USER.md)
