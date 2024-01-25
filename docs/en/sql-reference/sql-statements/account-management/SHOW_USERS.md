---
displayed_sidebar: "English"
---

# SHOW USERS

import UserManagementPriv from '../assets/commonMarkdown/userManagementPriv.md'

## Description

Displays all users in the system. Users mentioned here are user identities, not user names. For more information about user identities, see [CREATE USER](CREATE_USER.md). This command is supported from v3.0.

You can use `SHOW GRANTS FOR <user_identity>;` to view the privileges of a specific user. For more information, see [SHOW GRANTS](SHOW_GRANTS.md).

<UserManagementPriv />

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

[CREATE USER](CREATE_USER.md), [ALTER USER](ALTER_USER.md), [DROP USER](DROP_USER.md)
