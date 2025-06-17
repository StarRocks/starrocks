---
displayed_sidebar: docs
---

# SHOW USERS

import UserManagementPriv from '../../../_assets/commonMarkdown/userManagementPriv.md'

## 説明

システム内のすべてのユーザーを表示します。ここで言及されているユーザーはユーザーアイデンティティであり、ユーザー名ではありません。ユーザーアイデンティティの詳細については、[CREATE USER](CREATE_USER.md) を参照してください。このコマンドは v3.0 からサポートされています。

特定のユーザーの権限を表示するには、`SHOW GRANTS FOR <user_identity>;` を使用します。詳細については、[SHOW GRANTS](SHOW_GRANTS.md) を参照してください。

<UserManagementPriv />

## 構文

```SQL
SHOW USERS
```

返されるフィールド:

| **Field** | **Description**    |
| --------- | ------------------ |
| User      | ユーザーアイデンティティ。 |

## 例

システム内のすべてのユーザーを表示します。

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

## 参照

[CREATE USER](CREATE_USER.md), [ALTER USER](ALTER_USER.md), [DROP USER](DROP_USER.md)