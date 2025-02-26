---
displayed_sidebar: docs
---

# SHOW ROLES

import UserManagementPriv from '../../../_assets/commonMarkdown/userManagementPriv.md'

## 説明

システム内のすべてのロールを表示します。特定のロールの権限を表示するには、`SHOW GRANTS FOR ROLE <role_name>;` を使用します。詳細については、[SHOW GRANTS](SHOW_GRANTS.md) を参照してください。

このコマンドは v3.0 からサポートされています。

<UserManagementPriv />

## 構文

```SQL
SHOW ROLES
```

返されるフィールド:

| **Field** | **Description**       |
| --------- | --------------------- |
| Name      | ロールの名前です。    |

## 例

システム内のすべてのロールを表示します。

```SQL
mysql> SHOW ROLES;
+---------------+
| Name          |
+---------------+
| root          |
| db_admin      |
| cluster_admin |
| user_admin    |
| public        |
| testrole      |
+---------------+
```

## 参考

- [CREATE ROLE](CREATE_ROLE.md)
- [ALTER USER](ALTER_USER.md)
- [DROP ROLE](DROP_ROLE.md)