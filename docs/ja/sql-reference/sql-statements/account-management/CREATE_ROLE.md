---
displayed_sidebar: docs
---

# CREATE ROLE

import UserManagementPriv from '../../../_assets/commonMarkdown/userManagementPriv.mdx'

## 説明

ロールを作成します。ロールが作成された後、そのロールに権限を付与し、そのロールをユーザーまたは別のロールに割り当てることができます。この方法で、このロールに関連付けられた権限がユーザーやロールに引き継がれます。

<UserManagementPriv />

## 構文

```sql
CREATE ROLE <role_name>
```

## パラメーター

`role_name`: ロールの名前。命名規則については、[System limits](../../System_limit.md) を参照してください。

作成されたロール名は、[system-defined roles](../../../administration/user_privs/privilege_overview.md#system-defined-roles) と同じにすることはできません: `root`、`cluster_admin`、`db_admin`、`user_admin`、および `public`。

## 制限

デフォルトでは、ユーザーは最大64のロールを持つことができます。この設定は、FE の動的パラメーター `privilege_max_total_roles_per_user` を使用して調整できます。ロールは最大16の継承レベルを持つことができます。この設定は、FE の動的パラメーター `privilege_max_role_depth` を使用して調整できます。

## 例

ロールを作成します。

  ```sql
  CREATE ROLE role1;
  ```

## 参考

- [GRANT](GRANT.md)
- [SHOW ROLES](SHOW_ROLES.md)
- [DROP ROLE](DROP_ROLE.md)