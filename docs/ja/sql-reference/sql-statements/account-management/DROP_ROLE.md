---
displayed_sidebar: docs
---

# DROP ROLE

## 説明

ロールを削除します。ロールがユーザーに付与されている場合、そのロールが削除された後もユーザーはそのロールに関連する権限を保持します。

:::tip

- `user_admin` ロールを持つユーザーのみがロールを削除できます。
- [StarRocks システム定義ロール](../../../administration/user_privs/privilege_overview.md#system-defined-roles) は削除できません。

:::

## 構文

```sql
DROP ROLE <role_name>
```

## 例

ロールを削除します。

  ```sql
  DROP ROLE role1;
  ```