---
displayed_sidebar: docs
---

# DROP ROLE

## 説明

<<<<<<< HEAD
ロールを削除します。ロールがユーザーに付与されている場合、ロールが削除された後でも、そのユーザーはこのロールに関連付けられた権限を保持します。
=======
ロールを削除します。ユーザーにロールが付与されている場合、そのロールが削除されると、ユーザーは当該ロールに関連付けられた権限を失います。
>>>>>>> e3a2e73c2e ([Doc] Fix DROP ROLE Description (#62946))

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