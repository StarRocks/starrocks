---
displayed_sidebar: docs
---

# DROP ROLE

## 説明

ロールを削除します。ユーザーにロールが付与されている場合、そのロールが削除されると、ユーザーは当該ロールに関連付けられた権限を失います。

:::tip

- `user_admin` ロールを持つユーザーのみがロールを削除できます。
- [StarRocks システム定義ロール](../../../administration/user_privs/authorization/user_privs.md#system-defined-roles) は削除できません。

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
