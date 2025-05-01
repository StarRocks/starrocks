---
displayed_sidebar: docs
---

# CREATE ROLE

## 説明

このステートメントは、ユーザーがロールを作成することを可能にします。

## 構文

```sql
CREATE ROLE <role_name>
```

このコマンドは、"GRANT" コマンドを通じてロールに追加できる権限を持たないロールを作成します。

## 例

ロールを作成します。

  ```sql
  CREATE ROLE role1;
  ```