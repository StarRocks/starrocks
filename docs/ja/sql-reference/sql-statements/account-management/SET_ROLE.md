---
displayed_sidebar: docs
---

# SET ROLE

## 説明

現在のセッションで、ロールとその関連するすべての権限およびネストされたロールをアクティブにします。ロールがアクティブ化された後、ユーザーはこのロールを使用して操作を実行できます。

このコマンドを実行した後、`select is_role_in_session("<role_name>");` を実行して、このロールが現在のセッションでアクティブかどうかを確認できます。

このコマンドは v3.0 からサポートされています。

## 使用上の注意

ユーザーは、自分に割り当てられたロールのみをアクティブにできます。

ユーザーのロールを照会するには、[SHOW GRANTS](./SHOW_GRANTS.md) を使用します。

現在のユーザーのアクティブなロールを照会するには、`SELECT CURRENT_ROLE()` を使用します。詳細については、[current_role](../../sql-functions/utility-functions/current_role.md) を参照してください。

## 構文

```SQL
-- 特定のロールをアクティブにし、このロールとして操作を実行します。
SET ROLE <role_name>[,<role_name>,..];
-- 特定のロールを除いて、ユーザーのすべてのロールをアクティブにします。
SET ROLE ALL EXCEPT <role_name>[,<role_name>,..]; 
-- ユーザーのすべてのロールをアクティブにします。
SET ROLE ALL;
```

## パラメータ

`role_name`: ロール名

## 例

現在のユーザーのすべてのロールを照会します。

```SQL
SHOW GRANTS;
+--------------+---------+----------------------------------------------+
| UserIdentity | Catalog | Grants                                       |
+--------------+---------+----------------------------------------------+
| 'test'@'%'   | NULL    | GRANT 'db_admin', 'user_admin' TO 'test'@'%' |
+--------------+---------+----------------------------------------------+
```

`db_admin` ロールをアクティブにします。

```SQL
SET ROLE db_admin;
```

現在のユーザーのアクティブなロールを照会します。

```SQL
SELECT CURRENT_ROLE();
+--------------------+
| CURRENT_ROLE()     |
+--------------------+
| db_admin           |
+--------------------+
```

## 参考

- [CREATE ROLE](CREATE_ROLE.md): ロールを作成します。
- [GRANT](GRANT.md): ユーザーまたは他のロールにロールを割り当てます。
- [ALTER USER](ALTER_USER.md): ロールを変更します。
- [SHOW ROLES](SHOW_ROLES.md): システム内のすべてのロールを表示します。
- [current_role](../../sql-functions/utility-functions/current_role.md): 現在のユーザーのロールを表示します。
- [is_role_in_session](../../sql-functions/utility-functions/is_role_in_session.md): ロール（またはネストされたロール）が現在のセッションでアクティブかどうかを確認します。
- [DROP ROLE](DROP_ROLE.md): ロールを削除します。