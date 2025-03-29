---
displayed_sidebar: docs
---

# Privilege FAQ

## 必要なロールがユーザーに割り当てられているにもかかわらず、「no permission」というエラーメッセージが報告されるのはなぜですか？

このエラーは、ロールがアクティブ化されていない場合に発生することがあります。`select current_role();` を実行して、現在のセッションでユーザーにアクティブ化されているロールを確認できます。必要なロールがアクティブ化されていない場合は、[SET ROLE](../../sql-reference/sql-statements/account-management/SET_ROLE.md) を実行してこのロールをアクティブ化し、このロールを使用して操作を行ってください。

ログイン時にロールを自動的にアクティブ化したい場合、`user_admin` ロールは [SET DEFAULT ROLE](../../sql-reference/sql-statements/account-management/SET_DEFAULT_ROLE.md) または [ALTER USER DEFAULT ROLE](../../sql-reference/sql-statements/account-management/ALTER_USER.md) を実行して、各ユーザーのデフォルトロールを設定できます。デフォルトロールが設定されると、ユーザーがログインしたときに自動的にアクティブ化されます。

すべてのユーザーに割り当てられたすべてのロールをログイン時に自動的にアクティブ化したい場合は、次のコマンドを実行できます。この操作には、システムレベルでの OPERATE 権限が必要です。

```SQL
SET GLOBAL activate_all_roles_on_login = TRUE;
```

しかし、潜在的なリスクを防ぐために、権限が制限されたデフォルトロールを設定する「最小権限」の原則に従うことをお勧めします。例えば：

- 一般ユーザーは、SELECT 権限のみを持つ `read_only` ロールをデフォルトロールとして設定し、ALTER、DROP、INSERT などの権限を持つロールをデフォルトロールとして設定しないようにします。
- 管理者は、`db_admin` ロールをデフォルトロールとして設定し、ノードの追加や削除の権限を持つ `node_admin` ロールをデフォルトロールとして設定しないようにします。

このアプローチは、ユーザーに適切な権限を持つロールを割り当てるのに役立ち、意図しない操作のリスクを軽減します。

[GRANT](../../sql-reference/sql-statements/account-management/GRANT.md) を実行して、必要な権限やロールをユーザーに割り当てることができます。

## データベース内のすべてのテーブルに対する権限をユーザーに付与しました（`GRANT ALL ON ALL TABLES IN DATABASE <db_name> TO USER <user_identity>;`）が、ユーザーがデータベース内でテーブルを作成できません。なぜですか？

データベース内でテーブルを作成するには、データベースレベルの CREATE TABLE 権限が必要です。この権限をユーザーに付与する必要があります。

```SQL
GRANT CREATE TABLE ON DATABASE <db_name> TO USER <user_identity>;
```

## データベースに対するすべての権限をユーザーに付与しました（`GRANT ALL ON DATABASE <db_name> TO USER <user_identity>;`）が、ユーザーがこのデータベースで `SHOW TABLES;` を実行しても何も返されません。なぜですか？

`SHOW TABLES;` は、ユーザーが何らかの権限を持っているテーブルのみを返します。ユーザーがテーブルに対して権限を持っていない場合、そのテーブルは返されません。このデータベース内のすべてのテーブルに対して（例えば SELECT を使用して）ユーザーに何らかの権限を付与できます。

```SQL
GRANT SELECT ON ALL TABLES IN DATABASE <db_name> TO USER <user_identity>;
```

上記のステートメントは、v3.0 より前のバージョンで使用されていた `GRANT select_priv ON db.* TO <user_identity>;` と同等です。