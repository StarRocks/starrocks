---
displayed_sidebar: docs
---

# SET DEFAULT ROLE

## 説明

ユーザーがサーバーに接続した際にデフォルトで有効になるロールを設定します。

このコマンドは v3.0 からサポートされています。

:::tip
一般ユーザーは自分自身のデフォルトロールを設定できます。`user_admin` ロールを持つユーザーは他のユーザーのデフォルトロールを設定できます。この操作を行う前に、ユーザーにこれらのロールが既に割り当てられていることを確認してください。

ユーザーのロールは [SHOW GRANTS](SHOW_GRANTS.md) を使用して照会できます。
:::

## 構文

```SQL
-- 指定したロールをデフォルトロールとして設定します。
SET DEFAULT ROLE <role_name>[,<role_name>,..] TO <user_identity>;
-- ユーザーのすべてのロールを、今後このユーザーに割り当てられるロールも含めてデフォルトロールとして設定します。
SET DEFAULT ROLE ALL TO <user_identity>;
-- デフォルトロールは設定されませんが、ユーザーがログインした後も public ロールは有効です。
SET DEFAULT ROLE NONE TO <user_identity>; 
```

## パラメータ

`role_name`: ロール名

`user_identity`: ユーザー識別子

## 例

現在のユーザーのロールを照会します。

```SQL
SHOW GRANTS FOR test;
+--------------+---------+----------------------------------------------+
| UserIdentity | Catalog | Grants                                       |
+--------------+---------+----------------------------------------------+
| 'test'@'%'   | NULL    | GRANT 'db_admin', 'user_admin' TO 'test'@'%' |
+--------------+---------+----------------------------------------------+
```

例 1: ユーザー `test` に対して `db_admin` と `user_admin` をデフォルトロールとして設定します。

```SQL
SET DEFAULT ROLE db_admin TO test;
```

例 2: ユーザー `test` のすべてのロールを、今後このユーザーに割り当てられるロールも含めてデフォルトロールとして設定します。

```SQL
SET DEFAULT ROLE ALL TO test;
```

例 3: ユーザー `test` のすべてのデフォルトロールをクリアします。

```SQL
SET DEFAULT ROLE NONE TO test;
```