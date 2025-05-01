---
displayed_sidebar: docs
---

# REVOKE

## 説明

REVOKE ステートメントを使用して、次の操作を実行できます。

- ユーザーまたはロールから特定の権限を取り消します。
- 他のユーザーを偽装して操作を行うことを許可する権限を取り消します。この機能は、StarRocks 2.4 以降のバージョンでのみサポートされています。
- ユーザーからロールを取り消します。この機能は、StarRocks 2.4 以降のバージョンでのみサポートされています。

## 構文

- ユーザーまたはロールからデータベースおよびテーブルに対する特定の権限を取り消します。権限を取り消すロールは既に存在している必要があります。

    ```SQL
    REVOKE privilege_list ON db_name.tbl_name FROM {user_identity | ROLE 'role_name'}
    ```

- ユーザーまたはロールからリソースに対する特定の権限を取り消します。権限を取り消すロールは既に存在している必要があります。

    ```SQL
    REVOKE privilege_list ON RESOURCE 'resource_name' FROM {user_identity | ROLE 'role_name'};
    ```

- ユーザー `a` がユーザー `b` を偽装して操作を行うことを許可する権限を取り消します。

    ```SQL
    REVOKE IMPERSONATE ON user_identity_b FROM user_identity_a;
    ```

- ユーザーからロールを取り消します。ロールは既に存在している必要があります。

    ```SQL
    REVOKE 'role_name' FROM user_identity;
    ```

## パラメータ

### privilege_list

ユーザーまたはロールから取り消すことができる権限です。複数の権限を一度に取り消したい場合は、カンマ（`,`）で権限を区切ります。次の権限を取り消すことができます。

- `NODE_PRIV`: クラスターノードを管理する権限（ノードの有効化や無効化など）。この権限は root ユーザーにのみ付与できます。
- `ADMIN_PRIV`: `NODE_PRIV` を除くすべての権限。
- `GRANT_PRIV`: ユーザーやロールの作成、削除、権限の付与、取り消し、アカウントのパスワード設定などの操作を行う権限。
- `SELECT_PRIV`: データベースおよびテーブルの読み取り権限。
- `LOAD_PRIV`: データベースおよびテーブルへのデータのロード権限。
- `ALTER_PRIV`: データベースおよびテーブルのスキーマを変更する権限。
- `CREATE_PRIV`: データベースおよびテーブルを作成する権限。
- `DROP_PRIV`: データベースおよびテーブルを削除する権限。
- `USAGE_PRIV`: リソースを使用する権限。

### db_name.tbl_name

データベースおよびテーブル。このパラメータは次の3つの形式をサポートしています。

- `*.*`: クラスター内のすべてのデータベースおよびテーブルを示します。
- `db.*`: 特定のデータベースおよびそのデータベース内のすべてのテーブルを示します。
- `db.tbl`: 特定のデータベース内の特定のテーブルを示します。

> 注: `db.*` または `db.tbl` 形式を使用する場合、存在しないデータベースまたはテーブルを指定することができます。

### resource_name

リソース名。このパラメータは次の2つの形式をサポートしています。

- `*`: すべてのリソースを示します。
- `resource`: 特定のリソースを示します。

> 注: `resource` 形式を使用する場合、存在しないリソースを指定することができます。

### user_identity

このパラメータは `user_name` と `host` の2つの部分で構成されています。`user_name` はユーザー名を示します。`host` はユーザーの IP アドレスを示します。`host` を指定しない場合、または `host` にドメインを指定することができます。`host` を指定しない場合、`host` はデフォルトで `%` となり、任意のホストから StarRocks にアクセスできます。`host` にドメインを指定した場合、権限が有効になるまでに1分かかることがあります。`user_identity` パラメータは [CREATE USER](../account-management/CREATE_USER.md) ステートメントによって作成される必要があります。

## 例

例 1: データベース `db1` のすべてのテーブルに対する読み取り権限をユーザー `jack` から取り消します。

```SQL
REVOKE SELECT_PRIV ON db1.* FROM 'jack'@'192.%';
```

例 2: ユーザー `jack` から `spark_resource` を使用する権限を取り消します。

```SQL
REVOKE USAGE_PRIV ON RESOURCE 'spark_resource' FROM 'jack'@'192.%';
```

例 3: ユーザー `jack` から `my_role` を取り消します。

```SQL
REVOKE 'my_role' FROM 'jack'@'%';
```

例 4: ユーザー `jack` が `rose` を偽装して操作を行うことを許可する権限を取り消します。

```SQL
REVOKE IMPERSONATE ON 'rose'@'%' FROM 'jack'@'%';
```