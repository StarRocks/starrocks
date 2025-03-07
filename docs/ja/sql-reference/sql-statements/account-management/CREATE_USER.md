---
displayed_sidebar: docs
---

# CREATE USER

import UserManagementPriv from '../../../_assets/commonMarkdown/userManagementPriv.md'

## 説明

StarRocks ユーザーを作成します。StarRocks では、「user_identity」はユーザーを一意に識別します。v3.3.3 から、ユーザー作成時にユーザーのプロパティを設定することができます。

<UserManagementPriv />

### 構文

```SQL
CREATE USER [IF NOT EXISTS] <user_identity> 
[auth_option] 
[DEFAULT ROLE <role_name>[, <role_name>, ...]]
[PROPERTIES ("key"="value", ...)]
```

## パラメータ

- `user_identity` は「user_name」と「host」の2つの部分から成り、`username@'userhost'` の形式です。「host」部分には `%` を使用してあいまい一致を行うことができます。「host」が指定されていない場合、デフォルトで "%" が使用され、ユーザーは任意のホストから StarRocks に接続できます。

  ユーザー名の命名規則については、[システム制限](../../System_limit.md)を参照してください。

- `auth_option` は認証方法を指定します。現在、StarRocks ネイティブパスワード、mysql_native_password、および "authentication_ldap_simple" の3つの認証方法がサポートされています。StarRocks ネイティブパスワードは mysql_native_password と論理的には同じですが、構文がわずかに異なります。1つのユーザーアイデンティティは1つの認証方法しか使用できません。

    ```SQL
    auth_option: {
        IDENTIFIED BY 'auth_string'
        IDENTIFIED WITH mysql_native_password BY 'auth_string'
        IDENTIFIED WITH mysql_native_password AS 'auth_string'
        IDENTIFIED WITH authentication_ldap_simple AS 'auth_string'
        
    }
    ```

    | **認証方法**                 | **ユーザー作成時のパスワード** | **ログイン時のパスワード** |
    | ---------------------------- | ------------------------------ | -------------------------- |
    | ネイティブパスワード         | プレーンテキストまたは暗号文   | プレーンテキスト            |
    | `mysql_native_password BY`   | プレーンテキスト               | プレーンテキスト            |
    | `mysql_native_password WITH` | 暗号文                         | プレーンテキスト            |
    | `authentication_ldap_simple` | プレーンテキスト               | プレーンテキスト            |

> 注: StarRocks はユーザーのパスワードを暗号化して保存します。

- `DEFAULT ROLE <role_name>[, <role_name>, ...]`: このパラメータが指定されている場合、ロールはユーザーに自動的に割り当てられ、ユーザーがログインするとデフォルトで有効になります。指定されていない場合、このユーザーには特権がありません。指定されたすべてのロールが既に存在していることを確認してください。

- `PROPERTIES` はユーザープロパティを設定し、最大ユーザー接続数 (`max_user_connections`)、catalog、データベースまたはセッション変数をユーザーレベルで設定します。ユーザーレベルのセッション変数は、ユーザーがログインすると有効になります。この機能は v3.3.3 からサポートされています。

  ```SQL
  -- 最大ユーザー接続数を設定します。
  PROPERTIES ("max_user_connections" = "<Integer>")
  -- catalog を設定します。
  PROPERTIES ("catalog" = "<catalog_name>")
  -- データベースを設定します。
  PROPERTIES ("catalog" = "<catalog_name>", "database" = "<database_name>")
  -- セッション変数を設定します。
  PROPERTIES ("session.<variable_name>" = "<value>", ...)
  ```

  :::tip
  - `PROPERTIES` はユーザーに対して機能し、ユーザーアイデンティティには機能しません。
  - グローバル変数と読み取り専用変数は特定のユーザーに設定することはできません。
  - 変数は次の順序で有効になります: SET_VAR > セッション > ユーザープロパティ > グローバル。
  - 特定のユーザーのプロパティを表示するには、[SHOW PROPERTY](./SHOW_PROPERTY.md) を使用できます。
  :::

## 例

例 1: ホストを指定せずにプレーンテキストパスワードを使用してユーザーを作成します。これは `jack@'%'` と同等です。

```SQL
CREATE USER 'jack' IDENTIFIED BY '123456';
```

例 2: プレーンテキストパスワードを使用してユーザーを作成し、ユーザーが `'172.10.1.10'` からログインできるようにします。

```SQL
CREATE USER jack@'172.10.1.10' IDENTIFIED WITH mysql_native_password BY '123456';
```

例 3: 暗号文パスワードを使用してユーザーを作成し、ユーザーが `'172.10.1.10'` からログインできるようにします。

```SQL
CREATE USER jack@'172.10.1.10' IDENTIFIED BY PASSWORD '*6BB4837EB74329105EE4568DDA7DC67ED2CA2AD9';
CREATE USER jack@'172.10.1.10' IDENTIFIED WITH mysql_native_password AS '*6BB4837EB74329105EE4568DDA7DC67ED2CA2AD9';
```

> 注: 暗号化されたパスワードは password() 関数を使用して取得できます。

例 4: ドメイン名 'example_domain' からログインできるユーザーを作成します。

```SQL
CREATE USER 'jack'@['example_domain'] IDENTIFIED BY '123456';
```

例 5: LDAP 認証を使用するユーザーを作成します。

```SQL
CREATE USER jack@'172.10.1.10' IDENTIFIED WITH authentication_ldap_simple;
```

例 6: LDAP 認証を使用し、LDAP 内のユーザーの識別名 (DN) を指定してユーザーを作成します。

```SQL
CREATE USER jack@'172.10.1.10' IDENTIFIED WITH authentication_ldap_simple AS 'uid=jack,ou=company,dc=example,dc=com';
```

例 7: '192.168' サブネットからログインできるユーザーを作成し、`db_admin` と `user_admin` をユーザーのデフォルトロールとして設定します。

```SQL
CREATE USER 'jack'@'192.168.%' DEFAULT ROLE db_admin, user_admin;
```

例 8: ユーザーを作成し、最大ユーザー接続数を `600` に設定します。

```SQL
CREATE USER 'jack'@'192.168.%' PROPERTIES ("max_user_connections" = "600");
```

例 9: ユーザーを作成し、ユーザーの catalog を `hive_catalog` に設定します。

```SQL
CREATE USER 'jack'@'192.168.%' PROPERTIES ('catalog' = 'hive_catalog');
```

例 10: ユーザーを作成し、デフォルト catalog のデータベースを `test_db` に設定します。

```SQL
CREATE USER 'jack'@'192.168.%' PROPERTIES ('catalog' = 'default_catalog', 'database' = 'test_db');
```

例 11: ユーザーを作成し、セッション変数 `query_timeout` を `600` に設定します。

```SQL
CREATE USER 'jack'@'192.168.%' PROPERTIES ('session.query_timeout' = '600');
```