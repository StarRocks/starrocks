---
displayed_sidebar: docs
---

# ALTER USER

## 説明

ユーザー情報を変更します。これには、パスワード、認証方法、デフォルトロール、ユーザーのプロパティ（v3.3.3以降でサポート）を含みます。

:::tip
一般ユーザーはこのコマンドを使用して自分の情報を変更できます。他のユーザーの情報を変更できるのは、`user_admin` ロールを持つユーザーのみです。
:::

## 構文

```SQL
ALTER USER user_identity 
[auth_option] 
[default_role] 
[DEFAULT ROLE <role_name>[, <role_name>, ...]]
[SET PROPERTIES ("key"="value", ...)]
```

## パラメータ

- `user_identity` は "user_name" と "host" の2つの部分で構成され、形式は `username@'userhost'` です。"host" 部分には `%` を使用してあいまい一致が可能です。"host" が指定されていない場合、デフォルトで "%" が使用され、ユーザーは任意のホストから StarRocks に接続できます。ただし、`SET PROPERTIES` を使用してユーザーのプロパティを変更する場合は、`user_identity` ではなく `username` を指定する必要があります。

- `auth_option` は認証方法を指定します。現在、3つの認証方法がサポートされています: StarRocks ネイティブパスワード、mysql_native_password、および "authentication_ldap_simple"。StarRocks ネイティブパスワードは、ロジック的には mysql_native_password と同じですが、構文がわずかに異なります。1つのユーザーアイデンティティは1つの認証方法のみを使用できます。ALTER USER を使用してユーザーのパスワードと認証方法を変更できます。

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
    | ネイティブパスワード         | プレーンテキストまたは暗号化テキスト | プレーンテキスト            |
    | `mysql_native_password BY`   | プレーンテキスト               | プレーンテキスト            |
    | `mysql_native_password WITH` | 暗号化テキスト                 | プレーンテキスト            |
    | `authentication_ldap_simple` | プレーンテキスト               | プレーンテキスト            |

> 注: StarRocks はユーザーのパスワードを暗号化して保存します。

- `DEFAULT ROLE` はユーザーのデフォルトロールを設定します。

   ```SQL
    -- 指定したロールをデフォルトロールとして設定します。
    DEFAULT ROLE <role_name>[, <role_name>, ...]
    -- ユーザーのすべてのロールを、今後このユーザーに割り当てられるロールも含めてデフォルトロールとして設定します。
    DEFAULT ROLE ALL
    -- デフォルトロールは設定されませんが、ユーザーログイン後もパブリックロールは有効です。
    DEFAULT ROLE NONE
    ```

  ALTER USER を実行してデフォルトロールを設定する前に、すべてのロールがユーザーに割り当てられていることを確認してください。ユーザーが再度ログインすると、ロールは自動的に有効になります。

- `SET PROPERTIES` はユーザーのプロパティを設定します。これには、最大ユーザー接続数 (`max_user_connections`)、catalog、データベース、またはユーザーレベルのセッション変数が含まれます。ユーザーレベルのセッション変数は、ユーザーがログインすると有効になります。この機能は v3.3.3 からサポートされています。

  ```SQL
  -- 最大ユーザー接続数を設定します。
  SET PROPERTIES ("max_user_connections" = "<Integer>")
  -- catalog を設定します。
  SET PROPERTIES ("catalog" = "<catalog_name>")
  -- データベースを設定します。
  SET PROPERTIES ("catalog" = "<catalog_name>", "database" = "<database_name>")
  -- セッション変数を設定します。
  SET PROPERTIES ("session.<variable_name>" = "<value>", ...)
  -- ユーザーに設定されたプロパティをクリアします。
  SET PROPERTIES ("catalog" = "", "database" = "", "session.<variable_name>" = "");
  ```

  :::tip
  - `SET PROPERTIES` はユーザーに対して機能し、ユーザーアイデンティティではありません。したがって、ユーザーのプロパティを変更する場合は、`ALTER USER` ステートメントで `user_identity` ではなく `username` を指定する必要があります。
  - グローバル変数および読み取り専用変数は、特定のユーザーに設定することはできません。
  - 変数は次の順序で有効になります: SET_VAR > セッション > ユーザープロパティ > グローバル。
  - 特定のユーザーのプロパティを表示するには、[SHOW PROPERTY](./SHOW_PROPERTY.md) を使用できます。
  :::

## 例

例 1: ユーザーのパスワードをプレーンテキストのパスワードに変更します。

```SQL
ALTER USER 'jack' IDENTIFIED BY '123456';
ALTER USER jack@'172.10.1.10' IDENTIFIED WITH mysql_native_password BY '123456';
```

例 2: ユーザーのパスワードを暗号化されたパスワードに変更します。

```SQL
ALTER USER jack@'172.10.1.10' IDENTIFIED BY PASSWORD '*6BB4837EB74329105EE4568DDA7DC67ED2CA2AD9';
ALTER USER jack@'172.10.1.10' IDENTIFIED WITH mysql_native_password AS '*6BB4837EB74329105EE4568DDA7DC67ED2CA2AD9';
```

> 暗号化されたパスワードは password() 関数を使用して取得できます。

例 3: 認証方法を LDAP に変更します。

```SQL
ALTER USER jack@'172.10.1.10' IDENTIFIED WITH authentication_ldap_simple;
```

例 4: 認証方法を LDAP に変更し、LDAP 内のユーザーの識別名 (DN) を指定します。

```SQL
ALTER USER jack@'172.10.1.10' IDENTIFIED WITH authentication_ldap_simple AS 'uid=jack,ou=company,dc=example,dc=com';
```

例 5: ユーザーのデフォルトロールを `db_admin` と `user_admin` に変更します。ユーザーにはこれら2つのロールが割り当てられている必要があります。

```SQL
ALTER USER 'jack'@'192.168.%' DEFAULT ROLE db_admin, user_admin;
```

例 6: ユーザーのすべてのロールを、今後このユーザーに割り当てられるロールも含めてデフォルトロールとして設定します。

```SQL
ALTER USER 'jack'@'192.168.%' DEFAULT ROLE ALL;
```

例 7: ユーザーのすべてのデフォルトロールをクリアします。

```SQL
ALTER USER 'jack'@'192.168.%' DEFAULT ROLE NONE;
```

> 注: デフォルトで、`public` ロールはユーザーに対して有効です。

例 8: 最大ユーザー接続数を `600` に設定します。

```SQL
ALTER USER 'jack' SET PROPERTIES ("max_user_connections" = "600");
```

例 9: ユーザーの catalog を `hive_catalog` に設定します。

```SQL
ALTER USER 'jack' SET PROPERTIES ('catalog' = 'hive_catalog');
```

例 10: ユーザーのデータベースをデフォルト catalog の `test_db` に設定します。

```SQL
ALTER USER 'jack' SET PROPERTIES ('catalog' = 'default_catalog', 'database' = 'test_db');
```

例 11: ユーザーのセッション変数 `query_timeout` を `600` に設定します。

```SQL
ALTER USER 'jack' SET PROPERTIES ('session.query_timeout' = '600');
```

例 12: ユーザーに設定されたプロパティをクリアします。

```SQL
ALTER USER 'jack' SET PROPERTIES ('catalog' = '', 'database' = '', 'session.query_timeout' = '');
```

## 参考文献

- [CREATE USER](CREATE_USER.md)
- [GRANT](GRANT.md)
- [SHOW USERS](SHOW_USERS.md)
- [DROP USER](DROP_USER.md)