---
displayed_sidebar: docs
---

# ALTER USER

ALTER USER は、パスワード、認証方法、デフォルトロール、ユーザーのプロパティ（v3.3.3以降でサポート）を含むユーザー情報を変更します。

:::tip
一般ユーザーはこのコマンドを使用して自分の情報を変更できます。他のユーザーの情報を変更できるのは `user_admin` ロールを持つユーザーのみです。
:::

## Syntax

```SQL
ALTER USER user_identity 
[auth_option] 
[default_role] 
[DEFAULT ROLE <role_name>[, <role_name>, ...]]
[SET PROPERTIES ("key"="value", ...)]
```

## Parameters

- `user_identity` は "user_name" と "host" の2つの部分から成り、形式は `username@'userhost'` です。"host" 部分には `%` を使用してあいまい一致が可能です。"host" が指定されていない場合、デフォルトで "%" が使用され、ユーザーは任意のホストから StarRocks に接続できます。

- `auth_option` は認証方法を指定します。現在、3つの認証方法がサポートされています: StarRocks ネイティブパスワード、mysql_native_password、"authentication_ldap_simple"。StarRocks ネイティブパスワードは、論理的には mysql_native_password と同じですが、構文がわずかに異なります。1つのユーザーアイデンティティは1つの認証方法のみを使用できます。ALTER USER を使用してユーザーのパスワードと認証方法を変更できます。

    ```SQL
    auth_option: {
        IDENTIFIED BY 'auth_string'
        IDENTIFIED WITH mysql_native_password BY 'auth_string'
        IDENTIFIED WITH mysql_native_password AS 'auth_string'
        IDENTIFIED WITH authentication_ldap_simple AS 'auth_string'
        
    }
    ```

    | **Authentication method**    | **Password for user creation** | **Password for login** |
    | ---------------------------- | ------------------------------ | ---------------------- |
    | Native password              | Plaintext or ciphertext        | Plaintext              |
    | `mysql_native_password BY`   | Plaintext                      | Plaintext              |
    | `mysql_native_password WITH` | Ciphertext                     | Plaintext              |
    | `authentication_ldap_simple` | Plaintext                      | Plaintext              |

> Note: StarRocks はユーザーのパスワードを暗号化して保存します。

- `DEFAULT ROLE` はユーザーのデフォルトロールを設定します。

   ```SQL
    -- 指定されたロールをデフォルトロールとして設定します。
    DEFAULT ROLE <role_name>[, <role_name>, ...]
    -- ユーザーに割り当てられるロールを含む、すべてのロールをデフォルトロールとして設定します。
    DEFAULT ROLE ALL
    -- デフォルトロールは設定されませんが、ユーザーがログインした後も public ロールは有効です。
    DEFAULT ROLE NONE
    ```

  ALTER USER を実行してデフォルトロールを設定する前に、すべてのロールがユーザーに割り当てられていることを確認してください。ユーザーが再度ログインすると、ロールは自動的に有効になります。

- `SET PROPERTIES` は、最大ユーザー接続数 (`max_user_connections`)、catalog、データベース、またはユーザーレベルのセッション変数を含むユーザープロパティを設定します。ユーザーレベルのセッション変数は、ユーザーがログインすると有効になります。この機能は v3.3.3 からサポートされています。

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
  - グローバル変数と読み取り専用変数は特定のユーザーに設定できません。
  - 変数は次の順序で有効になります: SET_VAR > セッション > ユーザープロパティ > グローバル。
  - 特定のユーザーのプロパティを表示するには [SHOW PROPERTY](./SHOW_PROPERTY.md) を使用できます。
  :::

## Examples

Example 1: ユーザーのパスワードをプレーンテキストのパスワードに変更します。

```SQL
ALTER USER 'jack' IDENTIFIED BY '123456';
ALTER USER jack@'172.10.1.10' IDENTIFIED WITH mysql_native_password BY '123456';
```

Example 2: ユーザーのパスワードを暗号化されたパスワードに変更します。

```SQL
ALTER USER jack@'172.10.1.10' IDENTIFIED BY PASSWORD '*6BB4837EB74329105EE4568DDA7DC67ED2CA2AD9';
ALTER USER jack@'172.10.1.10' IDENTIFIED WITH mysql_native_password AS '*6BB4837EB74329105EE4568DDA7DC67ED2CA2AD9';
```

> 暗号化されたパスワードは password() 関数を使用して取得できます。

Example 3: 認証方法を LDAP に変更します。

```SQL
ALTER USER jack@'172.10.1.10' IDENTIFIED WITH authentication_ldap_simple;
```

Example 4: 認証方法を LDAP に変更し、LDAP 内のユーザーの識別名 (DN) を指定します。

```SQL
ALTER USER jack@'172.10.1.10' IDENTIFIED WITH authentication_ldap_simple AS 'uid=jack,ou=company,dc=example,dc=com';
```

Example 5: ユーザーのデフォルトロールを `db_admin` と `user_admin` に変更します。ユーザーはこれら2つのロールを割り当てられている必要があります。

```SQL
ALTER USER 'jack'@'192.168.%' DEFAULT ROLE db_admin, user_admin;
```

Example 6: ユーザーに割り当てられるロールを含む、すべてのロールをデフォルトロールとして設定します。

```SQL
ALTER USER 'jack'@'192.168.%' DEFAULT ROLE ALL;
```

Example 7: ユーザーのすべてのデフォルトロールをクリアします。

```SQL
ALTER USER 'jack'@'192.168.%' DEFAULT ROLE NONE;
```

> Note: デフォルトで、`public` ロールはユーザーに対して依然として有効です。

Example 8: 最大ユーザー接続数を `600` に設定します。

```SQL
ALTER USER 'jack'@'192.168.%' SET PROPERTIES ("max_user_connections" = "600");
```
> Note: デフォルトホスト (%) に一致するユーザーアイデンティティのプロパティを変更する場合、または明示的なホストが曖昧さを避けるために厳密に必要でない場合は、ユーザー名のみを指定することもできます（例: `ALTER USER 'jack' SET PROPERTIES...`）。

Example 9: ユーザーの catalog を `hive_catalog` に設定します。

```SQL
ALTER USER 'jack'@'192.168.%' SET PROPERTIES ('catalog' = 'hive_catalog');
```

Example 10: デフォルト catalog 内のユーザーのデータベースを `test_db` に設定します。

```SQL
ALTER USER 'jack'@'192.168.%' SET PROPERTIES ('catalog' = 'default_catalog', 'database' = 'test_db');
```

Example 11: ユーザーのセッション変数 `query_timeout` を `600` に設定します。

```SQL
ALTER USER 'jack'@'192.168.%' SET PROPERTIES ('session.query_timeout' = '600');
```

Example 12: ユーザーに設定されたプロパティをクリアします。

```SQL
ALTER USER 'jack'@'192.168.%' SET PROPERTIES ('catalog' = '', 'database' = '', 'session.query_timeout' = '');
```

## References

- [CREATE USER](CREATE_USER.md)
- [GRANT](GRANT.md)
- [SHOW USERS](SHOW_USERS.md)
- [DROP USER](DROP_USER.md)