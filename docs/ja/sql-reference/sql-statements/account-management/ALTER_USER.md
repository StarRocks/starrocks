---
displayed_sidebar: docs
---

# ALTER USER

## 説明

ユーザー情報を変更します。例えば、パスワード、認証方法、またはデフォルトのロールなどです。

:::tip
一般ユーザーはこのコマンドを使用して自分自身の情報を変更できます。他のユーザーの情報を変更できるのは `user_admin` ロールを持つユーザーのみです。
:::

## 構文

```SQL
ALTER USER user_identity [auth_option] [default_role]
```

## パラメータ

- `user_identity` は "user_name" と "host" の2つの部分で構成され、形式は `username@'userhost'` です。"host" 部分には `%` を使用してあいまい一致が可能です。"host" が指定されていない場合、デフォルトで "%" が使用され、ユーザーはどのホストからでも StarRocks に接続できます。

- `auth_option` は認証方法を指定します。現在、3つの認証方法がサポートされています: StarRocks ネイティブパスワード、mysql_native_password、および "authentication_ldap_simple" です。StarRocks ネイティブパスワードは論理的には mysql_native_password と同じですが、構文がわずかに異なります。1つのユーザーアイデンティティは1つの認証方法のみを使用できます。ALTER USER を使用してユーザーのパスワードと認証方法を変更できます。

    ```SQL
    auth_option: {
        IDENTIFIED BY 'auth_string'
        IDENTIFIED WITH mysql_native_password BY 'auth_string'
        IDENTIFIED WITH mysql_native_password AS 'auth_string'
        IDENTIFIED WITH authentication_ldap_simple AS 'auth_string'
        
    }
    ```

    | **認証方法**                 | **ユーザー作成時のパスワード** | **ログイン時のパスワード** |
    | ---------------------------- | ------------------------------ | ------------------------- |
    | ネイティブパスワード         | プレーンテキストまたは暗号文   | プレーンテキスト           |
    | `mysql_native_password BY`   | プレーンテキスト               | プレーンテキスト           |
    | `mysql_native_password WITH` | 暗号文                         | プレーンテキスト           |
    | `authentication_ldap_simple` | プレーンテキスト               | プレーンテキスト           |

> 注: StarRocks はユーザーのパスワードを保存する前に暗号化します。

- `DEFAULT ROLE`

   ```SQL
    -- 指定されたロールをデフォルトロールとして設定します。
    DEFAULT ROLE <role_name>[, <role_name>, ...]
    -- ユーザーのすべてのロールを、今後このユーザーに割り当てられるロールも含めて、デフォルトロールとして設定します。
    DEFAULT ROLE ALL
    -- デフォルトロールは設定されませんが、ユーザーログイン後もパブリックロールは有効です。
    DEFAULT ROLE NONE
    ```

  ALTER USER を実行してデフォルトロールを設定する前に、すべてのロールがユーザーに割り当てられていることを確認してください。ユーザーが再度ログインすると、ロールは自動的に有効になります。

## 例

例1: ユーザーのパスワードをプレーンテキストのパスワードに変更します。

```SQL
ALTER USER 'jack' IDENTIFIED BY '123456';
ALTER USER jack@'172.10.1.10' IDENTIFIED WITH mysql_native_password BY '123456';
```

例2: ユーザーのパスワードを暗号文のパスワードに変更します。

```SQL
ALTER USER jack@'172.10.1.10' IDENTIFIED BY PASSWORD '*6BB4837EB74329105EE4568DDA7DC67ED2CA2AD9';
ALTER USER jack@'172.10.1.10' IDENTIFIED WITH mysql_native_password AS '*6BB4837EB74329105EE4568DDA7DC67ED2CA2AD9';
```

> 暗号化されたパスワードは password() 関数を使用して取得できます。

例3: 認証方法を LDAP に変更します。

```SQL
ALTER USER jack@'172.10.1.10' IDENTIFIED WITH authentication_ldap_simple;
```

例4: 認証方法を LDAP に変更し、LDAP 内のユーザーの識別名 (DN) を指定します。

```SQL
CREATE USER jack@'172.10.1.10' IDENTIFIED WITH authentication_ldap_simple AS 'uid=jack,ou=company,dc=example,dc=com';
```

例5: ユーザーのデフォルトロールを `db_admin` と `user_admin` に変更します。これらの2つのロールがユーザーに割り当てられている必要があります。

```SQL
ALTER USER 'jack'@'192.168.%' DEFAULT ROLE db_admin, user_admin;
```

例6: ユーザーのすべてのロールを、今後このユーザーに割り当てられるロールも含めて、デフォルトロールとして設定します。

```SQL
ALTER USER 'jack'@'192.168.%' DEFAULT ROLE ALL;
```

例7: ユーザーのすべてのデフォルトロールをクリアします。

```SQL
ALTER USER 'jack'@'192.168.%' DEFAULT ROLE NONE;
```

> 注: デフォルトで、`public` ロールはユーザーに対して依然として有効です。

## 参考文献

- [CREATE USER](CREATE_USER.md)
- [GRANT](GRANT.md)
- [SHOW USERS](SHOW_USERS.md)
- [DROP USER](DROP_USER.md)