---
displayed_sidebar: docs
---

# CREATE USER

## 説明

### 構文

```SQL
CREATE USER
user_identity [auth_option]
[DEFAULT ROLE 'role_name']

user_identity:
    'user_name'@'host'

auth_option: {
    IDENTIFIED BY 'auth_string'
    IDENTIFIED WITH auth_plugin
    IDENTIFIED WITH auth_plugin BY 'auth_string'
    IDENTIFIED WITH auth_plugin AS 'auth_string'
}
```

1. コマンド "CREATE USER" は StarRocks ユーザーを作成するために使用されます。StarRocks では、"user_identity" がユーザーを一意に識別します。
2. "user_identity" は "user_name" と "host" の2つの部分で構成されています。"user_name" はユーザー名で、"host" はクライアントが接続するホストアドレスを識別します。"host" 部分は % を使用してあいまい一致が可能です。ホストが指定されていない場合、デフォルトは '%', つまりユーザーは任意のホストから StarRocks に接続できます。
3. "auth_option" はユーザー認証方法を指定します。現在は "mysql_native_password" と "authentication_ldap_simple" をサポートしています。

ロールが指定されている場合、そのロールが既に存在するという前提で、新しく作成されたユーザーにそのロールが所有するすべての権限が自動的に付与されます。ロールが指定されていない場合、ユーザーはデフォルトで権限を持ちません。

## 例

1. パスワードなしのユーザーを作成する（ホストを指定しない場合、jack@'%' と同等）

    ```SQL
    CREATE USER 'jack';
    ```

2. '172.10.1.10' からのログインを許可するパスワードユーザーを作成する。

    ```sql
    CREATE USER jack@'172.10.1.10' IDENTIFIED BY '123456';
    ```

    または

    ```SQL
    CREATE USER jack@'172.10.1.10' IDENTIFIED WITH mysql_native_password BY '123456';
    ```

3. 平文のパスワードを避けるために、ケース2は以下の方法でも作成できます。

    ```SQL
    CREATE USER jack@'172.10.1.10' IDENTIFIED BY PASSWORD '*6BB4837EB74329105EE4568DDA7DC67ED2CA2AD9';
    ```

    または

    ```SQL
    CREATE USER jack@'172.10.1.10' IDENTIFIED WITH mysql_native_password AS '*6BB4837EB74329105EE4568DDA7DC67ED2CA2AD9';
    ```

    暗号化された内容は PASSWORD() を通じて取得できます。例えば：

    ```sql
    SELECT PASSWORD('123456');
    ```

4. ldap で認証されたユーザーを作成する

    ```sql
    CREATE USER jack@'172.10.1.10' IDENTIFIED WITH authentication_ldap_simple
    ```

5. ldap でユーザーの DN (Distinguished Name) を指定して認証されたユーザーを作成する

    ```sql
    CREATE USER jack@'172.10.1.10' IDENTIFIED WITH authentication_ldap_simple AS 'uid=jack,ou=company,dc=example,dc=com'
    ```

6. '192.168' サブネットからのログインを許可し、同時にそのロールを example_role として指定するユーザーを作成する

    ```sql
    CREATE USER 'jack'@'192.168.%' DEFAULT ROLE 'example_role';
    ```

7. 'example_domain' という名前のドメインからのログインを許可するユーザーを作成する

    ```sql
    CREATE USER 'jack'@['example_domain'] IDENTIFIED BY '123456';
    ```

8. ユーザーを作成し、ロールを指定する

    ```sql
    CREATE USER 'jack'@'%' IDENTIFIED BY '12345' DEFAULT ROLE 'my_role';
    ```