---
displayed_sidebar: docs
---

# CREATE USER

import UserManagementPriv from '../../../_assets/commonMarkdown/userManagementPriv.md'

## 説明

StarRocks ユーザーを作成します。StarRocks では、"user_identity" がユーザーを一意に識別します。

<UserManagementPriv />

### 構文

```SQL
CREATE USER <user_identity> [auth_option] [DEFAULT ROLE <role_name>[, <role_name>, ...]]
```

## パラメータ

- `user_identity` は "user_name" と "host" の2つの部分から成り、形式は `username@'userhost'` です。"host" 部分には `%` を使用してあいまい一致が可能です。"host" が指定されていない場合、デフォルトで "%" が使用され、ユーザーは任意のホストから StarRocks に接続できます。

- `auth_option` は認証方法を指定します。現在、3つの認証方法がサポートされています: StarRocks ネイティブパスワード、mysql_native_password、および "authentication_ldap_simple"。StarRocks ネイティブパスワードは mysql_native_password と論理的には同じですが、構文がわずかに異なります。1つのユーザーアイデンティティは1つの認証方法しか使用できません。

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

> 注: StarRocks はユーザーのパスワードを保存する前に暗号化します。

- `DEFAULT ROLE <role_name>[, <role_name>, ...]`: このパラメータが指定されている場合、ロールは自動的にユーザーに割り当てられ、ユーザーがログインしたときにデフォルトでアクティブになります。指定されていない場合、このユーザーには特権がありません。指定されたすべてのロールが既に存在することを確認してください。

## 例

例 1: ホストを指定せずにプレーンテキストパスワードを使用してユーザーを作成します。これは `jack@'%'` と同等です。

```SQL
CREATE USER 'jack' IDENTIFIED BY '123456';
```

例 2: プレーンテキストパスワードを使用してユーザーを作成し、`'172.10.1.10'` からのログインを許可します。

```SQL
CREATE USER jack@'172.10.1.10' IDENTIFIED WITH mysql_native_password BY '123456';
```

例 3: 暗号文パスワードを使用してユーザーを作成し、`'172.10.1.10'` からのログインを許可します。

```SQL
CREATE USER jack@'172.10.1.10' IDENTIFIED BY PASSWORD '*6BB4837EB74329105EE4568DDA7DC67ED2CA2AD9';
CREATE USER jack@'172.10.1.10' IDENTIFIED WITH mysql_native_password AS '*6BB4837EB74329105EE4568DDA7DC67ED2CA2AD9';
```

> 注: 暗号化されたパスワードは password() 関数を使用して取得できます。

例 4: ドメイン名 'example_domain' からのログインを許可するユーザーを作成します。

```SQL
CREATE USER 'jack'@['example_domain'] IDENTIFIED BY '123456';
```

例 5: LDAP 認証を使用するユーザーを作成します。

```SQL
CREATE USER jack@'172.10.1.10' IDENTIFIED WITH authentication_ldap_simple;
```

例 6: LDAP 認証を使用し、LDAP 内のユーザーの識別名 (DN) を指定するユーザーを作成します。

```SQL
CREATE USER jack@'172.10.1.10' IDENTIFIED WITH authentication_ldap_simple AS 'uid=jack,ou=company,dc=example,dc=com';
```

例 7: '192.168' サブネットからのログインを許可し、`db_admin` と `user_admin` をデフォルトロールとして設定するユーザーを作成します。

```SQL
CREATE USER 'jack'@'192.168.%' DEFAULT ROLE db_admin, user_admin;
```