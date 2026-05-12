---
displayed_sidebar: docs
---

# CREATE USER

import UserManagementPriv from '../../../_assets/commonMarkdown/userManagementPriv.mdx'
import AuthOption from '../../../_assets/commonMarkdown/AuthOption.mdx'

CREATE USER は StarRocks ユーザーを作成します。StarRocks では、「user_identity」がユーザーを一意に識別します。v3.3.3 以降、StarRocks はユーザー作成時にユーザープロパティの設定をサポートしています。

<UserManagementPriv />

### 構文

```SQL
CREATE USER [IF NOT EXISTS] <user_identity> 
[auth_option] 
[DEFAULT ROLE <role_name>[, <role_name>, ...]]
[PROPERTIES ("key"="value", ...)]
```

## パラメータ

- `user_identity` は、「user_name」と「host」の2つの部分から構成され、`username@'userhost'` の形式です。「host」の部分には、`%` を使用してあいまい一致を行うことができます。「host」が指定されていない場合、デフォルトで「%」が使用され、ユーザーは任意のホストからStarRocksに接続できることを意味します。

  ユーザー名の命名規則については、以下を参照してください。[システム制限](../../System_limit.md)。

<AuthOption />

- `DEFAULT ROLE <role_name>[, <role_name>, ...]`: このパラメータが指定されている場合、ユーザーがログインすると、ロールは自動的にユーザーに割り当てられ、デフォルトでアクティブ化されます。指定されていない場合、このユーザーはどの権限も持ちません。指定されたすべてのロールが既に存在することを確認してください。

- `PROPERTIES` は、最大ユーザー接続数 (`max_user_connections`)、カタログ、データベース、またはユーザーレベルのセッション変数を含むユーザープロパティを設定します。ユーザーレベルのセッション変数は、ユーザーがログインすると有効になります。この機能はv3.3.3以降でサポートされています。

  ```SQL
  -- 最大ユーザー接続数を設定します。
  PROPERTIES ("max_user_connections" = "<Integer>")
  -- カタログを設定します。
  PROPERTIES ("catalog" = "<catalog_name>")
  -- データベースを設定します。
  PROPERTIES ("catalog" = "<catalog_name>", "database" = "<database_name>")
  -- セッション変数を設定します。
  PROPERTIES ("session.<variable_name>" = "<value>", ...)
  ```

  :::tip

  - `PROPERTIES` は、ユーザーIDではなくユーザーに対して機能します。
  - グローバル変数および読み取り専用変数は、特定のユーザーに対して設定できません。
  - 変数は次の順序で有効になります: SET_VAR > セッション > ユーザープロパティ > グローバル。
  - 以下を使用して、[SHOW PROPERTY](./SHOW_PROPERTY.md)特定のユーザーのプロパティを表示できます。
:::

## 例

例1: ホストを指定せずに平文パスワードを使用してユーザーを作成します。これは `jack@'%'` と同等です。

```SQL
CREATE USER 'jack' IDENTIFIED BY '123456';
```

例2: 平文パスワードを使用してユーザーを作成し、`'172.10.1.10'` からのログインを許可します。

```SQL
CREATE USER jack@'172.10.1.10' IDENTIFIED WITH mysql_native_password BY '123456';
```

例3: 暗号化されたパスワードを使用してユーザーを作成し、`'172.10.1.10'` からのログインを許可します。

```SQL
CREATE USER jack@'172.10.1.10' IDENTIFIED BY PASSWORD '*6BB4837EB74329105EE4568DDA7DC67ED2CA2AD9';
CREATE USER jack@'172.10.1.10' IDENTIFIED WITH mysql_native_password AS '*6BB4837EB74329105EE4568DDA7DC67ED2CA2AD9';
```

> 注: password() 関数を使用して暗号化されたパスワードを取得できます。

例4: ドメイン名 'example_domain' からのログインが許可されたユーザーを作成します。

```SQL
CREATE USER 'jack'@['example_domain'] IDENTIFIED BY '123456';
```

例5: LDAP認証を使用するユーザーを作成します。

```SQL
CREATE USER jack@'172.10.1.10' IDENTIFIED WITH authentication_ldap_simple;
```

例6: LDAP認証を使用し、LDAP内のユーザーの識別名 (DN) を指定してユーザーを作成します。

```SQL
CREATE USER jack@'172.10.1.10' IDENTIFIED WITH authentication_ldap_simple AS 'uid=jack,ou=company,dc=example,dc=com';
```

例7: '192.168' サブネットからのログインが許可されたユーザーを作成し、`db_admin` と `user_admin` をそのユーザーのデフォルトロールとして設定します。

```SQL
CREATE USER 'jack'@'192.168.%' DEFAULT ROLE db_admin, user_admin;
```

例8: ユーザーを作成し、その最大ユーザー接続数を `600` に設定します。

```SQL
CREATE USER 'jack'@'192.168.%' PROPERTIES ("max_user_connections" = "600");
```

例9: ユーザーを作成し、そのユーザーのカタログを `hive_catalog` に設定します。

```SQL
CREATE USER 'jack'@'192.168.%' PROPERTIES ('catalog' = 'hive_catalog');
```

例10: ユーザーを作成し、デフォルトカタログ内のそのユーザーのデータベースを `test_db` に設定します。

```SQL
CREATE USER 'jack'@'192.168.%' PROPERTIES ('catalog' = 'default_catalog', 'database' = 'test_db');
```

例11: ユーザーを作成し、そのユーザーのセッション変数 `query_timeout` を `600` に設定します。

```SQL
CREATE USER 'jack'@'192.168.%' PROPERTIES ('session.query_timeout' = '600');
```

例12: JSON Web Token認証を使用してユーザーを作成します。

```SQL
CREATE USER tom IDENTIFIED WITH authentication_jwt AS
'{
  "jwks_url": "http://localhost:38080/realms/master/protocol/jwt/certs",
  "principal_field": "preferred_username",
  "required_issuer": "http://localhost:38080/realms/master",
  "required_audience": "starrocks"
}';
```

例13: OAuth 2.0認証を使用してユーザーを作成します。

```SQL
CREATE USER tom IDENTIFIED WITH authentication_oauth2 AS 
'{
  "auth_server_url": "http://localhost:38080/realms/master/protocol/openid-connect/auth",
  "token_server_url": "http://localhost:38080/realms/master/protocol/openid-connect/token",
  "client_id": "12345",
  "client_secret": "LsWyD9vPcM3LHxLZfzJsuoBwWQFBLcoR",
  "redirect_url": "http://localhost:8030/api/oauth2",
  "jwks_url": "http://localhost:38080/realms/master/protocol/openid-connect/certs",
  "principal_field": "preferred_username",
  "required_issuer": "http://localhost:38080/realms/master",
  "required_audience": "12345"
}';
```
