---
displayed_sidebar: docs
sidebar_position: 10
---

# ネイティブ認証

StarRocks 内で SQL コマンドを使用して、ネイティブ認証を通じてユーザーを作成および管理します。

StarRocks のネイティブ認証は、パスワードベースの認証方法です。加えて、StarRocks は LDAP、OpenID Connect、OAuth 2.0 などの外部認証システムとの統合もサポートしています。詳細な手順については、[セキュリティインテグレーションで認証](./security_integration.md) を参照してください。

:::note

システム定義のロール `user_admin` を持つユーザーは、StarRocks 内でユーザーの作成、変更、削除が可能です。

:::

## ユーザーの作成

ユーザーを作成する際には、ユーザーの識別情報、認証方法、およびオプションでデフォルトのロールを指定できます。ユーザーに対してネイティブ認証を有効にするには、パスワードをプレーンテキストまたは暗号化された形式で明示的に指定する必要があります。

以下の例では、ユーザー `jack` を作成し、IP アドレス `172.10.1.10` からのみ接続を許可し、ネイティブ認証を有効にし、パスワードをプレーンテキストで `12345` に設定し、デフォルトのロールとして `example_role` を割り当てます。

```SQL
CREATE USER jack@'172.10.1.10' IDENTIFIED BY '12345' DEFAULT ROLE 'example_role';
```

:::note
- StarRocks はユーザーのパスワードを保存する前に暗号化します。暗号化されたパスワードは password() 関数を使用して取得できます。
- ユーザー作成時にデフォルトのロールが指定されていない場合、システム定義のデフォルトロール `PUBLIC` がユーザーに割り当てられます。
:::

ユーザーのデフォルトロールは、ユーザーが StarRocks に接続すると自動的にアクティブになります。接続後にユーザーのすべてのロール（デフォルトおよび付与された）を有効にする方法については、[すべてのロールを有効にする](../authorization/User_privilege.md#すべてのロールを有効にする) を参照してください。

ユーザーの作成に関する詳細情報と高度な手順については、[CREATE USER](../../../sql-reference/sql-statements/account-management/CREATE_USER.md) を参照してください。

## ユーザーの変更

ユーザーのパスワード、デフォルトロール、またはプロパティを変更できます。

ユーザーのデフォルトロールを変更する方法については、[ユーザーのデフォルトロールを変更する](../authorization/User_privilege.md#ユーザーのデフォルトロールを変更する) を参照してください。

### ユーザーのプロパティの変更

ユーザーのプロパティは [ALTER USER](../../../sql-reference/sql-statements/account-management/ALTER_USER.md) を使用して設定できます。

以下の例では、ユーザー `jack` の最大接続数を `1000` に設定します。同じユーザー名を持つユーザー識別情報は同じプロパティを共有します。

したがって、`jack` のプロパティを設定するだけで、この設定はユーザー名が `jack` のすべてのユーザー識別情報に適用されます。

```SQL
ALTER USER 'jack' SET PROPERTIES ("max_user_connections" = "1000");
```

### ユーザーのパスワードのリセット

ユーザーのパスワードは [SET PASSWORD](../../../sql-reference/sql-statements/account-management/SET_PASSWORD.md) または [ALTER USER](../../../sql-reference/sql-statements/account-management/ALTER_USER.md) を使用してリセットできます。

> **NOTE**
>
> - どのユーザーも自分のパスワードを特権なしでリセットできます。
> - `root` ユーザー自身のみがそのパスワードを設定できます。パスワードを忘れて StarRocks に接続できない場合は、[Reset lost root password](#reset-lost-root-password) を参照してください。

以下の例はどちらも `jack` のパスワードを `54321` にリセットします。

- SET PASSWORD を使用してパスワードをリセット:

  ```SQL
  SET PASSWORD FOR jack@'172.10.1.10' = PASSWORD('54321');
  ```

- ALTER USER を使用してパスワードをリセット:

  ```SQL
  ALTER USER jack@'172.10.1.10' IDENTIFIED BY '54321';
  ```

#### 失われた root パスワードのリセット

`root` ユーザーのパスワードを忘れて StarRocks に接続できない場合は、次の手順に従ってリセットできます。

1. **すべての FE ノード** の設定ファイル **fe/conf/fe.conf** に次の設定項目を追加して、ユーザー認証を無効にします。

   ```YAML
   enable_auth_check = false
   ```

2. 設定を有効にするために **すべての FE ノード** を再起動します。

   ```Bash
   ./fe/bin/stop_fe.sh
   ./fe/bin/start_fe.sh
   ```

3. MySQL クライアントから `root` ユーザーを介して StarRocks に接続します。ユーザー認証が無効な場合、パスワードを指定する必要はありません。

   ```Bash
   mysql -h <fe_ip_or_fqdn> -P<fe_query_port> -uroot
   ```

4. `root` ユーザーのパスワードをリセットします。

   ```SQL
   SET PASSWORD for root = PASSWORD('xxxxxx');
   ```

5. **すべての FE ノード** の設定ファイル **fe/conf/fe.conf** で設定項目 `enable_auth_check` を `true` に設定して、ユーザー認証を再度有効にします。

   ```YAML
   enable_auth_check = true
   ```

6. 設定を有効にするために **すべての FE ノード** を再起動します。

   ```Bash
   ./fe/bin/stop_fe.sh
   ./fe/bin/start_fe.sh
   ```

7. MySQL クライアントから `root` ユーザーと新しいパスワードを使用して StarRocks に接続し、パスワードが正常にリセットされたかどうかを確認します。

   ```Bash
   mysql -h <fe_ip_or_fqdn> -P<fe_query_port> -uroot -p<xxxxxx>
   ```

## ユーザーの削除

ユーザーは [DROP USER](../../../sql-reference/sql-statements/account-management/DROP_USER.md) を使用して削除できます。

以下の例では、ユーザー `jack` を削除します。

```SQL
DROP USER jack@'172.10.1.10';
```

## ユーザーの表示

StarRocks クラスター内のすべてのユーザーを SHOW USERS を使用して表示できます。

```SQL
SHOW USERS;
```

## ユーザーのプロパティの表示

ユーザーのプロパティは [SHOW PROPERTY](../../../sql-reference/sql-statements/account-management/SHOW_PROPERTY.md) を使用して表示できます。

以下の例では、ユーザー `jack` のプロパティを表示します。

```SQL
SHOW PROPERTY FOR 'jack';
```

または、特定のプロパティを表示するには:

```SQL
SHOW PROPERTY FOR 'jack' LIKE 'max_user_connections';
```
