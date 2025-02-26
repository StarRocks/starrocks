---
displayed_sidebar: docs
---

# Post-deployment setup

このトピックでは、StarRocks をデプロイした後に実行すべきタスクについて説明します。

新しい StarRocks クラスターを本番環境に導入する前に、初期アカウントを保護し、クラスターが適切に動作するために必要な変数とプロパティを設定する必要があります。

## 初期アカウントの保護

StarRocks クラスターを作成すると、クラスターの初期 `root` ユーザーが自動的に生成されます。`root` ユーザーにはクラスター内のすべての権限が付与されます。このユーザーアカウントを保護し、誤用を防ぐために本番環境では使用しないことをお勧めします。

StarRocks はクラスター作成時に `root` ユーザーに空のパスワードを自動的に割り当てます。以下の手順に従って、`root` ユーザーの新しいパスワードを設定してください。

1. MySQL クライアントを使用して、`root` ユーザー名と空のパスワードで StarRocks に接続します。

   ```Bash
   # <fe_address> を接続する FE ノードの IP アドレス (priority_networks) または FQDN に置き換え、
   # <query_port> を fe.conf で指定した query_port (デフォルト: 9030) に置き換えます。
   mysql -h <fe_address> -P<query_port> -uroot
   ```

2. 次の SQL を実行して `root` ユーザーのパスワードをリセットします。

   ```SQL
   -- <password> を root ユーザーに割り当てたいパスワードに置き換えます。
   SET PASSWORD = PASSWORD('<password>')
   ```

:::note
- パスワードをリセットした後は、適切に保管してください。パスワードを忘れた場合は、[Reset lost root password](../administration/user_privs/User_privilege.md#reset-lost-root-password) を参照して詳細な手順を確認してください。
- ポストデプロイメントのセットアップが完了したら、新しいユーザーとロールを作成してチーム内の権限を管理できます。詳細な手順は [Manage user privileges](../administration/user_privs/User_privilege.md) を参照してください。
:::

## 必要なシステム変数の設定

StarRocks クラスターが本番環境で適切に動作するように、以下のシステム変数を設定する必要があります。

### enable_profile                      

#### 説明
クエリのプロファイルを分析のために送信するかどうかを制御するブールスイッチです。
デフォルト値は `false` で、プロファイルは必要ありません。
この変数を `true` に設定すると、StarRocks の同時実行性に影響を与える可能性があります。

#### 推奨値
false

- `enable_profile` をグローバルに `false` に設定します。

  ```SQL
  SET GLOBAL enable_profile = false;
  ```

### enable_pipeline_engine              

#### 説明
パイプライン実行エンジンを有効にするかどうかを制御するブールスイッチです。
`true` は有効を示し、`false` は無効を示します。デフォルト値: `true`。

#### 推奨値
true

- `enable_pipeline_engine` をグローバルに `true` に設定します。

  ```SQL
  SET GLOBAL enable_pipeline_engine = true;
  ```

### parallel_fragment_exec_instance_num 

#### 説明
各 BE でノードをスキャンするために使用されるインスタンスの数です。デフォルト値は `1` です。

#### 推奨値
パイプラインエンジンを有効にしている場合、この変数を `1` に設定できます。
パイプラインエンジンを有効にしていない場合、CPU コア数の半分に設定する必要があります。

- `parallel_fragment_exec_instance_num` をグローバルに `1` に設定します。

  ```SQL
  SET GLOBAL parallel_fragment_exec_instance_num = 1;
  ```

システム変数の詳細については、[System variables](../sql-reference/System_variable.md) を参照してください。

## ユーザープロパティの設定

クラスター内で新しいユーザーを作成した場合、最大接続数を増やす必要があります（例えば `1000` に設定します）。

```SQL
-- <username> を最大接続数を増やしたいユーザー名に置き換えます。
ALTER USER '<username>' SET PROPERTIES ("max_user_connections" = "1000");
```

## 次に行うこと

StarRocks クラスターをデプロイしてセットアップした後、シナリオに最適なテーブルを設計することができます。テーブル設計の詳細な手順については、[Understand StarRocks table design](../table_design/table_design.md) を参照してください。