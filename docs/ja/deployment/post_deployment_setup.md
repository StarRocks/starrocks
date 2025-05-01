---
displayed_sidebar: docs
---

# Post-deployment setup

このトピックでは、StarRocks をデプロイした後に実行すべきタスクについて説明します。

新しい StarRocks クラスターを本番環境に導入する前に、初期アカウントを保護し、クラスターが適切に動作するために必要な変数とプロパティを設定する必要があります。

## 初期アカウントの保護

StarRocks クラスターを作成すると、クラスターの初期 `root` ユーザーが自動的に生成されます。`root` ユーザーにはクラスター内のすべての権限が付与されます。このユーザーアカウントを保護し、本番環境での誤用を防ぐために使用を避けることをお勧めします。

StarRocks はクラスター作成時に `root` ユーザーに空のパスワードを自動的に割り当てます。以下の手順に従って、`root` ユーザーの新しいパスワードを設定してください。

1. MySQL クライアントを使用して、ユーザー名 `root` と空のパスワードで StarRocks に接続します。

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

> **NOTE**
>
> - パスワードをリセットした後は適切に保管してください。パスワードを忘れた場合は、詳細な手順については [Reset lost root password](../administration/User_privilege.md#reset-lost-root-password) を参照してください。
> - デプロイ後の設定が完了したら、新しいユーザーとロールを作成してチーム内の権限を管理できます。詳細な手順については [Manage user privileges](../administration/User_privilege.md) を参照してください。

## 必要なシステム変数の設定

StarRocks クラスターが本番環境で適切に動作するようにするために、次のシステム変数を設定する必要があります。

| **Variable name**                   | **StarRocks Version** | **Recommended value**                                        | **Description**                                              |
| ----------------------------------- | --------------------- | ------------------------------------------------------------ | ------------------------------------------------------------ |
| is_report_success                   | v2.4 or earlier       | false                                                        | クエリのプロファイルを分析のために送信するかどうかを制御するブールスイッチ。デフォルト値は `false` で、プロファイルは不要です。この変数を `true` に設定すると、StarRocks の並行性に影響を与える可能性があります。 |
| enable_profile                      | v2.5 or later         | false                                                        | クエリのプロファイルを分析のために送信するかどうかを制御するブールスイッチ。デフォルト値は `false` で、プロファイルは不要です。この変数を `true` に設定すると、StarRocks の並行性に影響を与える可能性があります。 |
| enable_pipeline_engine              | v2.3 or later         | true                                                         | パイプライン実行エンジンを有効にするかどうかを制御するブールスイッチ。`true` は有効を示し、`false` は無効を示します。デフォルト値: `true`。 |
| parallel_fragment_exec_instance_num | v2.3 or later         | パイプラインエンジンを有効にしている場合、この変数を `1` に設定できます。パイプラインエンジンを有効にしていない場合、CPU コア数の半分に設定する必要があります。 | 各 BE でノードをスキャンするために使用されるインスタンスの数。デフォルト値は `1` です。 |
| pipeline_dop                        | v2.3, v2.4, and v2.5  | 0                                                            | パイプラインインスタンスの並行性で、クエリの並行性を調整するために使用されます。デフォルト値: `0`、システムが各パイプラインインスタンスの並行性を自動的に調整します。<br />v3.0 以降、StarRocks はクエリの並行性に基づいてこのパラメータを適応的に調整します。 |

- `is_report_success` をグローバルに `false` に設定します。

  ```SQL
  SET GLOBAL is_report_success = false;
  ```

- `enable_profile` をグローバルに `false` に設定します。

  ```SQL
  SET GLOBAL enable_profile = false;
  ```

- `enable_pipeline_engine` をグローバルに `true` に設定します。

  ```SQL
  SET GLOBAL enable_pipeline_engine = true;
  ```

- `parallel_fragment_exec_instance_num` をグローバルに `1` に設定します。

  ```SQL
  SET GLOBAL parallel_fragment_exec_instance_num = 1;
  ```

- `pipeline_dop` をグローバルに `0` に設定します。

  ```SQL
  SET GLOBAL pipeline_dop = 0;
  ```

システム変数の詳細については、[System variables](../reference/System_variable.md) を参照してください。

## ユーザープロパティの設定

クラスター内で新しいユーザーを作成した場合、その最大接続数を増やす必要があります（例: `1000`）。

```SQL
-- <username> を最大接続数を増やしたいユーザー名に置き換えます。
SET PROPERTY FOR '<username>' 'max_user_connections' = '1000';
```

## 次のステップ

StarRocks クラスターをデプロイして設定した後、次にシナリオに最適なテーブルを設計できます。テーブル設計の詳細な手順については、[Understand StarRocks table design](../table_design/Table_design.md) を参照してください。