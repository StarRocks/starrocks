---
displayed_sidebar: docs
---

# ALTER SYSTEM

## 説明

クラスタ内の FE、BE、CN、Broker ノード、およびメタデータスナップショットを管理します。

> **NOTE**
>
> この SQL ステートメントを実行する権限があるのは `cluster_admin` ロールのみです。

## 構文とパラメータ

### FE

- Follower FE を追加します。

  ```SQL
  ALTER SYSTEM ADD FOLLOWER "<fe_host>:<edit_log_port>"[, ...]
  ```

  新しい Follower FE のステータスは `SHOW PROC '/frontends'\G` を実行して確認できます。

- Follower FE を削除します。

  ```SQL
  ALTER SYSTEM DROP FOLLOWER "<fe_host>:<edit_log_port>"[, ...]
  ```

- Observer FE を追加します。

  ```SQL
  ALTER SYSTEM ADD OBSERVER "<fe_host>:<edit_log_port>"[, ...]
  ```

  新しい Observer FE のステータスは `SHOW PROC '/frontends'\G` を実行して確認できます。

- Observer FE を削除します。

  ```SQL
  ALTER SYSTEM DROP OBSERVER "<fe_host>:<edit_log_port>"[, ...]
  ```

| **パラメータ**      | **必須** | **説明**                                                     |
| ------------------ | ------------ | ------------------------------------------------------------------- |
| fe_host            | はい          | FE インスタンスのホスト名または IP アドレス。インスタンスに複数の IP アドレスがある場合は、設定項目 `priority_networks` の値を使用します。 |
| edit_log_port      | はい          | FE ノードの BDB JE 通信ポート。デフォルト: `9010`。          |

### BE

- BE ノードを追加します。

  ```SQL
  ALTER SYSTEM ADD BACKEND "<be_host>:<heartbeat_service_port>"[, ...]
  ```

  新しい BE のステータスは [SHOW BACKENDS](../Administration/SHOW_BACKENDS.md) を実行して確認できます。

- BE ノードを削除します。

  > **NOTE**
  >
  > 単一レプリカテーブルの tablet を格納している BE ノードは削除できません。

  ```SQL
  ALTER SYSTEM DROP BACKEND "<be_host>:<heartbeat_service_port>"[, ...]
  ```

- BE ノードを退役させます。

  ```SQL
  ALTER SYSTEM DECOMMISSION BACKEND "<be_host>:<heartbeat_service_port>"[, ...]
  ```

  BE ノードを削除するのとは異なり、BE ノードを退役させることは安全に削除することを意味します。これは非同期操作です。BE が退役されると、BE 上のデータは最初に他の BEs に移行され、その後クラスタから削除されます。データ移行中、データロードやクエリには影響しません。操作が成功したかどうかは [SHOW BACKENDS](../Administration/SHOW_BACKENDS.md) を使用して確認できます。操作が成功した場合、退役した BE は返されません。操作が失敗した場合、BE はオンラインのままです。[CANCEL DECOMMISSION](../Administration/CANCEL_DECOMMISSION.md) を使用して手動で操作をキャンセルできます。

| **パラメータ**          | **必須** | **説明**                                                                            |
| ---------------------- | ------------ | ------------------------------------------------------------------------------------------ |
| be_host                | はい          | BE インスタンスのホスト名または IP アドレス。インスタンスに複数の IP アドレスがある場合は、設定項目 `priority_networks` の値を使用します。|
| heartbeat_service_port | はい          | BE ハートビートサービスポート。BE はこのポートを使用して FE からのハートビートを受信します。デフォルト: `9050`。|

### CN

- CN ノードを追加します。

  ```SQL
  ALTER SYSTEM ADD COMPUTE NODE "<cn_host>:<heartbeat_service_port>"[, ...]
  ```

  新しい CN のステータスは [SHOW COMPUTE NODES](../Administration/SHOW_COMPUTE_NODES.md) を実行して確認できます。

- CN ノードを削除します。

  ```SQL
  ALTER SYSTEM DROP COMPUTE NODE "<cn_host>:<heartbeat_service_port>"[, ...]
  ```

> **NOTE**
>
> `ALTER SYSTEM DECOMMISSION` コマンドを使用して CN ノードを退役させることはできません。

| **パラメータ**          | **必須** | **説明**                                                                            |
| ---------------------- | ------------ | ------------------------------------------------------------------------------------------ |
| cn_host                | はい          | CN インスタンスのホスト名または IP アドレス。インスタンスに複数の IP アドレスがある場合は、設定項目 `priority_networks` の値を使用します。|
| heartbeat_service_port | はい          | CN ハートビートサービスポート。CN はこのポートを使用して FE からのハートビートを受信します。デフォルト: `9050`。|

### Broker

- Broker ノードを追加します。Broker ノードを使用して、HDFS またはクラウドストレージから StarRocks にデータをロードできます。詳細については、 [Load data from HDFS or cloud storage](../../../loading/BrokerLoad.md) を参照してください。

  ```SQL
  ALTER SYSTEM ADD BROKER <broker_name> "<broker_host>:<broker_ipc_port>"[, ...]
  ```

  1 つの SQL で複数の Broker ノードを追加できます。各 `<broker_host>:<broker_ipc_port>` ペアは 1 つの Broker ノードを表します。そして、それらは共通の `broker_name` を共有します。新しい Broker ノードのステータスは [SHOW BROKER](../Administration/SHOW_BROKER.md) を実行して確認できます。

- Broker ノードを削除します。

> **CAUTION**
>
> Broker ノードを削除すると、現在その上で実行中のタスクが終了します。

  - 同じ `broker_name` を持つ 1 つまたは複数の Broker ノードを削除します。

    ```SQL
    ALTER SYSTEM DROP BROKER <broker_name> "<broker_host>:<broker_ipc_port>"[, ...]
    ```

  - 同じ `broker_name` を持つすべての Broker ノードを削除します。

    ```SQL
    ALTER SYSTEM DROP ALL BROKER <broker_name>
    ```

| **パラメータ**   | **必須** | **説明**                                                              |
| --------------- | ------------ | ---------------------------------------------------------------------------- |
| broker_name     | はい          | Broker ノードの名前。複数の Broker ノードが同じ名前を使用できます。 |
| broker_host     | はい          | Broker インスタンスのホスト名または IP アドレス。インスタンスに複数の IP アドレスがある場合は、設定項目 `priority_networks` の値を使用します。|
| broker_ipc_port | はい          | Broker ノード上の Thrift サーバーポート。Broker ノードはこれを使用して FE または BE からのリクエストを受信します。デフォルト: `8000`。 |

### イメージの作成

イメージファイルを作成します。イメージファイルは FE メタデータのスナップショットです。

```SQL
ALTER SYSTEM CREATE IMAGE
```

イメージの作成は Leader FE 上での非同期操作です。操作の開始時間と終了時間は FE ログファイル **fe.log** で確認できます。`triggering a new checkpoint manually...` というログはイメージ作成が開始されたことを示し、`finished save image...` というログはイメージが作成されたことを示します。

## 使用上の注意

- FE、BE、CN、または Broker ノードの追加と削除は同期操作です。ノード削除操作をキャンセルすることはできません。
- 単一 FE クラスタ内の FE ノードを削除することはできません。
- 複数 FE クラスタ内の Leader FE ノードを直接削除することはできません。削除するには、最初に再起動する必要があります。StarRocks が新しい Leader FE を選出した後、以前のものを削除できます。
- 残りの BE ノードの数がデータレプリカの数より少ない場合、BE ノードを削除することはできません。たとえば、クラスタに 3 つの BE ノードがあり、データを 3 つのレプリカで保存している場合、BE ノードを削除することはできません。また、4 つの BE ノードと 3 つのレプリカがある場合、1 つの BE ノードを削除できます。
- BE ノードの削除と退役の違いは、BE ノードを削除する場合、StarRocks はそれをクラスタから強制的に削除し、削除後に削除された tablet を補完しますが、BE ノードを退役させる場合、StarRocks は最初に退役した BE ノード上の tablet を他のノードに移行し、その後ノードを削除します。

## 例

例 1: Follower FE ノードを追加します。

```SQL
ALTER SYSTEM ADD FOLLOWER "x.x.x.x:9010";
```

例 2: 2 つの Observer FE ノードを同時に削除します。

```SQL
ALTER SYSTEM DROP OBSERVER "x.x.x.x:9010","x.x.x.x:9010";
```

例 3: BE ノードを追加します。

```SQL
ALTER SYSTEM ADD BACKEND "x.x.x.x:9050";
```

例 4: 2 つの BE ノードを同時に削除します。

```SQL
ALTER SYSTEM DROP BACKEND "x.x.x.x:9050", "x.x.x.x:9050";
```

例 5: 2 つの BE ノードを同時に退役させます。

```SQL
ALTER SYSTEM DECOMMISSION BACKEND "x.x.x.x:9050", "x.x.x.x:9050";
```

例 6: 同じ `broker_name` - `hdfs` を持つ 2 つの Broker ノードを追加します。

```SQL
ALTER SYSTEM ADD BROKER hdfs "x.x.x.x:8000", "x.x.x.x:8000";
```

例 7: `amazon_s3` から 2 つの Broker ノードを削除します。

```SQL
ALTER SYSTEM DROP BROKER amazon_s3 "x.x.x.x:8000", "x.x.x.x:8000";
```

例 8: `amazon_s3` 内のすべての Broker ノードを削除します。

```SQL
ALTER SYSTEM DROP ALL BROKER amazon_s3;
```