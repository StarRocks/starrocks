---
displayed_sidebar: docs
---

# ALTER SYSTEM

## 説明

クラスタ内の FE、BE、CN、Broker ノード、およびメタデータスナップショットを管理します。

> **注意**
>
> この操作を実行する権限を持つのは `cluster_admin` ロールのみです。

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

  新しい BE のステータスは [SHOW BACKENDS](SHOW_BACKENDS.md) を実行して確認できます。

- BE ノードを削除します。

  > **注意**
  >
  > 単一レプリカテーブルのタブレットを格納している BE ノードは削除できません。

  ```SQL
  ALTER SYSTEM DROP BACKEND "<be_host>:<heartbeat_service_port>"[, ...]
  ```

- BE ノードを退役させます。

  ```SQL
  ALTER SYSTEM DECOMMISSION BACKEND "<be_host>:<heartbeat_service_port>"[, ...]
  ```

  BE ノードを削除するのとは異なり、退役は安全に削除することを意味します。これは非同期操作です。BE が退役されると、BE 上のデータはまず他の BE に移行され、その後クラスタから削除されます。データ移行中にデータロードやクエリは影響を受けません。操作が成功したかどうかは [SHOW BACKENDS](SHOW_BACKENDS.md) を使用して確認できます。操作が成功した場合、退役された BE は返されません。操作が失敗した場合、BE はまだオンラインのままです。[CANCEL DECOMMISSION](CANCEL_DECOMMISSION.md) を使用して手動で操作をキャンセルできます。

| **パラメータ**          | **必須** | **説明**                                                                            |
| ---------------------- | ------------ | ------------------------------------------------------------------------------------------ |
| be_host                | はい          | BE インスタンスのホスト名または IP アドレス。インスタンスに複数の IP アドレスがある場合は、設定項目 `priority_networks` の値を使用します。|
| heartbeat_service_port | はい          | BE ハートビートサービスポート。BE はこのポートを使用して FE からのハートビートを受信します。デフォルト: `9050`。|

### CN

- CN ノードを追加します。

  ```SQL
  ALTER SYSTEM ADD COMPUTE NODE "<cn_host>:<heartbeat_service_port>"[, ...]
  ```

  新しい CN のステータスは [SHOW COMPUTE NODES](SHOW_COMPUTE_NODES.md) を実行して確認できます。

- CN ノードを削除します。

  ```SQL
  ALTER SYSTEM DROP COMPUTE NODE "<cn_host>:<heartbeat_service_port>"[, ...]
  ```

> **注意**
>
> `ALTER SYSTEM DECOMMISSION` コマンドを使用して CN ノードを退役させることはできません。

| **パラメータ**          | **必須** | **説明**                                                                            |
| ---------------------- | ------------ | ------------------------------------------------------------------------------------------ |
| cn_host                | はい          | CN インスタンスのホスト名または IP アドレス。インスタンスに複数の IP アドレスがある場合は、設定項目 `priority_networks` の値を使用します。|
| heartbeat_service_port | はい          | CN ハートビートサービスポート。CN はこのポートを使用して FE からのハートビートを受信します。デフォルト: `9050`。|

### Broker

- Broker ノードを追加します。Broker ノードを使用して、HDFS やクラウドストレージから StarRocks にデータをロードできます。詳細は [Loading](../../../../loading/Loading_intro.md) を参照してください。

  ```SQL
  ALTER SYSTEM ADD BROKER <broker_name> "<broker_host>:<broker_ipc_port>"[, ...]
  ```

  1 つの SQL で複数の Broker ノードを追加できます。各 `<broker_host>:<broker_ipc_port>` ペアは 1 つの Broker ノードを表し、共通の `broker_name` を共有します。新しい Broker ノードのステータスは [SHOW BROKER](SHOW_BROKER.md) を実行して確認できます。

- Broker ノードを削除します。

> **注意**
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
| broker_ipc_port | はい          | Broker ノードの Thrift サーバーポート。Broker ノードはこれを使用して FE または BE からのリクエストを受信します。デフォルト: `8000`。 |

### イメージの作成

イメージファイルを作成します。イメージファイルは FE メタデータのスナップショットです。

```SQL
ALTER SYSTEM CREATE IMAGE
```

イメージの作成は Leader FE 上での非同期操作です。操作の開始時刻と終了時刻は FE ログファイル **fe.log** で確認できます。`triggering a new checkpoint manually...` のようなログはイメージ作成が開始されたことを示し、`finished save image...` のようなログはイメージが作成されたことを示します。

## 使用上の注意

- FE、BE、CN、または Broker ノードの追加と削除は同期操作です。ノード削除操作をキャンセルすることはできません。
- 単一 FE クラスタでは FE ノードを削除することはできません。
- 複数 FE クラスタでは Leader FE ノードを直接削除することはできません。削除するには、まず再起動する必要があります。StarRocks が新しい Leader FE を選出した後、以前のものを削除できます。
- 残りの BE ノードの数がデータレプリカの数より少ない場合、BE ノードを削除することはできません。たとえば、クラスタに 3 つの BE ノードがあり、データを 3 つのレプリカで保存している場合、BE ノードを削除することはできません。また、4 つの BE ノードと 3 つのレプリカがある場合、1 つの BE ノードを削除できます。
- BE ノードを削除することと退役させることの違いは、BE ノードを削除する場合、StarRocks はそれをクラスタから強制的に削除し、削除後にタブレットを補完しますが、BE ノードを退役させる場合、StarRocks はまず退役された BE ノード上のタブレットを他のノードに移行し、その後ノードを削除します。

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

例 8: `amazon_s3` のすべての Broker ノードを削除します。

```SQL
ALTER SYSTEM DROP ALL BROKER amazon_s3;
```