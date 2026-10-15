---
displayed_sidebar: docs
---

# MySQL からリアルタイムでデータを同期する

## Flink ジョブがエラーを報告した場合はどうすればよいですか？

Flink ジョブが次のエラーを報告します: `Could not execute SQL statement. Reason:org.apache.flink.table.api.ValidationException: One or more required options are missing.`

考えられる理由として、SMT 設定ファイル **config_prod.conf** の `[table-rule.1]` や `[table-rule.2]` など、複数のルールセットに必要な設定情報が欠落している可能性があります。

各ルールセット、例えば `[table-rule.1]` や `[table-rule.2]` が、必要なデータベース、テーブル、Flink コネクタ情報で設定されているかどうかを確認できます。

## Flink が失敗したタスクを自動的に再起動するようにするにはどうすればよいですか？

Flink は [checkpointing メカニズム](https://nightlies.apache.org/flink/flink-docs-master/docs/dev/datastream/fault-tolerance/checkpointing/) と [再起動戦略](https://nightlies.apache.org/flink/flink-docs-release-1.15/docs/ops/state/task_failure_recovery/) を通じて失敗したタスクを自動的に再起動します。

例えば、checkpointing メカニズムを有効にし、デフォルトの再起動戦略である固定遅延再起動戦略を使用する必要がある場合、設定ファイル **flink-conf.yaml** に次の情報を設定できます。

```Bash
execution.checkpointing.interval: 300000
state.backend: filesystem
state.checkpoints.dir: file:///tmp/flink-checkpoints-directory
```

パラメータの説明:

> **NOTE**
>
> Flink ドキュメントでのより詳細なパラメータ説明については、[Checkpointing](https://nightlies.apache.org/flink/flink-docs-master/docs/dev/datastream/fault-tolerance/checkpointing/) を参照してください。

- `execution.checkpointing.interval`: checkpointing の基本的な時間間隔。単位: ミリ秒。checkpointing メカニズムを有効にするには、このパラメータを `0` より大きい値に設定する必要があります。
- `state.backend`: 状態が内部的にどのように表現され、checkpointing 時にどのように、どこに保存されるかを決定するための状態バックエンドを指定します。一般的な値は `filesystem` または `rocksdb` です。checkpointing メカニズムが有効になると、状態はチェックポイントに保存され、データの損失を防ぎ、回復後のデータの一貫性を確保します。状態に関する詳細については、[State Backends](https://nightlies.apache.org/flink/flink-docs-master/docs/ops/state/state_backends/) を参照してください。
- `state.checkpoints.dir`: チェックポイントが書き込まれるディレクトリ。

## Flink ジョブを手動で停止し、後で停止前の状態に復元するにはどうすればよいですか？

Flink ジョブを停止する際に [savepoint](https://nightlies.apache.org/flink/flink-docs-master/docs/ops/state/savepoints/) を手動でトリガーできます（savepoint はストリーミング Flink ジョブの実行状態の一貫したイメージであり、checkpointing メカニズムに基づいて作成されます）。後で、指定された savepoint から Flink ジョブを復元できます。

1. savepoint を使用して Flink ジョブを停止します。次のコマンドは、Flink ジョブ `jobId` の savepoint を自動的にトリガーし、Flink ジョブを停止します。さらに、savepoint を保存するためのターゲットファイルシステムディレクトリを指定できます。

    ```Bash
    bin/flink stop --type [native/canonical] --savepointPath [:targetDirectory] :jobId
    ```

    パラメータの説明:

    - `jobId`: Flink WebUI から、またはコマンドラインで `flink list -running` を実行することで Flink ジョブ ID を確認できます。
    - `targetDirectory`: Flink 設定ファイル **flink-conf.yml** で `state.savepoints.dir` を savepoint を保存するためのデフォルトディレクトリとして指定できます。savepoint がトリガーされると、このデフォルトディレクトリに保存され、ディレクトリを指定する必要はありません。

    ```Bash
    state.savepoints.dir: [file:// または hdfs://]/home/user/savepoints_dir
    ```

2. 前述の savepoint を指定して Flink ジョブを再送信します。

    ```Bash
    ./flink run -c com.starrocks.connector.flink.tools.ExecuteSQL -s savepoints_dir/savepoints-xxxxxxxx flink-connector-starrocks-xxxx.jar -f flink-create.all.sql 
    ```