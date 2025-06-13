---
displayed_sidebar: docs
---

# Routine Load

## ロードパフォーマンスを向上させるにはどうすればよいですか？

**方法 1: 実際のロードタスクの並行性を増やす** ために、ロードジョブを可能な限り多くの並行ロードタスクに分割します。

> **注意**
>
> この方法は、より多くの CPU リソースを消費し、過剰な tablet バージョンを引き起こす可能性があります。

実際のロードタスクの並行性は、いくつかのパラメータで構成された以下の式によって決定され、上限は生存している BE ノードの数または消費されるパーティションの数です。

```Plaintext
min(alive_be_number, partition_number, desired_concurrent_number, max_routine_load_task_concurrent_num)
```

パラメータの説明:

- `alive_be_number`: 生存している BE ノードの数。
- `partition_number`: 消費されるパーティションの数。
- `desired_concurrent_number`: Routine Load ジョブのための希望するロードタスクの並行性。デフォルト値は `3` です。このパラメータの値を高く設定することで、実際のロードタスクの並行性を増やすことができます。
  - Routine Load ジョブを作成していない場合は、[CREATE ROUTINE LOAD](../../sql-reference/sql-statements/loading_unloading/routine_load/CREATE_ROUTINE_LOAD.md) を使用して Routine Load ジョブを作成する際にこのパラメータを設定する必要があります。
  - すでに Routine Load ジョブを作成している場合は、[ALTER ROUTINE LOAD](../../sql-reference/sql-statements/loading_unloading/routine_load/ALTER_ROUTINE_LOAD.md) を使用してこのパラメータを変更する必要があります。
- `max_routine_load_task_concurrent_num`: Routine Load ジョブのためのデフォルトの最大タスク並行性。デフォルト値は `5` です。このパラメータは FE の動的パラメータです。詳細情報と設定方法については、[パラメータ設定](../../administration/management/FE_configuration.md#loading-and-unloading) を参照してください。

したがって、消費されるパーティションの数と生存している BE ノードの数が他の 2 つのパラメータよりも大きい場合、`desired_concurrent_number` と `max_routine_load_task_concurrent_num` のパラメータの値を増やすことで、実際のロードタスクの並行性を増やすことができます。

例えば、消費されるパーティションの数が `7`、生存している BE ノードの数が `5`、`max_routine_load_task_concurrent_num` がデフォルト値 `5` の場合、ロードタスクの並行性を上限まで増やす必要がある場合は、`desired_concurrent_number` を `5` に設定する必要があります（デフォルト値は `3`）。その後、実際のタスク並行性 `min(5,7,5,5)` は `5` と計算されます。

パラメータの詳細については、[CREATE ROUTINE LOAD](../../sql-reference/sql-statements/loading_unloading/routine_load/CREATE_ROUTINE_LOAD.md) を参照してください。

**方法 2: Routine Load タスクが 1 つ以上のパーティションから消費するデータ量を増やす。**

> **注意**
>
> この方法は、データロードの遅延を引き起こす可能性があります。

Routine Load タスクが消費できるメッセージの数の上限は、ロードタスクが消費できる最大メッセージ数を意味するパラメータ `max_routine_load_batch_size` またはメッセージ消費の最大期間を意味するパラメータ `routine_load_task_consume_second` によって決定されます。ロードタスクがいずれかの要件を満たす十分なデータを消費すると、消費は完了します。これらの 2 つのパラメータは FE の動的パラメータです。詳細情報と設定方法については、[パラメータ設定](../../administration/management/FE_configuration.md#loading-and-unloading) を参照してください。

**be/log/be.INFO** のログを確認することで、どのパラメータがロードタスクが消費するデータ量の上限を決定しているかを分析できます。そのパラメータを増やすことで、ロードタスクが消費するデータ量を増やすことができます。

```Plaintext
I0325 20:27:50.410579 15259 data_consumer_group.cpp:131] consumer group done: 41448fb1a0ca59ad-30e34dabfa7e47a0. consume time(ms)=3261, received rows=179190, received bytes=9855450, eos: 1, left_time: -261, left_bytes: 514432550, blocking get time(us): 3065086, blocking put time(us): 24855
```

通常、ログのフィールド `left_bytes` が `0` 以上である場合、ロードタスクが消費するデータ量が `routine_load_task_consume_second` 内で `max_routine_load_batch_size` を超えていないことを示しています。これは、スケジュールされたロードタスクのバッチが Kafka からのすべてのデータを遅延なく消費できることを意味します。このシナリオでは、`routine_load_task_consume_second` の値を大きく設定することで、ロードタスクが 1 つ以上のパーティションから消費するデータ量を増やすことができます。

フィールド `left_bytes` が `0` 未満の場合、ロードタスクが消費するデータ量が `routine_load_task_consume_second` 内で `max_routine_load_batch_size` に達していることを意味します。Kafka からのデータがスケジュールされたロードタスクのバッチを満たすたびに、Kafka に消費されていないデータが残っている可能性が高く、消費の遅延を引き起こします。この場合、`max_routine_load_batch_size` の値を大きく設定することができます。

## SHOW ROUTINE LOAD の結果がロードジョブが `PAUSED` 状態であることを示している場合はどうすればよいですか？

- フィールド `ReasonOfStateChanged` を確認し、エラーメッセージ `Broker: Offset out of range` が報告されている場合。

  **原因分析:** ロードジョブのコンシューマオフセットが Kafka パーティションに存在しません。

  **解決策:** [SHOW ROUTINE LOAD](../../sql-reference/sql-statements/loading_unloading/routine_load/SHOW_ROUTINE_LOAD.md) を実行し、パラメータ `Progress` でロードジョブの最新のコンシューマオフセットを確認します。その後、対応するメッセージが Kafka パーティションに存在するかどうかを確認します。存在しない場合、以下の理由が考えられます。

  - ロードジョブが作成されたときに指定されたコンシューマオフセットが将来のオフセットである。
  - Kafka パーティションの指定されたコンシューマオフセットのメッセージがロードジョブによって消費される前に削除されている。ロード速度に基づいて、`log.retention.hours` や `log.retention.bytes` などの合理的な Kafka ログクリーニングポリシーとパラメータを設定することをお勧めします。

- フィールド `ReasonOfStateChanged` を確認し、エラーメッセージ `Broker: Offset out of range` が報告されていない場合。

  **原因分析:** ロードタスクのエラーロウの数がしきい値 `max_error_number` を超えています。

  **解決策:** フィールド `ReasonOfStateChanged` と `ErrorLogUrls` のエラーメッセージを使用して問題をトラブルシューティングし、修正します。

  - データソースのデータ形式が間違っている場合、データ形式を確認して問題を修正する必要があります。問題を正常に修正した後、[RESUME ROUTINE LOAD](../../sql-reference/sql-statements/loading_unloading/routine_load/RESUME_ROUTINE_LOAD.md) を使用して一時停止したロードジョブを再開できます。

  - StarRocks がデータソースのデータ形式を解析できない場合、しきい値 `max_error_number` を調整する必要があります。まず、[SHOW ROUTINE LOAD](../../sql-reference/sql-statements/loading_unloading/routine_load/SHOW_ROUTINE_LOAD.md) を実行して `max_error_number` の値を確認し、次に [ALTER ROUTINE LOAD](../../sql-reference/sql-statements/loading_unloading/routine_load/ALTER_ROUTINE_LOAD.md) を使用してしきい値を増やします。しきい値を変更した後、[RESUME ROUTINE LOAD](../../sql-reference/sql-statements/loading_unloading/routine_load/RESUME_ROUTINE_LOAD.md) を使用して一時停止したロードジョブを再開できます。

## SHOW ROUTINE LOAD の結果がロードジョブが `CANCELLED` 状態であることを示している場合はどうすればよいですか？

  **原因分析:** ロードジョブがロード中に例外に遭遇しました。例えば、テーブルが削除された場合です。

  **解決策:** 問題をトラブルシューティングし、修正する際には、フィールド `ReasonOfStateChanged` と `ErrorLogUrls` のエラーメッセージを参照できます。ただし、問題を修正した後でも、キャンセルされたロードジョブを再開することはできません。

## Routine Load は Kafka からの消費と StarRocks への書き込みにおいて一貫性のあるセマンティクスを保証できますか？

   Routine Load は、正確に一度のセマンティクスを保証します。

   各ロードタスクは個別のトランザクションです。トランザクションの実行中にエラーが発生した場合、トランザクションは中止され、FE はロードタスクの関連パーティションの消費進捗を更新しません。次回、FE がタスクキューからロードタスクをスケジュールする際、ロードタスクはパーティションの最後に保存された消費位置から消費要求を送信し、正確に一度のセマンティクスを保証します。