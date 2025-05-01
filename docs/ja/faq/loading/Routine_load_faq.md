---
displayed_sidebar: docs
---

# Routine Load

## ロードパフォーマンスを向上させるにはどうすればよいですか？

**方法 1: 実際のロードタスクの並行性を増やす** には、ロードジョブを可能な限り多くの並行ロードタスクに分割します。

> **注意**
>
> この方法は、より多くの CPU リソースを消費し、タブレットバージョンが多すぎる原因となる可能性があります。

実際のロードタスクの並行性は、いくつかのパラメータで構成される以下の式によって決定され、上限は生存している BE ノードの数または消費されるパーティションの数です。

```Plaintext
min(alive_be_number, partition_number, desired_concurrent_number, max_routine_load_task_concurrent_num)
```

パラメータの説明:

- `alive_be_number`: 生存している BE ノードの数。
- `partition_number`: 消費されるパーティションの数。
- `desired_concurrent_number`: Routine Load ジョブのための希望するロードタスクの並行性。デフォルト値は `3` です。このパラメータに対してより高い値を設定することで、実際のロードタスクの並行性を増やすことができます。
  - Routine Load ジョブを作成していない場合は、[CREATE ROUTINE LOAD](../../sql-reference/sql-statements/loading_unloading/routine_load/CREATE_ROUTINE_LOAD.md) を使用して Routine Load ジョブを作成する際にこのパラメータを設定する必要があります。
  - 既に Routine Load ジョブを作成している場合は、[ALTER ROUTINE LOAD](../../sql-reference/sql-statements/loading_unloading/routine_load/ALTER_ROUTINE_LOAD.md) を使用してこのパラメータを変更する必要があります。
- `max_routine_load_task_concurrent_num`: Routine Load ジョブのためのデフォルトの最大タスク並行性。デフォルト値は `5` です。このパラメータは FE の動的パラメータです。詳細と設定方法については、[パラメータ設定](../../administration/management/FE_configuration.md#loading-and-unloading) を参照してください。

したがって、消費されるパーティションの数と生存している BE ノードの数が他の 2 つのパラメータよりも大きい場合、`desired_concurrent_number` と `max_routine_load_task_concurrent_num` の値を増やすことで、実際のロードタスクの並行性を増やすことができます。

例えば、消費されるパーティションの数が `7`、生存している BE ノードの数が `5`、`max_routine_load_task_concurrent_num` がデフォルト値 `5` の場合、ロードタスクの並行性を上限まで増やす必要がある場合は、`desired_concurrent_number` を `5` に設定する必要があります（デフォルト値は `3`）。その後、実際のタスク並行性 `min(5,7,5,5)` は `5` と計算されます。

パラメータの詳細については、[CREATE ROUTINE LOAD](../../sql-reference/sql-statements/loading_unloading/routine_load/CREATE_ROUTINE_LOAD.md) を参照してください。

**方法 2: Routine Load タスクが 1 つ以上のパーティションから消費するデータ量を増やす。**

> **注意**
>
> この方法は、データロードの遅延を引き起こす可能性があります。

Routine Load タスクが消費できるメッセージの数の上限は、ロードタスクが消費できるメッセージの最大数を意味するパラメータ `max_routine_load_batch_size` またはメッセージ消費の最大期間を意味するパラメータ `routine_load_task_consume_second` によって決定されます。ロードタスクがいずれかの要件を満たすデータを十分に消費すると、消費は完了します。これらの 2 つのパラメータは FE の動的パラメータです。詳細と設定方法については、[パラメータ設定](../../administration/management/FE_configuration.md#loading-and-unloading) を参照してください。

**be/log/be.INFO** のログを確認することで、どのパラメータがロードタスクによって消費されるデータ量の上限を決定しているかを分析できます。そのパラメータを増やすことで、ロードタスクが消費するデータ量を増やすことができます。

```Plaintext
I0325 20:27:50.410579 15259 data_consumer_group.cpp:131] consumer group done: 41448fb1a0ca59ad-30e34dabfa7e47a0. consume time(ms)=3261, received rows=179190, received bytes=9855450, eos: 1, left_time: -261, left_bytes: 514432550, blocking get time(us): 3065086, blocking put time(us): 24855
```

通常、ログのフィールド `left_bytes` が `0` 以上であることは、ロードタスクが `routine_load_task_consume_second` 内で `max_routine_load_batch_size` を超えていないことを示しています。これは、スケジュールされたロードタスクのバッチが Kafka からのすべてのデータを遅延なく消費できることを意味します。このシナリオでは、`routine_load_task_consume_second` に対してより大きな値を設定することで、ロードタスクが 1 つ以上のパーティションから消費するデータ量を増やすことができます。

フィールド `left_bytes` が `0` 未満の場合、ロードタスクが `routine_load_task_consume_second` 内で `max_routine_load_batch_size` に達したことを意味します。Kafka からのデータがスケジュールされたロードタスクのバッチを満たすたびに、消費されていないデータが Kafka に残っている可能性が高く、消費の遅延を引き起こします。この場合、`max_routine_load_batch_size` に対してより大きな値を設定することができます。

## SHOW ROUTINE LOAD の結果がロードジョブが `PAUSED` 状態であることを示している場合はどうすればよいですか？

- フィールド `ReasonOfStateChanged` を確認し、エラーメッセージ `Broker: Offset out of range` が報告されているか確認します。

  **原因分析:** ロードジョブのコンシューマーオフセットが Kafka パーティションに存在しません。

  **解決策:** [SHOW ROUTINE LOAD](../../sql-reference/sql-statements/loading_unloading/routine_load/SHOW_ROUTINE_LOAD.md) を実行し、ロードジョブの最新のコンシューマーオフセットをパラメータ `Progress` で確認します。その後、対応するメッセージが Kafka パーティションに存在するか確認します。存在しない場合、ロードジョブが作成されたときに指定されたコンシューマーオフセットが将来のオフセットである可能性があります。また、Kafka パーティションの指定されたコンシューマーオフセットのメッセージがロードジョブによって消費される前に削除された可能性があります。ロード速度に基づいて、合理的な Kafka ログクリーニングポリシーとパラメータ（`log.retention.hours` や `log.retention.bytes` など）を設定することをお勧めします。

- フィールド `ReasonOfStateChanged` を確認し、エラーメッセージ `Broker: Offset out of range` が報告されていない場合。

  **原因分析:** ロードタスクのエラーロウの数がしきい値 `max_error_number` を超えています。

  **解決策:** フィールド `ReasonOfStateChanged` と `ErrorLogUrls` のエラーメッセージを使用して問題をトラブルシューティングし、修正できます。

  - データソースのデータ形式が正しくないことが原因である場合、データ形式を確認し、問題を修正する必要があります。問題を正常に修正した後、[RESUME ROUTINE LOAD](../../sql-reference/sql-statements/loading_unloading/routine_load/RESUME_ROUTINE_LOAD.md) を使用して一時停止したロードジョブを再開できます。

  - StarRocks がデータソースのデータ形式を解析できない場合、しきい値 `max_error_number` を調整する必要があります。まず、[SHOW ROUTINE LOAD](../../sql-reference/sql-statements/loading_unloading/routine_load/SHOW_ROUTINE_LOAD.md) を実行して `max_error_number` の値を確認し、次に [ALTER ROUTINE LOAD](../../sql-reference/sql-statements/loading_unloading/routine_load/ALTER_ROUTINE_LOAD.md) を使用してしきい値を増やします。しきい値を変更した後、[RESUME ROUTINE LOAD](../../sql-reference/sql-statements/loading_unloading/routine_load/RESUME_ROUTINE_LOAD.md) を使用して一時停止したロードジョブを再開できます。

## SHOW ROUTINE LOAD の結果がロードジョブが `CANCELLED` 状態であることを示している場合はどうすればよいですか？

  **原因分析:** ロードジョブがロード中に例外に遭遇した、例えばテーブルが削除されたなど。

  **解決策:** 問題をトラブルシューティングし、修正する際には、フィールド `ReasonOfStateChanged` と `ErrorLogUrls` のエラーメッセージを参照できます。ただし、問題を修正した後でも、キャンセルされたロードジョブを再開することはできません。

## Routine Load は Kafka からの消費と StarRocks への書き込みにおいて一貫性のあるセマンティクスを保証できますか？

   Routine Load は正確に一度のセマンティクスを保証します。

   各ロードタスクは個別のトランザクションです。トランザクションの実行中にエラーが発生した場合、トランザクションは中止され、FE はロードタスクの関連パーティションの消費進捗を更新しません。次回、FE がタスクキューからロードタスクをスケジュールする際、ロードタスクはパーティションの最後に保存された消費位置から消費要求を送信し、これにより正確に一度のセマンティクスを保証します。