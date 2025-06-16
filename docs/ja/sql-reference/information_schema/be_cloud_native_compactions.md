---
displayed_sidebar: docs
---

# be_cloud_native_compactions

`be_cloud_native_compactions` は、共有データクラスタの CN (または v3.0 の BE) で実行されている Compaction トランザクションに関する情報を提供します。Compaction トランザクションはパーティションレベルで複数のタスクに分割され、`be_cloud_native_compactions` の各行は Compaction トランザクション内のタスクに対応します。

`be_cloud_native_compactions` には次のフィールドが提供されています:

| **Field**   | **Description**                                              |
| ----------- | ------------------------------------------------------------ |
| BE_ID       | CN (BE) の ID。                                              |
| TXN_ID      | Compaction トランザクションの ID。同じ Compaction トランザクションに複数のタスクがあるため、重複することがあります。 |
| TABLET_ID   | タスクが対応する tablet の ID。                              |
| VERSION     | タスクに入力されるデータのバージョン。                       |
| SKIPPED     | タスクがスキップされたかどうか。                             |
| RUNS        | タスクの実行回数。`1` より大きい値はリトライが発生したことを示します。 |
| START_TIME  | タスクの開始時間。                                           |
| FINISH_TIME | タスクの終了時間。タスクが進行中の場合は `NULL` が返されます。 |
| PROGRESS    | 進捗の割合。0 から 100 の範囲。                              |
| STATUS      | タスクのステータス。                                         |
| PROFILE     | リアルタイムプロファイル。データ読み取り時間、データ書き込み時間、キュー内時間などを含みます。 |