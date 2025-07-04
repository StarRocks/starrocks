---
displayed_sidebar: docs
---

# be_txns

`be_txns` は各 BE ノード上のトランザクションに関する情報を提供します。

`be_txns` には以下のフィールドが提供されています:

| **フィールド**       | **説明**                                         |
| -------------- | ------------------------------------------------ |
| BE_ID          | BE ノードの ID。                                   |
| LOAD_ID        | ロードジョブの ID。                              |
| TXN_ID         | トランザクションの ID。                          |
| PARTITION_ID   | トランザクションに関与するパーティションの ID。  |
| TABLET_ID      | トランザクションに関与するタブレットの ID。      |
| CREATE_TIME    | トランザクションの作成時刻（Unix タイムスタンプ、秒）。 |
| COMMIT_TIME    | トランザクションのコミット時刻（Unix ���イムスタンプ、秒）。 |
| PUBLISH_TIME   | トランザクションの公開時刻（Unix タイムスタンプ、秒）。 |
| ROWSET_ID      | トランザクションに関与する Rowset の ID。        |
| NUM_SEGMENT    | Rowset 内のセグメント数。                        |
| NUM_DELFILE    | Rowset 内の削除ファイル数。                      |
| NUM_ROW        | Rowset 内の行数。                                |
| DATA_SIZE      | Rowset のデータサイズ（バイト）。                |
| VERSION        | Rowset のバージョン。                            |
