---
displayed_sidebar: docs
---

# be_tablets

`be_tablets` は各 BE ノード上のタブレットに関する情報を提供します。

`be_tablets` には以下のフィールドが提供されています:

| **フィールド**      | **説明**                                         |
| ------------- | ------------------------------------------------ |
| BE_ID         | BE ノードの ID。                                   |
| TABLE_ID      | タブレットが属するテーブルの ID。                |
| PARTITION_ID  | タブレットが属するパーティションの ID。          |
| TABLET_ID     | タブレットの ID。                                |
| NUM_VERSION   | タブレット内のバージョン数。                     |
| MAX_VERSION   | タブレットの最大バージョン。                     |
| MIN_VERSION   | タブレットの最小バージョン。                     |
| NUM_ROWSET    | タブレット内の Rowset 数。                       |
| NUM_ROW       | タブレット内の行数。                             |
| DATA_SIZE     | Rowset **データ**のディスクサイズ（セグメント列データ、バイト）。Rowset メタデータに記録される行内埋め込みインデックス（Short Key、Zone Map、Bloom Filter など）は含みません。 |
| INDEX_MEM     | タブレットのインデックスメモリ使用量（バイト）。 |
| CREATE_TIME   | タブレットの作成時刻（Unix タイムスタンプ、秒）。 |
| STATE         | タブレットの状態（例: `NORMAL`、`REPLICA_MISSING`）。 |
| TYPE          | タブレットのタイプ。                             |
| DATA_DIR      | タブレットが保存されているデータディレクトリ。   |
| SHARD_ID      | タブレットのシャード ID。                        |
| SCHEMA_HASH   | タブレットのスキーマハッシュ。                   |
| INDEX_DISK    | **主キー**タブレットでは**永続化主キーインデックス**のディスクサイズ（クラウドネイティブ PK の SSTable など）。Rowset 内埋め込みインデックスは含みません。その他のタイプでは 0 になる場合があります。 |
| MEDIUM_TYPE   | タブレットのメディアタイプ（例: `HDD`、`SSD`）。  |
| NUM_SEGMENT   | タブレット内のセグメント数。                     |
