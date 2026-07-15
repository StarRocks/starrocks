---
displayed_sidebar: docs
sidebar_label: "メモリとスピル"
sidebar_position: 4
description: "クエリのメモリ制限と、ディスクまたはリモートストレージへのスピルを制御するセッション変数。"
---

# システム変数 - メモリとスピル

変数の表示および設定方法については、[システム変数の概要](../System_variable.md) を参照してください。

### disable_spill_to_local_disk

* **説明**: セッションで `true` に設定すると、FE は BE に対してローカルディスクへのスピルを無効にし、代わりにリモートストレージへのスピルに依存するよう指示します（リモートスピルが構成されている場合）。このフラグは `enable_spill` = `true`、`enable_spill_to_remote_storage` = `true`、かつ有効な `spill_storage_volume` が FE によって検出されている場合にのみ意味を持ちます。値は TSpillToRemoteStorageOptions（BE に送信）に `disable_spill_to_local_disk` としてシリアライズされます。リモートスピルが構成されていないか、指定されたストレージボリュームが解決できない場合、この設定は効果を持ちません。注意して使用してください：ローカルディスクへのスピルを無効化するとネットワーク I/O とレイテンシが増加し、信頼性・高性能なリモートストレージを必要とします。
* **スコープ**: Session
* **デフォルト**: false
* **データ型**: boolean
* **導入バージョン**: v3.3.0, v3.4.0, v3.5.0

### enable_spill

* **説明**: 中間結果のスピルを有効にするかどうか。デフォルト: `false`。`true` に設定されている場合、StarRocks はクエリ内の集計、ソート、またはジョインオペレーターを処理する際にメモリ使用量を削減するために中間結果をディスクにスピルします。
* **デフォルト**: false
* **導入バージョン**: v3.0

### enable_spill_to_remote_storage

* **説明**: 中間結果をオブジェクトストレージにスピルするかどうか。`true` に設定されている場合、StarRocks はローカルディスクの容量制限に達した後、`spill_storage_volume` で指定されたストレージボリュームに中間結果をスピルします。詳細については、[Spill to object storage](../administration/management/resource_management/spill_to_disk.md#preview-spill-intermediate-result-to-object-storage) を参照してください。
* **デフォルト**: false
* **導入バージョン**: v3.3.0

### query_mem_limit

* **説明**: 各 BE ノードでのクエリのメモリ制限を設定するために使用されます。デフォルト値は 0 で、制限がないことを意味します。この項目はパイプラインエンジンが有効になった後にのみ有効です。`Memory Exceed Limit` エラーが発生した場合、この変数を増やすことができます。`0` に設定すると、制限が課されないことを示します。
* **デフォルト**: 0
* **単位**: バイト

### spill_encode_level

* **スコープ**: セッション
* **説明**: オペレータのスピルファイルに適用されるエンコード／圧縮の振る舞いを制御します。整数はビットフラグレベルで、意味は `transmission_encode_level` と同様です:
  * bit 1 (値 `1`) — 適応エンコーディングを有効にする;
  * bit 2 (値 `2`) — 整数のような列を streamvbyte でエンコードする;
  * bit 4 (値 `4`) — バイナリ／文字列列を LZ4 で圧縮する。
  関連する `transmission_encode_level` のコメントからの例の意味: `7` は数値と文字列の適応エンコーディングを有効にし、`6` は数値と文字列のエンコードを強制します。この値を変更するとスピル時の CPU とディスク I/O のトレードオフが調整されます（エンコードレベルが高いほど CPU 負荷は増えるがスピルサイズ／I/O は減少します）。
  `SessionVariable.java` のセッション変数として注釈された `SPILL_ENCODE_LEVEL`（getter `getSpillEncodeLevel()`）として実装されており、`spill_mem_table_size` のような他のスピル調整可能項目の近くに文書化されています。
* **デフォルト**: `7`
* **タイプ**: int
* **導入バージョン**: v3.2.0

### spill_mode (3.0 以降)

中間結果のスピルの実行モード。有効な値:

* `auto`: メモリ使用量のしきい値に達したときにスピルが自動的にトリガーされます。
* `force`: StarRocks はメモリ使用量に関係なく、すべての関連オペレーターに対してスピルを強制的に実行します。

この変数は、変数 `enable_spill` が `true` に設定されている場合にのみ有効です。

### spill_storage_volume

* **説明**: スピルをトリガーしたクエリの中間結果を保存するために使用するストレージボリューム。詳細については、[Spill to object storage](../administration/management/resource_management/spill_to_disk.md#preview-spill-intermediate-result-to-object-storage) を参照してください。
* **デフォルト**: 空の文字列
* **導入バージョン**: v3.3.0

