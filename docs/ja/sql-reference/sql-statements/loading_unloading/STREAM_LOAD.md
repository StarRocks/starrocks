---
displayed_sidebar: docs
toc_max_heading_level: 4
description: "STREAM LOADを使用すると、ローカルファイルシステムまたはストリーミングデータソースからデータをロードできます。"
---

import Tip from '../../../_assets/commonMarkdown/quickstart-shared-nothing-tip.mdx';
import TableURL from '../../../_assets/commonMarkdown/stream_load_table_url.mdx';
import TableURLTip from '../../../_assets/commonMarkdown/stream_load_table_url_tip.mdx';

# STREAM LOAD

STREAM LOADを使用すると、ローカルファイルシステムまたはストリーミングデータソースからデータをロードできます。ロードジョブを送信すると、システムはジョブを同期的に実行し、ジョブ終了後に結果を返します。ジョブの結果に基づいて、ジョブが成功したかどうかを判断できます。Stream Loadのアプリケーションシナリオ、制限、およびサポートされているデータファイル形式については、[Stream Loadを使用したローカルファイルシステムからのロード](../../../loading/StreamLoad.md)。

<Tip />

v3.2.7以降、Stream LoadはJSONデータの送信時の圧縮をサポートし、ネットワーク帯域幅のオーバーヘッドを削減します。ユーザーはパラメータ`compression`および`Content-Encoding`を使用して異なる圧縮アルゴリズムを指定できます。サポートされている圧縮アルゴリズムには、GZIP、BZIP2、LZ4_FRAME、ZSTDがあります。詳細については、[data_desc](#data_desc)。

v3.4.0以降、システムは複数のStream Loadリクエストのマージをサポートしています。詳細については、[Merge Commitパラメータ](#merge-commit-parameters)。

:::note

- Stream Loadを使用してネイティブテーブルにデータをロードすると、そのテーブルに作成されたマテリアライズドビューのデータも更新されます。
- ネイティブテーブルにデータをロードできるのは、そのテーブルに対してINSERT権限を持つユーザーのみです。INSERT権限がない場合は、[GRANT](../account-management/GRANT.md) の手順に従って、クラスターへの接続に使用するユーザーにINSERT権限を付与してください。
:::

## 構文

```Bash
curl --location-trusted -u <username>:<password> -XPUT <url>
(
    data_desc
)
[opt_properties]        
```

このトピックでは、`curl`を例として、Stream Loadを使用したデータのロード方法を説明します。`curl`以外にも、HTTPと互換性のある他のツールや言語を使用してStream Loadを実行できます。ロード関連のパラメータはHTTPリクエストのヘッダーフィールドに含まれます。これらのパラメータを入力する際は、以下の点に注意してください。

- このトピックで示すように、チャンク転送エンコーディングを使用できます。チャンク転送エンコーディングを選択しない場合は、データの整合性を確保するために、転送するコンテンツの長さを示す`Content-Length`ヘッダーフィールドを入力する必要があります。

  :::note
  `curl`を使用してStream Loadを実行する場合、システムは自動的に`Content-Length`ヘッダーフィールドを追加するため、手動で入力する必要はありません。
  :::

- `Expect`ヘッダーフィールドを追加し、その値を`100-continue`（例：`"Expect:100-continue"`）に指定する必要があります。これにより、ジョブリクエストが拒否された場合の不要なデータ転送を防ぎ、リソースのオーバーヘッドを削減できます。

StarRocksでは、一部のリテラルがSQL言語の予約キーワードとして使用されていることに注意してください。これらのキーワードをSQLステートメントで直接使用しないでください。SQLステートメントでそのようなキーワードを使用する場合は、バッククォート（`）で囲んでください。詳細については、[キーワード](../keywords.md)。

## パラメータ

### usernameとpassword

クラスターへの接続に使用するアカウントのユーザー名とパスワードを指定します。これは必須パラメータです。パスワードが設定されていないアカウントを使用する場合は、`<username>:`のみを入力する必要があります。

### XPUT

HTTPリクエストメソッドを指定します。これは必須パラメータです。Stream LoadはPUTメソッドのみをサポートしています。

### url

テーブルのURLを指定します。構文：

<TableURL />

### data_desc

ロードするデータファイルを記述します。`data_desc`ディスクリプタには、データファイルの名前、形式、列区切り文字、行区切り文字、宛先パーティション、およびテーブルに対する列マッピングを含めることができます。構文：

```Bash
-T <file_path>
-H "format: CSV | JSON"
-H "column_separator: <column_separator>"
-H "row_delimiter: <row_delimiter>"
-H "columns: <column1_name>[, <column2_name>, ... ]"
-H "partitions: <partition1_name>[, <partition2_name>, ...]"
-H "temporary_partitions: <temporary_partition1_name>[, <temporary_partition2_name>, ...]"
-H "jsonpaths: [ \"<json_path1>\"[, \"<json_path2>\", ...] ]"
-H "strip_outer_array: true | false"
-H "json_root: <json_path>"
-H "envelope: debezium"
-H "ignore_json_size: true | false"
-H "compression: <compression_algorithm> | Content-Encoding: <compression_algorithm>"
```

`data_desc`ディスクリプタのパラメータは、共通パラメータ、CSVパラメータ、JSONパラメータの3種類に分けられます。

#### 共通パラメータ

| パラメータ | 必須 | 説明 |
| ---------- | -------- | ------------------------------------------------------------ |
| file_path  | はい | データファイルの保存パス。ファイル名の拡張子は任意で含めることができます。|
| format     | いいえ | データファイルの形式。有効な値：`CSV`および`JSON`。デフォルト値：`CSV`。|
| partitions | いいえ | データファイルをロードするパーティション。デフォルトでは、このパラメータを指定しない場合、システムはデータファイルをテーブルのすべてのパーティションにロードします。|
| temporary_partitions| いいえ | データファイルをロードする[一時パーティション](../../../table_design/data_distribution/Temporary_partition.md) の名前。複数の一時パーティションを指定でき、カンマ（,）で区切る必要があります。|
| columns    | いいえ | データファイルとテーブル間の列マッピング。<br />データファイルのフィールドをテーブルの列に順番にマッピングできる場合は、このパラメータを指定する必要はありません。代わりに、このパラメータを使用してデータ変換を実装できます。たとえば、CSVデータファイルをロードし、そのファイルがテーブルの2つの列（`id`と`city`）に順番にマッピングできる2つの列で構成されている場合、`"columns: city,tmp_id, id = tmp_id * 100"`と指定できます。詳細については、このトピックの[列マッピング](#column-mapping)セクションを参照してください。|

#### CSVパラメータ

##### `column_separator`

必須: いいえ

説明: データファイル内でフィールドを区切るために使用される文字。このパラメータを指定しない場合、デフォルト値は `\t` となり、タブを示します。<br />このパラメータで指定する列区切り文字が、データファイルで使用されている列区切り文字と同じであることを確認してください。<br />**注意**<br />- CSVデータの場合、カンマ（,）、タブ、パイプ（|）などのUTF-8文字列（50バイト以内）をテキスト区切り文字として使用できます。<br />- データファイルが連続する非印刷文字（例: `\r\n`）を列区切り文字として使用している場合、このパラメータを `\\x0D0A` に設定する必要があります。

##### `row_delimiter`

必須: いいえ

説明: データファイル内で行を区切るために使用される文字。このパラメータを指定しない場合、デフォルト値は `\n` となります。<br />**注意**<br />データファイルが連続する非印刷文字（例: `\r\n`）を行区切り文字として使用している場合、このパラメータを `\\x0D0A` に設定する必要があります。

##### `skip_header`

必須: いいえ

説明: データファイルがCSV形式の場合に、データファイルの先頭数行をスキップするかどうかを指定します。型: INTEGER。デフォルト値: `0`。<br />一部のCSV形式のデータファイルでは、先頭の数行が列名や列データ型などのメタデータを定義するために使用されています。`skip_header` パラメータを設定することで、データ読み込み時にデータファイルの先頭数行をスキップするようにシステムを設定できます。例えば、このパラメータを `1` に設定すると、データ読み込み時にデータファイルの最初の1行がスキップされます。<br />データファイル内の先頭数行は、ロードコマンドで指定した行区切り文字を使用して区切られている必要があります。

##### `trim_space`

必須: いいえ
説明: データファイルがCSV形式の場合に、データファイルの列区切り文字の前後にあるスペースを削除するかどうかを指定します。型: BOOLEAN。デフォルト値: `false`。<br />一部のデータベースでは、データをCSV形式のデータファイルとしてエクスポートする際に、列区切り文字にスペースが追加されます。このようなスペースは、位置に応じて先頭スペースまたは末尾スペースと呼ばれます。`trim_space` パラメータを設定することで、データ読み込み時にこのような不要なスペースを削除するようにシステムを設定できます。<br />`enclose` で指定された文字のペアで囲まれたフィールド内のスペース（先頭スペースおよび末尾スペースを含む）は削除されないことに注意してください。例えば、以下のフィールド値はパイプ（`|`）を列区切り文字、二重引用符（`"`）を `enclose` で指定された文字として使用しています:<br />`|"Love StarRocks"|` <br />`|" Love StarRocks "|` <br />`| "Love StarRocks" |` <br />`trim_space` を `true` に設定した場合、システムは上記のフィールド値を次のように処理します:<br />`|"Love StarRocks"|` <br />`|" Love StarRocks "|` <br />`|"Love StarRocks"|`

##### `enclose`

必須: いいえ

説明: データファイルがCSV形式の場合に、[RFC4180](https://www.rfc-editor.org/rfc/rfc4180) に従ってデータファイル内のフィールド値を囲むために使用される文字を指定します。型: 1バイト文字。デフォルト値: `NONE`。最も一般的な文字は、一重引用符（`'`）と二重引用符（`"`）です。<br />`enclose` で指定された文字で囲まれたすべての特殊文字（行区切り文字や列区切り文字を含む）は通常の記号とみなされます。システムはRFC4180を超える機能を持ち、任意の1バイト文字を `enclose` で指定された文字として指定できます。<br />フィールド値に `enclose` で指定された文字が含まれている場合、同じ文字を使用してその `enclose` で指定された文字をエスケープできます。例えば、`enclose` を `"` に設定し、フィールド値が `a "quoted" c` の場合、データファイルにはフィールド値を `"a ""quoted"" c"` として入力できます。|

##### `escape`

必須: いいえ

説明: 行区切り文字、列区切り文字、エスケープ文字、`enclose` で指定された文字などのさまざまな特殊文字をエスケープするために使用される文字を指定します。エスケープされた文字は通常の文字とみなされ、それが存在するフィールド値の一部として解析されます。型: 1バイト文字。デフォルト値: `NONE`。最も一般的な文字はスラッシュ（`\`）で、SQLステートメントではダブルスラッシュ（`\\`）として記述する必要があります。<br />**注意**<br />`escape` で指定された文字は、`enclose` で指定された文字のペアの内側と外側の両方に適用されます。<br />以下に2つの例を示します:<ul><li>`enclose` を `"` に設定し、`escape` を `\` に設定した場合、システムは `"say \"Hello world\""` を `say "Hello world"` に解析します。</li><li>列区切り文字がカンマ（`,`）であると仮定します。`escape` を `\` に設定した場合、システムは `a, b\, c` を2つの別々のフィールド値 `a` と `b, c` に解析します。</li></ul>

:::note

- CSVデータの場合、カンマ（,）、タブ、パイプ（|）などのUTF-8文字列（50バイト以内）をテキスト区切り文字として使用できます。
- NULL値は `\N` を使用して表します。例えば、データファイルが3列で構成されており、あるレコードが1列目と3列目にデータを持ち、2列目にデータがない場合、2列目にNULL値を示すために `\N` を使用する必要があります。つまり、レコードは `a,,b` ではなく `a,\N,b` としてコンパイルする必要があります。`a,,b` は、レコードの2列目が空文字列を保持していることを示します。
- `skip_header`、`trim_space`、`enclose`、`escape` などのフォーマットオプションは、v3.0以降でサポートされています。
:::

#### JSONパラメータ

| パラメータ         | 必須 | 説明                                                  |
| ----------------- | -------- | ------------------------------------------------------------ |
| jsonpaths         | いいえ       | JSONデータファイルから読み込むキーの名前。このパラメータは、マッチモードを使用してJSONデータを読み込む場合にのみ指定する必要があります。このパラメータの値はJSON形式です。[JSONデータ読み込みの列マッピングを設定する](#configure-column-mapping-for-json-data-loading)を参照してください。           |
| strip_outer_array | いいえ       | 最外部の配列構造を除去するかどうかを指定します。有効な値: `true` および `false`。デフォルト値: `false`。<br />実際のビジネスシナリオでは、JSONデータが角括弧 `[]` のペアで示される最外部の配列構造を持つ場合があります。この場合、このパラメータを `true` に設定することを推奨します。これにより、システムは最外部の角括弧 `[]` を削除し、各内部配列を個別のデータレコードとして読み込みます。このパラメータを `false` に設定した場合、システムはJSONデータファイル全体を1つの配列として解析し、その配列を単一のデータレコードとして読み込みます。<br />たとえば、JSONデータが`[ {"category" : 1, "author" : 2}, {"category" : 3, "author" : 4} ]`の場合、このパラメータを`true`に設定すると、`{"category" : 1, "author" : 2}`と`{"category" : 3, "author" : 4}`は別々のデータレコードとして解析され、別々のテーブル行に読み込まれます。|
| json_root         | いいえ       | JSONデータファイルから読み込むJSONデータのルート要素です。このパラメータは、マッチモードを使用してJSONデータを読み込む場合にのみ指定する必要があります。このパラメータの値は有効なJsonPath文字列です。デフォルトでは、このパラメータの値は空であり、JSONデータファイルのすべてのデータが読み込まれることを示します。詳細については、このトピックの[ルート要素を指定したマッチモードでJSONデータを読み込む](#load-json-data-using-matched-mode-with-root-element-specified)セクションを参照してください。|
| envelope          | いいえ       | JSONデータのCDCエンベロープ形式を指定します。有効な値：`debezium`。デフォルト：未設定（エンベロープラッピングなし）。`debezium`に設定すると、StarRocksは各JSONメッセージをDebezium CDCイベントとして解析します。メッセージには`op`フィールド（`c`=作成、`u`=更新、`d`=削除、`r`=スナップショット読み取り）と、実際の行データを保持する`after`フィールド（c/u/rの場合）または`before`フィールド（dの場合）が含まれている必要があります。`payload`が`null`であるトゥームストーンメッセージは暗黙的にスキップされます。`json_root`または`strip_outer_array`と同時に使用することはできません。|
| ignore_json_size  | いいえ       | HTTPリクエスト内のJSON本文のサイズを確認するかどうかを指定します。<br />**注意**<br />デフォルトでは、HTTPリクエスト内のJSON本文のサイズは100 MBを超えることができません。JSON本文が100 MBを超える場合、「The size of this batch exceed the max size [104857600] of json type data data [8617627793]. Set ignore_json_size to skip check, although it may lead huge memory consuming.」というエラーが報告されます。このエラーを防ぐには、HTTPリクエストヘッダーに`"ignore_json_size:true"`を追加して、システムがJSON本文のサイズを確認しないように指示できます。|
| compression, Content-Encoding | いいえ | 送信中にデータに適用されるエンコードアルゴリズムです。サポートされているアルゴリズムには、GZIP、BZIP2、LZ4_FRAME、ZSTDが含まれます。例：`curl --location-trusted -u root:  -v '<table_url>' \-X PUT  -H "expect:100-continue" \-H 'format: json' -H 'compression: lz4_frame'   -T ./b.json.lz4`。|

JSONデータを読み込む際、1つのJSONオブジェクトのサイズが4 GBを超えることはできません。JSONデータファイル内の個々のJSONオブジェクトが4 GBを超える場合、「This parser can't support a document that big.」というエラーが報告されます。

### Merge Commitパラメータ

指定された時間ウィンドウ内で複数の同時Stream Loadリクエストに対してMerge Commitを有効にし、それらを単一のトランザクションにマージします。

:::warning

Merge Commit最適化は、単一テーブルに対して**同時** Stream Loadジョブを実行するシナリオに適しています。同時実行数が1の場合は推奨されません。また、`merge_commit_async`を`false`に設定したり、`merge_commit_interval_ms`を大きな値に設定したりする前によく検討してください。これらの設定は読み込みパフォーマンスの低下を引き起こす可能性があります。

:::

| **パラメータ**            | **必須** | **説明**                                              |
| ------------------------ | ------------ | ------------------------------------------------------------ |
| enable_merge_commit      | いいえ           | 読み込みリクエストに対してMerge Commitを有効にするかどうか。有効な値：`true`および`false`（デフォルト）。|
| merge_commit_async       | いいえ           | サーバーの返却モード。有効な値：<ul><li>`true`：非同期モードを有効にします。サーバーはデータを受信した後すぐに返却します。このモードでは読み込みの成功は保証されません。</li><li>`false`（デフォルト）：同期モードを有効にします。サーバーはマージされたトランザクションがコミットされた後にのみ返却し、読み込みの成功と可視性を保証します。</li></ul> |
| merge_commit_interval_ms | はい          | マージ時間ウィンドウのサイズ。単位：ミリ秒。Merge Commitは、このウィンドウ内に受信した読み込みリクエストを単一のトランザクションにマージしようとします。ウィンドウが大きいほどマージ効率が向上しますが、レイテンシが増加します。|
| merge_commit_parallel    | はい          | 各マージウィンドウに対して作成される読み込みプランの並列度。並列度は取り込みの負荷に基づいて調整できます。リクエスト数が多い場合や読み込むデータ量が多い場合はこの値を増やしてください。並列度はBEノード数に制限され、`min(merge_commit_parallel, number of BE nodes)`として計算されます。|

:::note

- Merge Commitは**同質** 読み込みリクエストを単一のデータベースとテーブルにマージすることのみをサポートします。「同質」とは、Stream Loadパラメータが同一であることを意味し、共通パラメータ、JSON形式パラメータ、CSV形式パラメータ、`opt_properties`、およびMerge Commitパラメータが含まれます。
- CSV形式のデータを読み込む場合、各行が行区切り文字で終わることを確認する必要があります。`skip_header`はサポートされていません。
- サーバーはトランザクションのラベルを自動的に生成します。指定された場合は無視されます。
- Merge Commitは複数の読み込みリクエストを単一のトランザクションにマージします。1つのリクエストにデータ品質の問題が含まれている場合、トランザクション内のすべてのリクエストが失敗します。

:::

### opt_properties

ロードジョブ全体に適用されるオプションパラメータを指定します。構文：

```Bash
-H "label: <label_name>"
-H "where: <condition1>[, <condition2>, ...]"
-H "max_filter_ratio: <num>"
-H "timeout: <num>"
-H "strict_mode: true | false"
-H "timezone: <string>"
-H "load_mem_limit: <num>"
-H "partial_update: true | false"
-H "partial_update_mode: row | column"
-H "merge_condition: <column_name>"
```

次の表はオプションパラメータを説明しています。

| パラメータ        | 必須 | 説明                                                  |
| ---------------- | -------- | ------------------------------------------------------------ |
| label            | いいえ       | ロードジョブのラベルです。このパラメータを指定しない場合、システムは自動的にロードジョブのラベルを生成します。<br />システムでは、1つのラベルを使用してデータバッチを複数回読み込むことはできません。これにより、同じデータが繰り返し読み込まれることを防ぎます。ラベルの命名規則については、[システム制限](../../System_limit.md)を参照してください。<br />デフォルトでは、システムは最近3日間に正常に完了したロードジョブのラベルを保持します。[FEパラメータ](../../../administration/management/FE_configuration.md) `label_keep_max_second`を使用してラベルの保持期間を変更できます。|
| where            | いいえ       | システムが前処理済みデータをフィルタリングする条件です。システムはWHERE句で指定されたフィルタ条件を満たす前処理済みデータのみを読み込みます。|
| max_filter_ratio | いいえ       | ロードジョブの最大エラー許容度です。エラー許容度は、ロードジョブが要求するすべてのデータレコードのうち、データ品質の不足によりフィルタリングされるデータレコードの最大割合です。有効な値：`0`から`1`。デフォルト値：`0`。<br />デフォルト値 `0` を保持することをお勧めします。こうすることで、不適格なデータレコードが検出された場合にロードジョブが失敗し、データの正確性が保証されます。<br />不適格なデータレコードを無視したい場合は、このパラメータを `0` より大きい値に設定できます。こうすることで、データファイルに不適格なデータレコードが含まれていても、ロードジョブを成功させることができます。<br />**注意**<br />不適格なデータレコードには、WHERE句によってフィルタリングされたデータレコードは含まれません。 |
| log_rejected_record_num | いいえ           | ログに記録できる不適格なデータ行の最大数を指定します。このパラメータはv3.1以降でサポートされています。有効な値: `0`、`-1`、および任意のゼロ以外の正の整数。デフォルト値: `0`。<ul><li>値 `0` は、フィルタリングされたデータ行がログに記録されないことを指定します。</li><li>値 `-1` は、フィルタリングされたすべてのデータ行がログに記録されることを指定します。</li><li>`n` などのゼロ以外の正の整数は、各BEまたはCNでフィルタリングされたデータ行を最大 `n` 行までログに記録できることを指定します。</li></ul> |
| timeout          | いいえ       | ロードジョブのタイムアウト期間。有効な値: `1` ～ `259200`。単位: 秒。デフォルト値: `600`。<br />**注意**`timeout` パラメータに加えて、[FEパラメータ](../../../administration/management/FE_configuration.md) `stream_load_default_timeout_second` を使用して、クラスター内のすべてのStream Loadジョブのタイムアウト期間を一元管理することもできます。`timeout` パラメータを指定した場合、`timeout` パラメータで指定されたタイムアウト期間が優先されます。`timeout` パラメータを指定しない場合、`stream_load_default_timeout_second` パラメータで指定されたタイムアウト期間が優先されます。 |
| strict_mode      | いいえ       | [ストリクトモード](../../../loading/load_concept/strict_mode.md)を有効にするかどうかを指定します。有効な値: `true` および `false`。デフォルト値: `false`。値 `true` はストリクトモードを有効にすることを指定し、値 `false` はストリクトモードを無効にすることを指定します。 |
| timezone         | いいえ       | ロードジョブで使用されるタイムゾーン。デフォルト値: `Asia/Shanghai`。このパラメータの値は、strftime、alignment_timestamp、from_unixtimeなどの関数が返す結果に影響します。このパラメータで指定されたタイムゾーンはセッションレベルのタイムゾーンです。詳細については、[タイムゾーンの設定](../../../administration/management/timezone.md)を参照してください。 |
| load_mem_limit   | いいえ       | ロードジョブにプロビジョニングできるメモリの最大量。単位: バイト。デフォルトでは、ロードジョブの最大メモリサイズは2 GBです。このパラメータの値は、各BEまたはCNにプロビジョニングできるメモリの最大量を超えることはできません。 |
| partial_update | いいえ | 部分更新を使用するかどうか。有効な値: `TRUE` および `FALSE`。デフォルト値: `FALSE`（この機能を無効にすることを示します）。 |
| partial_update_mode | いいえ | 部分更新のモードを指定します。有効な値: `row` および `column`。<ul><li>値 `row`（デフォルト）は行モードでの部分更新を意味し、多くの列と小さなバッチによるリアルタイム更新に適しています。</li><li>値 `column` は列モードでの部分更新を意味し、少ない列と多くの行によるバッチ更新に適しています。このようなシナリオでは、列モードを有効にすることでより高速な更新速度が得られます。たとえば、100列のテーブルで、全行に対して10列（全体の10%）のみを更新する場合、列モードの更新速度は10倍速くなります。</li></ul> |
| merge_condition  | いいえ       | 更新を有効にするかどうかを判断する条件として使用する列の名前を指定します。指定した列において、ソースデータレコードの値が宛先データレコードの値以上である場合にのみ、ソースレコードから宛先レコードへの更新が有効になります。システムはv2.5以降、条件付き更新をサポートしています。<br />**注意**<br />指定する列は主キー列にすることはできません。また、条件付き更新をサポートするのは、Primary Keyテーブルを使用するテーブルのみです。|

## 列マッピング

### CSVデータロードの列マッピングの設定

データファイルの列をテーブルの列に順番に1対1でマッピングできる場合は、データファイルとテーブル間の列マッピングを設定する必要はありません。

データファイルの列をテーブルの列に順番に1対1でマッピングできない場合は、`columns` パラメータを使用して、データファイルとテーブル間の列マッピングを設定する必要があります。これには以下の2つのユースケースが含まれます。

- **列数は同じだが列の順序が異なる場合。** **また、データファイルのデータは、対応するテーブル列にロードされる前に関数で計算する必要がない場合。**

  `columns` パラメータでは、データファイルの列の並び順と同じ順序でテーブル列の名前を指定する必要があります。

  たとえば、テーブルが `col1`、`col2`、`col3` の順に3列で構成され、データファイルも3列で構成されており、テーブル列 `col3`、`col2`、`col1` の順にマッピングできる場合、`"columns: col3, col2, col1"` を指定する必要があります。

- **列数と列の順序が異なる場合。また、データファイルのデータは、対応するテーブル列にロードされる前に関数で計算する必要がある場合。**

  `columns` パラメータでは、データファイルの列の並び順と同じ順序でテーブル列の名前を指定し、データの計算に使用する関数を指定する必要があります。以下に2つの例を示します。

  - テーブルは `col1`、`col2`、`col3` の順に3列で構成されています。データファイルは4列で構成されており、最初の3列はテーブル列 `col1`、`col2`、`col3` に順番にマッピングでき、4列目はどのテーブル列にもマッピングできません。この場合、データファイルの4列目に一時的な名前を指定する必要があり、その名前はテーブル列名と異なる必要があります。たとえば、`"columns: col1, col2, col3, temp"` を指定でき、この場合データファイルの4列目は一時的に `temp` と命名されます。
  - テーブルは `year`、`month`、`day` の順に3列で構成されています。データファイルは `yyyy-mm-dd hh:mm:ss` 形式の日付と時刻の値を格納する1列のみで構成されています。この場合、`"columns: col, year = year(col), month=month(col), day=day(col)"` を指定できます。ここで `col` はデータファイル列の一時的な名前であり、関数 `year = year(col)`、`month=month(col)`、`day=day(col)` はデータファイル列 `col` からデータを抽出し、対応するテーブル列にロードするために使用されます。たとえば、`year = year(col)` はデータファイル列 `col` から `yyyy` データを抽出し、テーブル列 `year` にロードするために使用されます。

詳細な例については、[列マッピングの設定](#configure-column-mapping)を参照してください。

### JSONデータロードの列マッピングの設定

JSONドキュメントのキーがテーブルの列と同じ名前を持つ場合、シンプルモードを使用してJSON形式のデータをロードできます。シンプルモードでは、`jsonpaths` パラメータを指定する必要はありません。このモードでは、JSON形式のデータは `{}` の波括弧で示されるオブジェクトである必要があります（例: `{"category": 1, "author": 2, "price": "3"}`）。この例では、`category`、`author`、`price` がキー名であり、これらのキーはテーブルの列 `category`、`author`、`price` に名前で1対1にマッピングできます。

JSONドキュメントのキーがテーブルの列名と異なる場合、マッチドモードを使用してJSONフォーマットのデータをロードできます。マッチドモードでは、`jsonpaths` および `COLUMNS` パラメータを使用して、JSONドキュメントとテーブル間の列マッピングを指定する必要があります。

- `jsonpaths` パラメータには、JSONドキュメント内に配置されている順序でJSONキーを指定します。
- `COLUMNS` パラメータには、JSONキーとテーブル列間のマッピングを指定します。
  - `COLUMNS` パラメータで指定された列名は、JSONキーに順番に1対1でマッピングされます。
  - `COLUMNS` パラメータで指定された列名は、テーブルの列に名前で1対1にマッピングされます。

マッチドモードを使用してJSONフォーマットのデータをロードする例については、[マッチドモードを使用してJSONデータをロードする](#load-json-data-using-matched-mode)をご参照ください。

## 戻り値

ロードジョブが完了すると、システムはジョブ結果をJSON形式で返します。例：

```JSON
{
    "TxnId": 1003,
    "Label": "label123",
    "Status": "Success",
    "Message": "OK",
    "NumberTotalRows": 1000000,
    "NumberLoadedRows": 999999,
    "NumberFilteredRows": 1,
    "NumberUnselectedRows": 0,
    "LoadBytes": 40888898,
    "LoadTimeMs": 2144,
    "BeginTxnTimeMs": 0,
    "StreamLoadPlanTimeMs": 1,
    "ReadDataTimeMs": 0,
    "WriteDataTimeMs": 11,
    "CommitAndPublishTimeMs": 16,
}
```

次の表は、返されたジョブ結果のパラメータを説明しています。

| パラメータ              | 説明                                                  |
| ---------------------- | ------------------------------------------------------------ |
| TxnId                  | ロードジョブのトランザクションID。                          |
| Label                  | ロードジョブのラベル。                                   |
| Status                 | ロードされたデータの最終ステータス。<ul><li>`Success`：データが正常にロードされ、クエリ可能です。</li><li>`Publish Timeout`：ロードジョブは正常に送信されましたが、データはまだクエリできません。データを再度ロードする必要はありません。</li><li>`Label Already Exists`：ロードジョブのラベルが別のロードジョブに使用されています。データは正常にロードされたか、ロード中の可能性があります。</li><li>`Fail`：データのロードに失敗しました。ロードジョブを再試行できます。</li></ul> |
| Message                | ロードジョブのステータス。ロードジョブが失敗した場合、詳細な失敗原因が返されます。 |
| NumberTotalRows        | 読み取られたデータレコードの総数。              |
| NumberLoadedRows       | 正常にロードされたデータレコードの総数。このパラメータは、`Status` の戻り値が `Success` の場合にのみ有効です。 |
| NumberFilteredRows     | データ品質が不十分なためにフィルタリングされたデータレコードの数。 |
| NumberUnselectedRows   | WHERE句によってフィルタリングされたデータレコードの数。 |
| LoadBytes              | ロードされたデータの量。単位：バイト。              |
| LoadTimeMs             | ロードジョブにかかった時間。単位：ms。  |
| BeginTxnTimeMs         | ロードジョブのトランザクションを実行するのにかかった時間。 |
| StreamLoadPlanTimeMs   | ロードジョブの実行計画を生成するのにかかった時間。 |
| ReadDataTimeMs         | ロードジョブのデータ読み取りにかかった時間。 |
| WriteDataTimeMs        | ロードジョブのデータ書き込みにかかった時間。 |
| CommitAndPublishTimeMs | ロードジョブのデータのコミットおよび公開にかかった時間。 |

ロードジョブが失敗した場合、システムは `ErrorURL` も返します。例：

```JSON
{"ErrorURL": "http://172.26.195.68:8045/api/_load_error_log?file=error_log_3a4eb8421f0878a6_9a54df29fd9206be"}
```

`ErrorURL` は、フィルタリングされた不適格なデータレコードの詳細を取得できるURLを提供します。ロードジョブを送信する際に設定するオプションパラメータ `log_rejected_record_num` を使用して、ログに記録できる不適格なデータ行の最大数を指定できます。

`curl "url"` を実行して、フィルタリングされた不適格なデータレコードの詳細を直接確認できます。また、`wget "url"` を実行してこれらのデータレコードの詳細をエクスポートすることもできます。

```Bash
wget http://172.26.195.68:8045/api/_load_error_log?file=error_log_3a4eb8421f0878a6_9a54df29fd9206be
```

エクスポートされたデータレコードの詳細は、`_load_error_log?file=error_log_3a4eb8421f0878a6_9a54df29fd9206be` に似た名前のローカルファイルに保存されます。`cat` コマンドを使用してファイルを確認できます。

その後、ロードジョブの設定を調整し、ロードジョブを再度送信できます。

## 例

<TableURLTip />

### CSVデータのロード

このセクションでは、CSVデータを例として、さまざまなパラメータ設定と組み合わせを使用して多様なロード要件を満たす方法を説明します。

#### タイムアウト期間の設定

データベース `test_db` には `table1` という名前のテーブルが含まれています。このテーブルは3つの列で構成されており、順番に `col1`、`col2`、`col3` です。

データファイル `example1.csv` も3つの列で構成されており、`table1` の `col1`、`col2`、`col3` に順番にマッピングできます。

`example1.csv` のすべてのデータを最大100秒以内に `table1` にロードする場合は、次のコマンドを実行します。

```Bash
curl --location-trusted -u <username>:<password> -H "label:label1" \
    -H "Expect:100-continue" \
    -H "timeout:100" \
    -H "max_filter_ratio:0.2" \
    -T example1.csv -XPUT \
    <table_url_prefix>/api/test_db/table1/_stream_load
```

#### エラー許容値の設定

データベース `test_db` には `table2` という名前のテーブルが含まれています。このテーブルは3つの列で構成されており、順番に `col1`、`col2`、`col3` です。

データファイル `example2.csv` も3つの列で構成されており、`table2` の `col1`、`col2`、`col3` に順番にマッピングできます。

`example2.csv` のすべてのデータを最大エラー許容値 `0.2` で `table2` にロードする場合は、次のコマンドを実行します。

```Bash
curl --location-trusted -u <username>:<password> -H "label:label2" \
    -H "Expect:100-continue" \
    -H "max_filter_ratio:0.2" \
    -T example2.csv -XPUT \
    <table_url_prefix>/api/test_db/table2/_stream_load
```

#### 列マッピングの設定

データベース `test_db` には `table3` という名前のテーブルが含まれています。このテーブルは3つの列で構成されており、順番に `col1`、`col2`、`col3` です。

データファイル `example3.csv` も3つの列で構成されており、`table3` の `col2`、`col1`、`col3` に順番にマッピングできます。

`example3.csv` のすべてのデータを `table3` にロードする場合は、次のコマンドを実行します。

```Bash
curl --location-trusted -u <username>:<password>  -H "label:label3" \
    -H "Expect:100-continue" \
    -H "columns: col2, col1, col3" \
    -T example3.csv -XPUT \
    <table_url_prefix>/api/test_db/table3/_stream_load
```

:::note
上記の例では、`example3.csv` の列は `table3` での配置順序と同じ順序で `table3` の列にマッピングできません。そのため、`columns` パラメータを使用して `example3.csv` と `table3` 間の列マッピングを設定する必要があります。
:::

#### フィルター条件の設定

データベース `test_db` には `table4` という名前のテーブルが含まれています。このテーブルは3つの列で構成されており、順番に `col1`、`col2`、`col3` です。

データファイル `example4.csv` も3つの列で構成されており、`table4` の `col1`、`col2`、`col3` に順番にマッピングできます。

`example4.csv` の最初の列の値が `20180601` と等しいデータレコードのみを `table4` にロードする場合は、次のコマンドを実行します。

```Bash
curl --location-trusted -u <username>:<password> -H "label:label4" \
    -H "Expect:100-continue" \
    -H "columns: col1, col2, col3"\
    -H "where: col1 = 20180601" \
    -T example4.csv -XPUT \
    <table_url_prefix>/api/test_db/table4/_stream_load
```

:::note
前述の例では、`example4.csv` と `table4` はマッピング可能な列数が同じで順番にマッピングできますが、列ベースのフィルター条件を指定するためにWHERE句を使用する必要があります。そのため、`example4.csv` の列に一時的な名前を定義するために `columns` パラメータを使用する必要があります。
:::

#### 宛先パーティションの設定

データベース `test_db` には `table5` という名前のテーブルが含まれています。このテーブルは3つの列で構成されており、順番に `col1`、`col2`、`col3` となっています。

データファイル `example5.csv` も3つの列で構成されており、`table5` の `col1`、`col2`、`col3` に順番にマッピングできます。

`example5.csv` のすべてのデータを `table5` のパーティション `p1` と `p2` にロードする場合は、次のコマンドを実行します。

```Bash
curl --location-trusted -u <username>:<password>  -H "label:label5" \
    -H "Expect:100-continue" \
    -H "partitions: p1, p2" \
    -T example5.csv -XPUT \
    <table_url_prefix>/api/test_db/table5/_stream_load
```

#### 厳格モードとタイムゾーンの設定

データベース `test_db` には `table6` という名前のテーブルが含まれています。このテーブルは3つの列で構成されており、順番に `col1`、`col2`、`col3` となっています。

データファイル `example6.csv` も3つの列で構成されており、`table6` の `col1`、`col2`、`col3` に順番にマッピングできます。

`example6.csv` のすべてのデータを厳格モードおよびタイムゾーン `Africa/Abidjan` を使用して `table6` にロードする場合は、次のコマンドを実行します。

```Bash
curl --location-trusted -u <username>:<password> \
    -H "Expect:100-continue" \
    -H "strict_mode: true" \
    -H "timezone: Africa/Abidjan" \
    -T example6.csv -XPUT \
    <table_url_prefix>/api/test_db/table6/_stream_load
```

#### HLL型列を含むテーブルへのデータのロード

データベース `test_db` には `table7` という名前のテーブルが含まれています。このテーブルは2つのHLL型列で構成されており、順番に `col1` と `col2` となっています。

データファイル `example7.csv` も2つの列で構成されており、最初の列は `table7` の `col1` にマッピングでき、2番目の列は `table7` のどの列にもマッピングできません。`example7.csv` の最初の列の値は、`table7` の `col1` にロードされる前に、関数を使用してHLL型データに変換できます。

`example7.csv` から `table7` にデータをロードする場合は、次のコマンドを実行します。

```Bash
curl --location-trusted -u <username>:<password> \
    -H "Expect:100-continue" \
    -H "columns: temp1, temp2, col1=hll_hash(temp1), col2=hll_empty()" \
    -T example7.csv -XPUT \
    <table_url_prefix>/api/test_db/table7/_stream_load
```

:::note
前述の例では、`example7.csv` の2つの列は `columns` パラメータを使用して順番に `temp1` と `temp2` と名付けられています。次に、以下のようにデータを変換するために関数が使用されます。

- `hll_hash` 関数は、`example7.csv` の `temp1` の値をHLL型データに変換し、`example7.csv` の `temp1` を `table7` の `col1` にマッピングするために使用されます。
- `hll_empty` 関数は、指定されたデフォルト値を `table7` の `col2` に埋めるために使用されます。
:::

関数 `hll_hash` と `hll_empty` の使用方法については、[hll_hash](../../sql-functions/scalar-functions/hll_hash.md) および [hll_empty](../../sql-functions/scalar-functions/hll_empty.md)を参照してください。

#### BITMAP型列を含むテーブルへのデータのロード

データベース `test_db` には `table8` という名前のテーブルが含まれています。このテーブルは2つのBITMAP型列で構成されており、順番に `col1` と `col2` となっています。

データファイル `example8.csv` も2つの列で構成されており、最初の列は `table8` の `col1` にマッピングでき、2番目の列は `table8` のどの列にもマッピングできません。`example8.csv` の最初の列の値は、`table8` の `col1` にロードされる前に、関数を使用して変換できます。

`example8.csv` から `table8` にデータをロードする場合は、次のコマンドを実行します。

```Bash
curl --location-trusted -u <username>:<password> \
    -H "Expect:100-continue" \
    -H "columns: temp1, temp2, col1=to_bitmap(temp1), col2=bitmap_empty()" \
    -T example8.csv -XPUT \
    <table_url_prefix>/api/test_db/table8/_stream_load
```

:::note
前述の例では、`example8.csv` の2つの列は `columns` パラメータを使用して順番に `temp1` と `temp2` と名付けられています。次に、以下のようにデータを変換するために関数が使用されます。

- `to_bitmap` 関数は、`example8.csv` の `temp1` の値をBITMAP型データに変換し、`example8.csv` の `temp1` を `table8` の `col1` にマッピングするために使用されます。
- `bitmap_empty` 関数は、指定されたデフォルト値を `table8` の `col2` に埋めるために使用されます。
:::

関数 `to_bitmap` と `bitmap_empty` の使用方法については、[to_bitmap](../../sql-functions/bitmap-functions/to_bitmap.md) および [bitmap_empty](../../sql-functions/bitmap-functions/bitmap_empty.md)を参照してください。

#### `skip_header`、`trim_space`、`enclose`、および `escape` の設定

データベース `test_db` には `table9` という名前のテーブルが含まれています。このテーブルは3つの列で構成されており、順番に `col1`、`col2`、`col3` となっています。

データファイル `example9.csv` も3つの列で構成されており、`table13` の `col2`、`col1`、`col3` に順番にマッピングされます。

`example9.csv` のすべてのデータを `table9` にロードする場合、`example9.csv` の最初の5行をスキップし、列区切り文字の前後のスペースを削除し、`enclose` を `\` に、`escape` を `\` に設定するには、次のコマンドを実行します。

```Bash
curl --location-trusted -u <username>:<password> -H "label:3875" \
    -H "Expect:100-continue" \
    -H "trim_space: true" -H "skip_header: 5" \
    -H "column_separator:," -H "enclose:\"" -H "escape:\\" \
    -H "columns: col2, col1, col3" \
    -T example9.csv -XPUT \
    <table_url_prefix>/api/test_db/tbl9/_stream_load
```

#### `column_separator` と `row_delimiter` の設定

データベース `test_db` には `table10` という名前のテーブルが含まれています。このテーブルは、`col1`、`col2`、`col3` の順に3つの列で構成されています。

データファイル `example10.csv` も3つの列で構成されており、`table10` の `col1`、`col2`、`col3` に順番にマッピングできます。データ行内の列はカンマ（`,`）で区切られ、データ行は2つの連続した非印字文字 `\r\n` で区切られます。

`example10.csv` からすべてのデータを `table10` にロードする場合は、次のコマンドを実行してください：

```Bash
curl --location-trusted -u <username>:<password> -H "label:label10" \
    -H "Expect:100-continue" \
    -H "column_separator:," \
    -H "row_delimiter:\\x0D0A" \
    -T example10.csv -XPUT \
    <table_url_prefix>/api/test_db/table10/_stream_load
```

### JSONデータのロード

このセクションでは、JSONデータをロードする際に注意が必要なパラメータ設定について説明します。

データベース `test_db` には `tbl1` という名前のテーブルが含まれており、そのスキーマは次のとおりです：

```SQL
`category` varchar(512) NULL COMMENT "",`author` varchar(512) NULL COMMENT "",`title` varchar(512) NULL COMMENT "",`price` double NULL COMMENT ""
```

#### シンプルモードを使用したJSONデータのロード

データファイル `example1.json` が次のデータで構成されているとします：

```JSON
{"category":"C++","author":"avc","title":"C++ primer","price":895}
```

`example1.json` からすべてのデータを `tbl1` にロードするには、次のコマンドを実行してください：

```Bash
curl --location-trusted -u <username>:<password> -H "label:label6" \
    -H "Expect:100-continue" \
    -H "format: json" \
    -T example1.json -XPUT \
    <table_url_prefix>/api/test_db/tbl1/_stream_load
```

:::note
上記の例では、パラメータ `columns` と `jsonpaths` は指定されていません。そのため、`example1.json` のキーは名前によって `tbl1` の列にマッピングされます。
:::

スループットを向上させるために、Stream Loadは複数のデータレコードを一度にロードすることをサポートしています。例：

```JSON
[{"category":"C++","author":"avc","title":"C++ primer","price":89.5},{"category":"Java","author":"avc","title":"Effective Java","price":95},{"category":"Linux","author":"avc","title":"Linux kernel","price":195}]
```

#### マッチモードを使用したJSONデータのロード

システムは次の手順でJSONデータのマッチングと処理を行います：

1. （オプション）`strip_outer_array` パラメータの設定に従って、最外部の配列構造を取り除きます。

   :::note
   この手順は、JSONデータの最外部レイヤーが角括弧 `[]` で示される配列構造である場合にのみ実行されます。`strip_outer_array` を `true` に設定する必要があります。
   :::

2. （オプション）`json_root` パラメータの設定に従って、JSONデータのルート要素をマッチングします。

   :::note
   この手順は、JSONデータにルート要素がある場合にのみ実行されます。`json_root` パラメータを使用してルート要素を指定する必要があります。
   :::

3. `jsonpaths` パラメータの設定に従って、指定されたJSONデータを抽出します。

##### ルート要素を指定しないマッチモードを使用したJSONデータのロード

データファイル `example2.json` が次のデータで構成されているとします：

```JSON
[{"category":"xuxb111","author":"1avc","title":"SayingsoftheCentury","price":895},{"category":"xuxb222","author":"2avc","title":"SayingsoftheCentury","price":895},{"category":"xuxb333","author":"3avc","title":"SayingsoftheCentury","price":895}]
```

`example2.json` から `category`、`author`、および `price` のみをロードするには、次のコマンドを実行してください：

```Bash
curl --location-trusted -u <username>:<password> -H "label:label7" \
    -H "Expect:100-continue" \
    -H "format: json" \
    -H "strip_outer_array: true" \
    -H "jsonpaths: [\"$.category\",\"$.price\",\"$.author\"]" \
    -H "columns: category, price, author" \
    -T example2.json -XPUT \
    <table_url_prefix>/api/test_db/tbl1/_stream_load
```

:::note
上記の例では、JSONデータの最外部レイヤーが角括弧 `[]` で示される配列構造です。この配列構造は、それぞれデータレコードを表す複数のJSONオブジェクトで構成されています。そのため、最外部の配列構造を取り除くために `strip_outer_array` を `true` に設定する必要があります。キー**タイトル**ロードしたくないキーはロード中に無視されます。
:::

##### ルート要素を指定したマッチモードを使用したJSONデータのロード

データファイル `example3.json` が次のデータで構成されているとします：

```JSON
{"id": 10001,"RECORDS":[{"category":"11","title":"SayingsoftheCentury","price":895,"timestamp":1589191587},{"category":"22","author":"2avc","price":895,"timestamp":1589191487},{"category":"33","author":"3avc","title":"SayingsoftheCentury","timestamp":1589191387}],"comments": ["3 records", "there will be 3 rows"]}
```

`example3.json` から `category`、`author`、および `price` のみをロードするには、次のコマンドを実行してください：

```Bash
curl --location-trusted -u <username>:<password> \
    -H "Expect:100-continue" \
    -H "format: json" \
    -H "json_root: $.RECORDS" \
    -H "strip_outer_array: true" \
    -H "jsonpaths: [\"$.category\",\"$.price\",\"$.author\"]" \
    -H "columns: category, price, author" -H "label:label8" \
    -T example3.json -XPUT \
    <table_url_prefix>/api/test_db/tbl1/_stream_load
```

:::note
上記の例では、JSONデータの最外部レイヤーが角括弧 `[]` で示される配列構造です。この配列構造は、それぞれデータレコードを表す複数のJSONオブジェクトで構成されています。そのため、最外部の配列構造を取り除くために `strip_outer_array` を `true` に設定する必要があります。ロードしたくないキー `title` と `timestamp` はロード中に無視されます。さらに、`json_root` パラメータはJSONデータのルート要素（配列）を指定するために使用されます。
:::

### Stream Loadリクエストのマージ

- 次のコマンドを実行して、同期モードでMerge Commitを有効にしたStream Loadジョブを開始し、マージウィンドウを `5000` ミリ秒、並列度を `2` に設定します：

  ```Bash
  curl --location-trusted -u <username>:<password> \
      -H "Expect:100-continue" \
      -H "column_separator:," \
      -H "columns: id, name, score" \
      -H "enable_merge_commit:true" \
      -H "merge_commit_interval_ms:5000" \
      -H "merge_commit_parallel:2" \
      -T example1.csv -XPUT \
      <table_url_prefix>/api/mydatabase/table1/_stream_load
  ```

- 次のコマンドを実行して、非同期モードでMerge Commitを有効にしたStream Loadジョブを開始し、マージウィンドウを `60000` ミリ秒、並列度を `2` に設定します：

  ```Bash
  curl --location-trusted -u <username>:<password> \
      -H "Expect:100-continue" \
      -H "column_separator:," \
      -H "columns: id, name, score" \
      -H "enable_merge_commit:true" \
      -H "merge_commit_async:true" \
      -H "merge_commit_interval_ms:60000" \
      -H "merge_commit_parallel:2" \
      -T example1.csv -XPUT \
      <table_url_prefix>/api/mydatabase/table1/_stream_load
  ```
