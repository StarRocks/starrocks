---
displayed_sidebar: docs
toc_max_heading_level: 4
---
import Tip from '../../../_assets/commonMarkdown/quickstart-shared-nothing-tip.mdx';

# STREAM LOAD

<Tip />

## 説明

StarRocks は、HTTP ベースの Stream Load というロード方法を提供しており、ローカルファイルシステムやストリーミングデータソースからデータをロードするのに役立ちます。ロードジョブを送信すると、StarRocks は同期的にジョブを実行し、ジョブが終了した後にその結果を返します。ジョブ結果に基づいて、ジョブが成功したかどうかを判断できます。Stream Load の適用シナリオ、制限、原則、およびサポートされているデータファイル形式については、[Loading from a local file system via Stream Load](../../../loading/StreamLoad.md#loading-from-a-local-file-system-via-stream-load) を参照してください。

バージョン v3.2.7 以降、Stream Load は転送中に JSON データを圧縮することをサポートし、ネットワーク帯域幅のオーバーヘッドを削減します。ユーザーは `compression` および `Content-Encoding` パラメータを使用して異なる圧縮アルゴリズムを指定できます。サポートされている圧縮アルゴリズムには、GZIP、BZIP2、LZ4_FRAME、および ZSTD が含まれます。詳細については、[data_desc](#data_desc) を参照してください。

バージョン v3.4.0 から、システムは複数の Stream Load リクエストをマージすることをサポートしています。詳細については、[Merge Commit parameters](#merge-commit-parameters) を参照してください。

> **注意**
>
> - Stream Load を使用して StarRocks テーブルにデータをロードした後、そのテーブルに作成されたマテリアライズドビューのデータも更新されます。
> - StarRocks テーブルにデータをロードするには、これらの StarRocks テーブルに対する INSERT 権限を持つユーザーとしてのみ可能です。INSERT 権限を持っていない場合は、[GRANT](../account-management/GRANT.md) に記載された手順に従って、StarRocks クラスターに接続するために使用するユーザーに INSERT 権限を付与してください。

## 構文

```Bash
curl --location-trusted -u <username>:<password> -XPUT <url>
(
    data_desc
)
[opt_properties]        
```

このトピックでは、curl を例として使用して、Stream Load を使用してデータをロードする方法を説明します。curl に加えて、他の HTTP 互換ツールや言語を使用して Stream Load を実行することもできます。ロード関連のパラメータは HTTP リクエストヘッダーフィールドに含まれています。これらのパラメータを入力する際には、次の点に注意してください。

- このトピックで示されているように、チャンク転送エンコーディングを使用できます。チャンク転送エンコーディングを選択しない場合は、`Content-Length` ヘッダーフィールドを入力して転送するコンテンツの長さを示し、データの整合性を確保する必要があります。

  > **注意**
  >
  > curl を使用して Stream Load を実行する場合、StarRocks は自動的に `Content-Length` ヘッダーフィールドを追加するため、手動で入力する必要はありません。

- `Expect` ヘッダーフィールドを追加し、その値を `100-continue` と指定する必要があります。これは、ジョブリクエストが拒否された場合に不要なデータ転送を防ぎ、リソースのオーバーヘッドを削減するのに役立ちます。

StarRocks では、いくつかのリテラルが SQL 言語によって予約キーワードとして使用されます。これらのキーワードを SQL ステートメントで直接使用しないでください。SQL ステートメントでそのようなキーワードを使用する場合は、バッククォート (`) で囲んでください。[Keywords](../keywords.md) を参照してください。

## パラメータ

### username と password

StarRocks クラスターに接続するために使用するアカウントのユーザー名とパスワードを指定します。これは必須のパラメータです。パスワードが設定されていないアカウントを使用する場合は、`<username>:` のみを入力する必要があります。

### XPUT

HTTP リクエストメソッドを指定します。これは必須のパラメータです。Stream Load は PUT メソッドのみをサポートします。

### url

StarRocks テーブルの URL を指定します。構文:

```Plain
http://<fe_host>:<fe_http_port>/api/<database_name>/<table_name>/_stream_load
```

次の表は、URL 内のパラメータを説明しています。

| パラメータ     | 必須 | 説明                                                  |
| ------------- | ---- | ----------------------------------------------------- |
| fe_host       | はい | StarRocks クラスター内の FE ノードの IP アドレス。<br/>**注意**<br/>特定の BE または CN ノードにロードジョブを送信する場合は、BE または CN ノードの IP アドレスを入力する必要があります。 |
| fe_http_port  | はい | StarRocks クラスター内の FE ノードの HTTP ポート番号。デフォルトのポート番号は `8030` です。<br/>**注意**<br/>特定の BE または CN ノードにロードジョブを送信する場合は、BE または CN ノードの HTTP ポート番号を入力する必要があります。デフォルトのポート番号は `8030` です。 |
| database_name | はい | StarRocks テーブルが所属するデータベースの名前。         |
| table_name    | はい | StarRocks テーブルの名前。                             |

> **注意**
>
> [SHOW FRONTENDS](../cluster-management/nodes_processes/SHOW_FRONTENDS.md) を使用して、FE ノードの IP アドレスと HTTP ポートを表示できます。

### data_desc

ロードしたいデータファイルを説明します。`data_desc` ディスクリプタには、データファイルの名前、形式、列セパレータ、行セパレータ、宛先パーティション、および StarRocks テーブルに対する列マッピングを含めることができます。構文:

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
-H "ignore_json_size: true | false"
-H "compression: <compression_algorithm> | Content-Encoding: <compression_algorithm>"
```

`data_desc` ディスクリプタのパラメータは、共通パラメータ、CSV パラメータ、および JSON パラメータの 3 種類に分けられます。

#### 共通パラメータ

| パラメータ  | 必須 | 説明                                                  |
| ---------- | ---- | ----------------------------------------------------- |
| file_path  | はい | データファイルの保存パス。ファイル名の拡張子を含めることができます。 |
| format     | いいえ | データファイルの形式。有効な値: `CSV` および `JSON`。デフォルト値: `CSV`。 |
| partitions | いいえ | データファイルをロードしたいパーティション。デフォルトでは、このパラメータを指定しない場合、StarRocks はデータファイルを StarRocks テーブルのすべてのパーティションにロードします。 |
| temporary_partitions| いいえ | データファイルをロードしたい [temporary partition](../../../table_design/data_distribution/Temporary_partition.md) の名前。複数の一時パーティションを指定する場合は、カンマ (,) で区切る必要があります。|
| columns    | いいえ | データファイルと StarRocks テーブル間の列マッピング。<br/>データファイルのフィールドが StarRocks テーブルの列に順番にマッピングできる場合、このパラメータを指定する必要はありません。代わりに、このパラメータを使用してデータ変換を実装できます。たとえば、CSV データファイルをロードし、そのファイルが StarRocks テーブルの `id` および `city` 列に順番にマッピングできる 2 つの列で構成されている場合、`"columns: city,tmp_id, id = tmp_id * 100"` と指定できます。詳細については、このトピックの「[Column mapping](#column-mapping)」セクションを参照してください。 |

#### CSV パラメータ

| パラメータ        | 必須 | 説明                                                  |
| ---------------- | ---- | ----------------------------------------------------- |
| column_separator | いいえ | データファイルでフィールドを区切るために使用される文字。指定しない場合、このパラメータはデフォルトで `\t`（タブ）になります。<br/>このパラメータで指定する列セパレータがデータファイルで使用されている列セパレータと同じであることを確認してください。<br/>**注意**<br/>CSV データの場合、カンマ (,) やタブ、パイプ (\|) などの UTF-8 文字列をテキストデリミタとして使用できますが、その長さは 50 バイトを超えてはなりません。 |
| row_delimiter    | いいえ | データファイルで行を区切るために使用される文字。指定しない場合、このパラメータはデフォルトで `\n` になります。 |
| skip_header      | いいえ | データファイルが CSV 形式の場合、データファイルの最初の数行をスキップするかどうかを指定します。タイプ: INTEGER。デフォルト値: `0`。<br />一部の CSV 形式のデータファイルでは、最初の数行が列名や列データ型などのメタデータを定義するために使用されます。`skip_header` パラメータを設定することで、StarRocks がデータロード中にデータファイルの最初の数行をスキップするようにできます。たとえば、このパラメータを `1` に設定すると、StarRocks はデータロード中にデータファイルの最初の行をスキップします。<br />データファイルの最初の数行は、ロードコマンドで指定した行セパレータで区切られている必要があります。 |
| trim_space       | いいえ | データファイルが CSV 形式の場合、データファイルから列セパレータの前後のスペースを削除するかどうかを指定します。タイプ: BOOLEAN。デフォルト値: `false`。<br />一部のデータベースでは、データを CSV 形式のデータファイルとしてエクスポートする際に、列セパレータにスペースが追加されます。これらのスペースは、その位置に応じて先行スペースまたは後続スペースと呼ばれます。`trim_space` パラメータを設定することで、StarRocks がデータロード中にこれらの不要なスペースを削除するようにできます。<br />StarRocks は、`enclose` で指定された文字で囲まれたフィールド内のスペース（先行スペースおよび後続スペースを含む）を削除しないことに注意してください。たとえば、次のフィールド値は、列セパレータとしてパイプ (<code class="language-text">&#124;</code>) を使用し、`enclose` で指定された文字として二重引用符 (`"`) を使用しています:<br /><code class="language-text">&#124;"Love StarRocks"&#124;</code> <br /><code class="language-text">&#124;" Love StarRocks "&#124;</code> <br /><code class="language-text">&#124; "Love StarRocks" &#124;</code> <br />`trim_space` を `true` に設定すると、StarRocks は前述のフィールド値を次のように処理します:<br /><code class="language-text">&#124;"Love StarRocks"&#124;</code> <br /><code class="language-text">&#124;" Love StarRocks "&#124;</code> <br /><code class="language-text">&#124;"Love StarRocks"&#124;</code> |
| enclose          | いいえ | データファイルが CSV 形式の場合、[RFC4180](https://www.rfc-editor.org/rfc/rfc4180) に従ってフィールド値を囲むために使用される文字を指定します。タイプ: 単一バイト文字。デフォルト値: `NONE`。最も一般的な文字は、単一引用符 (`'`) および二重引用符 (`"`) です。<br />`enclose` で指定された文字で囲まれたすべての特殊文字（行セパレータや列セパレータを含む）は通常の記号と見なされます。StarRocks は、`enclose` で指定された文字として任意の単一バイト文字を指定できるため、RFC4180 よりも多くのことができます。<br />フィールド値に `enclose` で指定された文字が含まれている場合、同じ文字を使用してその `enclose` で指定された文字をエスケープできます。たとえば、`enclose` を `"` に設定し、フィールド値が `a "quoted" c` の場合、このフィールド値をデータファイルに `"a ""quoted"" c"` として入力できます。 |
| escape           | いいえ | 行セパレータ、列セパレータ、エスケープ文字、および `enclose` で指定された文字などのさまざまな特殊文字をエスケープするために使用される文字を指定します。これらは StarRocks によって一般的な文字と見なされ、それらが存在するフィールド値の一部として解析されます。タイプ: 単一バイト文字。デフォルト値: `NONE`。最も一般的な文字はスラッシュ (`\`) であり、SQL ステートメントではダブルスラッシュ (`\\`) として記述する必要があります。<br />**注意**<br />`escape` で指定された文字は、各ペアの `enclose` で指定された文字の内側と外側の両方に適用されます。<br />次の 2 つの例を示します:<ul><li>`enclose` を `"` に設定し、`escape` を `\` に設定すると、StarRocks は `"say \"Hello world\""` を `say "Hello world"` に解析します。</li><li>列セパレータがカンマ (`,`) の場合、`escape` を `\` に設定すると、StarRocks は `a, b\, c` を 2 つの別々のフィールド値 `a` と `b, c` に解析します。</li></ul> |

> **注意**
>
> - CSV データの場合、カンマ (,) やタブ、パイプ (|) などの UTF-8 文字列をテキストデリミタとして使用できますが、その長さは 50 バイトを超えてはなりません。
> - Null 値は `\N` を使用して示されます。たとえば、データファイルが 3 列で構成され、データファイルのレコードが最初と 3 番目の列にデータを持ち、2 番目の列にデータがない場合、この状況では 2 番目の列に null 値を示すために `\N` を使用する必要があります。つまり、レコードは `a,\N,b` としてコンパイルされる必要があり、`a,,b` ではありません。`a,,b` は、レコードの 2 番目の列が空の文字列を持っていることを示します。
> - `skip_header`、`trim_space`、`enclose`、および `escape` などの形式オプションは、v3.0 以降でサポートされています。

#### JSON パラメータ

| パラメータ         | 必須 | 説明                                                  |
| ----------------- | ---- | ----------------------------------------------------- |
| jsonpaths         | いいえ | JSON データファイルからロードしたいキーの名前。マッチモードを使用して JSON データをロードする場合にのみ、このパラメータを指定する必要があります。このパラメータの値は JSON 形式です。[Configure column mapping for JSON data loading](#configure-column-mapping-for-json-data-loading) を参照してください。           |
| strip_outer_array | いいえ | 最外部の配列構造を削除するかどうかを指定します。有効な値: `true` および `false`。デフォルト値: `false`。<br/>実際のビジネスシナリオでは、JSON データには `[]` で示される最外部の配列構造がある場合があります。この場合、このパラメータを `true` に設定することをお勧めします。これにより、StarRocks は最外部の `[]` を削除し、各内部配列を別々のデータレコードとしてロードします。このパラメータを `false` に設定すると、StarRocks は JSON データファイル全体を 1 つの配列として解析し、その配列を単一のデータレコードとしてロードします。<br/>たとえば、JSON データが `[ {"category" : 1, "author" : 2}, {"category" : 3, "author" : 4} ]` の場合、このパラメータを `true` に設定すると、`{"category" : 1, "author" : 2}` と `{"category" : 3, "author" : 4}` が別々のデータレコードとして解析され、StarRocks テーブルの別々の行にロードされます。 |
| json_root         | いいえ | JSON データファイルからロードしたい JSON データのルート要素。マッチモードを使用して JSON データをロードする場合にのみ、このパラメータを指定する必要があります。このパラメータの値は有効な JsonPath 文字列です。デフォルトでは、このパラメータの値は空であり、JSON データファイルのすべてのデータがロードされることを示します。詳細については、このトピックの「[Load JSON data using matched mode with root element specified](#load-json-data-using-matched-mode-with-root-element-specified)」セクションを参照してください。 |
| ignore_json_size  | いいえ | HTTP リクエスト内の JSON 本体のサイズをチェックするかどうかを指定します。<br/>**注意**<br/>デフォルトでは、HTTP リクエスト内の JSON 本体のサイズは 100 MB を超えることはできません。JSON 本体が 100 MB を超える場合、エラー "The size of this batch exceed the max size [104857600] of json type data data [8617627793]. Set ignore_json_size to skip check, although it may lead huge memory consuming." が報告されます。このエラーを防ぐために、HTTP リクエストヘッダーに `"ignore_json_size:true"` を追加して、StarRocks に JSON 本体のサイズをチェックしないように指示できます。 |
| compression, Content-Encoding | いいえ | データ転送中に適用されるエンコーディングアルゴリズム。サポートされているアルゴリズムには、GZIP、BZIP2、LZ4_FRAME、および ZSTD が含まれます。例: `curl --location-trusted -u root:  -v 'http://127.0.0.1:18030/api/db0/tbl_simple/_stream_load' \-X PUT  -H "expect:100-continue" \-H 'format: json' -H 'compression: lz4_frame'   -T ./b.json.lz4`。 |

JSON データをロードする際には、各 JSON オブジェクトのサイズが 4 GB を超えないように注意してください。JSON データファイル内の個々の JSON オブジェクトが 4 GB を超える場合、エラー "This parser can't support a document that big." が報告されます。

### Merge Commit パラメータ

指定された時間ウィンドウ内で複数の同時 Stream Load リクエストをマージし、それらを単一のトランザクションにマージするために Merge Commit を有効にします。

| **パラメータ**            | **必須** | **説明**                                              |
| ------------------------ | -------- | ----------------------------------------------------- |
| enable_merge_commit      | いいえ   | ロードリクエストに対して Merge Commit を有効にするかどうか。有効な値: `true` および `false` (デフォルト)。 |
| merge_commit_async       | いいえ   | サーバーの返却モード。有効な値:<ul><li>`true`: 非同期モードを有効にし、サーバーはデータを受信した後すぐに返却します。このモードではロードの成功を保証しません。</li><li>`false`(デフォルト): 同期モードを有効にし、サーバーはマージされたトランザクションがコミットされた後にのみ返却し、ロードの成功と可視性を保証します。</li></ul> |
| merge_commit_interval_ms | はい     | マージ時間ウィンドウのサイズ。単位: ミリ秒。Merge Commit は、このウィンドウ内で受信したロードリクエストを単一のトランザクションにマージしようとします。ウィンドウが大きいほどマージ効率が向上しますが、レイテンシーが増加します。 |
| merge_commit_parallel    | はい     | 各マージウィンドウに対して作成されるロードプランの並行性の度合い。並行性は、取り込みの負荷に基づいて調整できます。リクエストが多い場合やロードするデータが多い場合は、この値を増やしてください。並行性は BE ノードの数に制限され、`max(merge_commit_parallel, number of BE nodes)` として計算されます。 |

:::note

- Merge Commit は、単一のデータベースとテーブルに対して **同質の** ロードリクエストをマージすることのみをサポートします。「同質」とは、Stream Load パラメータが共通パラメータ、JSON 形式パラメータ、CSV 形式パラメータ、`opt_properties`、および Merge Commit パラメータを含めて同一であることを意味します。
- CSV 形式のデータをロードする場合、各行が行セパレータで終わることを確認する必要があります。`skip_header` はサポートされていません。
- サーバーはトランザクションのラベルを自動的に生成します。指定された場合は無視されます。
- Merge Commit は、複数のロードリクエストを単一のトランザクションにマージします。1 つのリクエストにデータ品質の問題がある場合、そのトランザクション内のすべてのリクエストが失敗します。

:::

### opt_properties

ロードジョブ全体に適用されるいくつかのオプションパラメータを指定します。構文:

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

次の表は、オプションパラメータを説明しています。

| パラメータ        | 必須 | 説明                                                  |
| ---------------- | ---- | ----------------------------------------------------- |
| label            | いいえ | ロードジョブのラベル。このパラメータを指定しない場合、StarRocks はロードジョブのラベルを自動的に生成します。<br/>StarRocks は、1 つのラベルを使用してデータバッチを複数回ロードすることを許可しません。このため、StarRocks は同じデータが繰り返しロードされるのを防ぎます。ラベルの命名規則については、[System limits](../../System_limit.md) を参照してください。<br/>デフォルトでは、StarRocks は、最近 3 日間に正常に完了したロードジョブのラベルを保持します。[FE パラメータ](../../../administration/management/FE_configuration.md) `label_keep_max_second` を使用して、ラベルの保持期間を変更できます。 |
| where            | いいえ | StarRocks が事前処理されたデータをフィルタリングする条件。StarRocks は、WHERE 句で指定されたフィルタ条件を満たす事前処理されたデータのみをロードします。 |
| max_filter_ratio | いいえ | ロードジョブの最大エラー許容度。エラー許容度は、ロードジョブによって要求されたすべてのデータレコードの中で、データ品質が不十分なためにフィルタリングされる可能性のあるデータレコードの最大割合です。有効な値: `0` から `1`。デフォルト値: `0`。<br/>デフォルト値 `0` を保持することをお勧めします。これにより、不適格なデータレコードが検出された場合、ロードジョブが失敗し、データの正確性が保証されます。<br/>不適格なデータレコードを無視したい場合は、このパラメータを `0` より大きい値に設定できます。この方法では、データファイルに不適格なデータレコードが含まれていても、ロードジョブは成功することができます。<br/>**注意**<br/>不適格なデータレコードには、WHERE 句によってフィルタリングされたデータレコードは含まれません。 |
| log_rejected_record_num | いいえ           | ログに記録できる不適格なデータ行の最大数を指定します。このパラメータは v3.1 以降でサポートされています。有効な値: `0`、`-1`、および任意の非ゼロの正の整数。デフォルト値: `0`。<ul><li>値 `0` は、フィルタリングされたデータ行がログに記録されないことを指定します。</li><li>値 `-1` は、フィルタリングされたすべてのデータ行がログに記録されることを指定します。</li><li>非ゼロの正の整数 `n` は、各 BE または CN で最大 `n` のフィルタリングされたデータ行がログに記録されることを指定します。</li></ul> |
| timeout          | いいえ | ロードジョブのタイムアウト期間。有効な値: `1` から `259200`。単位: 秒。デフォルト値: `600`。<br/>**注意**`timeout` パラメータに加えて、[FE パラメータ](../../../administration/management/FE_configuration.md) `stream_load_default_timeout_second` を使用して、StarRocks クラスター内のすべての Stream Load ジョブのタイムアウト期間を一元的に制御できます。`timeout` パラメータを指定した場合、`timeout` パラメータで指定されたタイムアウト期間が優先されます。`timeout` パラメータを指定しない場合、`stream_load_default_timeout_second` パラメータで指定されたタイムアウト期間が優先されます。 |
| strict_mode      | いいえ | [strict mode](../../../loading/load_concept/strict_mode.md) を有効にするかどうかを指定します。有効な値: `true` および `false`。デフォルト値: `false`。値 `true` は strict mode を有効にし、値 `false` は strict mode を無効にします。 |
| timezone         | いいえ | ロードジョブで使用されるタイムゾーン。デフォルト値: `Asia/Shanghai`。このパラメータの値は、strftime、alignment_timestamp、from_unixtime などの関数によって返される結果に影響を与えます。このパラメータで指定されたタイムゾーンは、セッションレベルのタイムゾーンです。詳細については、[Configure a time zone](../../../administration/management/timezone.md) を参照してください。 |
| load_mem_limit   | いいえ | ロードジョブにプロビジョニングできる最大メモリ量。単位: バイト。デフォルトでは、ロードジョブの最大メモリサイズは 2 GB です。このパラメータの値は、各 BE または CN にプロビジョニングできる最大メモリ量を超えることはできません。 |
| partial_update | いいえ | 部分更新を使用するかどうか。有効な値: `TRUE` および `FALSE`。デフォルト値: `FALSE`、この機能を無効にすることを示します。 |
| partial_update_mode | いいえ | 部分更新のモードを指定します。有効な値: `row` および `column`。 <ul><li> 値 `row` (デフォルト) は行モードでの部分更新を意味し、多くの列と小さなバッチでのリアルタイム更新により適しています。</li><li>値 `column` は列モードでの部分更新を意味し、少ない列と多くの行でのバッチ更新により適しています。このようなシナリオでは、列モードを有効にすると更新速度が速くなります。たとえば、100 列のテーブルで、すべての行に対して 10 列（全体の 10%）のみが更新される場合、列モードの更新速度は 10 倍速くなります。</li></ul> |
| merge_condition  | いいえ | 更新が有効になるかどうかを判断するために使用する列の名前を指定します。ソースレコードから宛先レコードへの更新は、指定された列でソースデータレコードが宛先データレコードよりも大きいか等しい値を持つ場合にのみ有効になります。StarRocks は v2.5 以降で条件付き更新をサポートしています。<br/>**注意**<br/>指定する列は主キー列であってはなりません。さらに、主キーテーブルを使用するテーブルのみが条件付き更新をサポートします。 |

## 列マッピング

### CSV データロードのための列マッピングの設定

データファイルの列が StarRocks テーブルの列に順番に 1 対 1 でマッピングできる場合、データファイルと StarRocks テーブル間の列マッピングを設定する必要はありません。

データファイルの列が StarRocks テーブルの列に順番に 1 対 1 でマッピングできない場合、`columns` パラメータを使用してデータファイルと StarRocks テーブル間の列マッピングを設定する必要があります。これには次の 2 つのユースケースが含まれます:

- **同じ数の列だが、列の順序が異なる。また、データファイルからのデータは、StarRocks テーブルの対応する列にロードされる前に関数によって計算される必要はない。**

  `columns` パラメータでは、データファイルの列が配置されている順序と同じ順序で StarRocks テーブルの列名を指定する必要があります。

  たとえば、StarRocks テーブルは、順番に `col1`、`col2`、`col3` の 3 つの列で構成されており、データファイルも 3 つの列で構成されており、StarRocks テーブルの列 `col3`、`col2`、`col1` に順番にマッピングできます。この場合、`"columns: col3, col2, col1"` と指定する必要があります。

- **異なる数の列と異なる列の順序。また、データファイルからのデータは、StarRocks テーブルの対応する列にロードされる前に関数によって計算される必要がある。**

  `columns` パラメータでは、データファイルの列が配置されている順序と同じ順序で StarRocks テーブルの列名を指定し、データを計算するために使用する関数を指定する必要があります。次の 2 つの例を示します:

  - StarRocks テーブルは、順番に `col1`、`col2`、`col3` の 3 つの列で構成されています。データファイルは 4 つの列で構成されており、そのうち最初の 3 つの列は StarRocks テーブルの列 `col1`、`col2`、`col3` に順番にマッピングでき、4 番目の列は StarRocks テーブルのいずれの列にもマッピングできません。この場合、データファイルの 4 番目の列に一時的な名前を指定する必要があり、その一時的な名前は StarRocks テーブルの列名とは異なる必要があります。たとえば、`"columns: col1, col2, col3, temp"` と指定できます。この場合、データファイルの 4 番目の列は一時的に `temp` と名付けられます。
  - StarRocks テーブルは、順番に `year`、`month`、`day` の 3 つの列で構成されています。データファイルは `yyyy-mm-dd hh:mm:ss` 形式の日付と時刻の値を格納する 1 つの列のみで構成されています。この場合、`"columns: col, year = year(col), month=month(col), day=day(col)"` と指定できます。この場合、`col` はデータファイル列の一時的な名前であり、`year = year(col)`、`month=month(col)`、`day=day(col)` の各関数は、データファイル列 `col` からデータを抽出し、対応する StarRocks テーブルの列にデータをロードします。たとえば、`year = year(col)` は、データファイル列 `col` から `yyyy` データを抽出し、StarRocks テーブル列 `year` にデータをロードします。

詳細な例については、[Configure column mapping](#configure-column-mapping) を参照してください。

### JSON データロードのための列マッピングの設定

JSON ドキュメントのキーが StarRocks テーブルの列と同じ名前を持っている場合、シンプルモードを使用して JSON 形式のデータをロードできます。シンプルモードでは、`jsonpaths` パラメータを指定する必要はありません。このモードでは、JSON 形式のデータは `{}` で示されるオブジェクトである必要があります。たとえば、`{"category": 1, "author": 2, "price": "3"}` です。この例では、`category`、`author`、`price` はキー名であり、これらのキーは StarRocks テーブルの列 `category`、`author`、`price` に名前で 1 対 1 でマッピングできます。

JSON ドキュメントのキーが StarRocks テーブルの列と異なる名前を持っている場合、マッチモードを使用して JSON 形式のデータをロードできます。マッチモードでは、`jsonpaths` および `COLUMNS` パラメータを使用して JSON ドキュメントと StarRocks テーブル間の列マッピングを指定する必要があります:

- `jsonpaths` パラメータでは、JSON ドキュメントに配置されている順序で JSON キーを指定します。
- `COLUMNS` パラメータでは、JSON キーと StarRocks テーブルの列間のマッピングを指定します:
  - `COLUMNS` パラメータで指定された列名は、JSON キーに順番に 1 対 1 でマッピングされます。
  - `COLUMNS` パラメータで指定された列名は、名前で StarRocks テーブルの列に 1 対 1 でマッピングされます。

マッチモードを使用して JSON 形式のデータをロードする例については、[Load JSON data using matched mode](#load-json-data-using-matched-mode) を参照してください。

## 戻り値

ロードジョブが終了すると、StarRocks はジョブ結果を JSON 形式で返します。例:

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
| ---------------------- | ----------------------------------------------------- |
| TxnId                  | ロードジョブのトランザクション ID。                          |
| Label                  | ロードジョブのラベル。                                   |
| Status                 | ロードされたデータの最終ステータス。<ul><li>`Success`: データが正常にロードされ、クエリ可能です。</li><li>`Publish Timeout`: ロードジョブは正常に送信されましたが、データはまだクエリできません。データを再ロードする必要はありません。</li><li>`Label Already Exists`: ロードジョブのラベルが他のロードジョブで使用されています。データは正常にロードされたか、ロード中である可能性があります。</li><li>`Fail`: データのロードに失敗しました。ロードジョブを再試行できます。</li></ul> |
| Message                | ロードジョブのステータス。ロードジョブが失敗した場合、詳細な失敗原因が返されます。 |
| NumberTotalRows        | 読み取られたデータレコードの総数。                      |
| NumberLoadedRows       | 正常にロードされたデータレコードの総数。このパラメータは、`Status` が `Success` の場合にのみ有効です。 |
| NumberFilteredRows     | データ品質が不十分なためにフィルタリングされたデータレコードの数。 |
| NumberUnselectedRows   | WHERE 句によってフィルタリングされたデータレコードの数。 |
| LoadBytes              | ロードされたデータの量。単位: バイト。                  |
| LoadTimeMs             | ロードジョブにかかった時間。単位: ミリ秒。             |
| BeginTxnTimeMs         | ロードジョブのトランザクションを実行するのにかかった時間。 |
| StreamLoadPlanTimeMs   | ロードジョブの実行計画を生成するのにかかった時間。     |
| ReadDataTimeMs         | ロードジョブのデータを読み取るのにかかった時間。       |
| WriteDataTimeMs        | ロードジョブのデータを書き込むのにかかった時間。       |
| CommitAndPublishTimeMs | ロードジョブのデータをコミットおよび公開するのにかかった時間。 |

ロードジョブが失敗した場合、StarRocks は `ErrorURL` も返します。例:

```JSON
{"ErrorURL": "http://172.26.195.68:8045/api/_load_error_log?file=error_log_3a4eb8421f0878a6_9a54df29fd9206be"}
```

`ErrorURL` は、フィルタリングされた不適格なデータレコードの詳細を取得できる URL を提供します。ロードジョブを送信する際に設定されるオプションパラメータ `log_rejected_record_num` を使用して、ログに記録できる不適格なデータ行の最大数を指定できます。

`curl "url"` を実行して、フィルタリングされた不適格なデータレコードの詳細を直接表示できます。また、`wget "url"` を実行して、これらのデータレコードの詳細をエクスポートすることもできます:

```Bash
wget http://172.26.195.68:8045/api/_load_error_log?file=error_log_3a4eb8421f0878a6_9a54df29fd9206be
```

エクスポートされたデータレコードの詳細は、`_load_error_log?file=error_log_3a4eb8421f0878a6_9a54df29fd9206be` に似た名前のローカルファイルに保存されます。`cat` コマンドを使用してファイルを表示できます。

その後、ロードジョブの設定を調整し、ロードジョブを再送信できます。

## 例

### CSV データのロード

このセクションでは、さまざまなパラメータ設定と組み合わせを使用して、多様なロード要件を満たす方法を説明します。

#### タイムアウト期間の設定

StarRocks データベース `test_db` には、`table1` という名前のテーブルがあります。このテーブルは、順番に `col1`、`col2`、`col3` の 3 つの列で構成されています。

データファイル `example1.csv` も 3 つの列で構成されており、`table1` の `col1`、`col2`、`col3` に順番にマッピングできます。

`example1.csv` のすべてのデータを最大 100 秒以内に `table1` にロードしたい場合、次のコマンドを実行します:

```Bash
curl --location-trusted -u <username>:<password> -H "label:label1" \
    -H "Expect:100-continue" \
    -H "timeout:100" \
    -H "max_filter_ratio:0.2" \
    -T example1.csv -XPUT \
    http://<fe_host>:<fe_http_port>/api/test_db/table1/_stream_load
```

#### エラー許容度の設定

StarRocks データベース `test_db` には、`table2` という名前のテーブルがあります。このテーブルは、順番に `col1`、`col2`、`col3` の 3 つの列で構成されています。

データファイル `example2.csv` も 3 つの列で構成されており、`table2` の `col1`、`col2`、`col3` に順番にマッピングできます。

`example2.csv` のすべてのデータを最大エラー許容度 `0.2` で `table2` にロードしたい場合、次のコマンドを実行します:

```Bash
curl --location-trusted -u <username>:<password> -H "label:label2" \
    -H "Expect:100-continue" \
    -H "max_filter_ratio:0.2" \
    -T example2.csv -XPUT \
    http://<fe_host>:<fe_http_port>/api/test_db/table2/_stream_load
```

#### 列マッピングの設定

StarRocks データベース `test_db` には、`table3` という名前のテーブルがあります。このテーブルは、順番に `col1`、`col2`、`col3` の 3 つの列で構成されています。

データファイル `example3.csv` も 3 つの列で構成されており、`table3` の `col2`、`col1`、`col3` に順番にマッピングできます。

`example3.csv` のすべてのデータを `table3` にロードしたい場合、次のコマンドを実行します:

```Bash
curl --location-trusted -u <username>:<password>  -H "label:label3" \
    -H "Expect:100-continue" \
    -H "columns: col2, col1, col3" \
    -T example3.csv -XPUT \
    http://<fe_host>:<fe_http_port>/api/test_db/table3/_stream_load
```

> **注意**
>
> 前述の例では、`example3.csv` の列は、`table3` の列が配置されている順序と同じ順序でマッピングできません。そのため、`columns` パラメータを使用して `example3.csv` と `table3` 間の列マッピングを設定する必要があります。

#### フィルタ条件の設定

StarRocks データベース `test_db` には、`table4` という名前のテーブルがあります。このテーブルは、順番に `col1`、`col2`、`col3` の 3 つの列で構成されています。

データファイル `example4.csv` も 3 つの列で構成されており、`table4` の `col1`、`col2`、`col3` に順番にマッピングできます。

`example4.csv` の最初の列の値が `20180601` に等しいデータレコードのみを `table4` にロードしたい場合、次のコマンドを実行します:

```Bash
curl --location-trusted -u <username>:<password> -H "label:label4" \
    -H "Expect:100-continue" \
    -H "columns: col1, col2, col3"\
    -H "where: col1 = 20180601" \
    -T example4.csv -XPUT \
    http://<fe_host>:<fe_http_port>/api/test_db/table4/_stream_load
```

> **注意**
>
> 前述の例では、`example4.csv` と `table4` は順番にマッピングできる同じ数の列を持っていますが、WHERE 句を使用して列ベースのフィルタ条件を指定する必要があります。そのため、`columns` パラメータを使用して `example4.csv` の列に一時的な名前を定義する必要があります。

#### 宛先パーティションの設定

StarRocks データベース `test_db` には、`table5` という名前のテーブルがあります。このテーブルは、順番に `col1`、`col2`、`col3` の 3 つの列で構成されています。

データファイル `example5.csv` も 3 つの列で構成されており、`table5` の `col1`、`col2`、`col3` に順番にマッピングできます。

`example5.csv` のすべてのデータを `table5` のパーティション `p1` および `p2` にロードしたい場合、次のコマンドを実行します:

```Bash
curl --location-trusted -u <username>:<password>  -H "label:label5" \
    -H "Expect:100-continue" \
    -H "partitions: p1, p2" \
    -T example5.csv -XPUT \
    http://<fe_host>:<fe_http_port>/api/test_db/table5/_stream_load
```

#### strict mode とタイムゾーンの設定

StarRocks データベース `test_db` には、`table6` という名前のテーブルがあります。このテーブルは、順番に `col1`、`col2`、`col3` の 3 つの列で構成されています。

データファイル `example6.csv` も 3 つの列で構成されており、`table6` の `col1`、`col2`、`col3` に順番にマッピングできます。

`example6.csv` のすべてのデータを strict mode とタイムゾーン `Africa/Abidjan` を使用して `table6` にロードしたい場合、次のコマンドを実行します:

```Bash
curl --location-trusted -u <username>:<password> \
    -H "Expect:100-continue" \
    -H "strict_mode: true" \
    -H "timezone: Africa/Abidjan" \
    -T example6.csv -XPUT \
    http://<fe_host>:<fe_http_port>/api/test_db/table6/_stream_load
```

#### HLL 型の列を含むテーブルへのデータロード

StarRocks データベース `test_db` には、`table7` という名前のテーブルがあります。このテーブルは、順番に `col1` および `col2` の 2 つの HLL 型の列で構成されています。

データファイル `example7.csv` も 2 つの列で構成されており、そのうち最初の列は `table7` の `col1` にマッピングでき、2 番目の列は `table7` のいずれの列にもマッピングできません。`example7.csv` の最初の列の値は、関数を使用して `table7` の `col1` にロードされる前に HLL 型のデータに変換できます。

`example7.csv` のデータを `table7` にロードしたい場合、次のコマンドを実行します:

```Bash
curl --location-trusted -u <username>:<password> \
    -H "Expect:100-continue" \
    -H "columns: temp1, temp2, col1=hll_hash(temp1), col2=hll_empty()" \
    -T example7.csv -XPUT \
    http://<fe_host>:<fe_http_port>/api/test_db/table7/_stream_load
```

> **注意**
>
> 前述の例では、`example7.csv` の 2 つの列は、`columns` パラメータを使用して順番に `temp1` および `temp2` と名付けられています。その後、関数を使用してデータを変換します:
>
> - `hll_hash` 関数は、`example7.csv` の `temp1` の値を HLL 型データに変換し、`example7.csv` の `temp1` を `table7` の `col1` にマッピングします。
>
> - `hll_empty` 関数は、指定されたデフォルト値を `table7` の `col2` に埋め込みます。

関数 `hll_hash` および `hll_empty` の使用法については、[hll_hash](../../sql-functions/scalar-functions/hll_hash.md) および [hll_empty](../../sql-functions/scalar-functions/hll_empty.md) を参照してください。

#### BITMAP 型の列を含むテーブルへのデータロード

StarRocks データベース `test_db` には、`table8` という名前のテーブルがあります。このテーブルは、順番に `col1` および `col2` の 2 つの BITMAP 型の列で構成されています。

データファイル `example8.csv` も 2 つの列で構成されており、そのうち最初の列は `table8` の `col1` にマッピングでき、2 番目の列は `table8` のいずれの列にもマッピングできません。`example8.csv` の最初の列の値は、関数を使用して `table8` の `col1` にロードされる前に変換できます。

`example8.csv` のデータを `table8` にロードしたい場合、次のコマンドを実行します:

```Bash
curl --location-trusted -u <username>:<password> \
    -H "Expect:100-continue" \
    -H "columns: temp1, temp2, col1=to_bitmap(temp1), col2=bitmap_empty()" \
    -T example8.csv -XPUT \
    http://<fe_host>:<fe_http_port>/api/test_db/table8/_stream_load
```

> **注意**
>
> 前述の例では、`example8.csv` の 2 つの列は、`columns` パラメータを使用して順番に `temp1` および `temp2` と名付けられています。その後、関数を使用してデータを変換します:
>
> - `to_bitmap` 関数は、`example8.csv` の `temp1` の値を BITMAP 型データに変換し、`example8.csv` の `temp1` を `table8` の `col1` にマッピングします。
>
> - `bitmap_empty` 関数は、指定されたデフォルト値を `table8` の `col2` に埋め込みます。

関数 `to_bitmap` および `bitmap_empty` の使用法については、[to_bitmap](../../sql-functions/bitmap-functions/to_bitmap.md) および [bitmap_empty](../../sql-functions/bitmap-functions/bitmap_empty.md) を参照してください。

#### `skip_header`、`trim_space`、`enclose`、および `escape` の設定

StarRocks データベース `test_db` には、`table9` という名前のテーブルがあります。このテーブルは、順番に `col1`、`col2`、`col3` の 3 つの列で構成されています。

データファイル `example9.csv` も 3 つの列で構成されており、`table13` の `col2`、`col1`、`col3` に順番にマッピングされています。

`example9.csv` の最初の 5 行をスキップし、列セパレータの前後のスペースを削除し、`enclose` を `\` に、`escape` を `\` に設定して `table9` にすべてのデータをロードしたい場合、次のコマンドを実行します:

```Bash
curl --location-trusted -u <username>:<password> -H "label:3875" \
    -H "Expect:100-continue" \
    -H "trim_space: true" -H "skip_header: 5" \
    -H "column_separator:," -H "enclose:\"" -H "escape:\\" \
    -H "columns: col2, col1, col3" \
    -T example9.csv -XPUT \
    http://<fe_host>:<fe_http_port>/api/test_db/tbl9/_stream_load
```

### JSON データのロード

このセクションでは、JSON データをロードする際に注意すべきパラメータ設定を説明します。

StarRocks データベース `test_db` には、次のスキーマを持つ `tbl1` という名前のテーブルがあります:

```SQL
`category` varchar(512) NULL COMMENT "",`author` varchar(512) NULL COMMENT "",`title` varchar(512) NULL COMMENT "",`price` double NULL COMMENT ""
```

#### シンプルモードを使用した JSON データのロード

データファイル `example1.json` が次のデータで構成されているとします:

```JSON
{"category":"C++","author":"avc","title":"C++ primer","price":895}
```

`example1.json` のすべてのデータを `tbl1` にロードするには、次のコマンドを実行します:

```Bash
curl --location-trusted -u <username>:<password> -H "label:label6" \
    -H "Expect:100-continue" \
    -H "format: json" \
    -T example1.json -XPUT \
    http://<fe_host>:<fe_http_port>/api/test_db/tbl1/_stream_load
```

> **注意**
>
> 前述の例では、`columns` および `jsonpaths` パラメータは指定されていません。そのため、`example1.json` のキーは名前で `tbl1` の列にマッピングされます。

スループットを向上させるために、Stream Load は複数のデータレコードを一度にロードすることをサポートしています。例:

```JSON
[{"category":"C++","author":"avc","title":"C++ primer","price":89.5},{"category":"Java","author":"avc","title":"Effective Java","price":95},{"category":"Linux","author":"avc","title":"Linux kernel","price":195}]
```

#### マッチモードを使用した JSON データのロード

StarRocks は、JSON データをマッチし処理するために次のステップを実行します:

1. (オプション) `strip_outer_array` パラメータ設定に従って最外部の配列構造を削除します。

   > **注意**
   >
   > このステップは、JSON データの最外部のレイヤーが `[]` で示される配列構造である場合にのみ実行されます。`strip_outer_array` を `true` に設定する必要があります。

2. (オプション) `json_root` パラメータ設定に従って JSON データのルート要素をマッチします。

   > **注意**
   >
   > このステップは、JSON データにルート要素がある場合にのみ実行されます。`json_root` パラメータを使用してルート要素を指定する必要があります。

3. `jsonpaths` パラメータ設定に従って指定された JSON データを抽出します。

##### ルート要素を指定せずにマッチモードを使用して JSON データをロードする

データファイル `example2.json` が次のデータで構成されているとします:

```JSON
[{"category":"xuxb111","author":"1avc","title":"SayingsoftheCentury","price":895},{"category":"xuxb222","author":"2avc","title":"SayingsoftheCentury","price":895},{"category":"xuxb333","author":"3avc","title":"SayingsoftheCentury","price":895}]
```

`example2.json` から `category`、`author`、`price` のみをロードするには、次のコマンドを実行します:

```Bash
curl --location-trusted -u <username>:<password> -H "label:label7" \
    -H "Expect:100-continue" \
    -H "format: json" \
    -H "strip_outer_array: true" \
    -H "jsonpaths: [\"$.category\",\"$.price\",\"$.author\"]" \
    -H "columns: category, price, author" \
    -T example2.json -XPUT \
    http://<fe_host>:<fe_http_port>/api/test_db/tbl1/_stream_load
```

> **注意**
>
> 前述の例では、JSON データの最外部のレイヤーは `[]` で示される配列構造です。この配列構造は、各データレコードを表す複数の JSON オブジェクトで構成されています。そのため、最外部の配列構造を削除するために `strip_outer_array` を `true` に設定する必要があります。ロードしたくないキー **title** はロード中に無視されます。

##### ルート要素を指定してマッチモードを使用して JSON データをロードする

データファイル `example3.json` が次のデータで構成されているとします:

```JSON
{"id": 10001,"RECORDS":[{"category":"11","title":"SayingsoftheCentury","price":895,"timestamp":1589191587},{"category":"22","author":"2avc","price":895,"timestamp":1589191487},{"category":"33","author":"3avc","title":"SayingsoftheCentury","timestamp":1589191387}],"comments": ["3 records", "there will be 3 rows"]}
```

`example3.json` から `category`、`author`、`price` のみをロードするには、次のコマンドを実行します:

```Bash
curl --location-trusted -u <username>:<password> \
    -H "Expect:100-continue" \
    -H "format: json" \
    -H "json_root: $.RECORDS" \
    -H "strip_outer_array: true" \
    -H "jsonpaths: [\"$.category\",\"$.price\",\"$.author\"]" \
    -H "columns: category, price, author" -H "label:label8" \
    -T example3.json -XPUT \
    http://<fe_host>:<fe_http_port>/api/test_db/tbl1/_stream_load
```

> **注意**
>
> 前述の例では、JSON データの最外部のレイヤーは `[]` で示される配列構造です。この配列構造は、各データレコードを表す複数の JSON オブジェクトで構成されています。そのため、最外部の配列構造を削除するために `strip_outer_array` を `true` に設定する必要があります。ロードしたくないキー `title` および `timestamp` はロード中に無視されます。さらに、`json_root` パラメータは、JSON データのルート要素である配列を指定するために使用されます。

### Stream Load リクエストのマージ

- 次のコマンドを実行して、同期モードで Merge Commit を有効にした Stream Load ジョブを開始し、マージウィンドウを `5000` ミリ秒、並行度を `2` に設定します:

  ```Bash
  curl --location-trusted -u <username>:<password> \
      -H "Expect:100-continue" \
      -H "column_separator:," \
      -H "columns: id, name, score" \
      -H "enable_merge_commit:true" \
      -H "merge_commit_interval_ms:5000" \
      -H "merge_commit_parallel:2" \
      -T example1.csv -XPUT \
      http://<fe_host>:<fe_http_port>/api/mydatabase/table1/_stream_load
  ```

- 次のコマンドを実行して、非同期モードで Merge Commit を有効にした Stream Load ジョブを開始し、マージウィンドウを `60000` ミリ秒、並行度を `2` に設定します:

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
      http://<fe_host>:<fe_http_port>/api/mydatabase/table1/_stream_load
  ```