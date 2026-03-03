---
displayed_sidebar: docs
toc_max_heading_level: 4
---

import Tip from '../../../_assets/commonMarkdown/quickstart-shared-nothing-tip.mdx';
import TableURL from '../../../_assets/commonMarkdown/stream_load_table_url.mdx';
import TableURLTip from '../../../_assets/commonMarkdown/stream_load_table_url_tip.mdx';

# STREAM LOAD

STREAM LOADを使用すると、ローカルファイルシステムまたはストリーミングデータソースからデータをロードできます。ロードジョブを送信すると、システムはジョブを同期的に実行し、ジョブの完了後にジョブの結果を返します。ジョブの結果に基づいて、ジョブが成功したかどうかを判断できます。Stream Loadのアプリケーションシナリオ、制限、およびサポートされているデータファイル形式については、以下を参照してください。[Stream Loadを介したローカルファイルシステムからのロード](../../../loading/StreamLoad.md)。

<Tip />

v3.2.7以降、Stream Loadは転送中のJSONデータの圧縮をサポートし、ネットワーク帯域幅のオーバーヘッドを削減します。ユーザーは、パラメーターを使用して異なる圧縮アルゴリズムを指定できます。`compression`および`Content-Encoding`。サポートされている圧縮アルゴリズムには、GZIP、BZIP2、LZ4_FRAME、ZSTDが含まれます。詳細については、以下を参照してください。[data_desc](#data_desc)。

v3.4.0以降、システムは複数のStream Loadリクエストのマージをサポートします。詳細については、以下を参照してください。[マージコミットパラメーター](#merge-commit-parameters)。

:::note

- Stream Loadを使用してネイティブテーブルにデータをロードすると、そのテーブル上に作成されたマテリアライズドビューのデータも更新されます。
- ネイティブテーブルにデータをロードできるのは、それらのテーブルに対するINSERT権限を持つユーザーのみです。INSERT権限がない場合は、以下の指示に従って、[GRANT](../account-management/GRANT.md)を使用して、クラスターへの接続に使用するユーザーにINSERT権限を付与してください。
:::

## 構文

```Bash
curl --location-trusted -u <username>:<password> -XPUT <url>
(
    data_desc
)
[opt_properties]        
```

このトピックでは、`curl`を例として、Stream Loadを使用してデータをロードする方法を説明します。`curl`の他に、他のHTTP互換ツールや言語を使用してStream Loadを実行することもできます。ロード関連のパラメーターはHTTPリクエストヘッダーフィールドに含まれます。これらのパラメーターを入力する際は、以下の点に注意してください。

- このトピックで示されているように、チャンク転送エンコーディングを使用できます。チャンク転送エンコーディングを選択しない場合は、`Content-Length`ヘッダーフィールドを入力して、転送されるコンテンツの長さを指定し、データの整合性を確保する必要があります。

  :::note
`curl`を使用してStream Loadを実行すると、システムは自動的に「`Content-Length`」ヘッダーフィールドを追加するため、手動で入力する必要はありません。
:::

- 「`Expect`」ヘッダーフィールドを追加し、その値を「`100-continue`」と指定する必要があります。例: 「`"Expect:100-continue"`」。これは、ジョブ要求が拒否された場合に、不要なデータ転送を防ぎ、リソースのオーバーヘッドを削減するのに役立ちます。

StarRocksでは、一部のリテラルがSQL言語の予約済みキーワードとして使用されることに注意してください。これらのキーワードをSQLステートメントで直接使用しないでください。SQLステートメントでそのようなキーワードを使用したい場合は、バッククォート (`) で囲んでください。詳細については、[キーワード](../keywords.md)。

## パラメーター

### ユーザー名とパスワード

クラスターへの接続に使用するアカウントのユーザー名とパスワードを指定します。これは必須パラメーターです。パスワードが設定されていないアカウントを使用する場合は、`<username>:`のみを入力する必要があります。

### XPUT

HTTPリクエストメソッドを指定します。これは必須パラメーターです。Stream LoadはPUTメソッドのみをサポートします。

### URL

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
-H "ignore_json_size: true | false"
-H "compression: <compression_algorithm> | Content-Encoding: <compression_algorithm>"
```

のパラメータは、`data_desc`ディスクリプタは、共通パラメータ、CSVパラメータ、およびJSONパラメータの3つのタイプに分類できます。

#### 共通パラメータ

| パラメータ | 必須 | 説明 |
| ---------- | -------- | ------------------------------------------------------------ |
| file_path | はい | データファイルの保存パス。ファイル名の拡張子をオプションで含めることができます。 |
| format | いいえ | データファイルの形式。有効な値：`CSV`および`JSON`。デフォルト値：`CSV`。 |[一時パーティション](../../../table_design/data_distribution/Temporary_partition.md)にデータファイルをロードします。複数の一時パーティションを指定でき、それらはコンマ (,) で区切る必要があります。|<br />データファイルのフィールドをテーブルの列に順番にマッピングできる場合、このパラメータを指定する必要はありません。代わりに、このパラメータを使用してデータ変換を実装できます。たとえば、CSVデータファイルをロードし、そのファイルがテーブルの2つの列に順番にマッピングできる2つの列で構成されている場合、`id`および`city`の場合、`"columns: city,tmp_id, id = tmp_id * 100"`を指定できます。詳細については、このトピックの「[列マッピング](#column-mapping)」セクションを参照してください。 |

#### CSVパラメータ

| パラメータ | 必須 | 説明 |
| ---------------- | -------- | ------------------------------------------------------------ |
| column_separator | いいえ | データファイルでフィールドを区切るために使用される文字。このパラメータを指定しない場合、デフォルトは`\t`で、タブを示します。<br />このパラメータを使用して指定する列区切り文字が、データファイルで使用されている列区切り文字と同じであることを確認してください。<br />**注**<br />- CSVデータの場合、コンマ (,)、タブ、パイプ (|) などのUTF-8文字列をテキスト区切り文字として使用できます。その長さは50バイトを超えてはなりません。<br />- データファイルが連続する非表示文字 (例:`\r\n`) を列区切り文字として使用する場合、このパラメータを`\\x0D0A`に設定する必要があります。 |`\n`。<br />**注**<br />データファイルが連続する非表示文字 (例:`\r\n`) を行区切り文字として使用する場合、このパラメータを`\\x0D0A`に設定する必要があります。 |`0`。 |<br />一部のCSV形式のデータファイルでは、先頭の数行が列名や列データ型などのメタデータを定義するために使用されます。 `skip_header`パラメータを設定することで、データロード中にデータファイルの最初の数行をスキップするようにシステムを有効にできます。たとえば、このパラメータを`1`に設定すると、システムはデータロード中にデータファイルの最初の行をスキップします。<br />データファイルの先頭の数行は、ロードコマンドで指定する行区切り文字を使用して区切る必要があります。 |`false`。 |<br />一部のデータベースでは、データをCSV形式のデータファイルとしてエクスポートする際に、列区切り文字にスペースが追加されます。このようなスペースは、その位置に応じて先行スペースまたは後続スペースと呼ばれます。 `trim_space`パラメータを使用すると、データロード時にシステムがそのような不要なスペースを削除できるようになります。<br />システムは、一対の「」で囲まれたフィールド内のスペース（先頭のスペースと末尾のスペースを含む）を削除しないことに注意してください。`enclose`で指定された文字。たとえば、次のフィールド値はパイプ（<code class="language-text">|</code>）を列区切り文字として、二重引用符（`"`）`enclose`で指定された文字とします。<br /><code class="language-text">|"Love StarRocks"|</code> <br /><code class="language-text">|" Love StarRocks "|</code> <br /><code class="language-text">| "Love StarRocks" |</code> <br />「」を設定すると、`trim_space`に、`true`システムは上記のフィールド値を次のように処理します。<br /><code class="language-text">|"Love StarRocks"|</code> <br /><code class="language-text">|" Love StarRocks "|</code> <br /><code class="language-text">|"Love StarRocks"|</code> |
| enclose          | いいえ       | データファイルがCSV形式の場合に、「」に従ってデータファイル内のフィールド値を囲むために使用される文字を指定します。[RFC4180](https://www.rfc-editor.org/rfc/rfc4180)に従ってデータファイル内のフィールド値を囲むために使用される文字を指定します。タイプ：シングルバイト文字。デフォルト値：「」`NONE`。最も一般的な文字は、単一引用符（`'`）`"`と二重引用符（<br />「」で指定された文字を使用して囲まれたすべての特殊文字（行区切り文字と列区切り文字を含む）は、通常の記号と見なされます。`enclose`で指定された文字は通常の記号と見なされます。システムはRFC4180以上のことができ、任意のシングルバイト文字を「」として指定できます。`enclose`で指定された文字として指定できます。<br />フィールド値に「」が含まれている場合、`enclose`その「」で指定された文字をエスケープするために同じ文字を使用できます。`enclose`たとえば、「」を「」に設定し、`enclose`に設定し、`"`フィールド値が「」であるとします。`a "quoted" c`この場合、データファイルにフィールド値を「」として入力できます。`"a ""quoted"" c"`としてデータファイルに入力できます。 |
| escape           | いいえ       | 行区切り文字、列区切り文字、エスケープ文字、および「」で指定された文字など、さまざまな特殊文字をエスケープするために使用される文字を指定します。`enclose`で指定された文字など、さまざまな特殊文字をエスケープするために使用される文字を指定します。これらの文字は、共通の文字と見なされ、それらが存在するフィールド値の一部として解析されます。タイプ：シングルバイト文字。デフォルト値：「」`NONE`。最も一般的な文字はスラッシュ（`\`）です。これはSQLステートメントでは二重スラッシュ（`\\`）として記述する必要があります。<br />**注**<br />「」で指定された文字は、`escape`各「」で指定された文字のペアの内側と外側の両方に適用されます。`enclose`で指定された文字のペアの内側と外側の両方に適用されます。<br />2つの例を次に示します。<ul><li>をに設定し、`enclose`をに設定すると、`"`と`escape`をに設定すると、`\`、システムは`"say \"Hello world\""`を`say "Hello world"`に解析します。</li><li>列区切り文字がコンマ (`,`) であると仮定します。をに設定すると、`escape`をに設定すると、`\`、システムは`a, b\, c`を2つの別々のフィールド値 (`a`と`b, c`) に解析します。</li></ul> |

:::note

- CSVデータの場合、カンマ (,)、タブ、パイプ (|) などのUTF-8文字列をテキスト区切り文字として使用できます。その長さは50バイトを超えてはなりません。
- Null値は`\N`を使用して示されます。たとえば、データファイルが3つの列で構成され、そのデータファイルからのレコードが最初の列と3番目の列にデータを保持しているが、2番目の列にはデータがないとします。この状況では、2番目の列でnull値を示すために`\N`を使用する必要があります。これは、レコードが`a,\N,b`の代わりに`a,,b`としてコンパイルされなければならないことを意味します。`a,,b`は、レコードの2番目の列が空の文字列を保持していることを示します。
- を含むフォーマットオプションは、`skip_header`、`trim_space`、`enclose`、および`escape`は、v3.0以降でサポートされています。
:::

#### JSONパラメーター

| パラメーター | 必須 | 説明 |
| ----------------- | -------- | ------------------------------------------------------------ |
| jsonpaths | いいえ | JSONデータファイルからロードするキーの名前。このパラメーターは、一致モードを使用してJSONデータをロードする場合にのみ指定する必要があります。このパラメーターの値はJSON形式です。[JSONデータロードの列マッピングを構成する](#configure-column-mapping-for-json-data-loading)を参照してください。 |`true`と`false`。デフォルト値:`false`。<br />実際のビジネスシナリオでは、JSONデータは角括弧のペア (`[]`) で示されるような最外配列構造を持つ場合があります。この状況では、このパラメーターを`true`に設定することをお勧めします。そうすることで、システムは最外の角括弧 (`[]`) を削除し、各内部配列を個別のデータレコードとしてロードします。このパラメーターをに設定すると、`false`、システムはJSONデータファイル全体を1つの配列に解析し、その配列を単一のデータレコードとしてロードします。<br />例えば、JSONデータが`[ {"category" : 1, "author" : 2}, {"category" : 3, "author" : 4} ]`です。このパラメーターを`true`に設定すると、`{"category" : 1, "author" : 2}`と`{"category" : 3, "author" : 4}`は別々のデータレコードとして解析され、別々のテーブル行にロードされます。|
| json_root | いいえ | JSONデータファイルからロードしたいJSONデータのルート要素。このパラメーターは、マッチモードを使用してJSONデータをロードする場合にのみ指定する必要があります。このパラメーターの値は有効なJsonPath文字列です。デフォルトでは、このパラメーターの値は空であり、JSONデータファイルのすべてのデータがロードされることを示します。詳細については、このトピックの「[ルート要素を指定してマッチモードでJSONデータをロードする](#load-json-data-using-matched-mode-with-root-element-specified)」セクションを参照してください。|
| ignore_json_size | いいえ | HTTPリクエストのJSONボディのサイズをチェックするかどうかを指定します。<br />**注**<br />デフォルトでは、HTTPリクエストのJSONボディのサイズは100 MBを超えることはできません。JSONボディのサイズが100 MBを超えると、「このバッチのサイズがjson型データの最大サイズ[104857600]を超えています[8617627793]。チェックをスキップするにはignore_json_sizeを設定してください。ただし、これにより大量のメモリを消費する可能性があります。」というエラーが報告されます。このエラーを防ぐには、HTTPリクエストヘッダーに`"ignore_json_size:true"`を追加して、JSONボディのサイズをチェックしないようにシステムに指示できます。|
| compression, Content-Encoding | いいえ | 転送中にデータに適用されるエンコーディングアルゴリズム。サポートされているアルゴリズムには、GZIP、BZIP2、LZ4_FRAME、ZSTDが含まれます。例：`curl --location-trusted -u root:  -v '<table_url>' \-X PUT  -H "expect:100-continue" \-H 'format: json' -H 'compression: lz4_frame'   -T ./b.json.lz4`。|

JSONデータをロードする際、JSONオブジェクトあたりのサイズが4 GBを超えないことにも注意してください。JSONデータファイル内の個々のJSONオブジェクトのサイズが4 GBを超えると、「このパーサーは、そのサイズのドキュメントをサポートできません。」というエラーが報告されます。

### マージコミットパラメーター

指定された時間枠内で複数の同時Stream Loadリクエストに対してマージコミットを有効にし、それらを単一のトランザクションにマージします。

:::warning

マージコミット最適化は、単一のテーブルで**同時**Stream Loadジョブがあるシナリオに適していることに注意してください。並行度が1の場合は推奨されません。また、`merge_commit_async`を`false`に、`merge_commit_interval_ms`を大きな値に設定する前に慎重に検討してください。これらはロードパフォーマンスの低下を引き起こす可能性があります。

:::

|**パラメーター**|**必須**|**説明**|
| ------------------------ | ------------ | ------------------------------------------------------------ |
| enable_merge_commit | いいえ | ロードリクエストに対してマージコミットを有効にするかどうか。有効な値：`true`と`false`（デフォルト）。|
| merge_commit_async | いいえ | サーバーの戻りモード。有効な値：<ul><li>`true`: 非同期モードを有効にします。サーバーはデータを受信するとすぐに戻ります。このモードでは、ロードが成功することを保証しません。</li><li>`false`（デフォルト）：同期モードを有効にします。サーバーはマージされたトランザクションがコミットされた後にのみ戻り、ロードが成功して可視化されることを保証します。</li></ul>|
| merge_commit_interval_ms | はい | マージ時間枠のサイズ。単位：ミリ秒。マージコミットは、この時間枠内に受信したロードリクエストを単一のトランザクションにマージしようとします。時間枠が大きいほどマージ効率は向上しますが、レイテンシーが増加します。|
| merge_commit_parallel | はい | 各マージ時間枠に対して作成されるロード計画の並行度。並行度は、取り込みの負荷に基づいて調整できます。リクエストが多く、またはロードするデータ量が多い場合は、この値を増やしてください。並行度はBEノードの数に制限され、`min(merge_commit_parallel, number of BE nodes)`として計算されます。|

:::note

- マージコミットは、**同種**のロードリクエストを単一のデータベースとテーブルにマージすることのみをサポートします。「同種」とは、共通パラメーター、JSON形式パラメーター、CSV形式パラメーターなど、Stream Loadパラメーターが同一であることを示します。`opt_properties`、およびマージコミットのパラメーター。
- CSV形式のデータをロードする場合、各行が改行区切り文字で終わっていることを確認する必要があります。`skip_header`はサポートされていません。
- サーバーはトランザクションのラベルを自動的に生成します。指定された場合、それらは無視されます。
- マージコミットは、複数のロードリクエストを単一のトランザクションにマージします。いずれかのリクエストにデータ品質の問題が含まれている場合、トランザクション内のすべてのリクエストは失敗します。

:::

### opt_properties

ロードジョブ全体に適用されるオプションパラメーターを指定します。構文: 

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

次の表は、オプションパラメーターについて説明しています。

| パラメーター | 必須 | 説明 |
| ---------------- | -------- | ------------------------------------------------------------ |
| label | いいえ | ロードジョブのラベル。このパラメーターを指定しない場合、システムはロードジョブのラベルを自動的に生成します。<br />システムは、1つのラベルを使用してデータバッチを複数回ロードすることを許可しません。そのため、システムは同じデータが繰り返しロードされるのを防ぎます。ラベルの命名規則については、以下を参照してください。[システム制限](../../System_limit.md)。<br />デフォルトでは、システムは直近3日間に正常に完了したロードジョブのラベルを保持します。次のパラメーターを使用して、[FEパラメーター](../../../administration/management/FE_configuration.md) `label_keep_max_second`ラベル保持期間を変更できます。 |
| where | いいえ | システムが前処理済みデータをフィルタリングする条件。システムは、WHERE句で指定されたフィルター条件を満たす前処理済みデータのみをロードします。 |
| max_filter_ratio | いいえ | ロードジョブの最大エラー許容度。エラー許容度とは、ロードジョブによって要求されたすべてのデータレコードにおいて、データ品質の不備によりフィルタリングされる可能性のあるデータレコードの最大割合です。有効な値: `0`から`1`。デフォルト値: `0`。<br />デフォルト値の`0`を保持することをお勧めします。これにより、不適格なデータレコードが検出された場合、ロードジョブは失敗し、データの正確性が保証されます。<br />不適格なデータレコードを無視したい場合は、このパラメーターを`0`より大きい値に設定できます。これにより、データファイルに不適格なデータレコードが含まれていても、ロードジョブは成功できます。<br />**注**<br />不適格なデータレコードには、WHERE句によってフィルタリングされたデータレコードは含まれません。 |
| log_rejected_record_num | いいえ | ログに記録できる不適格なデータ行の最大数を指定します。このパラメーターはv3.1以降でサポートされています。有効な値: `0`、`-1`、および任意のゼロ以外の正の整数。デフォルト値: `0`。<ul><li>値`0`は、フィルタリングされたデータ行がログに記録されないことを指定します。</li><li>値`-1`は、フィルタリングされたすべてのデータ行がログに記録されることを指定します。</li><li>ゼロ以外の正の整数（例: `n`）は、各BEまたはCNで最大`n`のフィルタリングされたデータ行がログに記録されることを指定します。</li></ul> |
| timeout | いいえ | ロードジョブのタイムアウト期間。有効な値: `1`から`259200`。単位: 秒。デフォルト値: `600`。<br />**注**に加えて、`timeout`パラメーターに加えて、[FEパラメーター](../../../administration/management/FE_configuration.md) `stream_load_default_timeout_second`を使用して、クラスター内のすべてのStream Loadジョブのタイムアウト期間を一元的に制御できます。もし`timeout`パラメーターを指定した場合、`timeout`パラメーターで指定されたタイムアウト期間が優先されます。もし`timeout`パラメーターを指定しない場合、`stream_load_default_timeout_second`パラメーターで指定されたタイムアウト期間が優先されます。|
| strict_mode      | いいえ       | [厳格モード](../../../loading/load_concept/strict_mode.md)を有効にするかどうかを指定します。有効な値は次のとおりです。`true`と`false`です。デフォルト値は`false`です。値`true`は厳格モードを有効にすることを指定し、値`false`は厳格モードを無効にすることを指定します。|
| timezone         | いいえ       | ロードジョブで使用されるタイムゾーン。デフォルト値: `Asia/Shanghai`。このパラメーターの値は、strftime、alignment_timestamp、from_unixtimeなどの関数によって返される結果に影響します。このパラメーターで指定されたタイムゾーンはセッションレベルのタイムゾーンです。詳細については、[タイムゾーンの構成](../../../administration/management/timezone.md)を参照してください。|
| load_mem_limit   | いいえ       | ロードジョブにプロビジョニングできる最大メモリ量。単位: バイト。デフォルトでは、ロードジョブの最大メモリサイズは2 GBです。このパラメーターの値は、各BEまたはCNにプロビジョニングできる最大メモリ量を超えることはできません。|
| partial_update | いいえ | 部分更新を使用するかどうか。有効な値: `TRUE`と`FALSE`。デフォルト値: `FALSE`。この機能を無効にすることを示します。|
| partial_update_mode | いいえ | 部分更新のモードを指定します。有効な値: `row`と`column`。<ul><li>値`row`（デフォルト）は行モードでの部分更新を意味し、多数の列と少量のバッチでのリアルタイム更新に適しています。</li><li>値`column`は列モードでの部分更新を意味し、少数の列と多数の行でのバッチ更新に適しています。このようなシナリオでは、列モードを有効にすると更新速度が向上します。たとえば、100列のテーブルで、すべての行に対して10列（合計の10%）のみが更新される場合、列モードの更新速度は10倍速くなります。</li></ul>|
| merge_condition  | いいえ       | 更新が有効になるかどうかを決定するための条件として使用する列の名前を指定します。ソースレコードから宛先レコードへの更新は、ソースデータレコードが指定された列の宛先データレコードよりも大きいか等しい値を持つ場合にのみ有効になります。システムはv2.5以降、条件付き更新をサポートしています。<br />**注**<br />指定する列は主キー列であってはなりません。さらに、主キーテーブルを使用するテーブルのみが条件付き更新をサポートします。|

## 列マッピング

### CSVデータロードの列マッピングを構成する

データファイルの列がテーブルの列に順序通りに1対1でマッピングできる場合、データファイルとテーブル間の列マッピングを構成する必要はありません。

データファイルの列がテーブルの列に順序通りに1対1でマッピングできない場合、`columns`パラメーターを使用して、データファイルとテーブル間の列マッピングを構成する必要があります。これには、次の2つのユースケースが含まれます。

- **列数は同じだが、列の順序が異なる場合。** **また、データファイルからのデータは、対応するテーブル列にロードされる前に、関数によって計算される必要はありません。**

  `columns`パラメーターでは、データファイルの列が配置されているのと同じ順序でテーブル列の名前を指定する必要があります。

  たとえば、テーブルは3つの列で構成されており、それらは`col1`、`col2`、および`col3`の順で、データファイルも3つの列で構成されており、テーブルの列`col3`、`col2`、および`col1`に順にマッピングできます。この場合、`"columns: col3, col2, col1"`を指定する必要があります。

- **列の数が異なり、列の順序も異なります。また、データファイルからのデータは、対応するテーブル列にロードされる前に、関数によって計算される必要があります。**

  「`columns`」パラメーターでは、データファイルの列が配置されているのと同じ順序でテーブル列の名前を指定し、データを計算するために使用する関数を指定する必要があります。2つの例を以下に示します。

  - テーブルは3つの列で構成されており、それらは`col1`、`col2`、および`col3`の順です。データファイルは4つの列で構成されており、そのうち最初の3つの列はテーブルの列`col1`、`col2`、および`col3`に順にマッピングでき、4番目の列はどのテーブル列にもマッピングできません。この場合、データファイルの4番目の列に一時的な名前を指定する必要があり、その一時的な名前はどのテーブル列の名前とも異なる必要があります。たとえば、`"columns: col1, col2, col3, temp"`と指定できます。この場合、データファイルの4番目の列は一時的に`temp`と名付けられます。
  - テーブルは3つの列で構成されており、それらは`year`、`month`、および`day`の順です。データファイルは、`yyyy-mm-dd hh:mm:ss`形式の日付と時刻の値を格納する1つの列のみで構成されています。この場合、`"columns: col, year = year(col), month=month(col), day=day(col)"`と指定できます。この場合、`col`はデータファイル列の一時的な名前であり、関数`year = year(col)`、`month=month(col)`、および`day=day(col)`は、データファイル列`col`からデータを抽出し、マッピングテーブル列にデータをロードするために使用されます。たとえば、`year = year(col)`は、`yyyy`データをデータファイル列`col`から抽出し、テーブル列`year`にロードするために使用されます。

詳細な例については、[列マッピングの構成](#configure-column-mapping)を参照してください。

### JSONデータロードの列マッピングを構成する

JSONドキュメントのキーがテーブルの列と同じ名前である場合、シンプルモードを使用してJSON形式のデータをロードできます。シンプルモードでは、`jsonpaths`パラメーターを指定する必要はありません。このモードでは、JSON形式のデータが中括弧`{}`, 例えば`{"category": 1, "author": 2, "price": "3"}`. この例では、`category`、`author`、および`price`はキー名であり、これらのキーは名前によって列`category`、`author`、および`price`に1対1でマッピングできます。

JSONドキュメントのキーがテーブルの列と異なる名前を持つ場合、マッチモードを使用してJSON形式のデータをロードできます。マッチモードでは、`jsonpaths`と`COLUMNS`パラメーターを使用して、JSONドキュメントとテーブル間の列マッピングを指定する必要があります。

- 「`jsonpaths`」パラメーターでは、JSONドキュメントに配置されている順序でJSONキーを指定します。
- 「`COLUMNS`」パラメーターでは、JSONキーとテーブル列間のマッピングを指定します。
  - 「`COLUMNS`」パラメーターで指定された列名は、JSONキーに順序通りに1対1でマッピングされます。
  - 「`COLUMNS`」パラメーターで指定された列名は、名前によってテーブル列に1対1でマッピングされます。

マッチモードを使用してJSON形式のデータをロードする例については、[マッチモードを使用したJSONデータのロード](#load-json-data-using-matched-mode)を参照してください。

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

次の表は、返されるジョブ結果のパラメーターについて説明しています。

| パラメーター | 説明 |
| ---------------------- | ------------------------------------------------------------ |
| TxnId | ロードジョブのトランザクションID。 |
| Label | ロードジョブのラベル。 |
| Status | ロードされたデータの最終ステータス。<ul><li>`Success`: データは正常にロードされ、クエリ可能です。</li><li>`Publish Timeout`: ロードジョブは正常に送信されましたが、データはまだクエリできません。データのロードを再試行する必要はありません。</li><li>`Label Already Exists`: ロードジョブのラベルは別のロードジョブに使用されています。データは正常にロードされたか、現在ロード中です。</li><li>`Fail`: データのロードに失敗しました。ロードジョブを再試行できます。</li></ul> |
| Message | ロードジョブのステータス。ロードジョブが失敗した場合、詳細な失敗原因が返されます。 |
| NumberTotalRows | 読み取られたデータレコードの総数。 |
| NumberLoadedRows | 正常にロードされたデータレコードの総数。このパラメーターは、`Status`の戻り値が`Success`である場合にのみ有効です。 |
| NumberFilteredRows | データ品質が不十分なためにフィルタリングされたデータレコードの数。 |
| NumberUnselectedRows | WHERE句によってフィルタリングされたデータレコードの数。 |
| LoadBytes | ロードされたデータの量。単位：バイト。 |
| LoadTimeMs | ロードジョブにかかった時間。単位：ミリ秒。 |
| BeginTxnTimeMs | ロードジョブのトランザクションを実行するのにかかった時間。 |
| StreamLoadPlanTimeMs | ロードジョブの実行計画を生成するのにかかった時間。 |
| ReadDataTimeMs | ロードジョブのデータを読み取るのにかかった時間。 |
| WriteDataTimeMs | ロードジョブのデータを書き込むのにかかった時間。 |
| CommitAndPublishTimeMs | ロードジョブのデータをコミットして公開するのにかかった時間。 |

ロードジョブが失敗した場合、システムは`ErrorURL`も返します。例：

```JSON
{"ErrorURL": "http://172.26.195.68:8045/api/_load_error_log?file=error_log_3a4eb8421f0878a6_9a54df29fd9206be"}
```

`ErrorURL`は、フィルタリングされた不適格なデータレコードの詳細を取得できるURLを提供します。オプションパラメーター`log_rejected_record_num`を使用して、ロードジョブの送信時に設定される、ログに記録できる不適格なデータ行の最大数を指定できます。

を実行して、フィルタリングされた不適格なデータレコードの詳細を直接表示できます。また、`curl "url"`を実行して、フィルタリングされた不適格なデータレコードの詳細を直接表示できます。また、`wget "url"`を実行して、これらのデータレコードの詳細をエクスポートすることもできます。

```Bash
wget http://172.26.195.68:8045/api/_load_error_log?file=error_log_3a4eb8421f0878a6_9a54df29fd9206be
```

エクスポートされたデータレコードの詳細は、次のような名前のローカルファイルに保存されます。`_load_error_log?file=error_log_3a4eb8421f0878a6_9a54df29fd9206be`。`cat`コマンドを使用してファイルを表示できます。

その後、ロードジョブの設定を調整し、再度ロードジョブを送信できます。

## 例

<TableURLTip />

### CSVデータのロード

このセクションでは、CSVデータを例として、さまざまなパラメータ設定と組み合わせをどのように使用して、多様なロード要件を満たすことができるかを説明します。

#### タイムアウト期間の設定

データベース`test_db`には、という名前のテーブルが含まれています。`table1`。このテーブルは、次の3つの列で構成されています。`col1`、`col2`、および`col3`の順です。

データファイル`example1.csv`も3つの列で構成されており、これらは順に`col1`、`col2`、および`col3`の`table1`にマッピングできます。

からすべてのデータを`example1.csv`に`table1`最大100秒以内にロードしたい場合は、次のコマンドを実行します。

```Bash
curl --location-trusted -u <username>:<password> -H "label:label1" \
    -H "Expect:100-continue" \
    -H "timeout:100" \
    -H "max_filter_ratio:0.2" \
    -T example1.csv -XPUT \
    <table_url_prefix>/api/test_db/table1/_stream_load
```

#### エラー許容度の設定

データベース`test_db`には、という名前のテーブルが含まれています。`table2`。このテーブルは、次の3つの列で構成されています。`col1`、`col2`、および`col3`の順です。

データファイル`example2.csv`も3つの列で構成されており、これらは順に`col1`、`col2`、および`col3`の`table2`にマッピングできます。

からすべてのデータを`example2.csv`に`table2`最大エラー許容度`0.2`でロードしたい場合は、次のコマンドを実行します。

```Bash
curl --location-trusted -u <username>:<password> -H "label:label2" \
    -H "Expect:100-continue" \
    -H "max_filter_ratio:0.2" \
    -T example2.csv -XPUT \
    <table_url_prefix>/api/test_db/table2/_stream_load
```

#### 列マッピングを構成する

データベース`test_db`には、という名前のテーブルが含まれています。`table3`。このテーブルは3つの列で構成されており、それらは`col1`、`col2`、`col3`の順です。

データファイル`example3.csv`も3つの列で構成されており、それらは順に`col2`、`col1`、`col3`の`table3`にマッピングできます。

からすべてのデータを`example3.csv`にロードしたい場合は、`table3`次のコマンドを実行します。

```Bash
curl --location-trusted -u <username>:<password>  -H "label:label3" \
    -H "Expect:100-continue" \
    -H "columns: col2, col1, col3" \
    -T example3.csv -XPUT \
    <table_url_prefix>/api/test_db/table3/_stream_load
```

:::note
上記の例では、`example3.csv`の列を`table3`の列に、`table3`での列の配置と同じ順序でマッピングすることはできません。したがって、`columns`パラメーターを使用して、`example3.csv`と`table3`の間の列マッピングを構成する必要があります。
:::

#### フィルター条件を設定する

データベース`test_db`には、という名前のテーブルが含まれています。`table4`。このテーブルは3つの列で構成されており、それらは`col1`、`col2`、`col3`の順です。

データファイル`example4.csv`も3つの列で構成されており、それらは順に`col1`、`col2`、`col3`の`table4`にマッピングできます。

の最初の列の値が`example4.csv`と等しいデータレコードのみを`20180601`にロードしたい場合は、`table4`次のコマンドを実行します。

```Bash
curl --location-trusted -u <username>:<password> -H "label:label4" \
    -H "Expect:100-continue" \
    -H "columns: col1, col2, col3"\
    -H "where: col1 = 20180601" \
    -T example4.csv -XPUT \
    <table_url_prefix>/api/test_db/table4/_stream_load
```

:::note
上記の例では、`example4.csv`と`table4`は、順序通りにマッピングできる列の数が同じですが、列ベースのフィルター条件を指定するにはWHERE句を使用する必要があります。そのため、`columns`パラメーターを使用して、`example4.csv`の列の一時名を定義する必要があります。
:::

#### 宛先パーティションの設定

お使いのデータベース`test_db`には、`table5`という名前のテーブルが含まれています。このテーブルは、`col1`、`col2`、および`col3`の3つの列で構成されています。

お使いのデータファイル`example5.csv`も3つの列で構成されており、これらは`col1`、`col2`、および`col3`の`table5`に順序通りにマッピングできます。

からすべてのデータを`example5.csv`のパーティション`p1`と`p2`にロードする場合は、`table5`次のコマンドを実行します。

```Bash
curl --location-trusted -u <username>:<password>  -H "label:label5" \
    -H "Expect:100-continue" \
    -H "partitions: p1, p2" \
    -T example5.csv -XPUT \
    <table_url_prefix>/api/test_db/table5/_stream_load
```

#### 厳密モードとタイムゾーンの設定

お使いのデータベース`test_db`には、`table6`という名前のテーブルが含まれています。このテーブルは、`col1`、`col2`、および`col3`の3つの列で構成されています。

お使いのデータファイル`example6.csv`も3つの列で構成されており、これらは`col1`、`col2`、および`col3`の`table6`に順序通りにマッピングできます。

からすべてのデータを`example6.csv`に`table6`厳密モードとタイムゾーンを使用してロードする場合は、`Africa/Abidjan`次のコマンドを実行します。

```Bash
curl --location-trusted -u <username>:<password> \
    -H "Expect:100-continue" \
    -H "strict_mode: true" \
    -H "timezone: Africa/Abidjan" \
    -T example6.csv -XPUT \
    <table_url_prefix>/api/test_db/table6/_stream_load
```

#### HLL型列を含むテーブルにデータをロードする

データベース`test_db`には、という名前のテーブルが含まれています。`table7`。このテーブルは2つのHLL型列で構成されており、それらは`col1`と`col2`の順です。

データファイル`example7.csv`も2つの列で構成されており、そのうち最初の列は`col1`の`table7`にマッピングでき、2番目の列は`table7`のどの列にもマッピングできません。の最初の列の値は`example7.csv`にロードされる前に、関数を使用してHLL型データに変換できます。`col1`の`table7`。

から`example7.csv`へデータをロードしたい場合は、`table7`次のコマンドを実行します。

```Bash
curl --location-trusted -u <username>:<password> \
    -H "Expect:100-continue" \
    -H "columns: temp1, temp2, col1=hll_hash(temp1), col2=hll_empty()" \
    -T example7.csv -XPUT \
    <table_url_prefix>/api/test_db/table7/_stream_load
```

:::note
上記の例では、の2つの列は`example7.csv`と名付けられています。`temp1`と`temp2`は、パラメーターを使用して順に`columns`パラメーターを使用して順に名前が付けられています。次に、関数を使用してデータを次のように変換します。

- 関数は、`hll_hash`の値をHLL型データに変換し、`temp1`の`example7.csv`を`temp1`の`example7.csv`にマッピングするために使用されます。`col1`の`table7`。
- 関数は、`hll_empty`の指定されたデフォルト値を埋めるために使用されます。
:::`col2`の`table7`に指定されたデフォルト値を埋めるために使用されます。
:::

関数`hll_hash`および`hll_empty`の使用方法については、[hll_hash](../../sql-functions/scalar-functions/hll_hash.md)および[hll_empty](../../sql-functions/scalar-functions/hll_empty.md)。

#### BITMAP型列を含むテーブルにデータをロードする

お使いのデータベース`test_db`には、という名前のテーブルが含まれています。`table8`。このテーブルは、`col1`と`col2`の2つのBITMAP型列で構成されています。

お使いのデータファイル`example8.csv`も2つの列で構成されており、そのうち最初の列は`col1`の`table8`にマッピングでき、2番目の列は`table8`のどの列にもマッピングできません。`example8.csv`の最初の列の値は、`col1`の`table8`にロードされる前に、関数を使用して変換できます。

から`example8.csv`にデータをロードする場合は、`table8`次のコマンドを実行します。

```Bash
curl --location-trusted -u <username>:<password> \
    -H "Expect:100-continue" \
    -H "columns: temp1, temp2, col1=to_bitmap(temp1), col2=bitmap_empty()" \
    -T example8.csv -XPUT \
    <table_url_prefix>/api/test_db/table8/_stream_load
```

:::note
上記の例では、`example8.csv`の2つの列は、`temp1`と`temp2`パラメーターを使用して順に名前が付けられています。`columns`次に、関数を使用してデータを次のように変換します。

- `to_bitmap`関数は、`temp1`の`example8.csv`の値をBITMAP型データに変換し、`temp1`の`example8.csv`を`col1`の`table8`にマッピングするために使用されます。
- `bitmap_empty`関数は、指定されたデフォルト値を`col2`の`table8`に埋めるために使用されます。
:::

関数`to_bitmap`と`bitmap_empty`の使用方法については、[to_bitmap](../../sql-functions/bitmap-functions/to_bitmap.md)および[bitmap_empty](../../sql-functions/bitmap-functions/bitmap_empty.md)。

#### 設定 `skip_header`、`trim_space`、`enclose`、および `escape`

あなたのデータベース `test_db` には、 という名前のテーブルが含まれています。`table9`。このテーブルは3つの列で構成されており、それらは `col1`、`col2`、および `col3` の順です。

あなたのデータファイル `example9.csv` も3つの列で構成されており、それらは `col2`、`col1`、および `col3` の `table13` に順にマッピングされます。

もし `example9.csv` からすべてのデータを `table9` にロードし、` の最初の5行をスキップし、`example9.csv` 列区切り文字の前後のスペースを削除し、``enclose` を `\` に、そして `escape` を `\` に設定したい場合は、以下のコマンドを実行してください:

```Bash
curl --location-trusted -u <username>:<password> -H "label:3875" \
    -H "Expect:100-continue" \
    -H "trim_space: true" -H "skip_header: 5" \
    -H "column_separator:," -H "enclose:\"" -H "escape:\\" \
    -H "columns: col2, col1, col3" \
    -T example9.csv -XPUT \
    <table_url_prefix>/api/test_db/tbl9/_stream_load
```

#### 設定 `column_separator` および `row_delimiter`

あなたのデータベース `test_db` には、 という名前のテーブルが含まれています。`table10`。このテーブルは3つの列で構成されており、それらは `col1`、`col2`、および `col3` の順です。

あなたのデータファイル `example10.csv` も3つの列で構成されており、それらは `col1`、`col2`、および `col3` の `table10` に順にマッピングできます。データ行の列はカンマ（`,`)、データ行は2つの連続する非表示文字で区切られます。`\r\n`。

からすべてのデータをロードしたい場合は、`example10.csv`に`table10`を実行します。

```Bash
curl --location-trusted -u <username>:<password> -H "label:label10" \
    -H "Expect:100-continue" \
    -H "column_separator:," \
    -H "row_delimiter:\\x0D0A" \
    -T example10.csv -XPUT \
    <table_url_prefix>/api/test_db/table10/_stream_load
```

### JSONデータのロード

このセクションでは、JSONデータをロードする際に注意すべきパラメータ設定について説明します。

データベース`test_db`には、`tbl1`という名前のテーブルが含まれており、そのスキーマは次のとおりです。

```SQL
`category` varchar(512) NULL COMMENT "",`author` varchar(512) NULL COMMENT "",`title` varchar(512) NULL COMMENT "",`price` double NULL COMMENT ""
```

#### シンプルモードを使用したJSONデータのロード

データファイル`example1.json`が次のデータで構成されているとします。

```JSON
{"category":"C++","author":"avc","title":"C++ primer","price":895}
```

からすべてのデータを`example1.json`にロードするには、`tbl1`を実行します。

```Bash
curl --location-trusted -u <username>:<password> -H "label:label6" \
    -H "Expect:100-continue" \
    -H "format: json" \
    -T example1.json -XPUT \
    <table_url_prefix>/api/test_db/tbl1/_stream_load
```

:::note
上記の例では、パラメータ`columns`と`jsonpaths`は指定されていません。したがって、`example1.json`のキーは、`tbl1`の列に名前でマッピングされます。
:::

スループットを向上させるため、Stream Loadは複数のデータレコードを一度にロードすることをサポートしています。例：

```JSON
[{"category":"C++","author":"avc","title":"C++ primer","price":89.5},{"category":"Java","author":"avc","title":"Effective Java","price":95},{"category":"Linux","author":"avc","title":"Linux kernel","price":195}]
```

#### マッチモードを使用したJSONデータのロード

システムは、JSONデータを照合および処理するために次の手順を実行します。

1. (オプション) `strip_outer_array`パラメータ設定に従って、最外層の配列構造を削除します。

   。設定する必要があります。`[]`を`strip_outer_array`に`true`。

2. (オプション) `json_root`パラメータ設定に従って、JSONデータのルート要素を照合します。

   パラメータ。
:::`json_root`パラメータ。
:::

3. 指定されたJSONデータを、`jsonpaths`パラメータ設定に従って抽出します。

##### ルート要素を指定せずにマッチモードでJSONデータをロード

データファイル`example2.json`が次のデータで構成されているとします。

```JSON
[{"category":"xuxb111","author":"1avc","title":"SayingsoftheCentury","price":895},{"category":"xuxb222","author":"2avc","title":"SayingsoftheCentury","price":895},{"category":"xuxb333","author":"3avc","title":"SayingsoftheCentury","price":895}]
```

のみをロードするには、`category`、`author`、および`price`から`example2.json`の場合、次のコマンドを実行します。

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
上記の例では、JSONデータの最外層は、角括弧のペアで示される配列構造です。`[]`。配列構造は、それぞれデータレコードを表す複数のJSONオブジェクトで構成されています。したがって、`strip_outer_array`を`true`に設定して、最外層の配列構造を削除する必要があります。キー「**title**」はロードしたくないため、ロード中に無視されます。
:::

##### ルート要素を指定してマッチモードでJSONデータをロードする

データファイル`example3.json`が次のデータで構成されているとします。

```JSON
{"id": 10001,"RECORDS":[{"category":"11","title":"SayingsoftheCentury","price":895,"timestamp":1589191587},{"category":"22","author":"2avc","price":895,"timestamp":1589191487},{"category":"33","author":"3avc","title":"SayingsoftheCentury","timestamp":1589191387}],"comments": ["3 records", "there will be 3 rows"]}
```

のみをロードするには、`category`、`author`、および`price`から`example3.json`の場合、次のコマンドを実行します。

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
上記の例では、JSONデータの最外層は、角括弧のペアで示される配列構造です。`[]`。配列構造は、それぞれデータレコードを表す複数のJSONオブジェクトで構成されています。したがって、`strip_outer_array`を`true`に設定して、最外層の配列構造を削除する必要があります。キー「`title`」と「`timestamp`」はロードしたくないため、ロード中に無視されます。さらに、`json_root`パラメーターは、JSONデータのルート要素（配列）を指定するために使用されます。
:::

### ストリームロードリクエストをマージする

- 次のコマンドを実行して、同期モードでマージコミットが有効なストリームロードジョブを開始し、マージウィンドウを`5000`ミリ秒、並列度を`2`に設定します。

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

- 次のコマンドを実行して、非同期モードでマージコミットが有効なストリームロードジョブを開始し、マージウィンドウを`60000`ミリ秒、並列度を`2`に設定します。

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
