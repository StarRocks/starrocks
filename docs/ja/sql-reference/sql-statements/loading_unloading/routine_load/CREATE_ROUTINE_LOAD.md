---
displayed_sidebar: docs
---

import Tip from '../../../../_assets/commonMarkdown/quickstart-routine-load-tip.mdx';

# CREATE ROUTINE LOAD

<Tip />

Routine Load は Apache Kafka® からメッセージを継続的に消費し、データを StarRocks にロードすることができます。Routine Load は Kafka クラスターから CSV、JSON、Avro（v3.0.1 以降でサポート）データを消費し、`plaintext`、`ssl`、`sasl_plaintext`、`sasl_ssl` などの複数のセキュリティプロトコルを介して Kafka にアクセスできます。

このトピックでは、CREATE ROUTINE LOAD ステートメントの構文、パラメータ、および例について説明します。

> **NOTE**
>
> - Routine Load の適用シナリオ、原則、および基本操作については、 [Load data using Routine Load](../../../../loading/Loading_intro.md) を参照してください。
> - StarRocks テーブルにデータをロードするには、StarRocks テーブルに対して INSERT 権限を持つユーザーとしてのみ行うことができます。INSERT 権限がない場合は、 [GRANT](../../account-management/GRANT.md) に従って、使用するユーザーに INSERT 権限を付与してください。

## Syntax

```SQL
CREATE ROUTINE LOAD <database_name>.<job_name> ON <table_name>
[load_properties]
[job_properties]
FROM data_source
[data_source_properties]
```

## Parameters

### `database_name`, `job_name`, `table_name`

`database_name`

オプション。StarRocks データベースの名前。

`job_name`

必須。Routine Load ジョブの名前。1 つのテーブルは複数の Routine Load ジョブからデータを受け取ることができます。識別可能な情報、例えば Kafka トピック名やジョブ作成時刻などを使用して、意味のある Routine Load ジョブ名を設定することをお勧めします。同じデータベース内で Routine Load ジョブの名前は一意でなければなりません。

`table_name`

必須。データがロードされる StarRocks テーブルの名前。

### `load_properties`

オプション。データのプロパティ。構文:

```SQL
[COLUMNS TERMINATED BY '<column_separator>'],
[ROWS TERMINATED BY '<row_separator>'],
[COLUMNS (<column1_name>[, <column2_name>, <column_assignment>, ... ])],
[WHERE <expr>],
[PARTITION (<partition1_name>[, <partition2_name>, ...])]
[TEMPORARY PARTITION (<temporary_partition1_name>[, <temporary_partition2_name>, ...])]
```

`COLUMNS TERMINATED BY`

CSV 形式のデータのカラム区切り文字。デフォルトのカラム区切り文字は `\t`（タブ）です。例えば、カラム区切り文字をカンマに指定するには `COLUMNS TERMINATED BY ","` を使用します。

> **Note**
>
> - ここで指定したカラム区切り文字が、取り込むデータのカラム区切り文字と同じであることを確認してください。
> - カンマ（,）、タブ、パイプ（|）などの UTF-8 文字列をテキストデリミタとして使用できますが、その長さは 50 バイトを超えてはなりません。
> - Null 値は `\N` を使用して示されます。例えば、データレコードが 3 つのカラムで構成されており、データレコードが最初と 3 番目のカラムにデータを保持しているが、2 番目のカラムにはデータを保持していない場合、この状況では 2 番目のカラムに `\N` を使用して Null 値を示す必要があります。これは、レコードを `a,\N,b` としてコンパイルする必要があることを意味します。`a,,b` はレコードの 2 番目のカラムが空の文字列を保持していることを示します。

`ROWS TERMINATED BY`

CSV 形式のデータの行区切り文字。デフォルトの行区切り文字は `\n` です。

`COLUMNS`

ソースデータのカラムと StarRocks テーブルのカラム間のマッピング。詳細については、このトピックの [Column mapping](#column-mapping) を参照してください。

- `column_name`: ソースデータのカラムが計算なしで StarRocks テーブルのカラムにマッピングできる場合、カラム名を指定するだけで済みます。これらのカラムはマッピングされたカラムと呼ばれます。
- `column_assignment`: ソースデータのカラムが直接 StarRocks テーブルのカラムにマッピングできず、データロード前に関数を使用してカラムの値を計算する必要がある場合、`expr` で計算関数を指定する必要があります。これらのカラムは派生カラムと呼ばれます。StarRocks は最初にマッピングされたカラムを解析するため、派生カラムはマッピングされたカラムの後に配置することをお勧めします。

`WHERE`

フィルター条件。フィルター条件を満たすデータのみが StarRocks にロードされます。例えば、`col1` の値が `100` より大きく、`col2` の値が `1000` と等しい行のみを取り込みたい場合、`WHERE col1 > 100 and col2 = 1000` を使用できます。

> **NOTE**
>
> フィルター条件で指定されたカラムは、ソースカラムまたは派生カラムであることができます。

`PARTITION`

StarRocks テーブルがパーティション p0、p1、p2、p3 に分散されており、StarRocks で p1、p2、p3 にのみデータをロードし、p0 に保存されるデータをフィルタリングしたい場合、フィルター条件として `PARTITION(p1, p2, p3)` を指定できます。デフォルトでは、このパラメータを指定しない場合、データはすべてのパーティションにロードされます。例:

```SQL
PARTITION (p1, p2, p3)
```

`TEMPORARY PARTITION`

データをロードしたい [temporary partition](../../../../table_design/data_distribution/Temporary_partition.md) の名前。複数の一時パーティションを指定することができ、カンマ（,）で区切る必要があります。

### `job_properties`

必須。ロードジョブのプロパティ。構文:

```SQL
PROPERTIES ("<key1>" = "<value1>"[, "<key2>" = "<value2>" ...])
```

| **Property**              | **Required** | **Description**                                              |
| ------------------------- | ------------ | ------------------------------------------------------------ |
| desired_concurrent_number | No           | 単一の Routine Load ジョブの期待されるタスク並行性。デフォルト値: `3`。実際のタスク並行性は、複数のパラメータの最小値によって決定されます: `min(alive_be_number, partition_number, desired_concurrent_number, max_routine_load_task_concurrent_num)`。<ul><li>`alive_be_number`: 生存している BE ノードの数。</li><li>`partition_number`: 消費されるパーティションの数。</li><li>`desired_concurrent_number`: 単一の Routine Load ジョブの期待されるタスク並行性。デフォルト値: `3`。</li><li>`max_routine_load_task_concurrent_num`: Routine Load ジョブのデフォルトの最大タスク並行性で、`5` です。 [FE dynamic parameter](../../../../administration/management/FE_configuration.md#configure-fe-dynamic-parameters) を参照してください。</li></ul>最大の実際のタスク並行性は、生存している BE ノードの数または消費されるパーティションの数によって決まります。|
| max_batch_interval        | No           | タスクのスケジューリング間隔、つまりタスクが実行される頻度。単位: 秒。値の範囲: `5` ~ `60`。デフォルト値: `10`。10 秒以上の値を設定することをお勧めします。スケジューリングが 10 秒未満の場合、ロード頻度が高すぎるために多くのタブレットバージョンが生成されます。 |
| max_batch_rows            | No           | このプロパティはエラーデータ検出ウィンドウを定義するためにのみ使用されます。ウィンドウは、単一の Routine Load タスクによって消費されるデータの行数です。値は `10 * max_batch_rows` です。デフォルト値は `10 * 200000 = 2000000` です。Routine Load タスクは、エラーデータ検出ウィンドウ内でエラーデータを検出します。エラーデータとは、StarRocks が解析できないデータ、例えば無効な JSON 形式のデータを指します。 |
| max_error_number          | No           | エラーデータ検出ウィンドウ内で許容されるエラーデータ行の最大数。この値を超えると、ロードジョブは一時停止します。 [SHOW ROUTINE LOAD](./SHOW_ROUTINE_LOAD.md) を実行し、`ErrorLogUrls` を使用してエラーログを表示できます。その後、エラーログに従って Kafka のエラーを修正できます。デフォルト値は `0` で、エラーデータ行は許可されません。 <br />**NOTE** <br /> <ul><li>エラーデータ行が多すぎる場合、ロードジョブが一時停止する前に最後のバッチのタスクは **成功** します。つまり、適格なデータはロードされ、不適格なデータはフィルタリングされます。あまりにも多くの不適格なデータ行をフィルタリングしたくない場合は、パラメータ `max_filter_ratio` を設定してください。 </li><li>エラーデータ行には、WHERE 句によってフィルタリングされたデータ行は含まれません。</li><li>このパラメータは、次のパラメータ `max_filter_ratio` と共に、エラーデータレコードの最大数を制御します。`max_filter_ratio` が設定されていない場合、このパラメータの値が有効になります。`max_filter_ratio` が設定されている場合、エラーデータレコードの数がこのパラメータまたは `max_filter_ratio` パラメータで設定されたしきい値に達すると、ロードジョブは一時停止します。</li></ul>|
|max_filter_ratio|No|ロードジョブの最大エラー許容度。エラー許容度は、ロードジョブによって要求されたすべてのデータレコードの中で、データ品質が不十分なためにフィルタリングされる可能性のあるデータレコードの最大割合です。有効な値: `0` から `1`。デフォルト値: `1`（実際には効果を発揮しません）。<br/>`0` に設定することをお勧めします。これにより、不適格なデータレコードが検出された場合、ロードジョブが一時停止し、データの正確性が保証されます。<br/>不適格なデータレコードを無視したい場合は、このパラメータを `0` より大きい値に設定できます。これにより、データファイルに不適格なデータレコードが含まれていても、ロードジョブは成功します。  <br/>**NOTE**<br/><ul><li>エラーデータ行が `max_filter_ratio` を超える場合、最後のバッチのタスクは **失敗** します。これは `max_error_number` の効果とは少し **異なります**。</li><li>不適格なデータレコードには、WHERE 句によってフィルタリングされたデータレコードは含まれません。</li><li>このパラメータは、最後のパラメータ `max_error_number` と共に、エラーデータレコードの最大数を制御します。このパラメータが設定されていない場合（`max_filter_ratio = 1` と設定した場合と同じように動作します）、`max_error_number` パラメータの値が有効になります。このパラメータが設定されている場合、エラーデータレコードの数がこのパラメータまたは `max_error_number` パラメータで設定されたしきい値に達すると、ロードジョブは一時停止します。</li></ul>|
| strict_mode               | No           | [strict mode](../../../../loading/load_concept/strict_mode.md) を有効にするかどうかを指定します。有効な値: `true` と `false`。デフォルト値: `false`。strict mode が有効な場合、ロードされたデータのカラムの値が `NULL` であり、ターゲットテーブルがそのカラムに `NULL` 値を許可しない場合、データ行はフィルタリングされます。 |
| log_rejected_record_num | No | ログに記録できる不適格なデータ行の最大数を指定します。このパラメータは v3.1 以降でサポートされています。有効な値: `0`、`-1`、および任意の非ゼロの正の整数。デフォルト値: `0`。<ul><li>値 `0` は、フィルタリングされたデータ行がログに記録されないことを指定します。</li><li>値 `-1` は、フィルタリングされたすべてのデータ行がログに記録されることを指定します。</li><li>`n` のような非ゼロの正の整数は、各 BE でフィルタリングされた最大 `n` 行のデータがログに記録されることを指定します。</li></ul> |
| timezone                  | No           | ロードジョブで使用されるタイムゾーン。デフォルト値: `Asia/Shanghai`。このパラメータの値は、strftime()、alignment_timestamp()、from_unixtime() などの関数によって返される結果に影響します。このパラメータで指定されたタイムゾーンはセッションレベルのタイムゾーンです。詳細については、 [Configure a time zone](../../../../administration/management/timezone.md) を参照してください。 |
| partial_update | No | 部分更新を使用するかどうか。 有効な値: `TRUE` と `FALSE`。 デフォルト値: `FALSE`、この機能を無効にすることを示します。 |
| merge_condition           | No           | データを更新するかどうかを判断する条件として使用するカラムの名前を指定します。このカラムにロードされるデータの値がこのカラムの現在の値以上である場合にのみデータが更新されます。 **NOTE**<br />条件付き更新をサポートするのは主キーテーブルのみです。指定するカラムは主キーのカラムであってはなりません。 |
| format                    | No           | ロードするデータの形式。 有効な値: `CSV`、`JSON`、および `Avro`（v3.0.1 以降でサポート）。 デフォルト値: `CSV`。 |
| trim_space                | No           | データファイルが CSV 形式の場合、カラム区切り文字の前後のスペースを削除するかどうかを指定します。タイプ: BOOLEAN。デフォルト値: `false`。<br />一部のデータベースでは、データを CSV 形式のデータファイルとしてエクスポートする際に、カラム区切り文字にスペースが追加されます。これらのスペースは、その位置に応じて先行スペースまたは後続スペースと呼ばれます。`trim_space` パラメータを設定することで、StarRocks がデータロード中にこれらの不要なスペースを削除できるようにします。<br />StarRocks は、`enclose` で指定された文字で囲まれたフィールド内のスペース（先行スペースおよび後続スペースを含む）を削除しないことに注意してください。例えば、次のフィールド値はパイプ（<code class="language-text">&#124;</code>）をカラム区切り文字として使用し、二重引用符（`"`)を `enclose` で指定された文字として使用しています: <code class="language-text">&#124; "Love StarRocks" &#124;</code>。`trim_space` を `true` に設定すると、StarRocks は前述のフィールド値を <code class="language-text">&#124;"Love StarRocks"&#124;</code> として処理します。 |
| enclose                   | No           | データファイルが CSV 形式の場合、 [RFC4180](https://www.rfc-editor.org/rfc/rfc4180) に従ってフィールド値を囲むために使用される文字を指定します。タイプ: 単一バイト文字。デフォルト値: `NONE`。最も一般的な文字は単一引用符（`'`）および二重引用符（`"`）です。<br />`enclose` で指定された文字で囲まれたすべての特殊文字（行区切り文字およびカラム区切り文字を含む）は通常の記号と見なされます。StarRocks は、`enclose` で指定された文字として任意の単一バイト文字を指定できるため、RFC4180 よりも多くのことができます。<br />フィールド値に `enclose` で指定された文字が含まれている場合、同じ文字を使用してその `enclose` で指定された文字をエスケープできます。例えば、`enclose` を `"` に設定し、フィールド値が `a "quoted" c` の場合、このフィールド値をデータファイルに `"a ""quoted"" c"` として入力できます。 |
| escape                    | No           | 行区切り文字、カラム区切り文字、エスケープ文字、`enclose` で指定された文字などのさまざまな特殊文字をエスケープするために使用される文字を指定します。これらは StarRocks によって通常の文字と見なされ、それらが存在するフィールド値の一部として解析されます。タイプ: 単一バイト文字。デフォルト値: `NONE`。最も一般的な文字はスラッシュ（`\`）で、SQL ステートメントでは二重スラッシュ（`\\`）として記述する必要があります。<br />**NOTE**<br />`escape` で指定された文字は、各ペアの `enclose` で指定された文字の内側と外側の両方に適用されます。<br />次のような例があります:<br /><ul><li>`enclose` を `"` に設定し、`escape` を `\` に設定すると、StarRocks は `"say \"Hello world\""` を `say "Hello world"` に解析します。</li><li>カラム区切り文字がカンマ（`,`）の場合、`escape` を `\` に設定すると、StarRocks は `a, b\, c` を 2 つの別々のフィールド値 `a` と `b, c` に解析します。</li></ul> |
| strip_outer_array         | No           | JSON 形式のデータの最外部の配列構造を削除するかどうかを指定します。有効な値: `true` と `false`。デフォルト値: `false`。実際のビジネスシナリオでは、JSON 形式のデータには `[]` で示される最外部の配列構造がある場合があります。この状況では、このパラメータを `true` に設定することをお勧めします。これにより、StarRocks は最外部の角括弧 `[]` を削除し、各内部配列を個別のデータレコードとしてロードします。このパラメータを `false` に設定すると、StarRocks は JSON 形式のデータ全体を 1 つの配列として解析し、その配列を 1 つのデータレコードとしてロードします。JSON 形式のデータ `[{"category" : 1, "author" : 2}, {"category" : 3, "author" : 4} ]` を例として使用します。このパラメータを `true` に設定すると、`{"category" : 1, "author" : 2}` と `{"category" : 3, "author" : 4}` は 2 つの別々のデータレコードとして解析され、2 つの StarRocks データ行にロードされます。 |
| jsonpaths                 | No           | JSON 形式のデータからロードしたいフィールドの名前。このパラメータの値は有効な JsonPath 式です。詳細については、 [StarRocks table contains derived columns whose values are generated by using expressions](#starrocks-table-contains-derived-columns-whose-values-are-generated-by-using-expressions) を参照してください。 |
| json_root                 | No           | ロードする JSON 形式のデータのルート要素。StarRocks は `json_root` を通じてルートノードの要素を抽出して解析します。デフォルトでは、このパラメータの値は空であり、すべての JSON 形式のデータがロードされることを示します。詳細については、 [Specify the root element of the JSON-formatted data to be loaded](#specify-the-root-element-of-the-json-formatted-data-to-be-loaded) を参照してください。 |
| task_consume_second | No | 指定された Routine Load ジョブ内の各 Routine Load タスクがデータを消費する最大時間。単位: 秒。 [FE dynamic parameters](../../../../administration/management/FE_configuration.md) `routine_load_task_consume_second`（クラスター内のすべての Routine Load ジョブに適用される）とは異なり、このパラメータは個々の Routine Load ジョブに特化しており、より柔軟です。このパラメータは v3.1.0 以降でサポートされています。<ul> <li>`task_consume_second` と `task_timeout_second` が設定されていない場合、StarRocks は FE dynamic parameters `routine_load_task_consume_second` と `routine_load_task_timeout_second` を使用してロード動作を制御します。</li> <li>`task_consume_second` のみが設定されている場合、`task_timeout_second` のデフォルト値は `task_consume_second` * 4 として計算されます。</li> <li>`task_timeout_second` のみが設定されている場合、`task_consume_second` のデフォルト値は `task_timeout_second`/4 として計算されます。</li> </ul> |
|task_timeout_second|No|指定された Routine Load ジョブ内の各 Routine Load タスクのタイムアウト期間。単位: 秒。 [FE dynamic parameter](../../../../administration/management/FE_configuration.md) `routine_load_task_timeout_second`（クラスター内のすべての Routine Load ジョブに適用される）とは異なり、このパラメータは個々の Routine Load ジョブに特化しており、より柔軟です。このパラメータは v3.1.0 以降でサポートされています。 <ul> <li>`task_consume_second` と `task_timeout_second` が設定されていない場合、StarRocks は FE dynamic parameters `routine_load_task_consume_second` と `routine_load_task_timeout_second` を使用してロード動作を制御します。</li> <li>`task_timeout_second` のみが設定されている場合、`task_consume_second` のデフォルト値は `task_timeout_second`/4 として計算されます。</li> <li>`task_consume_second` のみが設定されている場合、`task_timeout_second` のデフォルト値は `task_consume_second` * 4 として計算されます。</li> </ul>|

### `data_source`, `data_source_properties`

必須。データソースと関連するプロパティ。

```sql
FROM <data_source>
 ("<key1>" = "<value1>"[, "<key2>" = "<value2>" ...])
```

`data_source`

必須。ロードしたいデータのソース。有効な値: `KAFKA`。

`data_source_properties`

データソースのプロパティ。

| Property          | Required | Description                                                  |
| ----------------- | -------- | ------------------------------------------------------------ |
| kafka_broker_list | Yes      | Kafka のブローカー接続情報。形式は `<kafka_broker_ip>:<broker_ port>` です。複数のブローカーはカンマ（,）で区切られます。Kafka ブローカーが使用するデフォルトのポートは `9092` です。例: `"kafka_broker_list" = ""xxx.xx.xxx.xx:9092,xxx.xx.xxx.xx:9092"`。 |
| kafka_topic       | Yes      | 消費される Kafka トピック。Routine Load ジョブは 1 つのトピックからのみメッセージを消費できます。 |
| kafka_partitions  | No       | 消費される Kafka パーティション。例: `"kafka_partitions" = "0, 1, 2, 3"`。このプロパティが指定されていない場合、デフォルトですべてのパーティションが消費されます。 |
| kafka_offsets     | No       | `kafka_partitions` で指定された Kafka パーティションでデータを消費し始めるオフセット。指定されていない場合、Routine Load ジョブは `kafka_partitions` の最新のオフセットからデータを消費します。有効な値:<ul><li>特定のオフセット: 特定のオフセットからデータを消費します。</li><li>`OFFSET_BEGINNING`: 可能な限り最も早いオフセットからデータを消費します。</li><li>`OFFSET_END`: 最新のオフセットからデータを消費します。</li></ul> 複数の開始オフセットはカンマ（,）で区切られます。例: `"kafka_offsets" = "1000, OFFSET_BEGINNING, OFFSET_END, 2000"`。|
| property.kafka_default_offsets| No| すべての消費者パーティションのデフォルトの開始オフセット。このプロパティでサポートされる値は、`kafka_offsets` プロパティの値と同じです。|
| confluent.schema.registry.url|No |Avro スキーマが登録されている Schema Registry の URL。StarRocks はこの URL を使用して Avro スキーマを取得します。形式は次のとおりです:<br />`confluent.schema.registry.url = http[s]://[<schema-registry-api-key>:<schema-registry-api-secret>@]<hostname or ip address>[:<port>]`|

#### データソース関連のプロパティをさらに指定する

Kafka コマンドライン `--property` を使用するのと同等の追加のデータソース（Kafka）関連のプロパティを指定できます。サポートされているプロパティの詳細については、 [librdkafka configuration properties](https://github.com/confluentinc/librdkafka/blob/master/CONFIGURATION.md) の Kafka コンシューマークライアントのプロパティを参照してください。

> **NOTE**
>
> プロパティの値がファイル名である場合、ファイル名の前にキーワード `FILE:` を追加してください。ファイルの作成方法については、 [CREATE FILE](../../cluster-management/file/CREATE_FILE.md) を参照してください。

- **消費されるすべてのパーティションのデフォルトの初期オフセットを指定する**

```SQL
"property.kafka_default_offsets" = "OFFSET_BEGINNING"
```

- **Routine Load ジョブで使用されるコンシューマーグループの ID を指定する**

```SQL
"property.group.id" = "group_id_0"
```

`property.group.id` が指定されていない場合、StarRocks は Routine Load ジョブの名前に基づいてランダムな値を生成します。形式は `{job_name}_{random uuid}` で、例: `simple_job_0a64fe25-3983-44b2-a4d8-f52d3af4c3e8`。

- **BE が Kafka にアクセスするために使用するセキュリティプロトコルと関連するパラメータを指定する**

  セキュリティプロトコルは `plaintext`（デフォルト）、`ssl`、`sasl_plaintext`、または `sasl_ssl` として指定できます。指定されたセキュリティプロトコルに応じて関連するパラメータを設定する必要があります。

  セキュリティプロトコルが `sasl_plaintext` または `sasl_ssl` に設定されている場合、次の SASL 認証メカニズムがサポートされています:

  - PLAIN
  - SCRAM-SHA-256 および SCRAM-SHA-512
  - OAUTHBEARER
  - GSSAPI (Kerberos)

  **例:**

  - SSL セキュリティプロトコルを使用して Kafka にアクセスする:

    ```SQL
    "property.security.protocol" = "ssl", -- セキュリティプロトコルを SSL として指定します。
    "property.ssl.ca.location" = "FILE:ca-cert", -- Kafka ブローカーのキーを検証するための CA 証明書のファイルまたはディレクトリパス。
    -- Kafka サーバーがクライアント認証を有効にしている場合、次の 3 つのパラメータも必要です:
    "property.ssl.certificate.location" = "FILE:client.pem", -- 認証に使用されるクライアントの公開鍵のパス。
    "property.ssl.key.location" = "FILE:client.key", -- 認証に使用されるクライアントの秘密鍵のパス。
    "property.ssl.key.password" = "xxxxxx" -- クライアントの秘密鍵のパスワード。
    ```

  - SASL_PLAINTEXT セキュリティプロトコルと SASL/PLAIN 認証メカニズムを使用して Kafka にアクセスする:

    ```SQL
    "property.security.protocol" = "SASL_PLAINTEXT", -- セキュリティプロトコルを SASL_PLAINTEXT として指定します。
    "property.sasl.mechanism" = "PLAIN", -- SASL メカニズムを PLAIN として指定します。これはシンプルなユーザー名/パスワード認証メカニズムです。
    "property.sasl.username" = "admin",  -- SASL ユーザー名。
    "property.sasl.password" = "xxxxxx"  -- SASL パスワード。
    ```

  - SASL_PLAINTEXT セキュリティプロトコルと SASL/GSSAPI (Kerberos) 認証メカニズムを使用して Kafka にアクセスする:

    ```sql
    "property.security.protocol" = "SASL_PLAINTEXT", -- セキュリティプロトコルを SASL_PLAINTEXT として指定します。
    "property.sasl.mechanism" = "GSSAPI", -- SASL 認証メカニズムを GSSAPI として指定します。デフォルト値は GSSAPI です。
    "property.sasl.kerberos.service.name" = "kafka", -- ブローカーサービス名。デフォルト値は kafka です。
    "property.sasl.kerberos.keytab" = "/home/starrocks/starrocks.keytab", -- クライアント keytab の場所。
    "property.sasl.kerberos.principal" = "starrocks@YOUR.COM" -- Kerberos プリンシパル。
    ```

    :::note

    - StarRocks v3.1.4 以降、SASL/GSSAPI (Kerberos) 認証がサポートされています。
    - SASL 関連のモジュールは BE マシンにインストールする必要があります。

        ```bash
        # Debian/Ubuntu:
        sudo apt-get install libsasl2-modules-gssapi-mit libsasl2-dev
        # CentOS/Redhat:
        sudo yum install cyrus-sasl-gssapi cyrus-sasl-devel
        ```

    :::

### FE と BE の設定項目

Routine Load に関連する FE と BE の設定項目については、 [configuration items](../../../../administration/management/FE_configuration.md) を参照してください。

## Column mapping

### CSV 形式のデータをロードするためのカラムマッピングを設定する

CSV 形式のデータのカラムが StarRocks テーブルのカラムに順番に 1 対 1 でマッピングできる場合、データと StarRocks テーブルの間のカラムマッピングを設定する必要はありません。

CSV 形式のデータのカラムが StarRocks テーブルのカラムに順番に 1 対 1 でマッピングできない場合、`columns` パラメータを使用してデータファイルと StarRocks テーブルの間のカラムマッピングを設定する必要があります。これには次の 2 つのユースケースが含まれます:

- **カラム数は同じだがカラムの順序が異なる。また、データファイルからのデータは StarRocks テーブルの対応するカラムにロードされる前に関数によって計算される必要はありません。**

  - `columns` パラメータでは、データファイルのカラムが配置されている順序と同じ順序で StarRocks テーブルのカラム名を指定する必要があります。

  - 例えば、StarRocks テーブルは順番に `col1`、`col2`、`col3` の 3 つのカラムで構成され、データファイルも 3 つのカラムで構成され、順番に StarRocks テーブルのカラム `col3`、`col2`、`col1` にマッピングできます。この場合、`"columns: col3, col2, col1"` と指定する必要があります。

- **カラム数が異なり、カラムの順序も異なる。また、データファイルからのデータは StarRocks テーブルの対応するカラムにロードされる前に関数によって計算される必要があります。**

  `columns` パラメータでは、データファイルのカラムが配置されている順序と同じ順序で StarRocks テーブルのカラム名を指定し、データを計算するために使用する関数を指定する必要があります。次の 2 つの例があります:

  - StarRocks テーブルは順番に `col1`、`col2`、`col3` の 3 つのカラムで構成されています。データファイルは 4 つのカラムで構成されており、そのうち最初の 3 つのカラムは順番に StarRocks テーブルのカラム `col1`、`col2`、`col3` にマッピングでき、4 番目のカラムは StarRocks テーブルのカラムにマッピングできません。この場合、データファイルの 4 番目のカラムに一時的な名前を指定する必要があり、その一時的な名前は StarRocks テーブルのカラム名と異なる必要があります。例えば、`"columns: col1, col2, col3, temp"` と指定できます。この場合、データファイルの 4 番目のカラムは一時的に `temp` と名付けられます。
  - StarRocks テーブルは順番に `year`、`month`、`day` の 3 つのカラムで構成されています。データファイルは `yyyy-mm-dd hh:mm:ss` 形式の日付と時刻の値を含む 1 つのカラムで構成されています。この場合、`"columns: col, year = year(col), month=month(col), day=day(col)"` と指定できます。この場合、`col` はデータファイルのカラムの一時的な名前であり、関数 `year = year(col)`、`month=month(col)`、`day=day(col)` はデータファイルのカラム `col` からデータを抽出し、対応する StarRocks テーブルのカラムにデータをロードするために使用されます。例えば、`year = year(col)` はデータファイルのカラム `col` から `yyyy` データを抽出し、StarRocks テーブルのカラム `year` にデータをロードするために使用されます。

詳細な例については、 [Configure column mapping](#configure-column-mapping) を参照してください。

### JSON 形式または Avro 形式のデータをロードするためのカラムマッピングを設定する

> **NOTE**
>
> v3.0.1 以降、StarRocks は Routine Load を使用して Avro データのロードをサポートしています。JSON または Avro データをロードする場合、カラムマッピングと変換の設定は同じです。したがって、このセクションでは、JSON データを例にとって設定を紹介します。

JSON 形式のデータのキーが StarRocks テーブルのカラムと同じ名前を持っている場合、シンプルモードを使用して JSON 形式のデータをロードできます。シンプルモードでは、`jsonpaths` パラメータを指定する必要はありません。このモードでは、JSON 形式のデータは `{}` で示されるオブジェクトである必要があります。例えば、`{"category": 1, "author": 2, "price": "3"}` のように。この例では、`category`、`author`、`price` はキー名であり、これらのキーは名前で 1 対 1 で StarRocks テーブルのカラム `category`、`author`、`price` にマッピングできます。詳細な例については、 [simple mode](#starrocks-table-column-names-consistent-with-json-key-names) を参照してください。

JSON 形式のデータのキーが StarRocks テーブルのカラムと異なる名前を持っている場合、マッチドモードを使用して JSON 形式のデータをロードできます。マッチドモードでは、`jsonpaths` および `COLUMNS` パラメータを使用して JSON 形式のデータと StarRocks テーブルの間のカラムマッピングを指定する必要があります:

- `jsonpaths` パラメータでは、JSON 形式のデータに配置されている順序で JSON キーを指定します。
- `COLUMNS` パラメータでは、JSON キーと StarRocks テーブルのカラム間のマッピングを指定します:
  - `COLUMNS` パラメータで指定されたカラム名は、JSON 形式のデータに順番に 1 対 1 でマッピングされます。
  - `COLUMNS` パラメータで指定されたカラム名は、名前で StarRocks テーブルのカラムに 1 対 1 でマッピングされます。

詳細な例については、 [StarRocks table contains derived columns whose values are generated by using expressions](#starrocks-table-contains-derived-columns-whose-values-are-generated-by-using-expressions) を参照してください。

## Examples

### CSV 形式のデータをロードする

このセクションでは、CSV 形式のデータを例にとり、さまざまなパラメータ設定と組み合わせを使用して、多様なロード要件を満たす方法を説明します。

**データセットを準備する**

例えば、Kafka トピック `ordertest1` から CSV 形式のデータをロードしたいとします。データセットの各メッセージには、注文 ID、支払い日、顧客名、国籍、性別、価格の 6 つのカラムが含まれています。

```plaintext
2020050802,2020-05-08,Johann Georg Faust,Deutschland,male,895
2020050802,2020-05-08,Julien Sorel,France,male,893
2020050803,2020-05-08,Dorian Grey,UK,male,1262
2020050901,2020-05-09,Anna Karenina,Russia,female,175
2020051001,2020-05-10,Tess Durbeyfield,US,female,986
2020051101,2020-05-11,Edogawa Conan,japan,male,8924
```

**テーブルを作成する**

CSV 形式のデータのカラムに基づいて、データベース `example_db` に `example_tbl1` という名前のテーブルを作成します。

```SQL
CREATE TABLE example_db.example_tbl1 ( 
    `order_id` bigint NOT NULL COMMENT "Order ID",
    `pay_dt` date NOT NULL COMMENT "Payment date", 
    `customer_name` varchar(26) NULL COMMENT "Customer name", 
    `nationality` varchar(26) NULL COMMENT "Nationality", 
    `gender` varchar(26) NULL COMMENT "Gender", 
    `price` double NULL COMMENT "Price") 
DUPLICATE KEY (order_id,pay_dt) 
DISTRIBUTED BY HASH(`order_id`); 
```

#### 指定されたパーティションの指定されたオフセットからデータを消費する

Routine Load ジョブが指定されたパーティションとオフセットからデータを消費する必要がある場合、`kafka_partitions` および `kafka_offsets` パラメータを設定する必要があります。

```SQL
CREATE ROUTINE LOAD example_db.example_tbl1_ordertest1 ON example_tbl1
COLUMNS TERMINATED BY ",",
COLUMNS (order_id, pay_dt, customer_name, nationality, gender, price)
FROM KAFKA
(
    "kafka_broker_list" ="<kafka_broker1_ip>:<kafka_broker1_port>,<kafka_broker2_ip>:<kafka_broker2_port>",
    "kafka_topic" = "ordertest1",
    "kafka_partitions" ="0,1,2,3,4", -- 消費されるパーティション
    "kafka_offsets" = "1000, OFFSET_BEGINNING, OFFSET_END, 2000" -- 対応する初期オフセット
);
```

#### タスク並行性を増やしてロードパフォーマンスを向上させる

ロードパフォーマンスを向上させ、累積消費を回避するために、Routine Load ジョブを作成する際に `desired_concurrent_number` の値を増やしてタスク並行性を増やすことができます。タスク並行性は、1 つの Routine Load ジョブを可能な限り多くの並行タスクに分割することを可能にします。

実際のタスク並行性は、次の複数のパラメータの最小値によって決定されます:

```SQL
min(alive_be_number, partition_number, desired_concurrent_number, max_routine_load_task_concurrent_num)
```

> **Note**
>
> 最大の実際のタスク並行性は、生存している BE ノードの数または消費されるパーティションの数です。

したがって、生存している BE ノードの数と消費されるパーティションの数が他の 2 つのパラメータ `max_routine_load_task_concurrent_num` と `desired_concurrent_number` の値よりも大きい場合、他の 2 つのパラメータの値を増やして実際のタスク並行性を増やすことができます。

消費されるパーティションの数が 7、生存している BE ノードの数が 5、`max_routine_load_task_concurrent_num` がデフォルト値 `5` であると仮定します。実際のタスク並行性を増やしたい場合、`desired_concurrent_number` を `5` に設定できます（デフォルト値は `3` です）。この場合、実際のタスク並行性 `min(5,7,5,5)` は `5` に設定されます。

```SQL
CREATE ROUTINE LOAD example_db.example_tbl1_ordertest1 ON example_tbl1
COLUMNS TERMINATED BY ",",
COLUMNS (order_id, pay_dt, customer_name, nationality, gender, price)
PROPERTIES
(
"desired_concurrent_number" = "5" -- desired_concurrent_number の値を 5 に設定
)
FROM KAFKA
(
    "kafka_broker_list" ="<kafka_broker1_ip>:<kafka_broker1_port>,<kafka_broker2_ip>:<kafka_broker2_port>",
    "kafka_topic" = "ordertest1"
);
```

#### カラムマッピングを設定する

CSV 形式のデータのカラムの順序がターゲットテーブルのカラムと一致しない場合、CSV 形式のデータの 5 番目のカラムがターゲットテーブルにインポートされる必要がないと仮定して、`COLUMNS` パラメータを通じて CSV 形式のデータとターゲットテーブルの間のカラムマッピングを指定する必要があります。

**ターゲットデータベースとテーブル**

CSV 形式のデータのカラムに基づいて、ターゲットデータベース `example_db` にターゲットテーブル `example_tbl2` を作成します。このシナリオでは、CSV 形式のデータの 5 つのカラムに対応する 5 つのカラムを作成する必要があります。ただし、性別を格納する 5 番目のカラムは除きます。

```SQL
CREATE TABLE example_db.example_tbl2 ( 
    `order_id` bigint NOT NULL COMMENT "Order ID",
    `pay_dt` date NOT NULL COMMENT "Payment date", 
    `customer_name` varchar(26) NULL COMMENT "Customer name", 
    `nationality` varchar(26) NULL COMMENT "Nationality", 
    `price` double NULL COMMENT "Price"
) 
DUPLICATE KEY (order_id,pay_dt) 
DISTRIBUTED BY HASH(order_id); 
```

**Routine Load ジョブ**

この例では、CSV 形式のデータの 5 番目のカラムをターゲットテーブルにロードする必要がないため、5 番目のカラムは `COLUMNS` で一時的に `temp_gender` と名付けられ、他のカラムはテーブル `example_tbl2` に直接マッピングされます。

```SQL
CREATE ROUTINE LOAD example_db.example_tbl2_ordertest1 ON example_tbl2
COLUMNS TERMINATED BY ",",
COLUMNS (order_id, pay_dt, customer_name, nationality, temp_gender, price)
FROM KAFKA
(
    "kafka_broker_list" ="<kafka_broker1_ip>:<kafka_broker1_port>,<kafka_broker2_ip>:<kafka_broker2_port>",
    "kafka_topic" = "ordertest1"
);
```

#### フィルター条件を設定する

特定の条件を満たすデータのみをロードしたい場合、`WHERE` 句でフィルター条件を設定できます。例えば、`price > 100` のように。

```SQL
CREATE ROUTINE LOAD example_db.example_tbl2_ordertest1 ON example_tbl2
COLUMNS TERMINATED BY ",",
COLUMNS (order_id, pay_dt, customer_name, nationality, gender, price),
WHERE price > 100 -- フィルター条件を設定
FROM KAFKA
(
    "kafka_broker_list" ="<kafka_broker1_ip>:<kafka_broker1_port>,<kafka_broker2_ip>:<kafka_broker2_port>",
    "kafka_topic" = "ordertest1"
);
```

#### NULL 値を持つ行をフィルタリングするために strict mode を有効にする

`PROPERTIES` で `"strict_mode" = "true"` を設定することができ、これにより Routine Load ジョブは strict mode になります。ソースカラムに `NULL` 値があるが、宛先の StarRocks テーブルカラムが NULL 値を許可しない場合、ソースカラムに NULL 値を持つ行はフィルタリングされます。

```SQL
CREATE ROUTINE LOAD example_db.example_tbl1_ordertest1 ON example_tbl1
COLUMNS TERMINATED BY ",",
COLUMNS (order_id, pay_dt, customer_name, nationality, gender, price)
PROPERTIES
(
"strict_mode" = "true" -- strict mode を有効にする
)
FROM KAFKA
(
    "kafka_broker_list" ="<kafka_broker1_ip>:<kafka_broker1_port>,<kafka_broker2_ip>:<kafka_broker2_port>",
    "kafka_topic" = "ordertest1"
);
```

#### エラー許容度を設定する

ビジネスシナリオで不適格なデータに対する許容度が低い場合、`max_batch_rows` および `max_error_number` パラメータを設定してエラーデータ検出ウィンドウとエラーデータ行の最大数を設定する必要があります。エラーデータ検出ウィンドウ内のエラーデータ行の数が `max_error_number` の値を超えると、Routine Load ジョブは一時停止します。

```SQL
CREATE ROUTINE LOAD example_db.example_tbl1_ordertest1 ON example_tbl1
COLUMNS TERMINATED BY ",",
COLUMNS (order_id, pay_dt, customer_name, nationality, gender, price)
PROPERTIES
(
"max_batch_rows" = "100000",-- max_batch_rows の値に 10 を掛けた値がエラーデータ検出ウィンドウになります。
"max_error_number" = "100" -- エラーデータ検出ウィンドウ内で許可されるエラーデータ行の最大数。
)
FROM KAFKA
(
    "kafka_broker_list" ="<kafka_broker1_ip>:<kafka_broker1_port>,<kafka_broker2_ip>:<kafka_broker2_port>",
    "kafka_topic" = "ordertest1"
);
```

#### セキュリティプロトコルを SSL として指定し、関連するパラメータを設定する

BE が Kafka にアクセスするために使用するセキュリティプロトコルを SSL として指定する必要がある場合、`"property.security.protocol" = "ssl"` および関連するパラメータを設定する必要があります。

```SQL
CREATE ROUTINE LOAD example_db.example_tbl1_ordertest1 ON example_tbl1
COLUMNS TERMINATED BY ",",
COLUMNS (order_id, pay_dt, customer_name, nationality, gender, price)
PROPERTIES
(
-- セキュリティプロトコルを SSL として指定します。
"property.security.protocol" = "ssl",
-- CA 証明書の場所。
"property.ssl.ca.location" = "FILE:ca-cert",
-- Kafka クライアントの認証が有効な場合、次のプロパティを設定する必要があります:
-- Kafka クライアントの公開鍵の場所。
"property.ssl.certificate.location" = "FILE:client.pem",
-- Kafka クライアントの秘密鍵の場所。
"property.ssl.key.location" = "FILE:client.key",
-- Kafka クライアントの秘密鍵のパスワード。
"property.ssl.key.password" = "abcdefg"
)
FROM KAFKA
(
    "kafka_broker_list" ="<kafka_broker1_ip>:<kafka_broker1_port>,<kafka_broker2_ip>:<kafka_broker2_port>",
    "kafka_topic" = "ordertest1"
);
```

#### trim_space、enclose、および escape を設定する

例えば、Kafka トピック `test_csv` から CSV 形式のデータをロードしたいとします。データセットの各メッセージには、注文 ID、支払い日、顧客名、国籍、性別、価格の 6 つのカラムが含まれています。

```Plaintext
 "2020050802" , "2020-05-08" , "Johann Georg Faust" , "Deutschland" , "male" , "895"
 "2020050802" , "2020-05-08" , "Julien Sorel" , "France" , "male" , "893"
 "2020050803" , "2020-05-08" , "Dorian Grey\,Lord Henry" , "UK" , "male" , "1262"
 "2020050901" , "2020-05-09" , "Anna Karenina" , "Russia" , "female" , "175"
 "2020051001" , "2020-05-10" , "Tess Durbeyfield" , "US" , "female" , "986"
 "2020051101" , "2020-05-11" , "Edogawa Conan" , "japan" , "male" , "8924"
```

Kafka トピック `test_csv` から `example_tbl1` にすべてのデータをロードし、カラム区切り文字の前後のスペースを削除し、`enclose` を `"` に設定し、`escape` を `\` に設定する場合、次のコマンドを実行します:

```SQL
CREATE ROUTINE LOAD example_db.example_tbl1_test_csv ON example_tbl1
COLUMNS TERMINATED BY ",",
COLUMNS (order_id, pay_dt, customer_name, nationality, gender, price)
PROPERTIES
(
    "trim_space"="true",
    "enclose"="\"",
    "escape"="\\",
)
FROM KAFKA
(
    "kafka_broker_list" ="<kafka_broker1_ip>:<kafka_broker1_port>,<kafka_broker2_ip>:<kafka_broker2_port>",
    "kafka_topic"="test_csv",
    "property.kafka_default_offsets"="OFFSET_BEGINNING"
);
```

### JSON 形式のデータをロードする

#### StarRocks テーブルのカラム名が JSON キー名と一致している場合

**データセットを準備する**

例えば、Kafka トピック `ordertest2` に次の JSON 形式のデータが存在します。

```SQL
{"commodity_id": "1", "customer_name": "Mark Twain", "country": "US","pay_time": 1589191487,"price": 875}
{"commodity_id": "2", "customer_name": "Oscar Wilde", "country": "UK","pay_time": 1589191487,"price": 895}
{"commodity_id": "3", "customer_name": "Antoine de Saint-Exupéry","country": "France","pay_time": 1589191487,"price": 895}
```

> **Note** 各 JSON オブジェクトは 1 つの Kafka メッセージ内にある必要があります。そうでない場合、JSON 形式のデータの解析に失敗したことを示すエラーが発生します。

**ターゲットデータベースとテーブル**

StarRocks クラスターのターゲットデータベース `example_db` にテーブル `example_tbl3` を作成します。カラム名は JSON 形式のデータのキー名と一致しています。

```SQL
CREATE TABLE example_db.example_tbl3 ( 
    commodity_id varchar(26) NULL, 
    customer_name varchar(26) NULL, 
    country varchar(26) NULL, 
    pay_time bigint(20) NULL, 
    price double SUM NULL COMMENT "Price") 
AGGREGATE KEY(commodity_id,customer_name,country,pay_time)
DISTRIBUTED BY HASH(commodity_id); 
```

**Routine Load ジョブ**

Routine Load ジョブにはシンプルモードを使用できます。つまり、Routine Load ジョブを作成する際に `jsonpaths` および `COLUMNS` パラメータを指定する必要はありません。StarRocks はターゲットテーブル `example_tbl3` のカラム名に基づいて Kafka クラスターのトピック `ordertest2` の JSON 形式のデータのキーを抽出し、JSON 形式のデータをターゲットテーブルにロードします。

```SQL
CREATE ROUTINE LOAD example_db.example_tbl3_ordertest2 ON example_tbl3
PROPERTIES
(
    "format" = "json"
 )
FROM KAFKA
(
    "kafka_broker_list" = "<kafka_broker1_ip>:<kafka_broker1_port>,<kafka_broker2_ip>:<kafka_broker2_port>",
    "kafka_topic" = "ordertest2"
);
```

> **Note**
>
> - JSON 形式のデータの最外部のレイヤーが配列構造である場合、`PROPERTIES` で `"strip_outer_array"="true"` を設定して最外部の配列構造を削除する必要があります。さらに、`jsonpaths` を指定する必要がある場合、JSON 形式のデータ全体のルート要素はフラットな JSON オブジェクトです。これは JSON 形式のデータの最外部の配列構造が削除されるためです。
> - `json_root` を使用して JSON 形式のデータのルート要素を指定できます。

#### StarRocks テーブルに式を使用して生成された値を持つ派生カラムが含まれている場合

**データセットを準備する**

例えば、Kafka クラスターのトピック `ordertest2` に次の JSON 形式のデータが存在します。

```SQL
{"commodity_id": "1", "customer_name": "Mark Twain", "country": "US","pay_time": 1589191487,"price": 875}
{"commodity_id": "2", "customer_name": "Oscar Wilde", "country": "UK","pay_time": 1589191487,"price": 895}
{"commodity_id": "3", "customer_name": "Antoine de Saint-Exupéry","country": "France","pay_time": 1589191487,"price": 895}
```

**ターゲットデータベースとテーブル**

StarRocks クラスターのデータベース `example_db` に `example_tbl4` という名前のテーブルを作成します。カラム `pay_dt` は、JSON 形式のデータのキー `pay_time` の値を計算して生成された派生カラムです。

```SQL
CREATE TABLE example_db.example_tbl4 ( 
    `commodity_id` varchar(26) NULL, 
    `customer_name` varchar(26) NULL, 
    `country` varchar(26) NULL,
    `pay_time` bigint(20) NULL,  
    `pay_dt` date NULL, 
    `price` double SUM NULL) 
AGGREGATE KEY(`commodity_id`,`customer_name`,`country`,`pay_time`,`pay_dt`) 
DISTRIBUTED BY HASH(`commodity_id`); 
```

**Routine Load ジョブ**

Routine Load ジョブにはマッチドモードを使用できます。つまり、Routine Load ジョブを作成する際に `jsonpaths` および `COLUMNS` パラメータを指定する必要があります。

`jsonpaths` パラメータでは、JSON 形式のデータのキーを指定し、順番に配置します。

また、JSON 形式のデータのキー `pay_time` の値は `example_tbl4` テーブルの `pay_dt` カラムに格納される前に DATE 型に変換する必要があるため、`COLUMNS` で `pay_dt=from_unixtime(pay_time,'%Y%m%d')` を使用して計算を指定する必要があります。JSON 形式のデータの他のキーの値は `example_tbl4` テーブルに直接マッピングできます。

```SQL
CREATE ROUTINE LOAD example_db.example_tbl4_ordertest2 ON example_tbl4
COLUMNS(commodity_id, customer_name, country, pay_time, pay_dt=from_unixtime(pay_time, '%Y%m%d'), price)
PROPERTIES
(
    "format" = "json",
    "jsonpaths" = "[\"$.commodity_id\",\"$.customer_name\",\"$.country\",\"$.pay_time\",\"$.price\"]"
 )
FROM KAFKA
(
    "kafka_broker_list" = "<kafka_broker1_ip>:<kafka_broker1_port>,<kafka_broker2_ip>:<kafka_broker2_port>",
    "kafka_topic" = "ordertest2"
);
```

> **Note**
>
> - JSON データの最外部のレイヤーが配列構造である場合、`PROPERTIES` で `"strip_outer_array"="true"` を設定して最外部の配列構造を削除する必要があります。さらに、`jsonpaths` を指定する必要がある場合、JSON データ全体のルート要素はフラットな JSON オブジェクトです。これは JSON データの最外部の配列構造が削除されるためです。
> - `json_root` を使用して JSON 形式のデータのルート要素を指定できます。

#### StarRocks テーブルに CASE 式を使用して生成された値を持つ派生カラムが含まれている場合

**データセットを準備する**

例えば、Kafka トピック `topic-expr-test` に次の JSON 形式のデータが存在します。

```JSON
{"key1":1, "key2": 21}
{"key1":12, "key2": 22}
{"key1":13, "key2": 23}
{"key1":14, "key2": 24}
```

**ターゲットデータベースとテーブル**

StarRocks クラスターのデータベース `example_db` に `tbl_expr_test` という名前のテーブルを作成します。ターゲットテーブル `tbl_expr_test` には 2 つのカラムが含まれており、`col2` カラムの値は JSON データに対する CASE 式を使用して計算される必要があります。

```SQL
CREATE TABLE tbl_expr_test (
    col1 string, col2 string)
DISTRIBUTED BY HASH (col1);
```

**Routine Load ジョブ**

ターゲットテーブルの `col2` カラムの値が CASE 式を使用して生成されるため、Routine Load ジョブの `COLUMNS` パラメータで対応する式を指定する必要があります。

```SQL
CREATE ROUTINE LOAD rl_expr_test ON tbl_expr_test
COLUMNS (
      key1,
      key2,
      col1 = key1,
      col2 = CASE WHEN key1 = "1" THEN "key1=1" 
                  WHEN key1 = "12" THEN "key1=12"
                  ELSE "nothing" END) 
PROPERTIES ("format" = "json")
FROM KAFKA
(
    "kafka_broker_list" = "<kafka_broker1_ip>:<kafka_broker1_port>,<kafka_broker2_ip>:<kafka_broker2_port>",
    "kafka_topic" = "topic-expr-test"
);
```

**StarRocks テーブルをクエリする**

StarRocks テーブルをクエリします。結果は、`col2` カラムの値が CASE 式の出力であることを示しています。

```SQL
MySQL [example_db]> SELECT * FROM tbl_expr_test;
+------+---------+
| col1 | col2    |
+------+---------+
| 1    | key1=1  |
| 12   | key1=12 |
| 13   | nothing |
| 14   | nothing |
+------+---------+
4 rows in set (0.015 sec)
```

#### ロードする JSON 形式のデータのルート要素を指定する

ロードする JSON 形式のデータのルート要素を指定するには、`json_root` を使用し、その値は有効な JsonPath 式である必要があります。

**データセットを準備する**

例えば、Kafka クラスターのトピック `ordertest3` に次の JSON 形式のデータが存在します。そして、ロードする JSON 形式のデータのルート要素は `$.RECORDS` です。

```SQL
{"RECORDS":[{"commodity_id": "1", "customer_name": "Mark Twain", "country": "US","pay_time": 1589191487,"price": 875},{"commodity_id": "2", "customer_name": "Oscar Wilde", "country": "UK","pay_time": 1589191487,"price": 895},{"commodity_id": "3", "customer_name": "Antoine de Saint-Exupéry","country": "France","pay_time": 1589191487,"price": 895}]}
```

**ターゲットデータベースとテーブル**

StarRocks クラスターのデータベース `example_db` に `example_tbl3` という名前のテーブルを作成します。

```SQL
CREATE TABLE example_db.example_tbl3 ( 
    commodity_id varchar(26) NULL, 
    customer_name varchar(26) NULL, 
    country varchar(26) NULL, 
    pay_time bigint(20) NULL, 
    price double SUM NULL) 
AGGREGATE KEY(commodity_id,customer_name,country,pay_time) 
ENGINE=OLAP
DISTRIBUTED BY HASH(commodity_id); 
```

**Routine Load ジョブ**

`PROPERTIES` で `"json_root" = "$.RECORDS"` を設定して、ロードする JSON 形式のデータのルート要素を指定できます。また、ロードする JSON 形式のデータが配列構造になっているため、最外部の配列構造を削除するために `"strip_outer_array" = "true"` も設定する必要があります。

```SQL
CREATE ROUTINE LOAD example_db.example_tbl3_ordertest3 ON example_tbl3
PROPERTIES
(
    "format" = "json",
    "json_root" = "$.RECORDS",
    "strip_outer_array" = "true"
 )
FROM KAFKA
(
    "kafka_broker_list" = "<kafka_broker1_ip>:<kafka_broker1_port>,<kafka_broker2_ip>:<kafka_broker2_port>",
    "kafka_topic" = "ordertest2"
);
```

### Avro 形式のデータをロードする

v3.0.1 以降、StarRocks は Routine Load を使用して Avro データのロードをサポートしています。

#### Avro スキーマがシンプルな場合

Avro スキーマが比較的シンプルで、Avro データのすべてのフィールドをロードする必要があると仮定します。

**データセットを準備する**

- **Avro スキーマ**

    1. 次の Avro スキーマファイル `avro_schema1.avsc` を作成します:

        ```json
        {
            "type": "record",
            "name": "sensor_log",
            "fields" : [
                {"name": "id", "type": "long"},
                {"name": "name", "type": "string"},
                {"name": "checked", "type" : "boolean"},
                {"name": "data", "type": "double"},
                {"name": "sensor_type", "type": {"type": "enum", "name": "sensor_type_enum", "symbols" : ["TEMPERATURE", "HUMIDITY", "AIR-PRESSURE"]}}  
            ]
        }
        ```

    2. Avro スキーマを [Schema Registry](https://docs.confluent.io/platform/current/schema-registry/index.html) に登録します。

- **Avro データ**

Avro データを準備し、Kafka トピック `topic_1` に送信します。

**ターゲットデータベースとテーブル**

Avro データのフィールドに基づいて、StarRocks クラスターのターゲットデータベース `sensor` に `sensor_log1` という名前のテーブルを作成します。テーブルのカラム名は Avro データのフィールド名と一致している必要があります。Avro データが StarRocks にロードされる際のデータ型のマッピングについては、 [Data types mapping](#Data types mapping) を参照してください。

```SQL
CREATE TABLE sensor.sensor_log1 ( 
    `id` bigint NOT NULL COMMENT "sensor id",
    `name` varchar(26) NOT NULL COMMENT "sensor name", 
    `checked` boolean NOT NULL COMMENT "checked", 
    `data` double NULL COMMENT "sensor data", 
    `sensor_type` varchar(26) NOT NULL COMMENT "sensor type"
) 
ENGINE=OLAP 
DUPLICATE KEY (id) 
DISTRIBUTED BY HASH(`id`); 
```

**Routine Load ジョブ**

Routine Load ジョブにはシンプルモードを使用できます。つまり、Routine Load ジョブを作成する際に `jsonpaths` パラメータを指定する必要はありません。次のステートメントを実行して、Kafka トピック `topic_1` の Avro メッセージを消費し、データをデータベース `sensor` のテーブル `sensor_log1` にロードする Routine Load ジョブ `sensor_log_load_job1` を送信します。

```sql
CREATE ROUTINE LOAD sensor.sensor_log_load_job1 ON sensor_log1  
PROPERTIES  
(  
  "format" = "avro"  
)  
FROM KAFKA  
(  
  "kafka_broker_list" = "<kafka_broker1_ip>:<kafka_broker1_port>,<kafka_broker2_ip>:<kafka_broker2_port>,...",
  "confluent.schema.registry.url" = "http://172.xx.xxx.xxx:8081",  
  "kafka_topic"= "topic_1",  
  "kafka_partitions" = "0,1,2,3,4,5",  
  "property.kafka_default_offsets" = "OFFSET_BEGINNING"  
);
```

#### Avro スキーマにネストされたレコード型フィールドが含まれている場合

Avro スキーマにネストされたレコード型フィールドが含まれており、StarRocks にネストされたレコード型フィールドのサブフィールドをロードする必要があると仮定します。

**データセットを準備する**

- **Avro スキーマ**

    1. 次の Avro スキーマファイル `avro_schema2.avsc` を作成します。外部の Avro レコードには、順番に `id`、`name`、`checked`、`sensor_type`、および `data` の 5 つのフィールドが含まれています。そして、フィールド `data` にはネストされたレコード `data_record` があります。

        ```JSON
        {
            "type": "record",
            "name": "sensor_log",
            "fields" : [
                {"name": "id", "type": "long"},
                {"name": "name", "type": "string"},
                {"name": "checked", "type" : "boolean"},
                {"name": "sensor_type", "type": {"type": "enum", "name": "sensor_type_enum", "symbols" : ["TEMPERATURE", "HUMIDITY", "AIR-PRESSURE"]}},
                {"name": "data", "type": 
                    {
                        "type": "record",
                        "name": "data_record",
                        "fields" : [
                            {"name": "data_x", "type" : "boolean"},
                            {"name": "data_y", "type": "long"}
                        ]
                    }
                }
            ]
        }
        ```

    2. Avro スキーマを [Schema Registry](https://docs.confluent.io/platform/current/schema-registry/index.html) に登録します。

- **Avro データ**

Avro データを準備し、Kafka トピック `topic_2` に送信します。

**ターゲットデータベースとテーブル**

Avro データのフィールドに基づいて、StarRocks クラスターのターゲットデータベース `sensor` に `sensor_log2` という名前のテーブルを作成します。

外部レコードのフィールド `id`、`name`、`checked`、および `sensor_type` に加えて、ネストされたレコード `data_record` のサブフィールド `data_y` もロードする必要があると仮定します。

```sql
CREATE TABLE sensor.sensor_log2 ( 
    `id` bigint NOT NULL COMMENT "sensor id",
    `name` varchar(26) NOT NULL COMMENT "sensor name", 
    `checked` boolean NOT NULL COMMENT "checked", 
    `sensor_type` varchar(26) NOT NULL COMMENT "sensor type",
    `data_y` long NULL COMMENT "sensor data" 
) 
ENGINE=OLAP 
DUPLICATE KEY (id) 
DISTRIBUTED BY HASH(`id`); 
```

**Routine Load ジョブ**

ロードジョブを送信し、`jsonpaths` を使用してロードする必要がある Avro データのフィールドを指定します。ネストされたレコードのサブフィールド `data_y` については、その `jsonpath` を `"$.data.data_y"` として指定する必要があります。

```sql
CREATE ROUTINE LOAD sensor.sensor_log_load_job2 ON sensor_log2  
PROPERTIES  
(  
  "format" = "avro",
  "jsonpaths" = "[\"$.id\",\"$.name\",\"$.checked\",\"$.sensor_type\",\"$.data.data_y\"]"
)  
FROM KAFKA  
(  
  "kafka_broker_list" = "<kafka_broker1_ip>:<kafka_broker1_port>,<kafka_broker2_ip>:<kafka_broker2_port>,...",
  "confluent.schema.registry.url" = "http://172.xx.xxx.xxx:8081",  
  "kafka_topic" = "topic_1",  
  "kafka_partitions" = "0,1,2,3,4,5",  
  "property.kafka_default_offsets" = "OFFSET_BEGINNING"  
);
```

#### Avro スキーマに Union フィールドが含まれている場合

**データセットを準備する**

Avro スキーマに Union フィールドが含まれており、Union フィールドを StarRocks にロードする必要があると仮定します。

- **Avro スキーマ**

    1. 次の Avro スキーマファイル `avro_schema3.avsc` を作成します。外部の Avro レコードには、順番に `id`、`name`、`checked`、`sensor_type`、および `data` の 5 つのフィールドが含まれています。そして、フィールド `data` は Union 型であり、2 つの要素 `null` とネストされたレコード `data_record` を含んでいます。

```JSON
{
    "type": "record",
    "name": "sensor_log",
    "fields" : [
        {"name": "id", "type": "long"},
        {"name": "name", "type": "string"},
        {"name": "checked", "type" : "boolean"},
        {"name": "sensor_type", "type": {"type": "enum", "name": "sensor_type_enum", "symbols" : ["TEMPERATURE", "HUMIDITY", "AIR-PRESSURE"]}},
        {"name": "data", "type": [null,
                {
                    "type": "record",
                    "name": "data_record",
                    "fields" : [
                        {"name": "data_x", "type" : "boolean"},
                        {"name": "data_y", "type": "long"}
                    ]
                }
            ]
        }
    ]
}
```

2. Avro スキーマを [Schema Registry](https://docs.confluent.io/platform/current/schema-registry/index.html) に登録します。

- **Avro データ**

Avro データを準備し、Kafka トピック `topic_3` に送信します。

**ターゲットデータベースとテーブル**

Avro データのフィールドに基づいて、StarRocks クラスターのターゲットデータベース `sensor` に `sensor_log3` という名前のテーブルを作成します。

外部レコードのフィールド `id`、`name`、`checked`、および `sensor_type` に加えて、Union 型フィールド `data` の要素 `data_record` のフィールド `data_y` もロードする必要があると仮定します。

```sql
CREATE TABLE sensor.sensor_log3 ( 
    `id` bigint NOT NULL COMMENT "sensor id",
    `name` varchar(26) NOT NULL COMMENT "sensor name", 
    `checked` boolean NOT NULL COMMENT "checked", 
    `sensor_type` varchar(26) NOT NULL COMMENT "sensor type",
    `data_y` long NULL COMMENT "sensor data" 
) 
ENGINE=OLAP 
DUPLICATE KEY (id) 
DISTRIBUTED BY HASH(`id`); 
```

**Routine Load ジョブ**

ロードジョブを送信し、`jsonpaths` を使用して Avro データでロードする必要があるフィールドを指定します。フィールド `data_y` については、その `jsonpath` を `"$.data.data_y"` として指定する必要があります。

```sql
CREATE ROUTINE LOAD sensor.sensor_log_load_job3 ON sensor_log3  
PROPERTIES  
(  
  "format" = "avro",
  "jsonpaths" = "[\"$.id\",\"$.name\",\"$.checked\",\"$.sensor_type\",\"$.data.data_y\"]"
)  
FROM KAFKA  
(  
  "kafka_broker_list" = "<kafka_broker1_ip>:<kafka_broker1_port>,<kafka_broker2_ip>:<kafka_broker2_port>,...",
  "confluent.schema.registry.url" = "http://172.xx.xxx.xxx:8081",  
  "kafka_topic" = "topic_1",  
  "kafka_partitions" = "0,1,2,3,4,5",  
  "property.kafka_default_offsets" = "OFFSET_BEGINNING"  
);
```

Union 型フィールド `data` の値が `null` の場合、StarRocks テーブルのカラム `data_y` にロードされる値は `null` です。Union 型フィールド `data` の値がデータレコードの場合、カラム `data_y` にロードされる値は Long 型です。