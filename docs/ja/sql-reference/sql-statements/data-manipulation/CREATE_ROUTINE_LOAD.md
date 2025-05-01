---
displayed_sidebar: docs
---

# CREATE ROUTINE LOAD

## 説明

Routine Load は Apache Kafka® からメッセージを継続的に消費し、StarRocks にデータをロードします。Routine Load は Kafka クラスターから CSV および JSON データを消費し、`plaintext`、`ssl`、`sasl_plaintext`、`sasl_ssl` などの複数のセキュリティプロトコルを介して Kafka にアクセスできます。

このトピックでは、CREATE ROUTINE LOAD ステートメントの構文、パラメーター、および例について説明します。

> **NOTE**
>
> Routine Load の適用シナリオ、原則、および基本操作については、 [Continuously load data from Apache Kafka®](../../../loading/RoutineLoad.md) を参照してください。

## 構文

```SQL
CREATE ROUTINE LOAD <database_name>.<job_name> ON <table_name>
[load_properties]
[job_properties]
FROM data_source
[data_source_properties]
```

## パラメーター

### `database_name`, `job_name`, `table_name`

`database_name`

オプション。StarRocks データベースの名前。

`job_name`

必須。Routine Load ジョブの名前。1 つのテーブルは複数の Routine Load ジョブからデータを受け取ることができます。識別可能な情報、例えば Kafka トピック名やおおよそのジョブ作成時間を使用して、意味のある Routine Load ジョブ名を設定することをお勧めします。同じデータベース内で Routine Load ジョブの名前は一意でなければなりません。

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

CSV 形式データのカラム区切り文字。デフォルトのカラム区切り文字は `\t` (タブ) です。例えば、カラム区切り文字をカンマに指定するには `COLUMNS TERMINATED BY ","` を使用します。

> **Note**
>
> - ここで指定したカラム区切り文字が、取り込むデータのカラム区切り文字と同じであることを確認してください。
> - カンマ (,) やタブ、パイプ (|) など、長さが 50 バイトを超えない UTF-8 文字列をテキストデリミタとして使用できます。
> - Null 値は `\N` を使用して示されます。例えば、データレコードが 3 つのカラムで構成されており、データレコードが第 1 カラムと第 3 カラムにデータを保持しているが、第 2 カラムにはデータを保持していない場合、この状況では第 2 カラムに `\N` を使用して Null 値を示す必要があります。つまり、レコードは `a,\N,b` としてコンパイルされなければならず、`a,,b` ではありません。`a,,b` はレコードの第 2 カラムが空の文字列を保持していることを示します。

`ROWS TERMINATED BY`

CSV 形式データの行区切り文字。デフォルトの行区切り文字は `\n` です。

`COLUMNS`

ソースデータのカラムと StarRocks テーブルのカラム間のマッピング。このトピックの [Column mapping](#column-mapping) を参照してください。

- `column_name`: ソースデータのカラムが計算なしで StarRocks テーブルのカラムにマッピングできる場合、カラム名を指定するだけで済みます。これらのカラムはマッピングされたカラムと呼ばれます。
- `column_assignment`: ソースデータのカラムが直接 StarRocks テーブルのカラムにマッピングできず、データロード前に関数を使用してカラムの値を計算する必要がある場合、`expr` で計算関数を指定する必要があります。これらのカラムは派生カラムと呼ばれます。StarRocks は最初にマッピングされたカラムを解析するため、派生カラムはマッピングされたカラムの後に配置することをお勧めします。

`WHERE`

フィルター条件。フィルター条件を満たすデータのみが StarRocks にロードされます。例えば、`col1` の値が `100` より大きく、`col2` の値が `1000` と等しい行のみを取り込みたい場合、`WHERE col1 > 100 and col2 = 1000` を使用できます。

> **NOTE**
>
> フィルター条件で指定されたカラムは、ソースカラムまたは派生カラムであることができます。

`PARTITION`

StarRocks テーブルがパーティション p0, p1, p2, p3 に分散されており、p1, p2, p3 にのみデータをロードし、p0 に保存されるデータをフィルタリングしたい場合、フィルター条件として `PARTITION(p1, p2, p3)` を指定できます。デフォルトでは、このパラメーターを指定しない場合、データはすべてのパーティションにロードされます。例:

```SQL
PARTITION (p1, p2, p3)
```

`TEMPORARY PARTITION`

データをロードしたい [temporary partition](../../../table_design/Temporary_partition.md) の名前。複数の一時パーティションを指定する場合、カンマ (,) で区切る必要があります。

### `job_properties`

必須。ロードジョブのプロパティ。構文:

```SQL
PROPERTIES ("<key1>" = "<value1>"[, "<key2>" = "<value2>" ...])
```

| **Property**              | **Required** | **Description**                                              |
| ------------------------- | ------------ | ------------------------------------------------------------ |
| desired_concurrent_number | No           | 単一の Routine Load ジョブの期待されるタスク並行性。デフォルト値: `3`。実際のタスク並行性は、複数のパラメーターの最小値によって決定されます: `min(alive_be_number, partition_number, desired_concurrent_number, max_routine_load_task_concurrent_num)`。<ul><li>`alive_be_number`: 生存している BE ノードの数。</li><li>`partition_number`: 消費されるパーティションの数。</li><li>`desired_concurrent_number`: 単一の Routine Load ジョブの期待されるタスク並行性。デフォルト値: `3`。</li><li>`max_routine_load_task_concurrent_num`: Routine Load ジョブのデフォルトの最大タスク並行性で、`5` です。 [FE dynamic parameter](../../../administration/Configuration.md#configure-fe-dynamic-parameters) を参照してください。</li></ul>最大の実際のタスク並行性は、生存している BE ノードの数または消費されるパーティションの数によって決定されます。|
| max_batch_interval        | No           | タスクのスケジューリング間隔、つまりタスクが実行される頻度。単位: 秒。値の範囲: `5` ~ `60`。デフォルト値: `10`。`10` より大きい値を設定することをお勧めします。スケジューリングが 10 秒未満の場合、ロード頻度が高すぎるために多くのタブレットバージョンが生成されます。 |
| max_batch_rows            | No           | このプロパティは、エラーデータ検出ウィンドウを定義するためにのみ使用されます。ウィンドウは、単一の Routine Load タスクによって消費されるデータ行の数です。値は `10 * max_batch_rows` です。デフォルト値は `10 * 200000 = 2000000` です。Routine Load タスクは、エラーデータ検出ウィンドウ内でエラーデータを検出します。エラーデータとは、無効な JSON 形式データなど、StarRocks が解析できないデータを指します。 |
| max_error_number          | No           | エラーデータ検出ウィンドウ内で許可されるエラーデータ行の最大数。この値を超えると、ロードジョブは一時停止します。 [SHOW ROUTINE LOAD](./SHOW_ROUTINE_LOAD.md) を実行し、`ErrorLogUrls` を使用してエラーログを表示できます。その後、エラーログに従って Kafka のエラーを修正できます。デフォルト値は `0` で、エラーデータ行は許可されません。<br />**NOTE** <br /> <ul><li>エラーデータ行には、WHERE 句によってフィルタリングされたデータ行は含まれません。</li><li>このパラメーターと次のパラメーター `max_filter_ratio` は、エラーデータレコードの最大数を制御します。`max_filter_ratio` が設定されていない場合、このパラメーターの値が有効です。`max_filter_ratio` が設定されている場合、エラーデータレコードの数がこのパラメーターまたは `max_filter_ratio` パラメーターで設定されたしきい値に達すると、ロードジョブは一時停止します。</li></ul>|
|max_filter_ratio|No|ロードジョブの最大エラー許容度。エラー許容度は、ロードジョブによって要求されたすべてのデータレコードの中で、データ品質が不十分なためにフィルタリングされるデータレコードの最大割合です。有効な値: `0` から `1`。デフォルト値: `0`。<br/>デフォルト値 `0` を保持することをお勧めします。これにより、不適格なデータレコードが検出された場合、ロードジョブが一時停止し、データの正確性が保証されます。<br/>不適格なデータレコードを無視したい場合、このパラメーターを `0` より大きい値に設定できます。これにより、データファイルに不適格なデータレコードが含まれていても、ロードジョブは成功します。  <br/>**NOTE**<br/><ul><li>不適格なデータレコードには、WHERE 句によってフィルタリングされたデータレコードは含まれません。</li><li>このパラメーターと最後のパラメーター `max_error_number` は、エラーデータレコードの最大数を制御します。このパラメーターが設定されていない場合、`max_error_number` パラメーターの値が有効です。このパラメーターが設定されている場合、エラーデータレコードの数がこのパラメーターまたは `max_error_number` パラメーターで設定されたしきい値に達すると、ロードジョブは一時停止します。</li></ul>|
| strict_mode               | No           | [strict mode](../../../loading/load_concept/strict_mode.md) を有効にするかどうかを指定します。有効な値: `true` と `false`。デフォルト値: `false`。strict mode が有効な場合、ロードされたデータのカラムの値が `NULL` であり、ターゲットテーブルがこのカラムに `NULL` 値を許可しない場合、データ行はフィルタリングされます。 |
| timezone                  | No           | ロードジョブで使用されるタイムゾーン。デフォルト値: `Asia/Shanghai`。このパラメーターの値は、strftime()、alignment_timestamp()、from_unixtime() などの関数によって返される結果に影響します。このパラメーターで指定されたタイムゾーンはセッションレベルのタイムゾーンです。詳細については、 [Configure a time zone](../../../administration/timezone.md) を参照してください。 |
| merge_condition           | No           | データを更新するかどうかを判断する条件として使用するカラムの名前を指定します。このカラムにロードされるデータの値がこのカラムの現在の値以上の場合にのみデータが更新されます。詳細については、 [Change data through loading](../../../loading/Load_to_Primary_Key_tables.md) を参照してください。<br />**NOTE**<br />主キーテーブルのみが条件付き更新をサポートします。指定するカラムは主キーのカラムではありません。 |
| format                    | No           | ロードするデータの形式。有効な値: `CSV` と `JSON`。デフォルト値: `CSV`。 |
| merge_condition           | No           | データを更新するかどうかを判断する条件として使用するカラムの名前を指定します。このカラムにロードされるデータの値がこのカラムの現在の値以上の場合にのみデータが更新されます。詳細については、 [Change data through loading](../../../loading/Load_to_Primary_Key_tables.md) を参照してください。<br />**NOTE**<br />主キーテーブルのみが条件付き更新をサポートします。指定するカラムは主キーのカラムではありません。 |
| strip_outer_array         | No           | JSON 形式データの最外部の配列構造を削除するかどうかを指定します。有効な値: `true` と `false`。デフォルト値: `false`。実際のビジネスシナリオでは、JSON 形式データは最外部の配列構造を持つことがあります。この場合、StarRocks が最外部の角括弧 `[]` を削除し、各内部配列を個別のデータレコードとしてロードするように、このパラメーターを `true` に設定することをお勧めします。このパラメーターを `false` に設定すると、StarRocks は JSON 形式データ全体を 1 つの配列として解析し、配列を 1 つのデータレコードとしてロードします。JSON 形式データ `[{"category" : 1, "author" : 2}, {"category" : 3, "author" : 4} ]` を例にとると、このパラメーターを `true` に設定すると、`{"category" : 1, "author" : 2}` と `{"category" : 3, "author" : 4}` は 2 つの個別のデータレコードとして解析され、2 つの StarRocks データ行にロードされます。 |
| jsonpaths                 | No           | JSON 形式データからロードしたいフィールドの名前。このパラメーターの値は有効な JsonPath 式です。詳細については、このトピックの [Configure column mapping for loading JSON-formatted data](#configure-column-mapping-for-loading-json-formatted-data) を参照してください。 |
| json_root                 | No           | ロードする JSON 形式データのルート要素。StarRocks は `json_root` を通じてルートノードの要素を抽出して解析します。デフォルトでは、このパラメーターの値は空で、すべての JSON 形式データがロードされることを示します。詳細については、このトピックの [Specify the root element of the JSON-formatted data to be loaded](#specify-the-root-element-of-the-json-formatted-data-to-be-loaded) を参照してください。 |

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
| kafka_broker_list | Yes      | Kafka のブローカー接続情報。形式は `<kafka_broker_ip>:<broker_ port>` です。複数のブローカーはカンマ (,) で区切られます。Kafka ブローカーが使用するデフォルトポートは `9092` です。例: `"kafka_broker_list" = ""xxx.xx.xxx.xx:9092,xxx.xx.xxx.xx:9092"`。 |
| kafka_topic       | Yes      | 消費される Kafka トピック。Routine Load ジョブは 1 つのトピックからのみメッセージを消費できます。 |
| kafka_partitions  | No       | 消費される Kafka パーティション。例: `"kafka_partitions" = "0, 1, 2, 3"`。このプロパティが指定されていない場合、デフォルトですべてのパーティションが消費されます。 |
| kafka_offsets     | No       | `kafka_partitions` で指定された Kafka パーティションでデータを消費し始めるオフセット。このプロパティが指定されていない場合、Routine Load ジョブは `kafka_partitions` の最新のオフセットからデータを消費します。有効な値:<ul><li>特定のオフセット: 特定のオフセットからデータを消費します。</li><li>`OFFSET_BEGINNING`: 可能な限り最も早いオフセットからデータを消費します。</li><li>`OFFSET_END`: 最新のオフセットからデータを消費します。</li></ul> 複数の開始オフセットはカンマ (,) で区切られます。例: `"kafka_offsets" = "1000, OFFSET_BEGINNING, OFFSET_END, 2000"`。|
| property.kafka_default_offsets| No| すべての消費者パーティションのデフォルトの開始オフセット。このプロパティでサポートされる値は、`kafka_offsets` プロパティの値と同じです。|

**その他のデータソース関連プロパティ**

Kafka コマンドライン `--property` を使用するのと同等の追加のデータソース (Kafka) 関連プロパティを指定できます。サポートされているその他のプロパティについては、 [librdkafka configuration properties](https://github.com/confluentinc/librdkafka/blob/master/CONFIGURATION.md) の Kafka コンシューマークライアントのプロパティを参照してください。

> **NOTE**
>
> プロパティの値がファイル名の場合、ファイル名の前にキーワード `FILE:` を追加してください。ファイルの作成方法については、 [CREATE FILE](../Administration/CREATE_FILE.md) を参照してください。

- **消費されるすべてのパーティションのデフォルトの初期オフセットを指定する**

```SQL
"property.kafka_default_offsets" = "OFFSET_BEGINNING"
```

- **Routine Load ジョブで使用されるコンシューマーグループの ID を指定する**

```SQL
"property.group.id" = "group_id_0"
```

`property.group.id` が指定されていない場合、StarRocks は Routine Load ジョブの名前に基づいてランダムな値を生成します。形式は `{job_name}_{random uuid}` で、例: `simple_job_0a64fe25-3983-44b2-a4d8-f52d3af4c3e8`。

- **BE が Kafka にアクセスするために使用するセキュリティプロトコルと関連パラメーターを指定する**

  セキュリティプロトコルは `plaintext` (デフォルト)、`ssl`、`sasl_plaintext`、または `sasl_ssl` として指定できます。指定されたセキュリティプロトコルに応じて関連するパラメーターを設定する必要があります。

  セキュリティプロトコルが `sasl_plaintext` または `sasl_ssl` に設定されている場合、次の SASL 認証メカニズムがサポートされます:

  - PLAIN
  - SCRAM-SHA-256 および SCRAM-SHA-512
  - OAUTHBEARER

  **例:**

  - SSL セキュリティプロトコルを使用して Kafka にアクセスする:

    ```SQL
    "property.security.protocol" = "ssl", -- セキュリティプロトコルを SSL として指定します。
    "property.ssl.ca.location" = "FILE:ca-cert", -- Kafka ブローカーのキーを検証するための CA 証明書のファイルまたはディレクトリパス。
    -- Kafka サーバーがクライアント認証を有効にしている場合、次の 3 つのパラメーターも必要です:
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

### FE および BE の設定項目

Routine Load に関連する FE および BE の設定項目については、 [configuration items](../../../administration/Configuration.md) を参照してください。

## カラムマッピング

### CSV 形式データのロード用カラムマッピングの設定

CSV 形式データのカラムが StarRocks テーブルのカラムに順番に 1 対 1 でマッピングできる場合、データと StarRocks テーブル間のカラムマッピングを設定する必要はありません。

CSV 形式データのカラムが StarRocks テーブルのカラムに順番に 1 対 1 でマッピングできない場合、`columns` パラメーターを使用してデータファイルと StarRocks テーブル間のカラムマッピングを設定する必要があります。これには次の 2 つのユースケースが含まれます:

- **カラム数は同じだがカラムの順序が異なる。また、データファイルからのデータは、StarRocks テーブルの対応するカラムにロードされる前に関数によって計算される必要はありません。**

  - `columns` パラメーターでは、データファイルのカラムが配置されている順序と同じ順序で StarRocks テーブルのカラム名を指定する必要があります。

  - 例えば、StarRocks テーブルは順番に `col1`、`col2`、`col3` の 3 つのカラムで構成され、データファイルも 3 つのカラムで構成されており、順番に StarRocks テーブルのカラム `col3`、`col2`、`col1` にマッピングできます。この場合、`"columns: col3, col2, col1"` を指定する必要があります。

- **カラム数が異なり、カラムの順序も異なる。また、データファイルからのデータは、StarRocks テーブルの対応するカラムにロードされる前に関数によって計算される必要があります。**

  `columns` パラメーターでは、データファイルのカラムが配置されている順序と同じ順序で StarRocks テーブルのカラム名を指定し、データを計算するために使用する関数を指定する必要があります。以下の 2 つの例があります:

  - StarRocks テーブルは順番に `col1`、`col2`、`col3` の 3 つのカラムで構成されています。データファイルは 4 つのカラムで構成されており、そのうち最初の 3 つのカラムは順番に StarRocks テーブルのカラム `col1`、`col2`、`col3` にマッピングでき、4 番目のカラムは StarRocks テーブルのカラムのいずれにもマッピングできません。この場合、データファイルの 4 番目のカラムに一時的な名前を指定する必要があり、その一時的な名前は StarRocks テーブルのカラム名のいずれとも異なる必要があります。例えば、`"columns: col1, col2, col3, temp"` と指定できます。この場合、データファイルの 4 番目のカラムは一時的に `temp` と名付けられます。
  - StarRocks テーブルは順番に `year`、`month`、`day` の 3 つのカラムで構成されています。データファイルは `yyyy-mm-dd hh:mm:ss` 形式の日付と時刻の値を含む 1 つのカラムのみで構成されています。この場合、`"columns: col, year = year(col), month=month(col), day=day(col)"` と指定できます。この場合、`col` はデータファイルのカラムの一時的な名前であり、関数 `year = year(col)`、`month=month(col)`、`day=day(col)` はデータファイルのカラム `col` からデータを抽出し、StarRocks テーブルの対応するカラムにデータをロードするために使用されます。例えば、`year = year(col)` はデータファイルのカラム `col` から `yyyy` データを抽出し、StarRocks テーブルのカラム `year` にデータをロードするために使用されます。

詳細な例については、 [Configure column mapping](#configure-column-mapping) を参照してください。

### JSON 形式データのロード用カラムマッピングの設定

JSON 形式データのキーが StarRocks テーブルのカラムと同じ名前を持つ場合、シンプルモードを使用して JSON 形式データをロードできます。シンプルモードでは、`jsonpaths` パラメーターを指定する必要はありません。このモードでは、JSON 形式データは `{}` で示されるオブジェクトである必要があります。例えば、`{"category": 1, "author": 2, "price": "3"}` のように。この例では、`category`、`author`、`price` はキー名であり、これらのキーは名前で 1 対 1 で StarRocks テーブルのカラム `category`、`author`、`price` にマッピングできます。詳細な例については、 [simple mode](#Column names of the target table are consistent with JSON keys) を参照してください。

JSON 形式データのキーが StarRocks テーブルのカラムと異なる名前を持つ場合、マッチモードを使用して JSON 形式データをロードできます。マッチモードでは、`jsonpaths` および `COLUMNS` パラメーターを使用して JSON 形式データと StarRocks テーブル間のカラムマッピングを指定する必要があります:

- `jsonpaths` パラメーターでは、JSON 形式データに配置されている順序で JSON キーを指定します。
- `COLUMNS` パラメーターでは、JSON キーと StarRocks テーブルのカラム間のマッピングを指定します:
  - `COLUMNS` パラメーターで指定されたカラム名は、JSON 形式データと順番に 1 対 1 でマッピングされます。
  - `COLUMNS` パラメーターで指定されたカラム名は、名前で 1 対 1 で StarRocks テーブルのカラムにマッピングされます。

詳細な例については、 [matched mode](#starrocks-table-column-names-different-from-json-key-names) を参照してください。

## 例

### CSV 形式データのロード

このセクションでは、CSV 形式データを例にとり、さまざまなパラメーター設定と組み合わせを使用して、さまざまなロード要件を満たす方法を説明します。

**データセットの準備**

例えば、Kafka トピック `ordertest1` から CSV 形式データをロードしたいとします。データセット内の各メッセージには、注文 ID、支払い日、顧客名、国籍、性別、価格の 6 つのカラムが含まれています。

```undefined
2020050802,2020-05-08,Johann Georg Faust,Deutschland,male,895
2020050802,2020-05-08,Julien Sorel,France,male,893
2020050803,2020-05-08,Dorian Grey,UK,male,1262
2020050901,2020-05-09,Anna Karenina,Russia,female,175
2020051001,2020-05-10,Tess Durbeyfield,US,female,986
2020051101,2020-05-11,Edogawa Conan,japan,male,8924
```

**テーブルの作成**

CSV 形式データのカラムに基づいて、データベース `example_db` に `example_tbl1` という名前のテーブルを作成します。

```SQL
CREATE TABLE example_db.example_tbl1 ( 
    `order_id` bigint NOT NULL COMMENT "Order ID",
    `pay_dt` date NOT NULL COMMENT "Payment date", 
    `customer_name` varchar(26) NULL COMMENT "Customer name", 
    `nationality` varchar(26) NULL COMMENT "Nationality", 
    `gender` varchar(26) NULL COMMENT "Gender", 
    `price` double NULL COMMENT "Price") 
DUPLICATE KEY (order_id,pay_dt) 
DISTRIBUTED BY HASH(`order_id`) BUCKETS 5; 
```

#### 指定されたパーティションの指定されたオフセットからデータを消費する

Routine Load ジョブが指定されたパーティションとオフセットからデータを消費する必要がある場合、`kafka_partitions` および `kafka_offsets` パラメーターを設定する必要があります。

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

> **Note**
>
> ロードパフォーマンスを向上させるためのその他の方法については、 [Routine Load FAQ](../../../faq/loading/Routine_load_faq.md) を参照してください。

実際のタスク並行性は、次の複数のパラメーターの最小値によって決定されます:

```SQL
min(alive_be_number, partition_number, desired_concurrent_number, max_routine_load_task_concurrent_num)
```

> **Note**
>
> 最大の実際のタスク並行性は、生存している BE ノードの数または消費されるパーティションの数です。

したがって、生存している BE ノードの数と消費されるパーティションの数が他の 2 つのパラメーター `max_routine_load_task_concurrent_num` および `desired_concurrent_number` の値よりも大きい場合、他の 2 つのパラメーターの値を増やして実際のタスク並行性を増やすことができます。

例えば、消費されるパーティションの数が 7、生存している BE ノードの数が 5、`max_routine_load_task_concurrent_num` がデフォルト値 `5` の場合、実際のタスク並行性を増やしたい場合は、`desired_concurrent_number` を `5` に設定できます（デフォルト値は `3` です）。この場合、実際のタスク並行性 `min(5,7,5,5)` は `5` に設定されます。

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

#### カラムマッピングの設定

CSV 形式データのカラムの順序がターゲットテーブルのカラムと一致しない場合、CSV 形式データの 5 番目のカラムがターゲットテーブルにインポートされる必要がないと仮定して、`COLUMNS` パラメーターを通じて CSV 形式データとターゲットテーブル間のカラムマッピングを指定する必要があります。

**ターゲットデータベースとテーブル**

CSV 形式データのカラムに基づいて、ターゲットデータベース `example_db` にターゲットテーブル `example_tbl2` を作成します。このシナリオでは、CSV 形式データの 5 つのカラムに対応する 5 つのカラムを作成する必要があります。ただし、性別を格納する 5 番目のカラムは除きます。

```SQL
CREATE TABLE example_db.example_tbl2 ( 
    `order_id` bigint NOT NULL COMMENT "Order ID",
    `pay_dt` date NOT NULL COMMENT "Payment date", 
    `customer_name` varchar(26) NULL COMMENT "Customer name", 
    `nationality` varchar(26) NULL COMMENT "Nationality", 
    `price` double NULL COMMENT "Price"
) 
DUPLICATE KEY (order_id,pay_dt) 
DISTRIBUTED BY HASH(order_id) BUCKETS 5; 
```

**Routine Load ジョブ**

この例では、CSV 形式データの 5 番目のカラムをターゲットテーブルにロードする必要がないため、`COLUMNS` で 5 番目のカラムを一時的に `temp_gender` と名付け、他のカラムは直接テーブル `example_tbl2` にマッピングされます。

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

#### フィルター条件の設定

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

`PROPERTIES` で `"strict_mode" = "true"` を設定することができ、これにより Routine Load ジョブが strict mode になります。ソースカラムに `NULL` 値があるが、宛先の StarRocks テーブルカラムが NULL 値を許可しない場合、ソースカラムに NULL 値を持つ行はフィルタリングされます。

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

#### エラー許容度の設定

ビジネスシナリオで不適格なデータに対する許容度が低い場合、`max_batch_rows` および `max_error_number` パラメーターを設定してエラーデータ検出ウィンドウとエラーデータ行の最大数を設定する必要があります。エラーデータ検出ウィンドウ内のエラーデータ行の数が `max_error_number` の値を超えると、Routine Load ジョブは一時停止します。

```SQL
CREATE ROUTINE LOAD example_db.example_tbl1_ordertest1 ON example_tbl1
COLUMNS TERMINATED BY ",",
COLUMNS (order_id, pay_dt, customer_name, nationality, gender, price)
PROPERTIES
(
"max_batch_rows" = "100000",-- max_batch_rows の値に 10 を掛けたものがエラーデータ検出ウィンドウです。
"max_error_number" = "100" -- エラーデータ検出ウィンドウ内で許可されるエラーデータ行の最大数。
)
FROM KAFKA
(
    "kafka_broker_list" ="<kafka_broker1_ip>:<kafka_broker1_port>,<kafka_broker2_ip>:<kafka_broker2_port>",
    "kafka_topic" = "ordertest1"
);
```

#### セキュリティプロトコルを SSL として指定し、関連するパラメーターを設定する

BE が Kafka にアクセスするために使用するセキュリティプロトコルを SSL として指定する必要がある場合、`"property.security.protocol" = "ssl"` および関連するパラメーターを設定する必要があります。

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

### JSON 形式データのロード

#### StarRocks テーブルのカラム名が JSON キー名と一致する場合

**データセットの準備**

例えば、Kafka トピック `ordertest2` に次の JSON 形式データが存在します。

```SQL
{"commodity_id": "1", "customer_name": "Mark Twain", "country": "US","pay_time": 1589191487,"price": 875}
{"commodity_id": "2", "customer_name": "Oscar Wilde", "country": "UK","pay_time": 1589191487,"price": 895}
{"commodity_id": "3", "customer_name": "Antoine de Saint-Exupéry","country": "France","pay_time": 1589191487,"price": 895}
```

> **Note** 各 JSON オブジェクトは 1 つの Kafka メッセージ内にある必要があります。そうでない場合、JSON 形式データの解析に失敗したことを示すエラーが発生します。

**ターゲットデータベースとテーブル**

StarRocks クラスターのターゲットデータベース `example_db` にテーブル `example_tbl3` を作成します。カラム名は JSON 形式データのキー名と一致しています。

```SQL
CREATE TABLE example_db.example_tbl3 ( 
    commodity_id varchar(26) NULL, 
    customer_name varchar(26) NULL, 
    country varchar(26) NULL, 
    pay_time bigint(20) NULL, 
    price double SUM NULL COMMENT "Price") 
AGGREGATE KEY(commodity_id,customer_name,country,pay_time)
DISTRIBUTED BY HASH(commodity_id) BUCKETS 5; 
```

**Routine Load ジョブ**

Routine Load ジョブにはシンプルモードを使用できます。つまり、Routine Load ジョブを作成する際に `jsonpaths` および `COLUMNS` パラメーターを指定する必要はありません。StarRocks はターゲットテーブル `example_tbl3` のカラム名に基づいて Kafka クラスターのトピック `ordertest2` の JSON 形式データのキーを抽出し、JSON 形式データをターゲットテーブルにロードします。

```SQL
CREATE ROUTINE LOAD example_db.example_tbl3_ordertest2 ON example_tbl3
PROPERTIES
(
    "format" ="json"
 )
FROM KAFKA
(
    "kafka_broker_list" ="<kafka_broker1_ip>:<kafka_broker1_port>,<kafka_broker2_ip>:<kafka_broker2_port>",
    "kafka_topic" = "ordertest2"
);
```

> **Note**
>
> - JSON 形式データの最外部層が配列構造である場合、`PROPERTIES` で `"strip_outer_array"="true"` を設定して最外部の配列構造を削除する必要があります。また、`jsonpaths` を指定する必要がある場合、JSON 形式データ全体のルート要素はフラットな JSON オブジェクトです。これは JSON 形式データの最外部の配列構造が削除されるためです。
> - `json_root` を使用して JSON 形式データのルート要素を指定できます。

#### StarRocks テーブルに式を使用して生成された派生カラムが含まれる場合

**データセットの準備**

例えば、Kafka クラスターのトピック `ordertest2` に次の JSON 形式データが存在します。

```SQL
{"commodity_id": "1", "customer_name": "Mark Twain", "country": "US","pay_time": 1589191487,"price": 875}
{"commodity_id": "2", "customer_name": "Oscar Wilde", "country": "UK","pay_time": 1589191487,"price": 895}
{"commodity_id": "3", "customer_name": "Antoine de Saint-Exupéry","country": "France","pay_time": 1589191487,"price": 895}
```

**ターゲットデータベースとテーブル**

StarRocks クラスターのデータベース `example_db` に `example_tbl4` という名前のテーブルを作成します。カラム `pay_dt` は JSON 形式データのキー `pay_time` の値を計算して生成される派生カラムです。

```SQL
CREATE TABLE example_db.example_tbl4 ( 
    `commodity_id` varchar(26) NULL, 
    `customer_name` varchar(26) NULL, 
    `country` varchar(26) NULL,
    `pay_time` bigint(20) NULL,  
    `pay_dt` date NULL, 
    `price` double SUM NULL) 
AGGREGATE KEY(`commodity_id`,`customer_name`,`country`,`pay_time`,`pay_dt`) 
DISTRIBUTED BY HASH(`commodity_id`) BUCKETS 5; 
```

**Routine Load ジョブ**

Routine Load ジョブにはマッチモードを使用できます。つまり、Routine Load ジョブを作成する際に `jsonpaths` および `COLUMNS` パラメーターを指定する必要があります。

JSON 形式データのキーを `jsonpaths` パラメーターで順番に指定する必要があります。

また、JSON 形式データのキー `pay_time` の値を `example_tbl4` テーブルの `pay_dt` カラムに格納する前に DATE 型に変換する必要があるため、`COLUMNS` で `pay_dt=from_unixtime(pay_time,'%Y%m%d')` を使用して計算を指定する必要があります。JSON 形式データの他のキーの値は `example_tbl4` テーブルに直接マッピングできます。

```SQL
CREATE ROUTINE LOAD example_db.example_tbl4_ordertest2 ON example_tbl4
COLUMNS(commodity_id, customer_name, country, pay_time, pay_dt=from_unixtime(pay_time, '%Y%m%d'), price)
PROPERTIES
(
    "format" ="json",
    "jsonpaths" ="[\"$.commodity_id\",\"$.customer_name\",\"$.country\",\"$.pay_time\",\"$.price\"]"
 )
FROM KAFKA
(
    "kafka_broker_list" ="<kafka_broker1_ip>:<kafka_broker1_port>,<kafka_broker2_ip>:<kafka_broker2_port>",
    "kafka_topic" = "ordertest2"
);
```

> **Note**
>
> - JSON データの最外部層が配列構造である場合、`PROPERTIES` で `"strip_outer_array"="true"` を設定して最外部の配列構造を削除する必要があります。また、`jsonpaths` を指定する必要がある場合、JSON データ全体のルート要素はフラットな JSON オブジェクトです。これは JSON データの最外部の配列構造が削除されるためです。
> - `json_root` を使用して JSON 形式データのルート要素を指定できます。

#### StarRocks テーブルに CASE 式を使用して生成された派生カラムが含まれる場合

**データセットの準備**

例えば、Kafka トピック `topic-expr-test` に次の JSON 形式データが存在します。

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

ターゲットテーブルの `col2` カラムの値が CASE 式を使用して生成されるため、Routine Load ジョブの `COLUMNS` パラメーターで対応する式を指定する必要があります。

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

**StarRocks テーブルのクエリ**

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

#### ロードする JSON 形式データのルート要素を指定する

ロードする JSON 形式データのルート要素を指定するために `json_root` を使用する必要があり、その値は有効な JsonPath 式でなければなりません。

**データセットの準備**

例えば、Kafka クラスターのトピック `ordertest3` に次の JSON 形式データが存在します。ロードする JSON 形式データのルート要素は `$.RECORDS` です。

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
DISTRIBUTED BY HASH(commodity_id) BUCKETS 5; 
```

**Routine Load ジョブ**

`PROPERTIES` で `"json_root" = "$.RECORDS"` を設定して、ロードする JSON 形式データのルート要素を指定できます。また、ロードする JSON 形式データが配列構造であるため、最外部の配列構造を削除するために `"strip_outer_array" = "true"` も設定する必要があります。

```SQL
CREATE ROUTINE LOAD example_db.example_tbl3_ordertest3 ON example_tbl3
PROPERTIES
(
    "format" ="json",
    "json_root" = "$.RECORDS",
    "strip_outer_array" = "true"
 )
FROM KAFKA
(
    "kafka_broker_list" ="<kafka_broker1_ip>:<kafka_broker1_port>,<kafka_broker2_ip>:<kafka_broker2_port>",
    "kafka_topic" = "ordertest2"
);
```