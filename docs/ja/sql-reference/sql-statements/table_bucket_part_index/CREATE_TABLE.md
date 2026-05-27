---
displayed_sidebar: docs
---

# CREATE TABLE

StarRocksで新しいテーブルを作成します。

:::tip
この操作には、宛先データベースに対するCREATE TABLE権限が必要です。
:::

## 構文

```SQL
CREATE [EXTERNAL] [TEMPORARY] TABLE [IF NOT EXISTS] [database.]table_name
(column_definition1[, column_definition2, ...]
[, index_definition1[, index_definition12,]])
[ENGINE = [olap|mysql|elasticsearch|hive|hudi|iceberg|jdbc]]
[key_desc]
[COMMENT "table comment"]
[partition_desc]
[distribution_desc]
[rollup_index]
[ORDER BY (column_name1,...)]
[PROPERTIES ("key"="value", ...)]
```

:::tip

- 作成するテーブル名、パーティション名、カラム名、インデックス名は、以下の命名規則に従う必要があります。[システム制限](../../System_limit.md)。
- データベース名、テーブル名、カラム名、またはパーティション名を指定する際、一部のリテラルはStarRocksで予約キーワードとして使用されることに注意してください。これらのキーワードをSQLステートメントで直接使用しないでください。SQLステートメントでそのようなキーワードを使用したい場合は、バッククォート (`) で囲んでください。詳細については、[キーワード](../keywords.md)これらの予約キーワードを参照してください。

:::

## キーワード

### `EXTERNAL`

:::caution
`EXTERNAL`キーワードは非推奨です。

代わりに、[外部カタログ](../../../data_source/catalog/catalog_overview.md)を使用してHive、Iceberg、Hudi、およびJDBCデータソースからデータをクエリすることをお勧めします。`EXTERNAL`キーワードを使用して外部テーブルを作成する代わりに。

:::

:::tip
**推奨事項**

v3.1以降、StarRocksはIcebergカタログでParquet形式のテーブルを作成することをサポートし、INSERT INTOを使用してこれらのParquet形式のIcebergテーブルにデータをシンクすることをサポートします。

v3.2以降、StarRocksはHiveカタログでParquet形式のテーブルを作成することをサポートし、INSERT INTOを使用してこれらのParquet形式のHiveテーブルにデータをシンクすることをサポートします。v3.3以降、StarRocksはHiveカタログでORCおよびTextfile形式のテーブルを作成することをサポートし、INSERT INTOを使用してこれらのORCおよびTextfile形式のHiveテーブルにデータをシンクすることをサポートします。
:::

非推奨の`EXTERNAL`キーワードを使用したい場合は、**`EXTERNAL`キーワードの詳細**

<details>
  <summary>`EXTERNAL`キーワードの詳細</summary>

  外部データソースをクエリするための外部テーブルを作成するには、`CREATE EXTERNAL TABLE`を指定し、`ENGINE`をこれらのいずれかの値に設定します。詳細については、[外部テーブル](../../../data_source/External_table.md)を参照してください。

  - MySQL外部テーブルの場合、以下のプロパティを指定します。

    ```plaintext
    PROPERTIES (
        "host" = "mysql_server_host",
        "port" = "mysql_server_port",
        "user" = "your_user_name",
        "password" = "your_password",
        "database" = "database_name",
        "table" = "table_name"
    )
    ```

    注:

    MySQLにおける「table_name」は実際のテーブル名を示す必要があります。対照的に、CREATE TABLEステートメントにおける「table_name」はStarRocks上のこのMySQLテーブルの名前を示します。これらは異なっていても同じでも構いません。

    StarRocksでMySQLテーブルを作成する目的は、MySQLデータベースにアクセスすることです。StarRocks自体はMySQLデータを維持または保存しません。

  - Elasticsearch外部テーブルの場合、以下のプロパティを指定します。

    ```plaintext
    PROPERTIES (
    "hosts" = "http://192.168.xx.xx:8200,http://192.168.xx0.xx:8200",
    "user" = "root",
    "password" = "root",
    "index" = "tindex",
    "type" = "doc"
    )
    ```

    - `hosts`: Elasticsearchクラスターに接続するために使用されるURLです。1つ以上のURLを指定できます。
    - `user`: 基本認証が有効になっているElasticsearchクラスターにログインするために使用されるルートユーザーのアカウントです。
    - `password`: 前述のルートアカウントのパスワードです。
    - `index`: Elasticsearchクラスター内のStarRocksテーブルのインデックスです。インデックス名はStarRocksテーブル名と同じです。このパラメーターをStarRocksテーブルのエイリアスに設定できます。
    - `type`: インデックスのタイプです。デフォルト値は`doc`です。

  - Hive外部テーブルの場合、以下のプロパティを指定します。

    ```plaintext
    PROPERTIES (
        "database" = "hive_db_name",
        "table" = "hive_table_name",
        "hive.metastore.uris" = "thrift://xx.xx.xx.xx:9083"
    )
    ```

    ここで、databaseはHiveテーブル内の対応するデータベース名です。TableはHiveテーブル名です。`hive.metastore.uris`はサーバーアドレスです。

  - JDBC外部テーブルの場合、以下のプロパティを指定します。

    ```plaintext
    PROPERTIES (
    "resource"="jdbc0",
    "table"="dest_tbl"
    )
    ```

    `resource` はJDBCリソース名、`table` は宛先テーブルです。

  - Iceberg外部テーブルの場合、以下のプロパティを指定します。

    ```plaintext
     PROPERTIES (
     "resource" = "iceberg0", 
     "database" = "iceberg", 
     "table" = "iceberg_table"
     )
    ```

    `resource` はIcebergリソース名です。`database` はIcebergデータベースです。`table` はIcebergテーブルです。

  - Hudi外部テーブルの場合、以下のプロパティを指定します。

    ```plaintext
      PROPERTIES (
      "resource" = "hudi0", 
      "database" = "hudi", 
      "table" = "hudi_table" 
      )
    ```

</details>

### `TEMPORARY`

一時テーブルを作成します。v3.3.1以降、StarRocksはDefault Catalogでの一時テーブル作成をサポートしています。詳細については、以下を参照してください。[一時テーブル](../../../table_design/StarRocks_table_design.md#temporary-table)。

:::note
一時テーブルを作成する際は、`ENGINE` を `olap` に設定する必要があります。
:::

## カラム定義

```SQL
col_name col_type [agg_type] [NULL | NOT NULL] [DEFAULT "default_value"] [AUTO_INCREMENT] [AS generation_expr]
```

### `col_name`

通常、`__op` または `__row` で始まる名前のカラムは作成できません。これらの名前形式はStarRocksで特別な目的のために予約されており、そのようなカラムを作成すると未定義の動作を引き起こす可能性があるためです。どうしてもそのようなカラムを作成する必要がある場合は、FE動的パラメータ[`allow_system_reserved_names`](../../../administration/management/FE_configuration.md#allow_system_reserved_names) を `TRUE` に設定してください。

### `col_type`

型や範囲など、特定のカラム情報：

- TINYINT (1バイト): -2^7 + 1 から 2^7 - 1 の範囲です。

- SMALLINT (2バイト): -2^15 + 1 から 2^15 - 1 の範囲です。

- INT (4バイト): -2^31 + 1 から 2^31 - 1 の範囲です。

- BIGINT (8バイト): -2^63 + 1 から 2^63 - 1 の範囲です。

- LARGEINT (16バイト): -2^127 + 1 から 2^127 - 1 の範囲です。

- FLOAT (4バイト): 科学表記をサポートします。

- DOUBLE (8バイト): 科学表記をサポートします。

- DECIMAL[(精度, スケール)] (16バイト)

  - デフォルト値: DECIMAL(10, 0)
  - 精度: 1 ~ 38
  - スケール: 0 ~ 精度
  - 整数部: 精度 - スケール

    科学表記はサポートされていません。

- DATE (3バイト): 0000-01-01 から 9999-12-31 の範囲です。

- DATETIME (8バイト): 0000-01-01 00:00:00 から 9999-12-31 23:59:59 の範囲です。

- CHAR[(長さ)]: 固定長文字列。範囲: 1 ~ 255。デフォルト値: 1。

- VARCHAR[(長さ)]: 可変長文字列。デフォルト値は1です。単位: バイト。StarRocks 2.1より前のバージョンでは、`length` の値の範囲は1～65533です。[プレビュー] StarRocks 2.1以降のバージョンでは、`length` の値の範囲は1～1048576です。

- HLL (1～16385バイト): HLL型の場合、長さやデフォルト値を指定する必要はありません。長さはデータ集計に応じてシステム内で制御されます。HLLカラムは、[hll_union_agg](../../sql-functions/aggregate-functions/hll_union_agg.md)、[Hll_cardinality](../../sql-functions/scalar-functions/hll_cardinality.md)、[hll_hash](../../sql-functions/scalar-functions/hll_hash.md)によってのみクエリまたは使用できます。

- BITMAP: ビットマップ型は、長さやデフォルト値を指定する必要がありません。符号なしのbigint数値のセットを表します。最大の要素は2^64 - 1まで可能です。

### `agg_type`

集計タイプ。指定しない場合、このカラムはキーカラムです。
指定した場合、値カラムです。サポートされている集計タイプは以下の通りです。

- `SUM`、`MAX`、`MIN`、`REPLACE`
- `HLL_UNION` (`HLL`型のみ)
- `BITMAP_UNION` (`BITMAP`のみ)
- `REPLACE_IF_NOT_NULL`: これは、インポートされたデータが非NULL値の場合にのみ置換されることを意味します。NULL値の場合、StarRocksは元の値を保持します。

:::note

- 集計タイプBITMAP_UNIONの列がインポートされる場合、その元のデータ型はTINYINT、SMALLINT、INT、BIGINTである必要があります。
- テーブル作成時にREPLACE_IF_NOT_NULL列でNOT NULLが指定されている場合でも、StarRocksはユーザーにエラーレポートを送信することなくデータをNULLに変換します。これにより、ユーザーは選択した列をインポートできます。
:::

この集計タイプは、key_descタイプがAGGREGATE KEYであるAggregateテーブルにのみ適用されます。v3.1.9以降、`REPLACE_IF_NOT_NULL`は新しくBITMAP型の列をサポートします。

### `NULL` | `NOT NULL`

列が`NULL`を許可されているかどうか。デフォルトでは、Duplicate Key、Aggregate、またはUnique Keyテーブルを使用するテーブルのすべての列に`NULL`が指定されます。Primary Keyテーブルを使用するテーブルでは、デフォルトで値列には`NULL`が指定され、キー列には`NOT NULL`が指定されます。生データに`NULL`値が含まれている場合は、`\N`で表現します。StarRocksはデータロード中に`\N`を`NULL`として扱います。

### `DEFAULT`

列のデフォルト値。StarRocksにデータをロードする際、列にマッピングされたソースフィールドが空の場合、StarRocksは自動的に列にデフォルト値を入力します。デフォルト値は以下のいずれかの方法で指定できます。

- **DEFAULT current_timestamp**: 現在時刻をデフォルト値として使用します。詳細については、以下を参照してください。[current_timestamp()](../../sql-functions/date-time-functions/current_timestamp.md)。
- **DEFAULT (<expr>)**: 指定された式または関数によって返される結果をデフォルト値として使用します。以下の式がサポートされています。
  - [uuid()](../../sql-functions/utility-functions/uuid.md) および [uuid_numeric()](../../sql-functions/utility-functions/uuid_numeric.md): 一意の識別子を生成します。
  - ARRAYリテラル式 (例: `[1, 2, 3]`): ARRAY型列の場合。
  - MAP式 (例: `map{key: value}`): MAP型列の場合。
  - row()関数 (例: `row(val1, val2)`): STRUCT型列の場合。
- **DEFAULT `<default_value>`**: 列のデータ型の指定された値をデフォルト値として使用します。StarRocksは異なる型に対してデフォルト値の指定をサポートしています。

  **基本型**: 文字列リテラルを使用してデフォルト値を指定します。

  ```sql
  -- 数値型
  age INT DEFAULT '18'
  price DECIMAL(10,2) DEFAULT '99.99'

  -- 文字列型
  name VARCHAR(50) DEFAULT 'Anonymous'

  -- 日付/時刻型
  created_at DATETIME DEFAULT '2024-01-01 00:00:00'

  -- 真偽値型
  is_active BOOLEAN DEFAULT 'true'  -- Supports 'true'/'false'/'1'/'0'
  ```

  **JSON型**: JSON形式の文字列を使用してデフォルト値を指定します。

  ```sql
  metadata JSON DEFAULT '{"status": "active"}'
  tags JSON DEFAULT '[1, 2, 3]'
  ```

  **VARBINARY型**: デフォルト値として空文字列のみがサポートされています。

  ```sql
  binary_data VARBINARY DEFAULT ''
  ```

  **BITMAPおよびHLL型**: デフォルト値として空文字列のみがサポートされており、AGGREGATE KEYテーブルのみに適用されます。

  ```sql
  -- AGGREGATE KEYテーブルで
  bm BITMAP BITMAP_UNION DEFAULT ''
  h HLL HLL_UNION DEFAULT ''
  ```

  **複合型 (ARRAY/MAP/STRUCT)**: 式構文を使用してデフォルト値を指定します。OLAPテーブルのみでサポートされています。

  :::note
複合型のデフォルト値は**`fast_schema_evolution = true`の場合のみサポートされます**。テーブルの`fast_schema_evolution`プロパティが明示的に`false`に設定されている場合、複合型にデフォルト値を追加するとエラーになります。
:::

  ```sql
  -- ARRAY型
  tags ARRAY<VARCHAR(20)> DEFAULT ['tag1', 'tag2']
  scores ARRAY<INT> DEFAULT [90, 85, 92]

  -- MAP型
  attrs MAP<VARCHAR(20), INT> DEFAULT map{'age': 25, 'score': 100}

  -- STRUCT型
  person STRUCT<name VARCHAR(20), age INT> DEFAULT row('John', 30)

  -- 複雑なネスト: STRUCT内にSTRUCT、ARRAY、MAPがネストされている
  user_profile STRUCT<
    id INT, 
    name VARCHAR(50), 
    contact STRUCT<email VARCHAR(100), phone VARCHAR(20)>,
    tags ARRAY<VARCHAR(20)>,
    attributes MAP<VARCHAR(20), VARCHAR(50)>
  > DEFAULT row(1, 'Alice', row('alice@example.com', '123-456-7890'), ['admin', 'user'], map{'level': 'premium', 'status': 'active'})
  ```

  **制限事項**:

  - TIMEおよびVARIANT型は、まだデフォルト値をサポートしていません。
  - 複合型（ARRAY/MAP/STRUCT）のデフォルト値は、OLAPテーブルでのみサポートされており、`fast_schema_evolution`プロパティを有効にする必要があります。

### `AUTO_INCREMENT`

`AUTO_INCREMENT`カラムを指定します。`AUTO_INCREMENT`カラムのデータ型はBIGINTである必要があります。自動インクリメントされるIDは1から始まり、1ステップずつ増加します。`AUTO_INCREMENT`カラムの詳細については、を参照してください。[AUTO_INCREMENT](auto_increment.md)。v3.0以降、StarRocksは`AUTO_INCREMENT`カラムをサポートしています。

### `AS`

生成されたカラムとその式を指定します。[生成されたカラム](../generated_columns.md)は、式の計算結果を事前に計算して保存するために使用でき、同じ複雑な式を持つクエリを大幅に高速化します。v3.1以降、StarRocksは生成されたカラムをサポートしています。

## インデックス定義

```SQL
INDEX index_name (col_name[, col_name, ...]) [USING BITMAP] COMMENT 'xxxxxx'
```

パラメータの説明と使用上の注意点の詳細については、を参照してください。[ビットマップインデックス](../../../table_design/indexes/Bitmap_index.md#create-an-index)。

## `ENGINE`

デフォルト値: `olap`。このパラメータが指定されていない場合、デフォルトでOLAPテーブル（StarRocksネイティブテーブル）が作成されます。

オプション値: `mysql`、`elasticsearch`、`hive`、`jdbc`、`iceberg`、および`hudi`。

## キー

構文:

```SQL
key_type(k1[,k2 ...])
```

データは指定されたキーカラムでシーケンスされ、キータイプごとに異なる属性を持ちます。

- AGGREGATE KEY: キーカラム内の同一の内容は、指定された集計タイプに従って値カラムに集計されます。これは通常、財務諸表や多次元分析などのビジネスシナリオに適用されます。
- UNIQUE KEY/PRIMARY KEY: キーカラム内の同一の内容は、インポートシーケンスに従って値カラムで置き換えられます。これは、キーカラムに対する追加、削除、変更、およびクエリを行うために適用できます。
- DUPLICATE KEY: キーカラム内の同一の内容はStarRocksに共存します。これは、詳細データや集計属性を持たないデータを保存するために使用できます。

  :::note
DUPLICATE KEYはデフォルトのタイプです。データはキーカラムに従ってシーケンスされます。
:::

:::note
AGGREGATE KEY以外のキータイプを使用してテーブルを作成する場合、値カラムは集計タイプを指定する必要はありません。
:::

### 範囲ベースの分散

v4.1以降、StarRocksは**範囲ベースの分散セマンティクス**（デフォルトでは無効）をサポートしており、FE設定`enable_range_distribution`によって制御されます。データはキーカラムのデータ範囲に従ってシーケンスされ、各タブレットは特定の範囲のデータを含みます。

範囲ベースの分散セマンティクスは、以下の点でデフォルトのセマンティクスとは異なります。

- キータイプ（AGGREGATE KEY/UNIQUE KEY/PRIMARY KEY/DUPLICATE KEY）が明示的に指定されており、DISTRIBUTED BY句が指定されていない場合、データはデフォルトで範囲によって分散されます。
- キータイプ、DISTRIBUTED BY句、またはORDER BYのいずれも指定されていない場合、ランダムバケット戦略を持つDuplicate Keyテーブルが作成されます。
- キータイプとDISTRIBUTED BY句が指定されていないが、ORDER BY句が指定されている場合、範囲ベースの分散戦略を持つDuplicate Keyテーブルが作成されます。この場合、DUPLICATE KEYはORDER BY句と同等であり、その逆も同様です。
- DUPLICATE KEYとORDER BY句の両方が指定されている場合、ORDER BY句のみが有効になり、DUPLICATE KEYは無視されます。

## `COMMENT`

テーブルを作成する際にテーブルコメントを追加できます（オプション）。COMMENTは`key_desc`の後に配置する必要があることに注意してください。そうしないと、テーブルを作成できません。

v3.1以降、`ALTER TABLE <table_name> COMMENT = "new table comment"`を使用してテーブルコメントを変更できます。

## パーティション

パーティションは以下の方法で管理できます。

### パーティションを動的に作成する

[動的パーティション分割](../../../table_design/data_distribution/dynamic_partitioning.md)は、パーティションの有効期間（TTL）管理を提供します。StarRocksは、データの鮮度を確保するために、新しいパーティションを事前に自動的に作成し、期限切れのパーティションを削除します。この機能を有効にするには、テーブル作成時に動的パーティション関連プロパティを設定できます。

### パーティションを1つずつ作成する

#### パーティションの上限のみを指定する

構文:

```sql
PARTITION BY RANGE ( <partitioning_column1> [, <partitioning_column2>, ... ] )
  PARTITION <partition1_name> VALUES LESS THAN ("<upper_bound_for_partitioning_column1>" [ , "<upper_bound_for_partitioning_column2>", ... ] )
  [ ,
  PARTITION <partition2_name> VALUES LESS THAN ("<upper_bound_for_partitioning_column1>" [ , "<upper_bound_for_partitioning_column2>", ... ] )
  , ... ] 
)
```

:::note
パーティショニングには、指定されたキー列と指定された値の範囲を使用してください。
:::

- パーティションの命名規則については、[システム制限](../../System_limit.md)を参照してください。
- v3.3.0より前では、範囲パーティショニングの列はTINYINT、SMALLINT、INT、BIGINT、LARGEINT、DATE、DATETIMEの型のみをサポートしていました。v3.3.0以降では、3つの特定の時間関数を範囲パーティショニングの列として使用できます。詳細な使用方法については、[データ分散](../../../table_design/data_distribution/Data_distribution.md#manually-create-partitions)を参照してください。
- パーティションは左閉右開です。最初のパーティションの左境界は最小値です。
- NULL値は、最小値を含むパーティションにのみ格納されます。最小値を含むパーティションが削除されると、NULL値はインポートできなくなります。
- パーティション列は単一列でも複数列でも構いません。パーティション値はデフォルトの最小値です。
- パーティショニング列として1つの列のみが指定されている場合、最新のパーティションのパーティショニング列の上限として`MAXVALUE`を設定できます。

  ```SQL
  PARTITION BY RANGE (pay_dt) (
    PARTITION p1 VALUES LESS THAN ("20210102"),
    PARTITION p2 VALUES LESS THAN ("20210103"),
    PARTITION p3 VALUES LESS THAN MAXVALUE
  )
  ```

:::note

- パーティションは、時間に関連するデータを管理するためによく使用されます。
- データの後方追跡が必要な場合、必要に応じて後でパーティションを追加するために、最初のパーティションを空にすることを検討してください。
:::

#### パーティションの下限と上限の両方を指定する

構文:

```SQL
PARTITION BY RANGE ( <partitioning_column1> [, <partitioning_column2>, ... ] )
(
    PARTITION <partition_name1> VALUES [( "<lower_bound_for_partitioning_column1>" [ , "<lower_bound_for_partitioning_column2>", ... ] ), ( "<upper_bound_for_partitioning_column1?" [ , "<upper_bound_for_partitioning_column2>", ... ] ) ) 
    [,
    PARTITION <partition_name2> VALUES [( "<lower_bound_for_partitioning_column1>" [ , "<lower_bound_for_partitioning_column2>", ... ] ), ( "<upper_bound_for_partitioning_column1>" [ , "<upper_bound_for_partitioning_column2>", ... ] ) ) 
    , ...]
)
```

:::note

- Fixed RangeはLESS THANよりも柔軟です。左右のパーティションをカスタマイズできます。
- Fixed Rangeは、その他の点ではLESS THANと同じです。
- パーティショニング列として1つの列のみが指定されている場合、最新のパーティションのパーティショニング列の上限として`MAXVALUE`を設定できます。
:::

  ```SQL
  PARTITION BY RANGE (pay_dt) (
    PARTITION p202101 VALUES [("20210101"), ("20210201")),
    PARTITION p202102 VALUES [("20210201"), ("20210301")),
    PARTITION p202103 VALUES [("20210301"), (MAXVALUE))
  )
  ```

### 複数のパーティションを一括で作成する

構文

- パーティショニング列が日付型の場合。

  ```sql
  PARTITION BY RANGE (<partitioning_column>) (
      START ("<start_date>") END ("<end_date>") EVERY (INTERVAL <N> <time_unit>)
  )
  ```

- パーティショニング列が整数型の場合。

  ```sql
  PARTITION BY RANGE (<partitioning_column>) (
      START ("<start_integer>") END ("<end_integer>") EVERY (<partitioning_granularity>)
  )
  ```

説明

`START()`と`END()`で開始値と終了値を、`EVERY()`で時間単位またはパーティショニングの粒度を指定して、複数のパーティションを一括で作成できます。

- v3.3.0より前では、範囲パーティショニングの列はTINYINT、SMALLINT、INT、BIGINT、LARGEINT、DATE、DATETIMEの型のみをサポートしていました。v3.3.0以降では、3つの特定の時間関数を範囲パーティショニングの列として使用できます。詳細な使用方法については、[データ分散](../../../table_design/data_distribution/Data_distribution.md#manually-create-partitions)を参照してください。
- パーティショニング列が日付型の場合、`INTERVAL`キーワードを使用して時間間隔を指定する必要があります。時間単位は、時間（v3.0以降）、日、週、月、または年として指定できます。パーティションの命名規則は、動的パーティションの場合と同じです。

詳細については、[データ分散](../../../table_design/data_distribution/Data_distribution.md)を参照してください。

## 分散

StarRocksはハッシュバケットとランダムバケットをサポートしています。バケットを設定しない場合、StarRocksはランダムバケットを使用し、デフォルトでバケット数を自動的に設定します。

- ランダムバケット (v3.1以降)

  パーティション内のデータについて、StarRocksは特定の列値に基づかず、すべてのバケットにデータをランダムに分散します。StarRocksにバケット数を自動的に設定させたい場合、バケット設定を指定する必要はありません。バケット数を手動で指定することを選択した場合の構文は次のとおりです。

  ```SQL
  DISTRIBUTED BY RANDOM BUCKETS <num>
  ```

  ただし、大量のデータをクエリし、特定の列を条件列として頻繁に使用する場合、ランダムバケットによって提供されるクエリパフォーマンスは理想的ではない可能性があることに注意してください。このシナリオでは、ハッシュバケットを使用することをお勧めします。これは、少数のバケットのみをスキャンおよび計算する必要があるため、クエリパフォーマンスが大幅に向上するためです。

  **注意事項**

  - Duplicate Keyテーブルを作成するには、ランダムバケットのみを使用できます。
  - 指定できません[コロケーショングループ](../../../using_starrocks/Colocate_join.md)ランダムにバケット化されたテーブルの場合。
  - Spark Loadは、ランダムにバケット化されたテーブルにデータをロードするために使用できません。
  - StarRocks v2.5.7以降、テーブル作成時にバケット数を設定する必要はありません。StarRocksは自動的にバケット数を設定します。このパラメータを設定したい場合は、以下を参照してください。[バケット数の設定](../../../table_design/data_distribution/Data_distribution.md#set-the-number-of-buckets)。

  詳細については、以下を参照してください。[ランダムバケット](../../../table_design/data_distribution/Data_distribution.md#random-bucketing-since-v31)。

- ハッシュバケット

  構文:

  ```SQL
  DISTRIBUTED BY HASH (k1[,k2 ...]) [BUCKETS num]
  ```

  パーティション内のデータは、バケット列のハッシュ値とバケット数に基づいてバケットに細分化できます。以下の2つの要件を満たす列をバケット列として選択することをお勧めします。

  - IDなどのカーディナリティの高い列
  - クエリで頻繁にフィルターとして使用される列

  そのような列が存在しない場合、クエリの複雑さに基づいてバケット列を決定できます。

  - クエリが複雑な場合は、バケット間のデータ分散を均等にし、クラスタリソースの利用率を向上させるために、カーディナリティの高い列をバケット列として選択することをお勧めします。
  - クエリが比較的単純な場合は、クエリ効率を向上させるために、クエリ条件として頻繁に使用される列をバケット列として選択することをお勧めします。

  1つのバケット列を使用してもパーティションデータを各バケットに均等に分散できない場合は、複数のバケット列（最大3つ）を選択できます。詳細については、以下を参照してください。[バケット列の選択](../../../table_design/data_distribution/Data_distribution.md#hash-bucketing)。

  **注意事項**:

  - **テーブルを作成する際、バケット列を指定する必要があります**。
  - バケット列の値は更新できません。
  - バケット列は、指定後に変更することはできません。
  - StarRocks v2.5.7以降、テーブル作成時にバケット数を設定する必要はありません。StarRocksは自動的にバケット数を設定します。このパラメータを設定したい場合は、以下を参照してください。[バケット数の設定](../../../table_design/data_distribution/Data_distribution.md#set-the-number-of-buckets)。

- 範囲ベースの分散

  v4.1以降、StarRocksは**範囲ベースの分散セマンティクス**（デフォルトでは無効）をサポートしており、FE設定`enable_range_distribution`によって制御されます。詳細については、以下を参照してください。[範囲ベースの分散](#range-based-distribution)。

## ロールアップインデックス

テーブル作成時にロールアップを一括で作成できます。

構文:

```SQL
ROLLUP (rollup_name (column_name1, column_name2, ...)
[FROM from_index_name]
[PROPERTIES ("key"="value", ...)],...)
```

## `ORDER BY`

v3.0以降、Primary Keyテーブルは`ORDER BY`を使用してソートキーの定義をサポートしています。v3.3以降、Duplicate Keyテーブル、Aggregateテーブル、およびUnique Keyテーブルは`ORDER BY`を使用してソートキーの定義をサポートしています。

ソートキーの詳細については、以下を参照してください。[ソートキーとプレフィックスインデックス](../../../table_design/indexes/Prefix_index_sort_key.md)。

## `PROPERTIES`

### ストレージとレプリカ

エンジンタイプが`OLAP`の場合、テーブル作成時に初期ストレージメディア（`storage_medium`）、自動ストレージクールダウン時間（`storage_cooldown_time`）または時間間隔（`storage_cooldown_ttl`）、およびレプリカ数（`replication_num`）を指定できます。

プロパティが有効になるスコープ：テーブルにパーティションが1つしかない場合、プロパティはテーブルに属します。テーブルが複数のパーティションに分割されている場合、プロパティは各パーティションに属します。特定のパーティションに異なるプロパティを設定する必要がある場合は、以下を実行できます。[ALTER TABLE ... ADD PARTITION または ALTER TABLE ... MODIFY PARTITION](ALTER_TABLE.md)テーブル作成後に。

#### 初期ストレージメディアと自動ストレージクールダウン時間の設定

```sql
PROPERTIES (
    "storage_medium" = "[SSD|HDD]",
    { "storage_cooldown_ttl" = "<num> { YEAR | MONTH | DAY | HOUR } "
    | "storage_cooldown_time" = "yyyy-MM-dd HH:mm:ss" }
)
```

**プロパティ**

- `storage_medium`: 初期ストレージメディア。`SSD`または`HDD`に設定できます。明示的に指定したストレージメディアのタイプが、BE静的パラメータ`storage_root_path`で指定されたStarRocksクラスターのBEディスクタイプと一致していることを確認してください。<br />

  FE設定項目`enable_strict_storage_medium_check`が`true`に設定されている場合、テーブル作成時にシステムはBEディスクタイプを厳密にチェックします。CREATE TABLEで指定したストレージメディアがBEディスクタイプと一致しない場合、「Failed to find enough host in all backends with storage medium is SSD|HDD.」というエラーが返され、テーブル作成は失敗します。`enable_strict_storage_medium_check`が`false`に設定されている場合、システムはこのエラーを無視し、強制的にテーブルを作成します。ただし、データロード後にクラスターのディスクスペースが不均一に分散される可能性があります。<br />

  v2.3.6、v2.4.2、v2.5.1、およびv3.0以降、`storage_medium`が明示的に指定されていない場合、システムはBEディスクタイプに基づいてストレージメディアを自動的に推測します。<br />

  - システムは以下のシナリオでこのパラメータを自動的にSSDに設定します。

    - BEによって報告されるディスクタイプ（`storage_root_path`）にSSDのみが含まれている場合。
    - BEによって報告されるディスクタイプ（`storage_root_path`）にSSDとHDDの両方が含まれている場合。v2.3.10、v2.4.5、v2.5.4、およびv3.0以降、BEによって報告される`storage_root_path`にSSDとHDDの両方が含まれ、かつプロパティ`storage_cooldown_time`が指定されている場合、システムは`storage_medium`をSSDに設定することに注意してください。

  - システムは以下のシナリオでこのパラメータを自動的にHDDに設定します。

    - BEによって報告されるディスクタイプ（`storage_root_path`）にHDDのみが含まれている場合。
    - 2.3.10、2.4.5、2.5.4、および3.0以降、BEによって報告される`storage_root_path`にSSDとHDDの両方が含まれ、かつプロパティ`storage_cooldown_time`が指定されていない場合、システムは`storage_medium`をHDDに設定します。

- `storage_cooldown_ttl`または`storage_cooldown_time`: 自動ストレージクールダウン時間または時間間隔。自動ストレージクールダウンとは、SSDからHDDへデータを自動的に移行することを指します。この機能は、初期ストレージメディアがSSDの場合にのみ有効です。

  - `storage_cooldown_ttl`: **時間間隔**このテーブルのパーティションに対する自動ストレージクールダウンの。最新のパーティションをSSDに保持し、一定の時間間隔後に古いパーティションを自動的にHDDにクールダウンする必要がある場合、このパラメータを使用できます。各パーティションの自動ストレージクールダウン時間は、このパラメータの値にパーティションの上限時間を加算して計算されます。

  サポートされている値は`<num> YEAR`、`<num> MONTH`、`<num> DAY`、および`<num> HOUR`です。`<num>`は非負の整数です。デフォルト値はnullで、ストレージクールダウンが自動的に実行されないことを示します。

  例えば、テーブル作成時に値を`"storage_cooldown_ttl"="1 DAY"`と指定し、`[2023-08-01 00:00:00,2023-08-02 00:00:00)`の範囲を持つパーティション`p20230801`が存在するとします。このパーティションの自動ストレージクールダウン時間は`2023-08-03 00:00:00`であり、これは`2023-08-02 00:00:00 + 1 DAY`です。テーブル作成時に値を`"storage_cooldown_ttl"="0 DAY"`と指定した場合、このパーティションの自動ストレージクールダウン時間は`2023-08-02 00:00:00`です。

  - `storage_cooldown_time`: 自動ストレージクールダウン時間（**絶対時間**）テーブルがSSDからHDDにクールダウンされるとき。指定された時間は現在時刻より後である必要があります。形式：「yyyy-MM-dd HH:mm:ss」。特定のパーティションに異なるプロパティを設定する必要がある場合は、以下を実行できます。[ALTER TABLE ... ADD PARTITION または ALTER TABLE ... MODIFY PARTITION](ALTER_TABLE.md)。

##### 使用法

- 自動ストレージクールダウンに関連するパラメータの比較は以下の通りです。
  - `storage_cooldown_ttl`: テーブル内のパーティションに対する自動ストレージクールダウンの時間間隔を指定するテーブルプロパティです。システムは`the value of this parameter plus the upper time bound of the partition`の時点でパーティションを自動的にクールダウンします。したがって、自動ストレージクールダウンはパーティション粒度で実行され、より柔軟です。
  - `storage_cooldown_time`: 自動ストレージクールダウン時間（**絶対時間**）をこのテーブルに指定するテーブルプロパティです。また、テーブル作成後に特定のパーティションに異なるプロパティを設定することもできます。
  - `storage_cooldown_second`: クラスター内のすべてのテーブルに対する自動ストレージクールダウンの遅延を指定する静的FEパラメータです。

- テーブルプロパティ`storage_cooldown_ttl`または`storage_cooldown_time`は、FE静的パラメータ`storage_cooldown_second`よりも優先されます。

- これらのパラメータを設定する際には、`"storage_medium = "SSD"`を指定する必要があります。

- これらのパラメーターを設定しない場合、自動ストレージクールダウンは自動的に実行されません。

- 各パーティションの自動ストレージクールダウン時間を表示するには、`SHOW PARTITIONS FROM <table_name>` を実行します。

##### 制限

- 式およびリストパーティショニングはサポートされていません。
- パーティション列は日付型である必要があります。
- 複数のパーティション列はサポートされていません。
- プライマリキーテーブルはサポートされていません。

#### パーティション内の各タブレットのレプリカ数を設定します

`replication_num`: パーティション内の各テーブルのレプリカ数。デフォルト数: `3`。

```sql
PROPERTIES (
    "replication_num" = "<num>"
)
```

### ブルームフィルターインデックス

エンジンタイプが `olap` の場合、ブルームフィルターインデックスを採用する列を指定できます。

ブルームフィルターインデックスを使用する場合、以下の制限が適用されます。

- Duplicate Key または Primary Key テーブルのすべての列にブルームフィルターインデックスを作成できます。Aggregate テーブルまたは Unique Key テーブルの場合、キー列にのみブルームフィルターインデックスを作成できます。
- TINYINT、FLOAT、DOUBLE、および DECIMAL 列は、ブルームフィルターインデックスの作成をサポートしていません。
- ブルームフィルターインデックスは、`in` および `=` 演算子を含むクエリ（例: `Select xxx from table where x in {}` および `Select xxx from table where column = xxx`）のパフォーマンスのみを向上させることができます。この列に離散値が多いほど、より正確なクエリが生成されます。

詳細については、以下を参照してください。[ブルームフィルターインデックス作成](../../../table_design/indexes/Bloomfilter_index.md)

```SQL
PROPERTIES (
    "bloom_filter_columns"="k1,k2,k3"
)
```

### コロケート結合

コロケート結合属性を使用する場合は、`properties` で指定します。

```SQL
PROPERTIES (
    "colocate_with"="table1"
)
```

### 動的パーティション

動的パーティション属性を使用する場合は、プロパティで指定してください。

```SQL
PROPERTIES (
    "dynamic_partition.enable" = "true|false",
    "dynamic_partition.time_unit" = "DAY|WEEK|MONTH",
    "dynamic_partition.start" = "${integer_value}",
    "dynamic_partition.end" = "${integer_value}",
    "dynamic_partition.prefix" = "${string_value}",
    "dynamic_partition.buckets" = "${integer_value}"
```

**`PROPERTIES`**

| パラメーター | 必須 | 説明 |
| --- | --- | --- |
| dynamic_partition.enable | いいえ | 動的パーティショニングを有効にするかどうか。有効な値: `TRUE` および `FALSE`。デフォルト値: `TRUE`。 |
| dynamic_partition.time_unit | はい | 動的に作成されるパーティションの時間粒度。必須パラメーターです。有効な値: `DAY`、`WEEK`、および `MONTH`。時間粒度によって、動的に作成されるパーティションのサフィックス形式が決まります。<br />  - 値が `DAY` の場合、動的に作成されるパーティションのサフィックス形式は `yyyyMMdd` です。パーティション名サフィックスの例は `20200321` です。<br />  - 値が `WEEK` の場合、動的に作成されるパーティションのサフィックス形式は `yyyy_ww` です。例えば、2020年の13週目は `2020_13` となります。<br />  - 値が `MONTH` の場合、動的に作成されるパーティションのサフィックス形式は `yyyyMM` です。例えば、`202003` となります。 |
| dynamic_partition.start | いいえ | 動的パーティショニングの開始オフセット。このパラメーターの値は負の整数である必要があります。このオフセットより前のパーティションは、`dynamic_partition.time_unit` によって決定される現在の日、週、または月に基づいて削除されます。デフォルト値は `Integer.MIN_VALUE`、つまり -2147483648 であり、これは履歴パーティションが削除されないことを意味します。 |
| dynamic_partition.end | はい | 動的パーティショニングの終了オフセット。このパラメーターの値は正の整数である必要があります。現在の日、週、または月から終了オフセットまでのパーティションは事前に作成されます。 |
| dynamic_partition.prefix | いいえ | 動的パーティションの名前に追加されるプレフィックス。デフォルト値: `p`。 |
| dynamic_partition.buckets | いいえ | 動的パーティションあたりのバケット数。デフォルト値は、予約語 `BUCKETS` によって決定されるバケット数、または StarRocks によって自動的に設定されるバケット数と同じです。 |

:::note

パーティション列が INT 型の場合、パーティションの時間粒度に関係なく、その形式は `yyyyMMdd` である必要があります。

:::

### ランダムバケット化によるバケットサイズ

v3.2以降、ランダムバケット化が設定されたテーブルの場合、テーブル作成時に `PROPERTIES` の `bucket_size` パラメーターを使用してバケットサイズを指定することで、オンデマンドかつ動的にバケット数を増やすことができます。単位: B。

```sql
PROPERTIES (
    "bucket_size" = "1073741824"
)
```

### データ圧縮アルゴリズム

テーブルを作成する際に、プロパティ `compression` を追加することで、テーブルのデータ圧縮アルゴリズムを指定できます。

`compression` の有効な値は次のとおりです。

- `LZ4`: LZ4アルゴリズム。
- `ZSTD`: Zstandardアルゴリズム。
- `ZLIB`: zlibアルゴリズム。
- `SNAPPY`: Snappyアルゴリズム。

v3.3.2以降、StarRocksはテーブル作成時にzstd圧縮形式の圧縮レベルを指定することをサポートしています。

構文:

```sql
PROPERTIES ("compression" = "zstd(<compression_level>)")
```

`compression_level`: ZSTD圧縮形式の圧縮レベル。型: 整数。範囲: [1,22]。デフォルト: `3` (推奨)。数値が大きいほど圧縮率が高くなります。圧縮レベルが高いほど、圧縮と解凍にかかる時間が長くなります。

例:

```sql
PROPERTIES ("compression" = "zstd(3)")
```

適切なデータ圧縮アルゴリズムの選択方法の詳細については、以下を参照してください。[データ圧縮](../../../table_design/data_compression.md)。

### データロードの書き込みクォーラム

StarRocksクラスターに複数のデータレプリカがある場合、テーブルごとに異なる書き込みクォーラムを設定できます。これは、StarRocksがロードタスクの成功を判断する前に、いくつのレプリカがロード成功を返す必要があるかを示します。テーブル作成時にプロパティ`write_quorum`を追加することで、書き込みクォーラムを指定できます。このプロパティはv2.5以降でサポートされています。

`write_quorum`の有効な値は次のとおりです。

- `MAJORITY`: デフォルト値。データレプリカの**過半数**がロード成功を返すと、StarRocksはロードタスクの成功を返します。それ以外の場合、StarRocksはロードタスクの失敗を返します。
- `ONE`: データレプリカの**1つ**がロード成功を返すと、StarRocksはロードタスクの成功を返します。それ以外の場合、StarRocksはロードタスクの失敗を返します。
- `ALL`: データレプリカの**すべて**がロード成功を返すと、StarRocksはロードタスクの成功を返します。それ以外の場合、StarRocksはロードタスクの失敗を返します。

:::caution

- ロードの書き込みクォーラムを低く設定すると、データにアクセスできなくなるリスク、さらにはデータ損失のリスクが高まります。たとえば、2つのレプリカを持つStarRocksクラスターで、書き込みクォーラムを1つに設定したテーブルにデータをロードし、データが1つのレプリカにのみ正常にロードされたとします。StarRocksはロードタスクが成功したと判断しますが、データの生存レプリカは1つだけです。ロードされたデータのタブレットを保存するサーバーがダウンした場合、これらのタブレット内のデータにはアクセスできなくなります。また、サーバーのディスクが損傷した場合、データは失われます。
- StarRocksは、すべてのデータレプリカがステータスを返した後でのみ、ロードタスクのステータスを返します。ロードステータスが不明なレプリカがある場合、StarRocksはロードタスクのステータスを返しません。レプリカでは、ロードタイムアウトもロード失敗と見なされます。
:::

### レプリカデータ書き込みおよびレプリケーションモード

StarRocksクラスターに複数のデータレプリカがある場合、`PROPERTIES`内の`replicated_storage`パラメーターを指定して、レプリカ間のデータ書き込みおよびレプリケーションモードを設定できます。

- `true` (v3.0以降のデフォルト) は「シングルリーダーレプリケーション」を示し、データはプライマリレプリカにのみ書き込まれます。他のレプリカはプライマリレプリカからデータを同期します。このモードは、複数のレプリカへのデータ書き込みによって発生するCPUコストを大幅に削減します。v2.5以降でサポートされています。
- `false` (v2.5のデフォルト) は「リーダーレスレプリケーション」を示し、データはプライマリレプリカとセカンダリレプリカを区別することなく、複数のレプリカに直接書き込まれます。CPUコストはレプリカの数に比例して増加します。

ほとんどの場合、デフォルト値を使用すると、データ書き込みパフォーマンスが向上します。レプリカ間のデータ書き込みおよびレプリケーションモードを変更したい場合は、ALTER TABLEコマンドを実行します。例:

```sql
ALTER TABLE example_db.my_table
SET ("replicated_storage" = "false");
```

### Delta Joinのユニークキー制約と外部キー制約

View Delta Joinシナリオでクエリリライトを有効にするには、Delta Joinで結合するテーブルに対して、ユニークキー制約`unique_constraints`と外部キー制約`foreign_key_constraints`を定義する必要があります。詳細については、[非同期マテリアライズドビュー - View Delta Joinシナリオでのクエリリライト](../../../using_starrocks/async_mv/use_cases/query_rewrite_with_materialized_views.md#query-delta-join-rewrite)を参照してください。

```SQL
PROPERTIES (
    "unique_constraints" = "<unique_key>[, ...]",
    "foreign_key_constraints" = "
    (<child_column>[, ...]) 
    REFERENCES 
    [catalog_name].[database_name].<parent_table_name>(<parent_column>[, ...])
    [;...]
    "
)
```

- `child_column`: テーブルの外部キー。複数の`child_column`を定義できます。
- `catalog_name`: 結合するテーブルが存在するカタログの名前。このパラメーターが指定されていない場合、デフォルトのカタログが使用されます。
- `database_name`: 結合するテーブルが存在するデータベースの名前。このパラメーターが指定されていない場合、現在のデータベースが使用されます。
- `parent_table_name`: 結合するテーブルの名前。
- `parent_column`: 結合する列。これらは、対応するテーブルの主キーまたはユニークキーである必要があります。

:::caution

- `unique_constraints`と`foreign_key_constraints`はクエリリライトにのみ使用されます。データがテーブルにロードされる際、外部キー制約のチェックは保証されません。テーブルにロードされるデータが制約を満たしていることを確認する必要があります。
- 主キーテーブルの主キーまたはユニークキーテーブルのユニークキーは、デフォルトで対応する`unique_constraints`です。手動で設定する必要はありません。
- テーブルの`foreign_key_constraints`内の`child_column`は、別のテーブルの`unique_constraints`内の`unique_key`を参照する必要があります。
- `child_column`と`parent_column`の数は一致している必要があります。
- `child_column`と対応する`parent_column`のデータ型は一致している必要があります。
:::

### 共有データクラスター向けのクラウドネイティブテーブル

StarRocks Shared-data クラスターを使用するには、以下のプロパティを持つクラウドネイティブテーブルを作成する必要があります。

```SQL
PROPERTIES (
    "storage_volume" = "<storage_volume_name>",
    "datacache.enable" = "{ true | false }",
    "datacache.partition_duration" = "<string_value>",
    "file_bundling" = "{ true | false }"
)
```

- `storage_volume`: 作成するクラウドネイティブテーブルを保存するために使用されるストレージボリュームの名前。このプロパティが指定されていない場合、デフォルトのストレージボリュームが使用されます。このプロパティはv3.1以降でサポートされています。

- `datacache.enable`: ローカルディスクキャッシュを有効にするかどうか。デフォルト: `true`。

  - このプロパティが`true`に設定されている場合、ロードされるデータはオブジェクトストレージとローカルディスク（クエリ高速化のためのキャッシュとして）に同時に書き込まれます。
  - このプロパティが`false`に設定されている場合、データはオブジェクトストレージにのみロードされます。

  :::note
ローカルディスクキャッシュを有効にするには、BE構成項目`storage_root_path`でディスクのディレクトリを指定する必要があります。
:::

- `datacache.partition_duration`: ホットデータの有効期間。ローカルディスクキャッシュが有効になっている場合、すべてのデータはキャッシュにロードされます。キャッシュがいっぱいになると、StarRocksは最近使用頻度の低いデータをキャッシュから削除します。クエリが削除されたデータをスキャンする必要がある場合、StarRocksはそのデータが有効期間内にあるかどうかを確認します。データが期間内にある場合、StarRocksはそのデータを再度キャッシュにロードします。データが期間内にない場合、StarRocksはそれをキャッシュにロードしません。このプロパティは文字列値であり、`YEAR`、`MONTH`、`DAY`、`HOUR`などの単位で指定できます（例: `7 DAY`、`12 HOUR`）。指定されていない場合、すべてのデータはホットデータとしてキャッシュされます。

  :::note
このプロパティは、`datacache.enable`が`true`に設定されている場合にのみ利用可能です。
:::

- `file_bundling` (オプション): クラウドネイティブテーブルのファイルバンドリング最適化を有効にするかどうか。v4.0以降でサポートされています。この機能が有効になっている場合（`true`に設定）、システムはロード、コンパクション、またはパブリッシュ操作によって生成されたデータファイルを自動的にバンドルし、外部ストレージシステムへの高頻度アクセスによって発生するAPIコストを削減します。

  :::note

  - ファイルバンドリングは、StarRocks v4.0以降の共有データクラスターでのみ利用可能です。
  - ファイルバンドリングは、v4.0以降で作成されたテーブルではデフォルトで有効になっており、FE構成`enable_file_bundling`（デフォルト: true）によって制御されます。
  - ファイルバンドリングが有効になった後、クラスターをv3.5.2以降にのみダウングレードできます。v3.5.2より前のバージョンにダウングレードしたい場合は、まずファイルバンドリングを有効にしたテーブルを削除する必要があります。
  - クラスターがv4.0にアップグレードされた後も、既存のテーブルではファイルバンドリングはデフォルトで無効のままです。
  - 既存のテーブルに対してファイルバンドリングを手動で有効にするには、[ALTER TABLE](ALTER_TABLE.md)ステートメントを使用し、以下の制限があります。
    - v4.0より前のバージョンで作成されたロールアップインデックスを持つテーブルに対しては、ファイルバンドリングを有効にできません。v4.0以降でインデックスを削除して再作成し、その後テーブルに対してファイルバンドリングを有効にすることができます。
    - `file_bundling`プロパティを**繰り返し**特定の期間内に変更することはできません。そうしないと、システムはエラーを返します。`file_bundling`プロパティが変更可能かどうかは、以下のSQLステートメントを実行して確認できます。

      ```SQL
      SELECT METADATA_SWITCH_VERSION FROM information_schema.partitions_meta WHERE TABLE_NAME = '<table_name>';
      ```

      `file_bundling`プロパティは、`0`が返された場合にのみ変更が許可されます。ゼロ以外の値は、`METADATA_SWITCH_VERSION`に対応するデータバージョンがGCメカニズムによってまだ回収されていないことを示します。データバージョンが回収されるまで待つ必要があります。

      FE動的構成`lake_autovacuum_grace_period_minutes`の値を低く設定することで、この間隔を短縮できます。ただし、`file_bundling`プロパティを変更した後、構成を元の値にリセットすることを忘れないでください。
:::

### 高速スキーマ進化

- `fast_schema_evolution`: テーブルの高速スキーマ進化を有効にするかどうか。有効な値は`TRUE`または`FALSE`（デフォルト）です。高速スキーマ進化を有効にすると、スキーマ変更の速度が向上し、列の追加または削除時のリソース使用量が削減されます。現在、このプロパティはテーブル作成時にのみ有効にでき、テーブル作成後にALTER TABLEを使用して変更することはできません。

  :::note

  - 高速スキーマ進化は、v3.2.0以降の共有なしクラスターでサポートされています。
  - 高速スキーマ進化は、v3.3以降の共有データクラスターでサポートされており、デフォルトで有効になっています。共有データクラスターでクラウドネイティブテーブルを作成する際に、このプロパティを指定する必要はありません。FE動的パラメータ`enable_fast_schema_evolution`（デフォルト: true）がこの動作を制御します。
:::

- `cloud_native_fast_schema_evolution_v2`: の高速スキーマ進化v2を有効にするかどうか。**クラウドネイティブテーブル**。v4.1以降でサポートされています。有効な値は`TRUE`（デフォルト）または`FALSE`です。高速スキーマ進化v2が有効になっている場合、スキーマ変更は同期プロセスになります。ALTER TABLEステートメントが正常に返されると、新しいスキーマはすぐに有効になります。システムはS3にあるタブレットメタデータではなくFEメタデータのみを変更するため、テーブル内のパーティションやタブレットの数に関係なく、常に秒レベルのレイテンシを達成できます。従来の動作では、スキーマ変更はタブレットメタデータを時間とともに更新する非同期ジョブとして実行されます。

  :::note

  - 高速スキーマ進化v2はv4.1以降でサポートされており、**クラウドネイティブテーブル**の共有データクラスターでのみ利用可能です。
  - デフォルトの動作:
    - v4.1クラスターで作成された新しいテーブルの場合、高速スキーマ進化v2はデフォルトで有効になっています。
    - v4.1にアップグレードされたクラスターの既存のテーブルの場合、高速スキーマ進化v2はデフォルトで無効になっています。このプロパティを`true`に明示的に設定することで有効にできます。[ALTER TABLE](ALTER_TABLE.md)。
  - ダウングレード要件:
    - 共有データクラスターをv4.1からv4.0.5以降にダウングレードするには、標準のダウングレード手順に従って直接ダウングレードできます。
    - 共有データクラスターをv4.1からv3.xまたはv4.0.5より前のパッチバージョンにダウングレードする前に、ALTER TABLEを介してFast Schema Evolution v2を有効にしたすべてのテーブルに対して、`cloud_native_fast_schema_evolution_v2`を`false`に手動で設定する必要があります。非同期ジョブがFINISHEDになるまで待つ必要があります。ジョブのステータスはSHOW ALTERで追跡できます。
:::

スキーマ変更ジョブは以下で確認できます。[SHOW ALTER TABLE COLUMN](./SHOW_ALTER.md)。

例:

```SQL
-- テーブル内の最近の列/スキーマ変更ジョブを一覧表示する
SHOW ALTER TABLE COLUMN FROM test_db WHERE TableName = "test_tbl";
```

Fast Schema Evolution v2が有効になっているクラウドネイティブテーブルの場合、変更はFEメタデータの更新によってのみ適用されるため、スキーマ変更ジョブは通常FINISHEDとして表示されます。

### ベースコンパクションの禁止

`base_compaction_forbidden_time_ranges`: テーブルのベースコンパクションが禁止される時間範囲。このプロパティが設定されている場合、システムは指定された時間範囲外でのみ、対象となるタブレットに対してベースコンパクションを実行します。このプロパティはv3.2.13以降でサポートされています。

:::note
ベースコンパクションが禁止されている期間中、テーブルへのデータロード数が500を超えないようにしてください。
:::

`base_compaction_forbidden_time_ranges`の値は[Quartz cron構文](https://productresources.collibra.com/docs/collibra/latest/Content/Cron/co_quartz-cron-syntax.htm)に従い、`<minute> <hour> <day-of-the-month> <month> <day-of-the-week>`のフィールドのみをサポートします。ここで`<minute>`は`*`である必要があります。

```SQL
crontab_param_value ::= [ "" | crontab ]

crontab ::= * <hour> <day-of-the-month> <month> <day-of-the-week>
```

- このプロパティが設定されていないか、`""`（空の文字列）に設定されている場合、ベースコンパクションはいつでも禁止されません。
- このプロパティが`* * * * *`に設定されている場合、ベースコンパクションは常に禁止されます。
- その他の値はQuartz cron構文に従います。
  - 独立した値はフィールドの単位時間を示します。例えば、`<hour>`フィールドの`8`は8:00-8:59を意味します。
  - 値の範囲はフィールドの時間範囲を示します。例えば、`<hour>`フィールドの`8-9`は8:00-9:59を意味します。
  - コンマで区切られた複数の値の範囲は、フィールドの複数の時間範囲を示します。
  - `<day of the week>`は日曜日の開始値が`1`で、`7`は土曜日を表します。

例:

```SQL
-- 毎日午前8時から午後9時までベースコンパクションを禁止する。
'base_compaction_forbidden_time_ranges' = '* 8-20 * * *'

-- 毎日午前0時から午前5時までと午後9時から午後11時までベースコンパクションを禁止する。
'base_compaction_forbidden_time_ranges' = '* 0-4,21-22 * * *'

-- 月曜日から金曜日までベースコンパクションを禁止する（つまり、土曜日と日曜日は許可する）。
'base_compaction_forbidden_time_ranges' = '* * * * 2-6'

-- 毎週営業日（つまり月曜日から金曜日）の午前8時から午後9時までベースコンパクションを禁止する。
'base_compaction_forbidden_time_ranges' = '* 8-20 * * 2-6'
```

### 共通パーティション式TTLの指定

v3.5.0以降、StarRocksネイティブテーブルは共通パーティション式TTLをサポートしています。

`partition_retention_condition`: 動的に保持されるパーティションを宣言する式。式内の条件を満たさないパーティションは定期的に削除されます。

- 式にはパーティション列と定数のみを含めることができます。非パーティション列はサポートされていません。
- 共通パーティション式は、リストパーティションと範囲パーティションに異なる方法で適用されます。
  - リストパーティションを持つテーブルの場合、StarRocksは共通パーティション式によってフィルタリングされたパーティションの削除をサポートします。
  - 範囲パーティションを持つテーブルの場合、StarRocksはFEのパーティションプルーニング機能を使用してのみパーティションをフィルタリングおよび削除できます。パーティションプルーニングでサポートされていない述語に対応するパーティションは、フィルタリングおよび削除できません。

例:

```SQL
-- 過去3ヶ月間のデータを保持する。列dtはテーブルのパーティション列である。
"partition_retention_condition" = "dt >= CURRENT_DATE() - INTERVAL 3 MONTH"
```

この機能を無効にするには、ALTER TABLEステートメントを使用してこのプロパティを空の文字列として設定します。

```SQL
ALTER TABLE tbl SET('partition_retention_condition' = '');
```

### テーブルレベルでFlat JSONプロパティを設定する

v3.3で、StarRocksは[Flat JSON](../../../using_starrocks/Flat_json.md)機能を導入し、JSONデータクエリの効率を向上させ、JSONの使用の複雑さを軽減しました。この機能は特定のBE構成項目とシステム変数によって制御されていました。そのため、グローバルにのみ有効化（または無効化）できました。

v4.0以降、Flat JSON関連のプロパティをテーブルレベルで設定できます。

```SQL
PROPERTIES (
    "flat_json.enable" = "{ true | false }",
    "flat_json.null.factor" = "",
    "flat_json.sparsity.factor" = "",
    "flat_json.column.max" = ""
)
```

- `flat_json.enable` (オプション): Flat JSON機能を有効にするかどうか。この機能が有効になると、新しくロードされたJSONデータは自動的にフラット化され、JSONクエリのパフォーマンスが向上します。
- `flat_json.null.factor` (オプション): 列内のNULL値の割合のしきい値。NULL値の割合がこのしきい値よりも高い場合、その列はFlat JSONによって抽出されません。このパラメータは、`flat_json.enable`が`true`に設定されている場合にのみ有効です。デフォルト値: `0.3`。
- `flat_json.sparsity.factor` (オプション): 同名の列の割合のしきい値。同名の列の割合がこの値よりも低い場合、その列はFlat JSONによって抽出されません。このパラメータは、`flat_json.enable`が`true`に設定されている場合にのみ有効です。デフォルト値: `0.3`。
- `flat_json.column.max` (オプション): Flat JSONによって抽出できるサブフィールドの最大数。このパラメータは、`flat_json.enable`が`true`に設定されている場合にのみ有効です。デフォルト値: `100`。

## 例

### ハッシュバケットとカラム型ストレージを持つ集計テーブル

```SQL
CREATE TABLE example_db.table_hash
(
    k1 TINYINT,
    k2 DECIMAL(10, 2) DEFAULT "10.5",
    v1 CHAR(10) REPLACE,
    v2 INT SUM
)
ENGINE=olap
AGGREGATE KEY(k1, k2)
COMMENT "my first starrocks table"
DISTRIBUTED BY HASH(k1)
PROPERTIES ("storage_type"="column");
```

### ストレージメディアとクールダウン時間が設定された集計テーブル

```SQL
CREATE TABLE example_db.table_hash
(
    k1 BIGINT,
    k2 LARGEINT,
    v1 VARCHAR(2048) REPLACE,
    v2 SMALLINT SUM DEFAULT "10"
)
ENGINE=olap
UNIQUE KEY(k1, k2)
DISTRIBUTED BY HASH (k1, k2)
PROPERTIES(
    "storage_type"="column",
    "storage_medium" = "SSD",
    "storage_cooldown_time" = "2015-06-04 00:00:00"
);
```

### レンジパーティション、ハッシュバケット、カラムベースストレージ、ストレージメディア、クールダウン時間を持つDuplicate Keyテーブル

未満

```SQL
CREATE TABLE example_db.table_range
(
    k1 DATE,
    k2 INT,
    k3 SMALLINT,
    v1 VARCHAR(2048),
    v2 DATETIME DEFAULT "2014-02-04 15:36:00"
)
ENGINE=olap
DUPLICATE KEY(k1, k2, k3)
PARTITION BY RANGE (k1)
(
    PARTITION p1 VALUES LESS THAN ("2014-01-01"),
    PARTITION p2 VALUES LESS THAN ("2014-06-01"),
    PARTITION p3 VALUES LESS THAN ("2014-12-01")
)
DISTRIBUTED BY HASH(k2)
PROPERTIES(
    "storage_medium" = "SSD", 
    "storage_cooldown_time" = "2015-06-04 00:00:00"
);
```

注:

このステートメントは3つのデータパーティションを作成します:

```SQL
( {    MIN     },   {"2014-01-01"} )
[ {"2014-01-01"},   {"2014-06-01"} )
[ {"2014-06-01"},   {"2014-12-01"} )
```

これらの範囲外のデータはロードされません。

固定範囲

```SQL
CREATE TABLE table_range
(
    k1 DATE,
    k2 INT,
    k3 SMALLINT,
    v1 VARCHAR(2048),
    v2 DATETIME DEFAULT "2014-02-04 15:36:00"
)
ENGINE=olap
DUPLICATE KEY(k1, k2, k3)
PARTITION BY RANGE (k1, k2, k3)
(
    PARTITION p1 VALUES [("2014-01-01", "10", "200"), ("2014-01-01", "20", "300")),
    PARTITION p2 VALUES [("2014-06-01", "100", "200"), ("2014-07-01", "100", "300"))
)
DISTRIBUTED BY HASH(k2)
PROPERTIES(
    "storage_medium" = "SSD"
);
```

### MySQL外部テーブル

```SQL
CREATE EXTERNAL TABLE example_db.table_mysql
(
    k1 DATE,
    k2 INT,
    k3 SMALLINT,
    k4 VARCHAR(2048),
    k5 DATETIME
)
ENGINE=mysql
PROPERTIES
(
    "host" = "127.0.0.1",
    "port" = "8239",
    "user" = "mysql_user",
    "password" = "mysql_passwd",
    "database" = "mysql_db_test",
    "table" = "mysql_table_test"
)
```

### HLLカラムを持つテーブル

```SQL
CREATE TABLE example_db.example_table
(
    k1 TINYINT,
    k2 DECIMAL(10, 2) DEFAULT "10.5",
    v1 HLL HLL_UNION,
    v2 HLL HLL_UNION
)
ENGINE=olap
AGGREGATE KEY(k1, k2)
DISTRIBUTED BY HASH(k1)
PROPERTIES ("storage_type"="column");
```

### BITMAP_UNION集計タイプを使用するテーブル

`v1` および `v2` カラムの元のデータ型は、TINYINT、SMALLINT、または INT である必要があります。

```SQL
CREATE TABLE example_db.example_table
(
    k1 TINYINT,
    k2 DECIMAL(10, 2) DEFAULT "10.5",
    v1 BITMAP BITMAP_UNION,
    v2 BITMAP BITMAP_UNION
)
ENGINE=olap
AGGREGATE KEY(k1, k2)
DISTRIBUTED BY HASH(k1)
PROPERTIES ("storage_type"="column");
```

### Colocate Joinをサポートするテーブル

```SQL
CREATE TABLE `t1` 
(
     `id` int(11) COMMENT "",
    `value` varchar(8) COMMENT ""
) 
ENGINE=OLAP
DUPLICATE KEY(`id`)
DISTRIBUTED BY HASH(`id`)
PROPERTIES 
(
    "colocate_with" = "t1"
);

CREATE TABLE `t2` 
(
    `id` int(11) COMMENT "",
    `value` varchar(8) COMMENT ""
) 
ENGINE=OLAP
DUPLICATE KEY(`id`)
DISTRIBUTED BY HASH(`id`)
PROPERTIES 
(
    "colocate_with" = "t1"
);
```

### ビットマップインデックスを持つテーブル

```SQL
CREATE TABLE example_db.table_hash
(
    k1 TINYINT,
    k2 DECIMAL(10, 2) DEFAULT "10.5",
    v1 CHAR(10) REPLACE,
    v2 INT SUM,
    INDEX k1_idx (k1) USING BITMAP COMMENT 'xxxxxx'
)
ENGINE=olap
AGGREGATE KEY(k1, k2)
COMMENT "my first starrocks table"
DISTRIBUTED BY HASH(k1)
PROPERTIES ("storage_type"="column");
```

### 動的パーティションテーブル

動的パーティショニング機能は、FE設定で有効にする必要があります（"dynamic_partition.enable" = "true"）。詳細については、[動的パーティションの設定](#configure-dynamic-partitions)。

この例では、今後3日間のパーティションを作成し、3日前に作成されたパーティションを削除します。例えば、今日が2020-01-08の場合、p20200108、p20200109、p20200110、p20200111という名前のパーティションが作成され、その範囲は次のとおりです:

```plaintext
[types: [DATE]; keys: [2020-01-08]; ‥types: [DATE]; keys: [2020-01-09]; )
[types: [DATE]; keys: [2020-01-09]; ‥types: [DATE]; keys: [2020-01-10]; )
[types: [DATE]; keys: [2020-01-10]; ‥types: [DATE]; keys: [2020-01-11]; )
[types: [DATE]; keys: [2020-01-11]; ‥types: [DATE]; keys: [2020-01-12]; )
```

```SQL
CREATE TABLE example_db.dynamic_partition
(
    k1 DATE,
    k2 INT,
    k3 SMALLINT,
    v1 VARCHAR(2048),
    v2 DATETIME DEFAULT "2014-02-04 15:36:00"
)
ENGINE=olap
DUPLICATE KEY(k1, k2, k3)
PARTITION BY RANGE (k1)
(
    PARTITION p1 VALUES LESS THAN ("2014-01-01"),
    PARTITION p2 VALUES LESS THAN ("2014-06-01"),
    PARTITION p3 VALUES LESS THAN ("2014-12-01")
)
DISTRIBUTED BY HASH(k2)
PROPERTIES(
    "storage_medium" = "SSD",
    "dynamic_partition.enable" = "true",
    "dynamic_partition.time_unit" = "DAY",
    "dynamic_partition.start" = "-3",
    "dynamic_partition.end" = "3",
    "dynamic_partition.prefix" = "p",
    "dynamic_partition.buckets" = "10"
);
```

### バッチで複数のパーティションが作成され、整数カラムがパーティショニングカラムとして使用されるテーブル

以下の例では、パーティショニングカラム `datekey` はINT型です。すべてのパーティションは、1つのシンプルなパーティション句 `START ("1") END ("5") EVERY (1)` のみによって作成されます。すべてのパーティションの範囲は `1` から始まり `5` で終わり、パーティションの粒度は `1` です:

> **注**
>
> パーティショニングカラムの値は**START()**と**END()**は引用符で囲む必要がありますが、**EVERY()**のパーティション粒度は引用符で囲む必要はありません。

```SQL
CREATE TABLE site_access (
    datekey INT,
    site_id INT,
    city_code SMALLINT,
    user_name VARCHAR(32),
    pv BIGINT DEFAULT '0'
)
ENGINE=olap
DUPLICATE KEY(datekey, site_id, city_code, user_name)
PARTITION BY RANGE (datekey) (START ("1") END ("5") EVERY (1)
)
DISTRIBUTED BY HASH(site_id)
PROPERTIES ("replication_num" = "3");
```

### Hive外部テーブル

Hive外部テーブルを作成する前に、Hiveリソースとデータベースを作成しておく必要があります。詳細については、[外部テーブル](../../../data_source/External_table.md#deprecated-hive-external-table)。

```SQL
CREATE EXTERNAL TABLE example_db.table_hive
(
    k1 TINYINT,
    k2 VARCHAR(50),
    v INT
)
ENGINE=hive
PROPERTIES
(
    "resource" = "hive0",
    "database" = "hive_db_name",
    "table" = "hive_table_name"
);
```

### 特定のソートキーを持つPrimary Keyテーブル

ユーザーの住所や最終アクティブ時間などのディメンションからユーザー行動をリアルタイムで分析する必要があるとします。テーブルを作成する際、`user_id` カラムをプライマリキーとして定義し、`address` と `last_active` カラムの組み合わせをソートキーとして定義できます。

```SQL
create table users (
    user_id bigint NOT NULL,
    name string NOT NULL,
    email string NULL,
    address string NULL,
    age tinyint NULL,
    sex tinyint NULL,
    last_active datetime,
    property0 tinyint NOT NULL,
    property1 tinyint NOT NULL,
    property2 tinyint NOT NULL,
    property3 tinyint NOT NULL
) 
PRIMARY KEY (`user_id`)
DISTRIBUTED BY HASH(`user_id`)
ORDER BY(`address`,`last_active`)
PROPERTIES(
    "replication_num" = "3",
    "enable_persistent_index" = "true"
);
```

### パーティション化された一時テーブル

```SQL
CREATE TEMPORARY TABLE example_db.temp_table
(
    k1 DATE,
    k2 INT,
    k3 SMALLINT,
    v1 VARCHAR(2048),
    v2 DATETIME DEFAULT "2014-02-04 15:36:00"
)
ENGINE=olap
DUPLICATE KEY(k1, k2, k3)
PARTITION BY RANGE (k1)
(
    PARTITION p1 VALUES LESS THAN ("2014-01-01"),
    PARTITION p2 VALUES LESS THAN ("2014-06-01"),
    PARTITION p3 VALUES LESS THAN ("2014-12-01")
)
DISTRIBUTED BY HASH(k2);
```

### フラットJSONプロパティを持つテーブル

```SQL
CREATE TABLE example_db.example_table
(
    k1 DATE,
    k2 INT,
    v1 VARCHAR(2048),
    v2 JSON
)
ENGINE=olap
DUPLICATE KEY(k1, k2)
DISTRIBUTED BY HASH(k2)
PROPERTIES (
    "flat_json.enable" = "true",
    "flat_json.null.factor" = "0.5",
    "flat_json.sparsity.factor" = "0.5",
    "flat_json.column.max" = "50"
);
```

## 参照

- [SHOW CREATE TABLE](SHOW_CREATE_TABLE.md)
- [テーブルを表示](SHOW_TABLES.md)
- [使用](../Database/USE.md)
- [テーブルを変更](ALTER_TABLE.md)
- [テーブルを削除](DROP_TABLE.md)
