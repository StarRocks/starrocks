---
displayed_sidebar: docs
---

# CREATE TABLE

StarRocks に新しいテーブルを作成します。

:::tip
この操作には、宛先データベースに対する CREATE TABLE 権限が必要です。
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

- 作成するテーブル名、パーティション名、列名、およびインデックス名は、以下の命名規則に従う必要があります。[システム制限](../../System_limit.md)。
- データベース名、テーブル名、列名、またはパーティション名を指定する際、一部のリテラルは StarRocks の予約キーワードとして使用されることに注意してください。これらのキーワードを SQL ステートメントで直接使用しないでください。SQL ステートメントでそのようなキーワードを使用したい場合は、バッククォート (`) で囲んでください。詳細については、[キーワード](../keywords.md)これらの予約キーワードを参照してください。

:::

## キーワード

### `EXTERNAL`

:::caution
`EXTERNAL` キーワードは非推奨です。

代わりに、[external catalogs を使用することをお勧めします。](../../../data_source/catalog/catalog_overview.md)Hive、Iceberg、Hudi、および JDBC データソースからデータをクエリするために、`EXTERNAL` キーワードを使用して外部テーブルを作成するのではなく、

:::

:::tip
**推奨事項**

v3.1 以降、StarRocks は Iceberg catalogs に Parquet 形式のテーブルを作成することをサポートし、INSERT INTO を使用してこれらの Parquet 形式の Iceberg テーブルにデータをシンクすることをサポートします。

v3.2 以降、StarRocks は Hive catalogs に Parquet 形式のテーブルを作成することをサポートし、INSERT INTO を使用してこれらの Parquet 形式の Hive テーブルにデータをシンクすることをサポートします。v3.3 以降、StarRocks は Hive catalogs に ORC および Textfile 形式のテーブルを作成することをサポートし、INSERT INTO を使用してこれらの ORC および Textfile 形式の Hive テーブルにデータをシンクすることをサポートします。
:::

非推奨の `EXTERNAL` キーワードを使用したい場合は、**`EXTERNAL` キーワードの詳細**

<details>
  <summary>`EXTERNAL` キーワードの詳細</summary>

  外部データソースをクエリするための外部テーブルを作成するには、`CREATE EXTERNAL TABLE` を指定し、`ENGINE` をこれらの値のいずれかに設定します。詳細については、[外部テーブル](../../../data_source/External_table.md)を参照してください。

  - MySQL 外部テーブルの場合、以下のプロパティを指定します。

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

    MySQL の "table_name" は実際のテーブル名を示す必要があります。対照的に、CREATE TABLE ステートメントの "table_name" は、StarRocks 上のこの MySQL テーブルの名前を示します。これらは異なっていても同じでも構いません。

    StarRocks で MySQL テーブルを作成する目的は、MySQL データベースにアクセスすることです。StarRocks 自体は MySQL データを維持または保存しません。

  - Elasticsearch 外部テーブルの場合、以下のプロパティを指定します。

    ```plaintext
    PROPERTIES (
    "hosts" = "http://192.168.xx.xx:8200,http://192.168.xx0.xx:8200",
    "user" = "root",
    "password" = "root",
    "index" = "tindex",
    "type" = "doc"
    )
    ```

    - `hosts`: Elasticsearch クラスタへの接続に使用される URL です。1つ以上の URL を指定できます。
    - `user`: 基本認証が有効になっている Elasticsearch クラスタにログインするために使用されるルートユーザーのアカウントです。
    - `password`: 前述のルートアカウントのパスワードです。
    - `index`: Elasticsearch クラスタ内の StarRocks テーブルのインデックスです。インデックス名は StarRocks テーブル名と同じです。このパラメータを StarRocks テーブルのエイリアスに設定できます。
    - `type`: インデックスのタイプです。デフォルト値は `doc` です。

  - Hive 外部テーブルの場合、以下のプロパティを指定します。

    ```plaintext
    PROPERTIES (
        "database" = "hive_db_name",
        "table" = "hive_table_name",
        "hive.metastore.uris" = "thrift://xx.xx.xx.xx:9083"
    )
    ```

    ここで、database は Hive テーブル内の対応するデータベースの名前です。Table は Hive テーブルの名前です。`hive.metastore.uris` はサーバーアドレスです。

  - JDBC 外部テーブルの場合、以下のプロパティを指定します。

    ```plaintext
    PROPERTIES (
    "resource"="jdbc0",
    "table"="dest_tbl"
    )
    ```

    `resource` は JDBC リソース名で、 `table` は宛先テーブルです。

  - Iceberg 外部テーブルの場合、以下のプロパティを指定します。

    ```plaintext
     PROPERTIES (
     "resource" = "iceberg0", 
     "database" = "iceberg", 
     "table" = "iceberg_table"
     )
    ```

    `resource` は Iceberg リソース名です。 `database` は Iceberg データベースです。 `table` は Iceberg テーブルです。

  - Hudi 外部テーブルの場合、以下のプロパティを指定します。

    ```plaintext
      PROPERTIES (
      "resource" = "hudi0", 
      "database" = "hudi", 
      "table" = "hudi_table" 
      )
    ```
</details>

### `TEMPORARY`

一時テーブルを作成します。v3.3.1 以降、 StarRocks は Default Catalog で一時テーブルの作成をサポートしています。詳細については、[一時テーブル](../../../table_design/StarRocks_table_design.md#temporary-table)。

:::note
一時テーブルを作成する際は、 `ENGINE` を `olap` に設定する必要があります。
:::

## 列定義

```SQL
col_name col_type [agg_type] [NULL | NOT NULL] [DEFAULT "default_value"] [AUTO_INCREMENT] [AS generation_expr]
```

### `col_name`

通常、 `__op` または `__row` で始まる名前の列を作成することはできません。これらの名前形式は StarRocks で特別な目的のために予約されており、そのような列を作成すると未定義の動作になる可能性があるためです。もしそのような列を作成する必要がある場合は、FE 動的パラメータ[`allow_system_reserved_names`](../../../administration/management/FE_configuration.md#allow_system_reserved_names) を `TRUE` に設定します。

### `col_type`

タイプや範囲など、特定の列情報:

- TINYINT (1 バイト): -2^7 + 1 から 2^7 - 1 の範囲です。

- SMALLINT (2 バイト): -2^15 + 1 から 2^15 - 1 の範囲です。

- INT (4 バイト): -2^31 + 1 から 2^31 - 1 の範囲です。

- BIGINT (8 バイト): -2^63 + 1 から 2^63 - 1 の範囲です。

- LARGEINT (16 バイト): -2^127 + 1 から 2^127 - 1 の範囲です。

- FLOAT (4 バイト): 科学表記をサポートします。

- DOUBLE (8 バイト): 科学表記をサポートします。

- DECIMAL[(精度, スケール)] (16 バイト)

  - デフォルト値: DECIMAL(10, 0)
  - 精度: 1 ~ 38
  - スケール: 0 ~ 精度
  - 整数部: 精度 - スケール

    科学表記はサポートされていません。

- DATE (3 バイト): 0000-01-01 から 9999-12-31 の範囲です。

- DATETIME (8 バイト): 0000-01-01 00:00:00 から 9999-12-31 23:59:59 の範囲です。

- CHAR[(長さ)]: 固定長文字列。範囲: 1 ~ 255。デフォルト値: 1。

- VARCHAR[(長さ)]: 可変長文字列。デフォルト値は 1 です。単位: バイト。 StarRocks 2.1 より前のバージョンでは、 `length` の値の範囲は 1～65533 です。[プレビュー] StarRocks 2.1 以降のバージョンでは、 `length` の値の範囲は 1～1048576 です。

- HLL (1~16385 バイト): HLL タイプの場合、長さやデフォルト値を指定する必要はありません。長さはデータ集計に応じてシステム内で制御されます。 HLL 列は、[hll_union_agg](../../sql-functions/aggregate-functions/hll_union_agg.md)、[Hll_cardinality](../../sql-functions/scalar-functions/hll_cardinality.md)、および[hll_hash](../../sql-functions/scalar-functions/hll_hash.md) のみでクエリまたは使用できます。

- BITMAP: ビットマップタイプは、指定された長さやデフォルト値を必要としません。符号なし bigint 数値のセットを表します。最大要素は 2^64 - 1 まで可能です。

### `agg_type`

集計タイプ。指定しない場合、この列はキー列です。
指定した場合、値列です。サポートされている集計タイプは次のとおりです。

- `SUM`、 `MAX`、 `MIN`、 `REPLACE`
- `HLL_UNION` ( `HLL` タイプのみ)
- `BITMAP_UNION` ( `BITMAP` のみ)
- `REPLACE_IF_NOT_NULL`: これは、インポートされたデータが非NULL値の場合にのみ置換されることを意味します。NULL値の場合、 StarRocks は元の値を保持します。

:::note

- 集計タイプ BITMAP_UNION の列がインポートされる場合、その元のデータ タイプは TINYINT、 SMALLINT、 INT、および BIGINT である必要があります。
- テーブル作成時に REPLACE_IF_NOT_NULL 列によって NOT NULL が指定されている場合でも、 StarRocks はユーザーにエラーレポートを送信することなくデータを NULL に変換します。これにより、ユーザーは選択した列をインポートできます。
:::

この集計タイプは、key_desc タイプが AGGREGATE KEY である集計テーブルに ONLY を適用します。v3.1.9 以降、 `REPLACE_IF_NOT_NULL` は BITMAP タイプの列を新たにサポートします。

### `NULL` | `NOT NULL`

列が `NULL` を許可されているかどうか。デフォルトでは、重複キーテーブル、集計テーブル、またはユニークキーテーブルを使用するテーブルのすべての列に `NULL` が指定されます。主キーテーブルを使用するテーブルでは、デフォルトで値列には `NULL` が指定され、キー列には `NOT NULL` が指定されます。生データに `NULL` 値が含まれている場合は、 `\N` で表現します。 StarRocks はデータロード中に `\N` を `NULL` として扱います。

### `DEFAULT`

列のデフォルト値。 StarRocks にデータをロードする際、列にマッピングされたソースフィールドが空の場合、 StarRocks は自動的に列にデフォルト値を入力します。デフォルト値は、以下のいずれかの方法で指定できます。

- **DEFAULT current_timestamp**: 現在時刻をデフォルト値として使用します。詳細については、以下を参照してください。[current_timestamp()](../../sql-functions/date-time-functions/current_timestamp.md)。
- **DEFAULT (<expr>)**: 指定された式または関数によって返される結果をデフォルト値として使用します。以下の式がサポートされています。
  - [uuid()](../../sql-functions/utility-functions/uuid.md)および[uuid_numeric()](../../sql-functions/utility-functions/uuid_numeric.md): 一意の識別子を生成します。
  - ARRAY リテラル式 (例: `[1, 2, 3]`): ARRAY タイプ列の場合。
  - MAP 式 (例: `map{key: value}`): MAP タイプ列の場合。
  - `row()` 関数 (例: `row(val1, val2)`): STRUCT タイプ列の場合。
- **DEFAULT `<default_value>`**: 列のデータタイプの指定された値をデフォルト値として使用します。 StarRocks は、異なるタイプに対してデフォルト値を指定することをサポートしています。

  **基本タイプ**: 文字列リテラルを使用してデフォルト値を指定します。

  ```sql
  -- 数値タイプ
  age INT DEFAULT '18'
  price DECIMAL(10,2) DEFAULT '99.99'

  -- 文字列タイプ
  name VARCHAR(50) DEFAULT 'Anonymous'

  -- 日付/時刻タイプ
  created_at DATETIME DEFAULT '2024-01-01 00:00:00'

  -- ブールタイプ
  is_active BOOLEAN DEFAULT 'true'  -- Supports 'true'/'false'/'1'/'0'
  ```

  **JSON タイプ**: JSON 形式の文字列を使用してデフォルト値を指定します。

  ```sql
  metadata JSON DEFAULT '{"status": "active"}'
  tags JSON DEFAULT '[1, 2, 3]'
  ```

  **VARBINARY タイプ**: 空の文字列のみがデフォルト値としてサポートされています。

  ```sql
  binary_data VARBINARY DEFAULT ''
  ```

  **BITMAP および HLL タイプ**: 空の文字列のみがデフォルト値としてサポートされており、 AGGREGATE KEY テーブルのみに適用されます。

  ```sql
  -- AGGREGATE KEY テーブルでは
  bm BITMAP BITMAP_UNION DEFAULT ''
  h HLL HLL_UNION DEFAULT ''
  ```

  **複合タイプ ( ARRAY / MAP / STRUCT )**: 式構文を使用してデフォルト値を指定します。OLAP テーブルのみでサポートされています。

  :::note
複合タイプのデフォルト値は**`fast_schema_evolution = true` の場合にのみサポートされます**です。テーブルの `fast_schema_evolution` プロパティが明示的に `false` に設定されている場合、複合タイプにデフォルト値を追加するとエラーが発生します。
:::

  ```sql
  -- ARRAY タイプ
  tags ARRAY<VARCHAR(20)> DEFAULT ['tag1', 'tag2']
  scores ARRAY<INT> DEFAULT [90, 85, 92]

  -- MAP タイプ
  attrs MAP<VARCHAR(20), INT> DEFAULT map{'age': 25, 'score': 100}

  -- STRUCT タイプ
  person STRUCT<name VARCHAR(20), age INT> DEFAULT row('John', 30)

  -- 複雑なネスト: ネストされた STRUCT、ARRAY、および MAP を持つ STRUCT
  user_profile STRUCT<
    id INT, 
    name VARCHAR(50), 
    contact STRUCT<email VARCHAR(100), phone VARCHAR(20)>,
    tags ARRAY<VARCHAR(20)>,
    attributes MAP<VARCHAR(20), VARCHAR(50)>
  > DEFAULT row(1, 'Alice', row('alice@example.com', '123-456-7890'), ['admin', 'user'], map{'level': 'premium', 'status': 'active'})
  ```

  **制限事項**:

  - TIME および VARIANT タイプは、まだデフォルト値をサポートしていません。
  - 複合タイプ (ARRAY/MAP/STRUCT) のデフォルト値は、OLAP テーブルでのみサポートされており、`fast_schema_evolution` プロパティを有効にする必要があります。

### `AUTO_INCREMENT`

`AUTO_INCREMENT` 列を指定します。`AUTO_INCREMENT` 列のデータタイプは BIGINT である必要があります。自動増分された ID は 1 から始まり、1 ステップで増加します。`AUTO_INCREMENT` 列の詳細については、[AUTO_INCREMENT](auto_increment.md)を参照してください。v3.0 以降、StarRocks は `AUTO_INCREMENT` 列をサポートしています。

### `AS`

生成列とその式を指定します。[生成列](../generated_columns.md)は、式の計算結果を事前に計算して保存するために使用でき、同じ複雑な式を持つクエリを大幅に高速化します。v3.1 以降、StarRocks は生成列をサポートしています。

## インデックス定義

```SQL
INDEX index_name (col_name[, col_name, ...]) [USING BITMAP] COMMENT 'xxxxxx'
```

パラメータの説明と使用上の注意の詳細については、[ビットマップインデックス](../../../table_design/indexes/Bitmap_index.md#create-an-index)を参照してください。

## `ENGINE`

デフォルト値: `olap`。このパラメータが指定されていない場合、デフォルトで OLAP テーブル (StarRocks 内部テーブル) が作成されます。

オプション値: `mysql`、`elasticsearch`、`hive`、`jdbc`、`iceberg`、および `hudi`。

## キー

構文:

```SQL
key_type(k1[,k2 ...])
```

データは指定されたキー列でシーケンスされ、異なるキータイプに対して異なる属性を持ちます。

- AGGREGATE KEY: キー列内の同一コンテンツは、指定された集計タイプに従って値列に集計されます。これは通常、財務諸表や多次元分析などのビジネスシナリオに適用されます。
- UNIQUE KEY/PRIMARY KEY: キー列内の同一コンテンツは、インポートシーケンスに従って値列で置き換えられます。これは、キー列に対する追加、削除、変更、およびクエリを行うために適用できます。
- DUPLICATE KEY: キー列内の同一コンテンツは、StarRocks に共存します。これは、詳細データや集計属性のないデータを保存するために使用できます。

  :::note
DUPLICATE KEY がデフォルトのタイプです。データはキー列に従ってシーケンスされます。
:::

:::note
AGGREGATE KEY を除く他のキータイプを使用してテーブルを作成する場合、値列は集計タイプを指定する必要はありません。
:::

### レンジベースの分散

v4.1 以降、StarRocks は **レンジベースの分散セマンティクス** (デフォルトでは無効) をサポートしており、FE 設定 `enable_range_distribution` によって制御されます。データはキー列のデータレンジに従ってシーケンスされ、各 tablet には特定のレンジのデータが含まれます。

レンジベースの分散セマンティクスは、以下の点でデフォルトのセマンティクスとは異なります。

- キータイプ (AGGREGATE KEY/UNIQUE KEY/PRIMARY KEY/DUPLICATE KEY) が明示的に指定されており、DISTRIBUTED BY 句が指定されていない場合、データはデフォルトでレンジによって分散されます。
- キータイプ、DISTRIBUTED BY 句、または ORDER BY のいずれも指定されていない場合、ランダムバケット法戦略の重複キーテーブルが作成されます。
- キータイプと DISTRIBUTED BY 句が指定されていないが、ORDER BY 句が指定されている場合、レンジベースの分散戦略の重複キーテーブルが作成されます。この場合、DUPLICATE KEY は ORDER BY 句と同等であり、その逆も同様です。
- DUPLICATE KEY と ORDER BY 句の両方が指定されている場合、ORDER BY 句のみが有効になり、DUPLICATE KEY は無視されます。

## `COMMENT`

テーブル作成時にテーブルコメントを追加できます (オプション)。COMMENT は `key_desc` の後に配置する必要があることに注意してください。そうしないと、テーブルを作成できません。

v3.1 以降、`ALTER TABLE <table_name> COMMENT = "new table comment"` を使用してテーブルコメントを変更できます。

## パーティション

パーティションは以下の方法で管理できます。

### パーティションを動的に作成する

[動的パーティション化](../../../table_design/data_distribution/dynamic_partitioning.md)は、パーティションの有効期間 (TTL) 管理を提供します。StarRocks は、新しいパーティションを事前に自動的に作成し、期限切れのパーティションを削除して、データの鮮度を確保します。この機能を有効にするには、テーブル作成時に動的パーティション化関連のプロパティを設定できます。

### パーティションを一つずつ作成する

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
パーティション化には、指定されたキー列と指定された値の範囲を使用してください。
:::

- パーティションの命名規則については、以下を参照してください。[システム制限](../../System_limit.md)。
- v3.3.0より前では、レンジパーティション化の列は、TINYINT、SMALLINT、INT、BIGINT、LARGEINT、DATE、および DATETIME のタイプのみをサポートします。v3.3.0以降では、3つの特定の時間関数をレンジパーティション化の列として使用できます。詳細な使用方法については、以下を参照してください。[データ分散](../../../table_design/data_distribution/Data_distribution.md#manually-create-partitions)。
- パーティションは左閉右開です。最初のパーティションの左境界は最小値です。
- NULL 値は、最小値を含むパーティションにのみ保存されます。最小値を含むパーティションが削除されると、NULL 値はインポートできなくなります。
- パーティション列は、単一の列または複数の列のいずれかです。パーティション値はデフォルトの最小値です。
- パーティション列として1つの列のみが指定されている場合、最新のパーティションのパーティション列の上限として `MAXVALUE` を設定できます。

  ```SQL
  PARTITION BY RANGE (pay_dt) (
    PARTITION p1 VALUES LESS THAN ("20210102"),
    PARTITION p2 VALUES LESS THAN ("20210103"),
    PARTITION p3 VALUES LESS THAN MAXVALUE
  )
  ```

:::note

- パーティションは、時間に関連するデータを管理するためによく使用されます。
- データ遡及が必要な場合、必要に応じて後でパーティションを追加するために、最初のパーティションを空にすることを検討するとよいでしょう。
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

- Fixed Range は LESS THAN よりも柔軟です。左右のパーティションをカスタマイズできます。
- Fixed Range はその他の点では LESS THAN と同じです。
- パーティション列として1つの列のみが指定されている場合、最新のパーティションのパーティション列の上限として `MAXVALUE` を設定できます。
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

- パーティション列が日付タイプの場合。

  ```sql
  PARTITION BY RANGE (<partitioning_column>) (
      START ("<start_date>") END ("<end_date>") EVERY (INTERVAL <N> <time_unit>)
  )
  ```

- パーティション列が整数タイプの場合。

  ```sql
  PARTITION BY RANGE (<partitioning_column>) (
      START ("<start_integer>") END ("<end_integer>") EVERY (<partitioning_granularity>)
  )
  ```

説明

`START()` と `END()` で開始値と終了値を、`EVERY()` で時間単位またはパーティション化の粒度を指定して、複数のパーティションを一括で作成できます。

- v3.3.0より前では、レンジパーティション化の列は、TINYINT、SMALLINT、INT、BIGINT、LARGEINT、DATE、および DATETIME のタイプのみをサポートします。v3.3.0以降では、3つの特定の時間関数をレンジパーティション化の列として使用できます。詳細な使用方法については、以下を参照してください。[データ分散](../../../table_design/data_distribution/Data_distribution.md#manually-create-partitions)。
- パーティション列が日付タイプの場合、`INTERVAL` キーワードを使用して時間間隔を指定する必要があります。時間単位は、時間（v3.0以降）、日、週、月、または年として指定できます。パーティションの命名規則は、動的パーティション化の場合と同じです。

詳細については、以下を参照してください。[データ分散](../../../table_design/data_distribution/Data_distribution.md)。

## 分散

StarRocks はハッシュバケットとランダムバケット法をサポートしています。バケットを設定しない場合、StarRocks はランダムバケット法を使用し、デフォルトでバケット数を自動的に設定します。

- ランダムバケット法（v3.1以降）

  パーティション内のデータについて、StarRocks は特定の列値に基づかず、すべてのバケットにデータをランダムに分散します。StarRocks にバケット数を自動的に設定させたい場合、バケット設定を指定する必要はありません。バケット数を手動で指定する場合は、構文は次のとおりです。

  ```SQL
  DISTRIBUTED BY RANDOM BUCKETS <num>
  ```

  ただし、大量のデータをクエリし、特定の列を条件列として頻繁に使用する場合、ランダムバケット法によって提供されるクエリパフォーマンスは理想的ではない可能性があることに注意してください。このシナリオでは、ハッシュバケットを使用することをお勧めします。少数のバケットのみをスキャンして計算する必要があるため、クエリパフォーマンスが大幅に向上します。

  **注意事項**

  - ランダムバケット法を使用して作成できるのは、重複キーテーブルのみです。
  - 指定することはできません。[Colocation Group](../../../using_starrocks/Colocate_join.md)ランダムバケット化されたテーブルの場合。
  - Spark Load は、ランダムバケット化されたテーブルにデータをロードするために使用できません。
  - StarRocks v2.5.7 以降、テーブル作成時にバケット数を設定する必要はありません。StarRocks が自動的にバケット数を設定します。このパラメータを設定したい場合は、以下を参照してください。[バケット数を設定する](../../../table_design/data_distribution/Data_distribution.md#set-the-number-of-buckets)。

  詳細については、以下を参照してください。[ランダムバケット法](../../../table_design/data_distribution/Data_distribution.md#random-bucketing-since-v31)。

- ハッシュバケット化

  構文:

  ```SQL
  DISTRIBUTED BY HASH (k1[,k2 ...]) [BUCKETS num]
  ```

  パーティション内のデータは、バケッティング列のハッシュ値とバケット数に基づいてバケットに細分化できます。以下の2つの要件を満たす列をバケッティング列として選択することをお勧めします。

  - ID などの高基数な列
  - クエリで頻繁にフィルタとして使用される列

  そのような列が存在しない場合は、クエリの複雑さに基づいてバケッティング列を決定できます。

  - クエリが複雑な場合は、バケット間のデータ分散を均等にし、クラスタリソースの利用率を向上させるために、高基数な列をバケッティング列として選択することをお勧めします。
  - クエリが比較的単純な場合は、クエリ条件として頻繁に使用される列をバケッティング列として選択し、クエリ効率を向上させることをお勧めします。

  1つのバケッティング列を使用してもパーティションデータを各バケットに均等に分散できない場合は、複数のバケッティング列（最大3つ）を選択できます。詳細については、以下を参照してください。[バケッティング列の選択](../../../table_design/data_distribution/Data_distribution.md#hash-bucketing)。

  **注意事項**:

  - **テーブルを作成する際、バケッティング列を指定する必要があります**。
  - バケッティング列の値は更新できません。
  - バケッティング列は指定後に変更できません。
  - StarRocks v2.5.7 以降、テーブル作成時にバケット数を設定する必要はありません。StarRocks が自動的にバケット数を設定します。このパラメータを設定したい場合は、以下を参照してください。[バケット数を設定する](../../../table_design/data_distribution/Data_distribution.md#set-the-number-of-buckets)。

- レンジベースの分散

  v4.1 以降、StarRocks は以下をサポートします。**レンジベースの分散セマンティクス** （デフォルトで無効）、FE 設定 `enable_range_distribution` によって制御されます。詳細については、以下を参照してください。[レンジベースの分散](#range-based-distribution)。

## ロールアップインデックス

テーブル作成時にロールアップを一括で作成できます。

構文:

```SQL
ROLLUP (rollup_name (column_name1, column_name2, ...)
[FROM from_index_name]
[PROPERTIES ("key"="value", ...)],...)
```

## `ORDER BY`

v3.0 以降、主キーテーブルは `ORDER BY` を使用したソートキーの定義をサポートしています。v3.3 以降、重複キーテーブル、集計テーブル、ユニークキーテーブルは `ORDER BY` を使用したソートキーの定義をサポートしています。

ソートキーの詳細については、以下を参照してください。[ソートキーとプレフィックスインデックス](../../../table_design/indexes/Prefix_index_sort_key.md)。

## `PROPERTIES`

### ストレージとレプリカ

エンジンタイプが `OLAP` の場合、テーブル作成時に初期記憶媒体 (`storage_medium`)、自動クールダウン時間 (`storage_cooldown_time`) または時間間隔 (`storage_cooldown_ttl`)、およびレプリカ数 (`replication_num`) を指定できます。

プロパティが適用されるスコープ: テーブルにパーティションが1つしかない場合、プロパティはテーブルに属します。テーブルが複数のパーティションに分割されている場合、プロパティは各パーティションに属します。特定のパーティションに異なるプロパティを設定する必要がある場合は、以下を実行できます。[ALTER TABLE ... ADD PARTITION または ALTER TABLE ... MODIFY PARTITION](ALTER_TABLE.md) テーブル作成後に。

#### 初期記憶媒体と自動クールダウン時間を設定する

```sql
PROPERTIES (
    "storage_medium" = "[SSD|HDD]",
    { "storage_cooldown_ttl" = "<num> { YEAR | MONTH | DAY | HOUR } "
    | "storage_cooldown_time" = "yyyy-MM-dd HH:mm:ss" }
)
```

**プロパティ**

- `storage_medium`: 初期記憶媒体。`SSD` または `HDD` に設定できます。明示的に指定した記憶媒体のタイプが、BE 静的パラメータ `storage_root_path` で指定された StarRocks クラスタの BE ディスクタイプと一致していることを確認してください。<br />

  FE 設定項目 `enable_strict_storage_medium_check` が `true` に設定されている場合、テーブル作成時にシステムは BE ディスクタイプを厳密にチェックします。CREATE TABLE で指定した記憶媒体が BE ディスクタイプと一致しない場合、「Failed to find enough host in all backends with storage medium is SSD|HDD.」というエラーが返され、テーブル作成は失敗します。`enable_strict_storage_medium_check` が `false` に設定されている場合、システムはこのエラーを無視し、強制的にテーブルを作成します。ただし、データロード後にクラスタのディスクスペースが不均一に分散される可能性があります。<br />

  v2.3.6、v2.4.2、v2.5.1、および v3.0 以降、`storage_medium` が明示的に指定されていない場合、システムは BE ディスクタイプに基づいて記憶媒体を自動的に推論します。<br />

  - システムは次のシナリオでこのパラメータを自動的に SSD に設定します。

    - BE が報告するディスクタイプ (`storage_root_path`) に SSD のみ含まれる場合。
    - BE が報告するディスクタイプ (`storage_root_path`) に SSD と HDD の両方が含まれる場合。なお、v2.3.10、v2.4.5、v2.5.4、および v3.0 以降、BE が報告する `storage_root_path` に SSD と HDD の両方が含まれ、かつプロパティ `storage_cooldown_time` が指定されている場合、システムは `storage_medium` を SSD に設定します。

  - システムは次のシナリオでこのパラメータを自動的に HDD に設定します。

    - BE が報告するディスクタイプ (`storage_root_path`) に HDD のみ含まれる場合。
    - 2.3.10、2.4.5、2.5.4、および 3.0 以降、BE が報告する `storage_root_path` に SSD と HDD の両方が含まれ、かつプロパティ `storage_cooldown_time` が指定されていない場合、システムは `storage_medium` を HDD に設定します。

- `storage_cooldown_ttl` または `storage_cooldown_time`: 自動クールダウン時間または時間間隔。自動クールダウンとは、SSD から HDD へデータを自動的に移行することを指します。この機能は、初期記憶媒体が SSD の場合にのみ有効です。

  - `storage_cooldown_ttl`: このテーブルのパーティションに対する**時間間隔**自動クールダウンの時間間隔です。最新のパーティションを SSD に保持し、一定の時間間隔後に古いパーティションを自動的に HDD にクールダウンする必要がある場合、このパラメータを使用できます。各パーティションの自動クールダウン時間は、このパラメータの値にパーティションの上限時間を加算して計算されます。

  サポートされている値は `<num> YEAR`、`<num> MONTH`、`<num> DAY`、および `<num> HOUR` です。`<num>` は非負の整数です。デフォルト値は null であり、ストレージのクールダウンが自動的に実行されないことを示します。

  例えば、テーブル作成時に値を `"storage_cooldown_ttl"="1 DAY"` と指定し、`[2023-08-01 00:00:00,2023-08-02 00:00:00)` の範囲を持つパーティション `p20230801` が存在するとします。このパーティションの自動クールダウン時間は `2023-08-03 00:00:00` であり、これは `2023-08-02 00:00:00 + 1 DAY` です。テーブル作成時に値を `"storage_cooldown_ttl"="0 DAY"` と指定した場合、このパーティションの自動クールダウン時間は `2023-08-02 00:00:00` です。

  - `storage_cooldown_time`: 自動クールダウン時間 (**絶対時間**) で、テーブルが SSD から HDD にクールダウンされる時刻です。指定された時刻は現在時刻よりも後である必要があります。フォーマット: 「yyyy-MM-dd HH:mm:ss」。特定のパーティションに異なるプロパティを設定する必要がある場合は、以下を実行できます。[ALTER TABLE ... ADD PARTITION または ALTER TABLE ... MODIFY PARTITION](ALTER_TABLE.md)。

##### 使用法

- 自動クールダウンに関連するパラメータの比較は次のとおりです。
  - `storage_cooldown_ttl`: テーブル内のパーティションに対する自動クールダウンの時間間隔を指定するテーブルプロパティです。システムは `the value of this parameter plus the upper time bound of the partition` の時刻にパーティションを自動的にクールダウンします。したがって、自動クールダウンはパーティション粒度で実行され、より柔軟です。
  - `storage_cooldown_time`: 自動クールダウン時間 (**絶対時間**) を指定するテーブルプロパティです。また、テーブル作成後に特定のパーティションに異なるプロパティを設定することもできます。
  - `storage_cooldown_second`: クラスタ内のすべてのテーブルに対する自動クールダウンの遅延を指定する静的 FE パラメータです。

- テーブルプロパティ `storage_cooldown_ttl` または `storage_cooldown_time` は、FE 静的パラメータ `storage_cooldown_second` よりも優先されます。

- これらのパラメータを設定する際は、`"storage_medium = "SSD"` を指定する必要があります。

- これらのパラメータを設定しない場合、自動クールダウンは自動的に実行されません。

- `SHOW PARTITIONS FROM <table_name>` を実行して、各パーティションの自動クールダウン時間を表示します。

##### 制限事項

- 式に基づくパーティション化とリストパーティション化はサポートされていません。
- パーティション列は DATE タイプである必要があります。
- 複数のパーティション列はサポートされていません。
- 主キーテーブルはサポートされていません。

#### パーティション内の各 tablet のレプリカ数を設定します

`replication_num`: パーティション内の各テーブルのレプリカ数。デフォルト値: `3`。

```sql
PROPERTIES (
    "replication_num" = "<num>"
)
```

### ブルームフィルターインデックス

Engine タイプが `olap` の場合、ブルームフィルターインデックスを採用する列を指定できます。

ブルームフィルターインデックスを使用する場合、以下の制限が適用されます:

- 重複キーテーブルまたは主キーテーブルのすべての列に対してブルームフィルターインデックスを作成できます。集計テーブルまたはユニークキーテーブルの場合、キー列に対してのみブルームフィルターインデックスを作成できます。
- TINYINT、FLOAT、DOUBLE、および DECIMAL 列は、ブルームフィルターインデックスの作成をサポートしていません。
- ブルームフィルターインデックスは、`in` および `=` オペレーターを含むクエリ（`Select xxx from table where x in {}` や `Select xxx from table where column = xxx` など）のクエリパフォーマンスのみを向上させることができます。この列の離散値が多いほど、より精密なクエリ結果が得られます。

詳細については、以下を参照してください。[ブルームフィルターインデックス](../../../table_design/indexes/Bloomfilter_index.md)

```SQL
PROPERTIES (
    "bloom_filter_columns"="k1,k2,k3"
)
```

### Colocate Join

Colocate Join 属性を使用したい場合は、`properties` で指定します。

```SQL
PROPERTIES (
    "colocate_with"="table1"
)
```

### 動的パーティション

動的パーティション属性を使用したい場合は、プロパティで指定してください。

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

| パラメータ | 必須 | 説明 |
|---|---|---|
| dynamic_partition.enable | いいえ | 動的パーティション化を有効にするかどうか。有効な値: `TRUE` および `FALSE`。デフォルト値: `TRUE`。 |
| dynamic_partition.time_unit | はい | 動的に作成されるパーティションの時間粒度。必須パラメータです。有効な値: `DAY`、`WEEK`、および `MONTH`。時間粒度は、動的に作成されるパーティションのサフィックス形式を決定します。<br />  - 値が `DAY` の場合、動的に作成されるパーティションのサフィックス形式は `yyyyMMdd` です。パーティション名のサフィックスの例は `20200321` です。<br />  - 値が `WEEK` の場合、動的に作成されるパーティションのサフィックス形式は `yyyy_ww` です。例えば、2020年の13週目は `2020_13` となります。<br />  - 値が `MONTH` の場合、動的に作成されるパーティションのサフィックス形式は `yyyyMM` です。例えば `202003` です。 |
| dynamic_partition.start | いいえ | 動的パーティション化の開始オフセット。このパラメータの値は負の整数である必要があります。このオフセットより前のパーティションは、`dynamic_partition.time_unit` で決定される現在の日、週、または月に基づいて削除されます。デフォルト値は `Integer.MIN_VALUE`、つまり -2147483648 であり、これは履歴パーティションが削除されないことを意味します。 |
| dynamic_partition.end | はい | 動的パーティション化の終了オフセット。このパラメータの値は正の整数である必要があります。現在の日、週、または月から終了オフセットまでのパーティションは事前に作成されます。 |
| dynamic_partition.prefix | いいえ | 動的パーティションの名前に追加されるプレフィックス。デフォルト値: `p`。 |
| dynamic_partition.buckets | いいえ | 動的パーティションあたりのバケット数。デフォルト値は、予約語 `BUCKETS` によって決定されるバケット数、または StarRocks によって自動的に設定されるバケット数と同じです。 |

:::note

パーティション列が INT タイプの場合、パーティションの時間粒度に関係なく、その形式は `yyyyMMdd` である必要があります。

:::

### ランダムバケット法におけるバケットサイズ

v3.2 以降、ランダムバケット法で構成されたテーブルの場合、テーブル作成時に `PROPERTIES` の `bucket_size` パラメータを使用してバケットサイズを指定することで、バケット数のオンデマンドかつ動的な増加を可能にできます。単位: B。

```sql
PROPERTIES (
    "bucket_size" = "1073741824"
)
```

### データ圧縮アルゴリズム

テーブルを作成する際に、プロパティ `compression` を追加することで、テーブルのデータ圧縮アルゴリズムを指定できます。

`compression` の有効な値は次のとおりです:

- `LZ4`: LZ4 アルゴリズム。
- `ZSTD`: Zstandard アルゴリズム。
- `ZLIB`: zlib アルゴリズム。
- `SNAPPY`: Snappy アルゴリズム。

v3.3.2 以降、StarRocks はテーブル作成時に zstd 圧縮形式の圧縮レベルを指定することをサポートしています。

構文:

```sql
PROPERTIES ("compression" = "zstd(<compression_level>)")
```

`compression_level`: ZSTD 圧縮形式の圧縮レベル。タイプ: 整数。範囲: [1,22]。デフォルト: `3` (推奨)。数値が大きいほど圧縮率が高くなります。圧縮レベルが高いほど、圧縮と解凍にかかる時間が長くなります。

例:

```sql
PROPERTIES ("compression" = "zstd(3)")
```

適切なデータ圧縮アルゴリズムの選択方法の詳細については、以下を参照してください。[データ圧縮](../../../table_design/data_compression.md)。

### データロードの書き込みクォーラム

StarRocks クラスタに複数のデータレプリカがある場合、テーブルごとに異なる書き込みクォーラムを設定できます。これは、StarRocks がロードタスクの成功を判断する前に、いくつのレプリカがロード成功を返す必要があるかを示します。テーブル作成時にプロパティ `write_quorum` を追加することで、書き込みクォーラムを指定できます。このプロパティは v2.5 以降でサポートされています。

`write_quorum` の有効な値は次のとおりです。

- `MAJORITY`: デフォルト値。データレプリカの**過半数**がロード成功を返すと、StarRocks はロードタスクの成功を返します。それ以外の場合、StarRocks はロードタスクの失敗を返します。
- `ONE`: データレプリカの**1つ**がロード成功を返すと、StarRocks はロードタスクの成功を返します。それ以外の場合、StarRocks はロードタスクの失敗を返します。
- `ALL`: データレプリカの**すべて**がロード成功を返すと、StarRocks はロードタスクの成功を返します。それ以外の場合、StarRocks はロードタスクの失敗を返します。

:::caution

- ロードの書き込みクォーラムを低く設定すると、データにアクセスできなくなる、あるいはデータが失われるリスクが高まります。たとえば、2つのレプリカを持つ StarRocks クラスタで、書き込みクォーラムが1つのテーブルにデータをロードし、データが1つのレプリカにのみ正常にロードされたとします。StarRocks はロードタスクが成功したと判断しますが、データの生存レプリカは1つだけです。ロードされたデータの tablet を保存するサーバーがダウンした場合、これらの tablet 内のデータにはアクセスできなくなります。また、サーバーのディスクが損傷した場合、データは失われます。
- StarRocks は、すべてのデータレプリカがステータスを返した後にのみ、ロードタスクのステータスを返します。ロードステータスが不明なレプリカがある場合、StarRocks はロードタスクのステータスを返しません。レプリカでは、ロードタイムアウトもロード失敗と見なされます。
:::

### レプリカのデータ書き込みおよびレプリケーションモード

StarRocks クラスタに複数のデータレプリカがある場合、`PROPERTIES` の `replicated_storage` パラメータを指定して、レプリカ間のデータ書き込みおよびレプリケーションモードを設定できます。

- `true` (v3.0 以降のデフォルト) は「シングルリーダーレプリケーション」を示します。これは、データがプライマリレプリカにのみ書き込まれることを意味します。他のレプリカはプライマリレプリカからデータを同期します。このモードは、複数のレプリカへのデータ書き込みによって引き起こされる CPU コストを大幅に削減します。v2.5 以降でサポートされています。
- `false` (v2.5 のデフォルト) は「リーダーレスレプリケーション」を示します。これは、プライマリレプリカとセカンダリレプリカを区別せずに、データが複数のレプリカに直接書き込まれることを意味します。CPU コストはレプリカの数に比例して増加します。

ほとんどの場合、デフォルト値を使用すると、より良いデータ書き込みパフォーマンスが得られます。レプリカ間のデータ書き込みおよびレプリケーションモードを変更したい場合は、ALTER TABLE コマンドを実行します。例:

```sql
ALTER TABLE example_db.my_table
SET ("replicated_storage" = "false");
```

### Delta Join のユニークキー制約と外部キー制約

View Delta Join シナリオでクエリの書き換えを有効にするには、Delta Join で結合するテーブルにユニークキー制約 `unique_constraints` と外部キー制約 `foreign_key_constraints` を定義する必要があります。詳細については、[非同期マテリアライズドビュー - View Delta Join シナリオでのクエリの書き換え](./async_mv_rewrite_queries_in_view_delta_join_scenario.md) を参照してください。[非同期マテリアライズドビュー - View Delta Join シナリオでのクエリの書き換え](../../../using_starrocks/async_mv/use_cases/query_rewrite_with_materialized_views.md#query-delta-join-rewrite)

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

- `child_column`: テーブルの外部キー。複数の `child_column` を定義できます。
- `catalog_name`: 結合するテーブルが存在する catalog の名前。このパラメータが指定されていない場合は、デフォルトの catalog が使用されます。
- `database_name`: 結合するテーブルが存在するデータベースの名前。このパラメータが指定されていない場合は、現在のデータベースが使用されます。
- `parent_table_name`: 結合するテーブルの名前。
- `parent_column`: 結合する列。これらは、対応するテーブルの主キーまたはユニークキーである必要があります。

:::caution

- `unique_constraints` と `foreign_key_constraints` はクエリの書き換えにのみ使用されます。テーブルにデータがロードされる際に、外部キー制約のチェックは保証されません。テーブルにロードされるデータが制約を満たしていることを確認する必要があります。
- 主キーテーブルの主キー、またはユニークキーテーブルのユニークキーは、デフォルトで対応する `unique_constraints` です。手動で設定する必要はありません。
- テーブルの `child_column` 内の `foreign_key_constraints` は、別のテーブルの `unique_key` 内の `unique_constraints` を参照する必要があります。
- `child_column` と `parent_column` の数は一致する必要があります。
- `child_column` と対応する `parent_column` のデータタイプは一致する必要があります。
:::

### 共有データクラスタ用のクラウドネイティブテーブル

StarRocks 共有データクラスタを使用するには、次のプロパティを持つクラウドネイティブテーブルを作成する必要があります。

```SQL
PROPERTIES (
    "storage_volume" = "<storage_volume_name>",
    "datacache.enable" = "{ true | false }",
    "datacache.partition_duration" = "<string_value>",
    "file_bundling" = "{ true | false }"
)
```

- `storage_volume`: 作成するクラウドネイティブテーブルを保存するために使用されるストレージボリュームの名前。このプロパティが指定されていない場合、デフォルトのストレージボリュームが使用されます。このプロパティは v3.1 以降でサポートされています。

- `datacache.enable`: ローカルディスクキャッシュを有効にするかどうか。デフォルト: `true`。

  - このプロパティが `true` に設定されている場合、ロードされるデータはオブジェクトストレージとローカルディスク (クエリアクセラレーションのキャッシュとして) の両方に同時に書き込まれます。
  - このプロパティが `false` に設定されている場合、データはオブジェクトストレージにのみロードされます。

  :::note
ローカルディスクキャッシュを有効にするには、BE 設定項目 `storage_root_path` でディスクのディレクトリを指定する必要があります。
:::

- `datacache.partition_duration`: ホットデータの有効期間。ローカルディスクキャッシュが有効になっている場合、すべてのデータはキャッシュにロードされます。キャッシュが満杯になると、StarRocks は最近使用頻度の低いデータをキャッシュから削除します。クエリが削除されたデータをスキャンする必要がある場合、StarRocks はデータが有効期間内にあるかどうかを確認します。データが期間内にある場合、StarRocks はデータを再度キャッシュにロードします。データが期間内にない場合、StarRocks はデータをキャッシュにロードしません。このプロパティは文字列値であり、`YEAR`、`MONTH`、`DAY`、および `HOUR` の単位で指定できます。例えば、`7 DAY` や `12 HOUR` のように指定します。指定されていない場合、すべてのデータはホットデータとしてキャッシュされます。

  :::note
このプロパティは、`datacache.enable` が `true` に設定されている場合にのみ利用可能です。
:::

- `file_bundling` (オプション): クラウドネイティブテーブルの File Bundling 最適化を有効にするかどうか。v4.0 以降でサポートされています。この機能が有効になっている場合 (`true` に設定)、システムはロード、Compaction、または Publish 操作によって生成されたデータファイルを自動的にバンドルし、外部ストレージシステムへの高頻度アクセスによって発生する API コストを削減します。

  :::note

  - File Bundling は、StarRocks v4.0 以降の共有データクラスタでのみ利用可能です。
  - File Bundling は、v4.0 以降で作成されたテーブルではデフォルトで有効になっており、FE 設定 `enable_file_bundling` (デフォルト: true) によって制御されます。
  - File Bundling が有効になった後、クラスタを v3.5.2 以降にのみダウングレードできます。v3.5.2 より前のバージョンにダウングレードしたい場合は、まず File Bundling が有効になっているテーブルを削除する必要があります。
  - クラスタが v4.0 にアップグレードされた後も、既存のテーブルでは File Bundling はデフォルトで無効のままです。
  - 既存のテーブルに対して File Bundling を手動で有効にするには、[ALTER TABLE](ALTER_TABLE.md)ステートメントを使用し、以下の制限があります。
    - v4.0 より前のバージョンで作成された Rollup Indexes を持つテーブルでは、File Bundling を有効にできません。v4.0 以降でインデックスを削除して再作成し、その後テーブルに対して File Bundling を有効にすることができます。
    - `file_bundling` プロパティを**繰り返し**特定の期間内に変更することはできません。変更しようとすると、システムはエラーを返します。`file_bundling` プロパティが変更可能かどうかは、以下の SQL ステートメントを実行して確認できます。

      ```SQL
      SELECT METADATA_SWITCH_VERSION FROM information_schema.partitions_meta WHERE TABLE_NAME = '<table_name>';
      ```

      `file_bundling` プロパティは、`0` が返された場合にのみ変更が許可されます。ゼロ以外の値は、`METADATA_SWITCH_VERSION` に対応するデータバージョンが GC メカニズムによってまだ回収されていないことを示します。データバージョンが回収されるまで待つ必要があります。

      この間隔は、FE 動的設定 `lake_autovacuum_grace_period_minutes` の値を小さく設定することで短縮できます。ただし、`file_bundling` プロパティを変更した後には、設定を元の値にリセットすることを忘れないでください。
:::

### 高速スキーマ進化

- `fast_schema_evolution`: テーブルの高速スキーマ進化を有効にするかどうか。有効な値は `TRUE` または `FALSE` (デフォルト) です。高速スキーマ進化を有効にすると、列の追加または削除時の schema change の速度が向上し、リソース使用量が削減されます。現在、このプロパティはテーブル作成時にのみ有効にでき、テーブル作成後に ALTER TABLE を使用して変更することはできません。

  :::note

  - 高速スキーマ進化は、v3.2.0 以降の共有なしクラスタでサポートされています。
  - 高速スキーマ進化は、v3.3 以降の共有データクラスタでサポートされており、デフォルトで有効になっています。共有データクラスタでクラウドネイティブテーブルを作成する際に、このプロパティを指定する必要はありません。FE 動的パラメータ `enable_fast_schema_evolution` (デフォルト: true) がこの動作を制御します。
:::

- `cloud_native_fast_schema_evolution_v2`: の高速スキーマ進化 v2 を有効にするかどうか。**クラウドネイティブテーブル**。v4.1 以降でサポートされています。有効な値は `TRUE` (デフォルト) または `FALSE` です。高速スキーマ進化 v2 が有効になっている場合、schema change は同期プロセスになります。ALTER TABLE ステートメントが正常に返されると、新しいスキーマはすぐに有効になります。システムは S3 にある tablet メタデータではなく、FE メタデータのみを変更するため、テーブル内のパーティションや tablet の数に関係なく、常に秒レベルのレイテンシを達成できます。従来の動作では、schema change は時間をかけて tablet メタデータを更新する非同期ジョブとして実行されます。

  :::note

  - 高速スキーマ進化 v2 は v4.1 以降でサポートされており、**クラウドネイティブテーブルにのみ対応しています。**共有データクラスタの
  - デフォルトの動作:
    - v4.1 クラスタで作成された新しいテーブルでは、高速スキーマ進化 v2 はデフォルトで有効になっています。
    - v4.1 にアップグレードされたクラスタの既存のテーブルでは、高速スキーマ進化 v2 はデフォルトで無効になっています。このプロパティを `true` に明示的に設定することで有効にできます。[ALTER TABLE](ALTER_TABLE.md)。
  - ダウングレード要件:
    - 共有データクラスタを v4.1 から v4.0.5 以降にダウングレードするには、標準のダウングレード手順に従って直接ダウングレードできます。
    - 共有データクラスタを v4.1 から v3.x または v4.0.5 より前のパッチバージョンにダウングレードする前に、ALTER TABLE を介して高速スキーマ進化 v2 を有効にしているすべてのテーブルについて、`cloud_native_fast_schema_evolution_v2` を `false` に手動で設定する必要があります。非同期ジョブが FINISHED になるまで待つ必要があります。ジョブのステータスは SHOW ALTER を介して追跡できます。
:::

schema change ジョブは[SHOW ALTER TABLE COLUMN](./SHOW_ALTER.md)を介して検査できます。

例:

```SQL
-- テーブル内の最近の列/schema change ジョブをリストする
SHOW ALTER TABLE COLUMN FROM test_db WHERE TableName = "test_tbl";
```

Fast Schema Evolution v2 が有効なクラウドネイティブテーブルの場合、変更は FE メタデータのみを更新することで適用されるため、schema change ジョブは通常 FINISHED として表示されます。

### Base Compaction の禁止

`base_compaction_forbidden_time_ranges`: テーブルに対して Base Compaction が禁止される時間範囲。このプロパティが設定されている場合、システムは指定された時間範囲外でのみ、対象となる tablets に対して Base Compaction を実行します。このプロパティは v3.2.13 以降でサポートされています。

:::note
Base Compaction が禁止されている期間中、テーブルへのデータロード数が 500 を超えないようにしてください。
:::

`base_compaction_forbidden_time_ranges` の値は、[Quartz cron 構文](https://productresources.collibra.com/docs/collibra/latest/Content/Cron/co_quartz-cron-syntax.htm)、これらのフィールドのみをサポートします: `<minute> <hour> <day-of-the-month> <month> <day-of-the-week>`。ここで `<minute>` は `*` である必要があります。

```SQL
crontab_param_value ::= [ "" | crontab ]

crontab ::= * <hour> <day-of-the-month> <month> <day-of-the-week>
```

- このプロパティが設定されていないか、`""` (空文字列) に設定されている場合、Base Compaction は常に禁止されません。
- このプロパティが `* * * * *` に設定されている場合、Base Compaction は常に禁止されます。
- その他の値は Quartz cron 構文に従います。
  - 独立した値は、フィールドの単位時間を表します。例えば、`<hour>` フィールドの `8` は 8:00-8:59 を意味します。
  - 値の範囲は、フィールドの時間範囲を表します。例えば、`<hour>` フィールドの `8-9` は 8:00-9:59 を意味します。
  - コンマで区切られた複数の値の範囲は、フィールドの複数の時間範囲を示します。
  - `<day of the week>` は日曜日の開始値が `1` で、`7` は土曜日を表します。

例:

```SQL
-- 毎日午前8時から午後9時まで、ベース Compaction を禁止します。
'base_compaction_forbidden_time_ranges' = '* 8-20 * * *'

-- 毎日午前0時から午前5時まで、および午後9時から午後11時まで、ベース Compaction を禁止します。
'base_compaction_forbidden_time_ranges' = '* 0-4,21-22 * * *'

-- 月曜日から金曜日まで、ベース Compaction を禁止します（つまり、土曜日と日曜日は許可します）。
'base_compaction_forbidden_time_ranges' = '* * * * 2-6'

-- 毎営業日（つまり、月曜日から金曜日まで）の午前8時から午後9時まで、ベース Compaction を禁止します。
'base_compaction_forbidden_time_ranges' = '* 8-20 * * 2-6'
```

### 共通パーティション式 TTL の指定

v3.5.0 以降、StarRocks 内部テーブルは共通パーティション式 TTL をサポートします。

`partition_retention_condition`: 動的に保持するパーティションを宣言する式。式の条件を満たさないパーティションは定期的に削除されます。

- 式にはパーティション列と定数のみを含めることができます。非パーティション列はサポートされていません。
- 共通パーティション式は、リストパーティションとレンジパーティションに異なる方法で適用されます。
  - リストパーティションを持つテーブルの場合、StarRocks は共通パーティション式でフィルタリングされたパーティションの削除をサポートします。
  - レンジパーティションを持つテーブルの場合、StarRocks は FE のパーティションプルーニング機能を使用してパーティションをフィルタリングおよび削除することしかできません。パーティションプルーニングでサポートされていない述語に対応するパーティションは、フィルタリングおよび削除できません。

例:

```SQL
-- 過去3ヶ月間のデータを保持します。列 `dt` はテーブルのパーティション列です。
"partition_retention_condition" = "dt >= CURRENT_DATE() - INTERVAL 3 MONTH"
```

この機能を無効にするには、ALTER TABLE ステートメントを使用して、このプロパティを空文字列として設定できます。

```SQL
ALTER TABLE tbl SET('partition_retention_condition' = '');
```

### テーブルレベルで Flat JSON プロパティを設定する

v3.3 で、StarRocks は[Flat JSON](../../../using_starrocks/Flat_json.md)機能を導入し、JSON データクエリの効率を向上させ、JSON の使用の複雑さを軽減しました。この機能は、特定の BE 設定項目とシステム変数によって制御されていました。その結果、グローバルにのみ有効化 (または無効化) できます。

v4.0 以降、Flat JSON 関連のプロパティをテーブルレベルで設定できます。

```SQL
PROPERTIES (
    "flat_json.enable" = "{ true | false }",
    "flat_json.null.factor" = "",
    "flat_json.sparsity.factor" = "",
    "flat_json.column.max" = ""
)
```

- `flat_json.enable` (オプション): Flat JSON 機能を有効にするかどうか。この機能が有効になると、新しくロードされた JSON データは自動的にフラット化され、JSON クエリパフォーマンスが向上します。
- `flat_json.null.factor` (オプション): 列内の NULL 値の割合のしきい値。列内の NULL 値の割合がこのしきい値よりも高い場合、その列は Flat JSON によって抽出されません。このパラメータは、`flat_json.enable` が `true` に設定されている場合にのみ有効になります。デフォルト値: `0.3`。
- `flat_json.sparsity.factor` (オプション): 同じ名前の列の割合のしきい値。同じ名前の列の割合がこの値よりも低い場合、その列は Flat JSON によって抽出されません。このパラメータは、`flat_json.enable` が `true` に設定されている場合にのみ有効になります。デフォルト値: `0.3`。
- `flat_json.column.max` (オプション): Flat JSON によって抽出できるサブフィールドの最大数。このパラメータは、`flat_json.enable` が `true` に設定されている場合にのみ有効になります。デフォルト値: `100`。

## 例

### ハッシュバケット化と列指向（カラムナ）ストレージを持つ集計テーブル

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

### 記憶媒体とクールダウン時間が設定された集計テーブル

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

### レンジパーティション、ハッシュバケット化、列ベースのストレージ、記憶媒体、およびクールダウン時間を持つ重複キーテーブル

LESS THAN

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

このステートメントは、3つのデータパーティションを作成します:

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

### 私のSQL 外部テーブル

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

### HLL 列を持つテーブル

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

### BITMAP_UNION 集計タイプを使用するテーブル

`v1` および `v2` 列の元のデータタイプは、TINYINT、SMALLINT、または INT である必要があります。

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

### Colocate Join をサポートするテーブル

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

動的パーティション化機能は、FE 設定で ("dynamic_partition.enable" = "true") を有効にする必要があります。詳細については、以下を参照してください [動的パーティションの設定](#configure-dynamic-partitions).

この例では、今後3日間のパーティションを作成し、3日前に作成されたパーティションを削除します。例えば、今日が2020-01-08の場合、p20200108、p20200109、p20200110、p20200111 という名前のパーティションが作成され、それらの範囲は次のとおりです。

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

### バッチで複数のパーティションが作成され、整数列をパーティション列とするテーブル

以下の例では、パーティション列 `datekey` は INT タイプです。すべてのパーティションは、1つのシンプルなパーティション句 `START ("1") END ("5") EVERY (1)` によってのみ作成されます。すべてのパーティションの範囲は `1` から始まり `5` で終わり、パーティションの粒度は `1` です。

> **NOTE**
>
> パーティション列の値は **START()** および **END()** 引用符で囲む必要がありますが、パーティションの粒度は **EVERY()** 引用符で囲む必要はありません。

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

### Hive 外部テーブル

Hive 外部テーブルを作成する前に、Hive リソースとデータベースを作成しておく必要があります。詳細については、以下を参照してください [外部テーブル](../../../data_source/External_table.md#deprecated-hive-external-table).

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

### 特定のソートキーを持つ主キーテーブル

ユーザーの住所や最終アクティブ時間などのディメンションから、ユーザーの行動をリアルタイムで分析する必要があるとします。テーブルを作成する際、`user_id` 列を主キーとして定義し、`address` と `last_active` 列の組み合わせをソートキーとして定義できます。

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

### Flat JSON プロパティを持つテーブル

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
- [SHOW TABLES](SHOW_TABLES.md)
- [USE](../Database/USE.md)
- [ALTER TABLE](ALTER_TABLE.md)
- [DROP TABLE](DROP_TABLE.md)
