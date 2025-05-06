---
displayed_sidebar: docs
---

# CREATE TABLE

StarRocks で新しいテーブルを作成します。

:::tip
この操作には、対象データベースに対する CREATE TABLE 権限が必要です。
:::

## 構文

```plaintext
CREATE [EXTERNAL] TABLE [IF NOT EXISTS] [database.]table_name
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
[BROKER PROPERTIES ("key"="value", ...)]
```

## パラメータ

:::tip

- 作成するテーブル名、パーティション名、列名、およびインデックス名は、[システム制限](../../System_limit.md) に従う必要があります。
- データベース名、テーブル名、列名、またはパーティション名を指定する際、StarRocks では一部のリテラルが予約キーワードとして使用されます。これらのキーワードを SQL ステートメントで直接使用しないでください。SQL ステートメントでそのようなキーワードを使用する場合は、バッククォート (`) で囲んでください。これらの予約キーワードについては、[キーワード](../keywords.md) を参照してください。

:::

### column_definition

構文:

```SQL
col_name col_type [agg_type] [NULL | NOT NULL] [DEFAULT "default_value"] [AUTO_INCREMENT] [AS generation_expr]
```

**col_name**: 列名。

通常、`__op` または `__row` で始まる名前の列を作成することはできません。これらの名前形式は StarRocks で特別な目的のために予約されており、そのような列を作成すると未定義の動作が発生する可能性があります。そのような列を作成する必要がある場合は、FE 動的パラメータ [`allow_system_reserved_names`](../../../administration/management/FE_configuration.md#allow_system_reserved_names) を `TRUE` に設定してください。

**col_type**: 列の型。特定の列情報、型と範囲:

- TINYINT (1 バイト): -2^7 + 1 から 2^7 - 1 までの範囲。
- SMALLINT (2 バイト): -2^15 + 1 から 2^15 - 1 までの範囲。
- INT (4 バイト): -2^31 + 1 から 2^31 - 1 までの範囲。
- BIGINT (8 バイト): -2^63 + 1 から 2^63 - 1 までの範囲。
- LARGEINT (16 バイト): -2^127 + 1 から 2^127 - 1 までの範囲。
- FLOAT (4 バイト): 科学的記数法をサポート。
- DOUBLE (8 バイト): 科学的記数法をサポート。
- DECIMAL[(precision, scale)] (16 バイト)

  - デフォルト値: DECIMAL(10, 0)
  - precision: 1 ~ 38
  - scale: 0 ~ precision
  - 整数部分: precision - scale

    科学的記数法はサポートされていません。

- DATE (3 バイト): 0000-01-01 から 9999-12-31 までの範囲。
- DATETIME (8 バイト): 0000-01-01 00:00:00 から 9999-12-31 23:59:59 までの範囲。
- CHAR[(length)]: 固定長文字列。範囲: 1 ~ 255。デフォルト値: 1。
- VARCHAR[(length)]: 可変長文字列。デフォルト値は 1。単位: バイト。StarRocks 2.1 より前のバージョンでは、`length` の値の範囲は 1–65533 です。[プレビュー] StarRocks 2.1 以降のバージョンでは、`length` の値の範囲は 1–1048576 です。
- HLL (1~16385 バイト): HLL 型の場合、長さやデフォルト値を指定する必要はありません。長さはデータ集約に応じてシステム内で制御されます。HLL 列は [hll_union_agg](../../sql-functions/aggregate-functions/hll_union_agg.md), [Hll_cardinality](../../sql-functions/scalar-functions/hll_cardinality.md), および [hll_hash](../../sql-functions/scalar-functions/hll_hash.md) でのみクエリまたは使用できます。
- BITMAP: ビットマップ型は指定された長さやデフォルト値を必要としません。これは符号なし bigint 数の集合を表します。最大要素は 2^64 - 1 まで可能です。

**agg_type**: 集約タイプ。指定されていない場合、この列はキー列です。
指定されている場合、それは値列です。サポートされている集約タイプは次のとおりです:

- SUM, MAX, MIN, REPLACE
- HLL_UNION (HLL 型のみ)
- BITMAP_UNION (BITMAP のみ)
- REPLACE_IF_NOT_NULL: インポートされたデータが非 null 値の場合にのみ置き換えられることを意味します。null 値の場合、StarRocks は元の値を保持します。

> NOTE
>
> - 集約タイプ BITMAP_UNION の列がインポートされるとき、その元のデータ型は TINYINT, SMALLINT, INT, および BIGINT でなければなりません。
> - テーブル作成時に REPLACE_IF_NOT_NULL 列で NOT NULL が指定されている場合、StarRocks はデータを NULL に変換してもユーザーにエラーレポートを送信しません。これにより、ユーザーは選択した列をインポートできます。

この集約タイプは、キータイプが AGGREGATE KEY の集計テーブルにのみ適用されます。v3.1.9 以降、`REPLACE_IF_NOT_NULL` は BITMAP 型の列を新たにサポートします。

**NULL | NOT NULL**: 列が `NULL` を許可するかどうか。デフォルトでは、重複キーテーブル、集計テーブル、またはユニークキーテーブルを使用するテーブル内のすべての列に `NULL` が指定されます。主キーテーブルを使用するテーブルでは、デフォルトで値列に `NULL` が指定され、キー列には `NOT NULL` が指定されます。生データに `NULL` 値が含まれている場合は、`\N` で表現します。StarRocks はデータロード中に `\N` を `NULL` として扱います。

**DEFAULT "default_value"**: 列のデフォルト値。StarRocks にデータをロードする際、列にマッピングされたソースフィールドが空の場合、StarRocks は自動的にデフォルト値をその列に埋め込みます。デフォルト値は次のいずれかの方法で指定できます:

- **DEFAULT current_timestamp**: 現在の時刻をデフォルト値として使用します。詳細については、[current_timestamp()](../../sql-functions/date-time-functions/current_timestamp.md) を参照してください。
- **DEFAULT `<default_value>`**: 列のデータ型の指定された値をデフォルト値として使用します。たとえば、列のデータ型が VARCHAR の場合、`DEFAULT "beijing"` のように、デフォルト値として beijing という VARCHAR 文字列を指定できます。デフォルト値は次の型にはできません: ARRAY, BITMAP, JSON, HLL, および BOOLEAN。
- **DEFAULT (\<expr\>)**: 指定された関数の結果をデフォルト値として使用します。サポートされているのは [uuid()](../../sql-functions/utility-functions/uuid.md) および [uuid_numeric()](../../sql-functions/utility-functions/uuid_numeric.md) 式のみです。

**AUTO_INCREMENT**: `AUTO_INCREMENT` 列を指定します。`AUTO_INCREMENT` 列のデータ型は BIGINT でなければなりません。自動インクリメント ID は 1 から始まり、1 ずつ増加します。`AUTO_INCREMENT` 列の詳細については、[AUTO_INCREMENT](auto_increment.md) を参照してください。v3.0 以降、StarRocks は `AUTO_INCREMENT` 列をサポートします。

**AS generation_expr**: 生成列とその式を指定します。[生成列](../generated_columns.md) は、式の結果を事前に計算して保存するために使用でき、同じ複雑な式を持つクエリを大幅に高速化します。v3.1 以降、StarRocks は生成列をサポートします。

### index_definition

```SQL
INDEX index_name (col_name[, col_name, ...]) [USING BITMAP] COMMENT 'xxxxxx'
```

パラメータの説明と使用上の注意については、[ビットマップインデックス](../../../table_design/indexes/Bitmap_index.md#create-a-bitmap-index) を参照してください。

### ENGINE type

デフォルト値: `olap`。このパラメータが指定されていない場合、デフォルトで OLAP テーブル (StarRocks 内部テーブル) が作成されます。

オプション値: `mysql`, `elasticsearch`, `hive`, `jdbc` (2.3 以降), `iceberg`, および `hudi` (2.2 以降)。外部データソースをクエリする外部テーブルを作成する場合は、`CREATE EXTERNAL TABLE` を指定し、`ENGINE` をこれらのいずれかの値に設定します。詳細については、[外部テーブル](../../../data_source/External_table.md) を参照してください。

**Hive, Iceberg, Hudi, および JDBC データソースからデータをクエリするには、カタログを使用することをお勧めします。外部テーブルは非推奨です。**

**v3.1 以降、StarRocks は Iceberg カタログで Parquet 形式のテーブルを作成することをサポートし、INSERT INTO を使用してこれらの Parquet 形式の Iceberg テーブルにデータを挿入できます。**

**v3.2 以降、StarRocks は Hive カタログで Parquet 形式のテーブルを作成することをサポートし、INSERT INTO を使用してこれらの Parquet 形式の Hive テーブルにデータをシンクすることをサポートします。**

- MySQL の場合、次のプロパティを指定します:

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

    注意:

    MySQL の "table_name" は実際のテーブル名を示す必要があります。対照的に、CREATE TABLE ステートメントの "table_name" は StarRocks 上のこの MySQL テーブルの名前を示します。これらは異なる場合も同じ場合もあります。

    StarRocks で MySQL テーブルを作成する目的は、MySQL データベースにアクセスすることです。StarRocks 自体は MySQL データを維持または保存しません。

- Elasticsearch の場合、次のプロパティを指定します:

    ```plaintext
    PROPERTIES (

    "hosts" = "http://192.168.0.1:8200,http://192.168.0.2:8200",
    "user" = "root",
    "password" = "root",
    "index" = "tindex",
    "type" = "doc"
    )
    ```

  - `hosts`: Elasticsearch クラスタに接続するために使用される URL。1 つ以上の URL を指定できます。
  - `user`: 基本認証が有効になっている Elasticsearch クラスタにログインするために使用されるルートユーザーのアカウント。
  - `password`: 前述のルートアカウントのパスワード。
  - `index`: Elasticsearch クラスタ内の StarRocks テーブルのインデックス。インデックス名は StarRocks テーブル名と同じです。このパラメータを StarRocks テーブルのエイリアスに設定できます。
  - `type`: インデックスタイプ。デフォルト値は `doc` です。

- Hive の場合、次のプロパティを指定します:

    ```plaintext
    PROPERTIES (

        "database" = "hive_db_name",
        "table" = "hive_table_name",
        "hive.metastore.uris" = "thrift://xx.xx.xx.xx:9083"
    )
    ```

    ここで、database は Hive テーブル内の対応するデータベースの名前です。table は Hive テーブルの名前です。`hive.metastore.uris` はサーバーアドレスです。

- JDBC の場合、次のプロパティを指定します:

    ```plaintext
    PROPERTIES (
    "resource"="jdbc0",
    "table"="dest_tbl"
    )
    ```

    `resource` は JDBC リソース名であり、`table` は宛先テーブルです。

- Iceberg の場合、次のプロパティを指定します:

   ```plaintext
    PROPERTIES (
    "resource" = "iceberg0", 
    "database" = "iceberg", 
    "table" = "iceberg_table"
    )
    ```

    `resource` は Iceberg リソース名です。`database` は Iceberg データベースです。`table` は Iceberg テーブルです。

- Hudi の場合、次のプロパティを指定します:

  ```plaintext
    PROPERTIES (
    "resource" = "hudi0", 
    "database" = "hudi", 
    "table" = "hudi_table" 
    )
    ```

### key_desc

構文:

```SQL
key_type(k1[,k2 ...])
```

データは指定されたキー列で順序付けされ、異なるキータイプに対して異なる属性を持ちます:

- AGGREGATE KEY: キー列内の同一の内容は、指定された集約タイプに従って値列に集約されます。通常、財務報告書や多次元分析などのビジネスシナリオに適用されます。
- UNIQUE KEY/PRIMARY KEY: キー列内の同一の内容は、インポート順に従って値列に置き換えられます。キー列の追加、削除、変更、およびクエリを行うために適用できます。
- DUPLICATE KEY: StarRocks に同時に存在するキー列内の同一の内容。詳細データや集約属性のないデータを保存するために使用できます。**DUPLICATE KEY はデフォルトのタイプです。データはキー列に従って順序付けされます。**

> **NOTE**
>
> AGGREGATE KEY を除いて、他の key_type を使用してテーブルを作成する場合、値列は集約タイプを指定する必要はありません。

### COMMENT

テーブルを作成する際にテーブルコメントを追加できます（オプション）。COMMENT は `key_desc` の後に配置する必要があります。それ以外の場合、テーブルは作成されません。

v3.1 以降、`ALTER TABLE <table_name> COMMENT = "new table comment"` を使用してテーブルコメントを変更できます。

### partition_desc

パーティションの説明は次の方法で使用できます:

#### パーティションを動的に作成する

[動的パーティション化](../../../table_design/data_distribution/dynamic_partitioning.md) は、パーティションのタイムトゥリブ (TTL) 管理を提供します。StarRocks はデータの新鮮さを確保するために、事前に新しいパーティションを自動的に作成し、期限切れのパーティションを削除します。この機能を有効にするには、テーブル作成時に動的パーティション化関連のプロパティを設定します。

#### パーティションを一つずつ作成する

**パーティションの上限のみを指定する**

構文:

```sql
PARTITION BY RANGE ( <partitioning_column1> [, <partitioning_column2>, ... ] )
  PARTITION <partition1_name> VALUES LESS THAN ("<upper_bound_for_partitioning_column1>" [ , "<upper_bound_for_partitioning_column2>", ... ] )
  [ ,
  PARTITION <partition2_name> VALUES LESS THAN ("<upper_bound_for_partitioning_column1>" [ , "<upper_bound_for_partitioning_column2>", ... ] )
  , ... ] 
)
```

注意:

指定されたキー列と指定された値範囲を使用してパーティション化してください。

- パーティションの命名規則については、[システム制限](../../System_limit.md) を参照してください。
- 範囲パーティションの列は、次の型のみをサポートします: TINYINT, SMALLINT, INT, BIGINT, LARGEINT, DATE, および DATETIME。
- パーティションは左閉右開です。最初のパーティションの左境界は最小値です。
- NULL 値は最小値を含むパーティションにのみ保存されます。最小値を含むパーティションが削除されると、NULL 値はインポートできなくなります。
- パーティション列は単一列または複数列のいずれかです。パーティション値はデフォルトの最小値です。
- パーティション列として 1 つの列のみを指定する場合、最近のパーティションのパーティション列の上限として `MAXVALUE` を設定できます。

  ```SQL
  PARTITION BY RANGE (pay_dt) (
    PARTITION p1 VALUES LESS THAN ("20210102"),
    PARTITION p2 VALUES LESS THAN ("20210103"),
    PARTITION p3 VALUES LESS THAN MAXVALUE
  )
  ```

注意してください:

- パーティションは、時間に関連するデータを管理するためによく使用されます。
- データのバックトラッキングが必要な場合、後でパーティションを追加するために最初のパーティションを空にすることを検討するかもしれません。

**パーティションの下限と上限の両方を指定する**

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

注意:

- 固定範囲は LESS THAN より柔軟です。左と右のパーティションをカスタマイズできます。
- 固定範囲は他の面で LESS THAN と同じです。
- パーティション列として 1 つの列のみを指定する場合、最近のパーティションのパーティション列の上限として `MAXVALUE` を設定できます。

  ```SQL
  PARTITION BY RANGE (pay_dt) (
    PARTITION p202101 VALUES [("20210101"), ("20210201")),
    PARTITION p202102 VALUES [("20210201"), ("20210301")),
    PARTITION p202103 VALUES [("20210301"), (MAXVALUE))
  )
  ```

#### 複数のパーティションを一括で作成する

構文

- パーティション列が日付型の場合。

    ```sql
    PARTITION BY RANGE (<partitioning_column>) (
        START ("<start_date>") END ("<end_date>") EVERY (INTERVAL <N> <time_unit>)
    )
    ```

- パーティション列が整数型の場合。

    ```sql
    PARTITION BY RANGE (<partitioning_column>) (
        START ("<start_integer>") END ("<end_integer>") EVERY (<partitioning_granularity>)
    )
    ```

説明

`START()` と `END()` で開始値と終了値を指定し、`EVERY()` で時間単位またはパーティショングラニュラリティを指定して、複数のパーティションを一括で作成できます。

- パーティション列は日付型または整数型である必要があります。
- パーティション列が日付型の場合、`INTERVAL` キーワードを使用して時間間隔を指定する必要があります。時間単位は、時間 (v3.0 以降)、日、週、月、または年として指定できます。パーティションの命名規則は動的パーティションと同じです。

詳細については、[データ分布](../../../table_design/data_distribution/Data_distribution.md) を参照してください。

### distribution_desc

StarRocks はハッシュバケット法とランダムバケット法をサポートしています。バケット法を設定しない場合、StarRocks はデフォルトでランダムバケット法を使用し、バケット数を自動的に設定します。

- ランダムバケット法 (v3.1 以降)

  パーティション内のデータは、特定の列値に基づかずに、すべてのバケットにランダムに分配されます。StarRocks にバケット数を自動的に設定させたい場合、バケット設定を指定する必要はありません。バケット数を手動で指定する場合、構文は次のとおりです:

  ```SQL
  DISTRIBUTED BY RANDOM BUCKETS <num>
  ```
  
  ただし、ランダムバケット法によるクエリパフォーマンスは、大量のデータをクエリし、特定の列を条件列として頻繁に使用する場合には理想的でない可能性があります。このシナリオでは、ハッシュバケット法を使用することをお勧めします。これにより、スキャンおよび計算が必要なバケットの数が少なくなり、クエリパフォーマンスが大幅に向上します。

  **注意事項**
  - ランダムバケット法は重複キーテーブルの作成にのみ使用できます。
  - ランダムにバケットされたテーブルに [Colocation Group](../../../using_starrocks/Colocate_join.md) を指定することはできません。
  - Spark Load を使用してランダムにバケットされたテーブルにデータをロードすることはできません。
  - StarRocks v2.5.7 以降、テーブル作成時にバケット数を設定する必要はありません。StarRocks はバケット数を自動的に設定します。このパラメータを設定する方法については、[バケット数の設定](../../../table_design/data_distribution/Data_distribution.md#set-the-number-of-buckets) を参照してください。

  詳細については、[ランダムバケット法](../../../table_design/data_distribution/Data_distribution.md#random-bucketing-since-v31) を参照してください。

- ハッシュバケット法

  構文:

  ```SQL
  DISTRIBUTED BY HASH (k1[,k2 ...]) [BUCKETS num]
  ```

  パーティション内のデータは、バケット列のハッシュ値とバケット数に基づいてバケットに細分化されます。次の 2 つの要件を満たす列をバケット列として選択することをお勧めします。

  - 高いカーディナリティを持つ列 (例: ID)
  - クエリでフィルタとして頻繁に使用される列

  そのような列が存在しない場合、クエリの複雑さに応じてバケット列を決定できます。

  - クエリが複雑な場合、バケット列として高いカーディナリティを持つ列を選択することをお勧めします。これにより、バケット間でのデータ分布が均等になり、クラスタリソースの利用率が向上します。
  - クエリが比較的単純な場合、クエリ条件として頻繁に使用される列をバケット列として選択することをお勧めします。これにより、クエリ効率が向上します。

  1 つのバケット列を使用してパーティションデータを均等に分配できない場合、複数のバケット列 (最大 3 つ) を選択できます。詳細については、[バケット列の選択](../../../table_design/data_distribution/Data_distribution.md#hash-bucketing) を参照してください。

  **注意事項**:

  - **テーブルを作成する際、バケット列を指定する必要があります**。
  - バケット列の値は更新できません。
  - バケット列は指定後に変更できません。
  - StarRocks v2.5.7 以降、テーブル作成時にバケット数を設定する必要はありません。StarRocks はバケット数を自動的に設定します。このパラメータを設定する方法については、[バケット数の設定](../../../table_design/data_distribution/Data_distribution.md#set-the-number-of-buckets) を参照してください。

### ORDER BY

バージョン 3.0 以降、主キーとソートキーは主キーテーブルで分離されています。ソートキーは `ORDER BY` キーワードで指定され、任意の列の組み合わせで構成できます。

> **NOTICE**
>
> ソートキーが指定されている場合、プレフィックスインデックスはソートキーに基づいて構築されます。ソートキーが指定されていない場合、プレフィックスインデックスは主キーに基づいて構築されます。

### PROPERTIES

#### 初期記憶媒体、ストレージクールダウン時間、自動レプリカ数の指定

エンジンタイプが `OLAP` の場合、テーブル作成時に初期記憶媒体 (`storage_medium`)、自動ストレージクールダウン時間 (`storage_cooldown_time`) または時間間隔 (`storage_cooldown_ttl`)、およびレプリカ数 (`replication_num`) を指定できます。

プロパティが適用される範囲: テーブルにパーティションが 1 つしかない場合、プロパティはテーブルに属します。テーブルが複数のパーティションに分割されている場合、プロパティは各パーティションに属します。指定されたパーティションに異なるプロパティを設定する必要がある場合、テーブル作成後に [ALTER TABLE ... ADD PARTITION または ALTER TABLE ... MODIFY PARTITION](ALTER_TABLE.md) を実行できます。

**初期記憶媒体と自動ストレージクールダウン時間の設定**

```sql
PROPERTIES (
    "storage_medium" = "[SSD|HDD]",
    { "storage_cooldown_ttl" = "<num> { YEAR | MONTH | DAY | HOUR } "
    | "storage_cooldown_time" = "yyyy-MM-dd HH:mm:ss" }
)
```

- `storage_medium`: 初期記憶媒体で、`SSD` または `HDD` に設定できます。明示的に指定した記憶媒体のタイプが StarRocks クラスタの BE ディスクタイプと一致していることを確認してください。<br />

    FE 設定項目 `enable_strict_storage_medium_check` が `true` に設定されている場合、テーブル作成時にシステムは BE ディスクタイプを厳密にチェックします。CREATE TABLE で指定した記憶媒体が BE ディスクタイプと一致しない場合、エラー "Failed to find enough host in all backends with storage medium is SSD|HDD." が返され、テーブル作成が失敗します。`enable_strict_storage_medium_check` が `false` に設定されている場合、システムはこのエラーを無視し、強制的にテーブルを作成します。ただし、データがロードされた後、クラスタディスクスペースが不均一に分布する可能性があります。<br />

    v2.3.6, v2.4.2, v2.5.1, および v3.0 以降、`storage_medium` が明示的に指定されていない場合、システムは BE ディスクタイプに基づいて記憶媒体を自動的に推測します。<br />

  - システムは次のシナリオでこのパラメータを SSD に自動設定します:

    - BE によって報告されたディスクタイプ (`storage_root_path`) が SSD のみを含む場合。
    - BE によって報告されたディスクタイプ (`storage_root_path`) が SSD と HDD の両方を含む場合。v2.3.10, v2.4.5, v2.5.4, および v3.0 以降、BE によって報告された `storage_root_path` が SSD と HDD の両方を含み、プロパティ `storage_cooldown_time` が指定されている場合、システムは `storage_medium` を SSD に設定します。

  - システムは次のシナリオでこのパラメータを HDD に自動設定します:

    - BE によって報告されたディスクタイプ (`storage_root_path`) が HDD のみを含む場合。
    - 2.3.10, 2.4.5, 2.5.4, および 3.0 以降、BE によって報告された `storage_root_path` が SSD と HDD の両方を含み、プロパティ `storage_cooldown_time` が指定されていない場合、システムは `storage_medium` を HDD に設定します。

- `storage_cooldown_ttl` または `storage_cooldown_time`: 自動ストレージクールダウン時間または時間間隔。自動ストレージクールダウンは、SSD から HDD へのデータの自動移行を指します。この機能は、初期記憶媒体が SSD の場合にのみ有効です。

  **パラメータ**

  - `storage_cooldown_ttl`: このテーブルのパーティションに対する自動ストレージクールダウンの**時間間隔**。最新のパーティションを SSD に保持し、一定の時間間隔後に古いパーティションを HDD に自動的にクールダウンする必要がある場合、このパラメータを使用できます。各パーティションの自動ストレージクールダウン時間は、このパラメータの値とパーティションの上限時間を使用して計算されます。

  サポートされている値は `<num> YEAR`, `<num> MONTH`, `<num> DAY`, および `<num> HOUR` です。`<num>` は非負整数です。デフォルト値は null であり、ストレージクールダウンが自動的に実行されないことを示します。

  たとえば、テーブル作成時に `"storage_cooldown_ttl"="1 DAY"` と指定し、範囲が `[2023-08-01 00:00:00,2023-08-02 00:00:00)` のパーティション `p20230801` が存在する場合、このパーティションの自動ストレージクールダウン時間は `2023-08-03 00:00:00` であり、これは `2023-08-02 00:00:00 + 1 DAY` です。テーブル作成時に `"storage_cooldown_ttl"="0 DAY"` と指定した場合、このパーティションの自動ストレージクールダウン時間は `2023-08-02 00:00:00` です。

  - `storage_cooldown_time`: テーブルが SSD から HDD にクールダウンされるときの自動ストレージクールダウン時間 (**絶対時間**)。指定された時間は現在の時間より後である必要があります。形式: "yyyy-MM-dd HH:mm:ss"。指定されたパーティションに異なるプロパティを設定する必要がある場合、テーブル作成後に [ALTER TABLE ... ADD PARTITION または ALTER TABLE ... MODIFY PARTITION](ALTER_TABLE.md) を実行できます。

**使用法**

- 自動ストレージクールダウンに関連するパラメータの比較は次のとおりです:
  - `storage_cooldown_ttl`: テーブル内のパーティションに対する自動ストレージクールダウンの時間間隔を指定するテーブルプロパティ。システムは、`このパラメータの値とパーティションの上限時間` を加算した時点でパーティションを自動的にクールダウンします。したがって、自動ストレージクールダウンはパーティショングラニュラリティで実行され、より柔軟です。
  - `storage_cooldown_time`: このテーブルに対する自動ストレージクールダウン時間 (**絶対時間**) を指定するテーブルプロパティ。また、テーブル作成後に指定されたパーティションに対して異なるプロパティを設定できます。
  - `storage_cooldown_second`: クラスタ内のすべてのテーブルに対する自動ストレージクールダウン遅延を指定する静的 FE パラメータ。

- テーブルプロパティ `storage_cooldown_ttl` または `storage_cooldown_time` は、FE 静的パラメータ `storage_cooldown_second` より優先されます。
- これらのパラメータを設定する際、`"storage_medium = "SSD"` を指定する必要があります。
- これらのパラメータを設定しない場合、自動ストレージクールダウンは自動的に実行されません。
- 各パーティションの自動ストレージクールダウン時間を表示するには、`SHOW PARTITIONS FROM <table_name>` を実行します。

**制限**

- 式とリストパーティション化はサポートされていません。
- パーティション列は日付型である必要があります。
- 複数のパーティション列はサポートされていません。
- 主キーテーブルはサポートされていません。

**各パーティション内のタブレットのレプリカ数を設定する**

`replication_num`: 各パーティション内のテーブルのレプリカ数。デフォルト数: `3`。

```sql
PROPERTIES (
    "replication_num" = "<num>"
)
```

#### 列にブルームフィルターインデックスを追加する

エンジンタイプが olap の場合、ブルームフィルターインデックスを採用する列を指定できます。

ブルームフィルターインデックスを使用する際の制限は次のとおりです:

- 重複キーテーブルまたは主キーテーブルのすべての列にブルームフィルターインデックスを作成できます。集計テーブルまたはユニークキーテーブルの場合、キー列にのみブルームフィルターインデックスを作成できます。
- TINYINT, FLOAT, DOUBLE, および DECIMAL 列はブルームフィルターインデックスの作成をサポートしていません。
- ブルームフィルターインデックスは、`in` および `=` 演算子を含むクエリのパフォーマンスを向上させることができます。たとえば、`Select xxx from table where x in {}` や `Select xxx from table where column = xxx` などです。この列により多くの離散値があると、より正確なクエリが可能になります。

詳細については、[ブルームフィルターインデックス](../../../table_design/indexes/Bloomfilter_index.md) を参照してください。

```SQL
PROPERTIES (
    "bloom_filter_columns"="k1,k2,k3"
)
```

#### Colocate Join を使用する

Colocate Join 属性を使用する場合、`properties` に指定してください。

```SQL
PROPERTIES (
    "colocate_with"="table1"
)
```

#### 動的パーティションを設定する

動的パーティション属性を使用する場合、properties に指定してください。

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

| パラメータ                   | 必須 | 説明                                                  |
| --------------------------- | -------- | ------------------------------------------------------------ |
| dynamic_partition.enable    | いいえ       | 動的パーティション化を有効にするかどうか。 有効な値: `TRUE` および `FALSE`。 デフォルト値: `TRUE`。 |
| dynamic_partition.time_unit | はい      | 動的に作成されるパーティションの時間粒度。必須パラメータです。 有効な値: `DAY`, `WEEK`, および `MONTH`。時間粒度は動的に作成されるパーティションのサフィックス形式を決定します。<br/>  - 値が `DAY` の場合、動的に作成されるパーティションのサフィックス形式は `yyyyMMdd` です。例として、パーティション名のサフィックスは `20200321` です。<br/>  - 値が `WEEK` の場合、動的に作成されるパーティションのサフィックス形式は `yyyy_ww` で、たとえば `2020_13` は 2020 年の第 13 週を表します。<br/>  - 値が `MONTH` の場合、動的に作成されるパーティションのサフィックス形式は `yyyyMM` で、たとえば `202003` です。 |
| dynamic_partition.start     | いいえ       | 動的パーティション化の開始オフセット。このパラメータの値は負の整数でなければなりません。このオフセットより前のパーティションは、`dynamic_partition.time_unit` によって決定される現在の日、週、または月に基づいて削除されます。デフォルト値は `Integer.MIN_VALUE`、つまり -2147483648 で、これは履歴パーティションが削除されないことを意味します。 |
| dynamic_partition.end       | はい      | 動的パーティション化の終了オフセット。このパラメータの値は正の整数でなければなりません。現在の日、週、または月から終了オフセットまでのパーティションが事前に作成されます。 |
| dynamic_partition.prefix    | いいえ       | 動的パーティションの名前に追加されるプレフィックス。デフォルト値: `p`。 |
| dynamic_partition.buckets   | いいえ       | 動的パーティションごとのバケット数。デフォルト値は、予約語 `BUCKETS` によって決定されるバケット数と同じです。または、StarRocks によって自動的に設定されます。 |

#### ランダムバケット法で設定されたテーブルのバケットサイズ (`bucket_size`) を指定する

v3.2 以降、ランダムバケット法で設定されたテーブルの場合、テーブル作成時に `PROPERTIES` の `bucket_size` パラメータを使用してバケットサイズを指定し、オンデマンドでバケット数を動的に増やすことができます。単位: B。

```sql
PROPERTIES (
    "bucket_size" = "1073741824"
)
```

#### データ圧縮アルゴリズムを設定する

テーブル作成時にプロパティ `compression` を追加することで、テーブルのデータ圧縮アルゴリズムを指定できます。

`compression` の有効な値は次のとおりです:

- `LZ4`: LZ4 アルゴリズム。
- `ZSTD`: Zstandard アルゴリズム。
- `ZLIB`: zlib アルゴリズム。
- `SNAPPY`: Snappy アルゴリズム。

適切なデータ圧縮アルゴリズムの選択方法については、[データ圧縮](../../../table_design/data_compression.md) を参照してください。

#### データロードの書き込みクォーラムを設定する

StarRocks クラスタに複数のデータレプリカがある場合、テーブルごとに異なる書き込みクォーラムを設定できます。つまり、StarRocks がロードタスクを成功と判断する前に、ロード成功を返す必要があるレプリカの数です。テーブル作成時にプロパティ `write_quorum` を追加することで、書き込みクォーラムを指定できます。このプロパティは v2.5 からサポートされています。

`write_quorum` の有効な値は次のとおりです:

- `MAJORITY`: デフォルト値。データレプリカの**過半数**がロード成功を返した場合、StarRocks はロードタスク成功を返します。それ以外の場合、StarRocks はロードタスク失敗を返します。
- `ONE`: データレプリカの**1 つ**がロード成功を返した場合、StarRocks はロードタスク成功を返します。それ以外の場合、StarRocks はロードタスク失敗を返します。
- `ALL`: データレプリカの**すべて**がロード成功を返した場合、StarRocks はロードタスク成功を返します。それ以外の場合、StarRocks はロードタスク失敗を返します。

> **CAUTION**
>
> - ロードのために低い書き込みクォーラムを設定すると、データのアクセス不能やさらには損失のリスクが増加します。たとえば、2 つのレプリカを持つ StarRocks クラスタで 1 つの書き込みクォーラムを持つテーブルにデータをロードし、データが 1 つのレプリカにのみ正常にロードされた場合、StarRocks はロードタスクが成功したと判断しますが、データの生き残ったレプリカは 1 つだけです。ロードされたデータのタブレットを保存するサーバーがダウンした場合、これらのタブレット内のデータはアクセス不能になります。また、サーバーのディスクが破損した場合、データは失われます。
> - StarRocks は、すべてのデータレプリカがステータスを返した後にのみロードタスクのステータスを返します。ロードステータスが不明なレプリカがある場合、StarRocks はロードタスクのステータスを返しません。レプリカ内でのロードタイムアウトもロード失敗と見なされます。

#### レプリカ間のデータ書き込みおよびレプリケーションモードを指定する

StarRocks クラスタに複数のデータレプリカがある場合、`PROPERTIES` の `replicated_storage` パラメータを指定して、レプリカ間のデータ書き込みおよびレプリケーションモードを構成できます。

- `true` (v3.0 以降のデフォルト) は「単一リーダーレプリケーション」を示し、データはプライマリレプリカにのみ書き込まれます。他のレプリカはプライマリレプリカからデータを同期します。このモードは、複数のレプリカへのデータ書き込みによる CPU コストを大幅に削減します。v2.5 からサポートされています。
- `false` (v2.5 のデフォルト) は「リーダーレスレプリケーション」を示し、データはプライマリおよびセカンダリレプリカを区別せずに直接複数のレプリカに書き込まれます。CPU コストはレプリカの数に比例して増加します。

ほとんどの場合、デフォルト値を使用することで、より良いデータ書き込みパフォーマンスが得られます。レプリカ間のデータ書き込みおよびレプリケーションモードを変更したい場合は、ALTER TABLE コマンドを実行してください。例:

```sql
    ALTER TABLE example_db.my_table
    SET ("replicated_storage" = "false");
```

#### 一括でロールアップを作成する

テーブルを作成する際に、一括でロールアップを作成できます。

構文:

```SQL
ROLLUP (rollup_name (column_name1, column_name2, ...)
[FROM from_index_name]
[PROPERTIES ("key"="value", ...)],...)
```

#### View Delta Join クエリの書き換えのためにユニークキー制約と外部キー制約を定義する

View Delta Join シナリオでクエリの書き換えを有効にするには、Delta Join で結合されるテーブルに対してユニークキー制約 `unique_constraints` と外部キー制約 `foreign_key_constraints` を定義する必要があります。詳細については、[非同期マテリアライズドビュー - View Delta Join シナリオでのクエリ書き換え](../../../using_starrocks/async_mv/use_cases/query_rewrite_with_materialized_views.md#query-delta-join-rewrite) を参照してください。

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
- `catalog_name`: 結合されるテーブルが存在するカタログの名前。指定されていない場合、デフォルトのカタログが使用されます。
- `database_name`: 結合されるテーブルが存在するデータベースの名前。指定されていない場合、現在のデータベースが使用されます。
- `parent_table_name`: 結合されるテーブルの名前。
- `parent_column`: 結合される列。これらは対応するテーブルの主キーまたはユニークキーでなければなりません。

> **CAUTION**
>
> - `unique_constraints` と `foreign_key_constraints` はクエリの書き換えにのみ使用されます。テーブルにデータがロードされる際に外部キー制約のチェックは保証されません。テーブルにロードされるデータが制約を満たしていることを確認する必要があります。
> - 主キーテーブルの主キーまたはユニークキーテーブルのユニークキーは、デフォルトで対応する `unique_constraints` です。手動で設定する必要はありません。
> - テーブルの `foreign_key_constraints` 内の `child_column` は、他のテーブルの `unique_constraints` 内の `unique_key` を参照する必要があります。
> - `child_column` と `parent_column` の数は一致している必要があります。
> - `child_column` と対応する `parent_column` のデータ型は一致している必要があります。

#### StarRocks 共有データクラスタ用のクラウドネイティブテーブルを作成する

StarRocks 共有データクラスタを使用するには、次のプロパティを持つクラウドネイティブテーブルを作成する必要があります:

```SQL
PROPERTIES (
    "storage_volume" = "<storage_volume_name>",
    "datacache.enable" = "{ true | false }",
    "datacache.partition_duration" = "<string_value>"
)
```

- `storage_volume`: 作成するクラウドネイティブテーブルを保存するために使用されるストレージボリュームの名前。このプロパティが指定されていない場合、デフォルトのストレージボリュームが使用されます。このプロパティは v3.1 以降でサポートされています。

- `datacache.enable`: ローカルディスクキャッシュを有効にするかどうか。デフォルト: `true`。

  - このプロパティが `true` に設定されている場合、ロードされるデータはオブジェクトストレージとローカルディスク (クエリアクセラレーションのキャッシュとして) に同時に書き込まれます。
  - このプロパティが `false` に設定されている場合、データはオブジェクトストレージにのみロードされます。

  > **NOTE**
  >
  > ローカルディスクキャッシュを有効にするには、BE 設定項目 `storage_root_path` にディスクのディレクトリを指定する必要があります。

- `datacache.partition_duration`: ホットデータの有効期間。ローカルディスクキャッシュが有効な場合、すべてのデータがキャッシュにロードされます。キャッシュがいっぱいになると、StarRocks はキャッシュから最近使用されていないデータを削除します。クエリが削除されたデータをスキャンする必要がある場合、StarRocks はデータが有効期間内かどうかを確認します。データが有効期間内であれば、StarRocks はデータを再びキャッシュにロードします。データが有効期間外であれば、StarRocks はデータをキャッシュにロードしません。このプロパティは、`YEAR`, `MONTH`, `DAY`, および `HOUR` という単位で指定できる文字列値です。たとえば、`7 DAY` や `12 HOUR` です。指定されていない場合、すべてのデータがホットデータとしてキャッシュされます。

  > **NOTE**
  >
  > このプロパティは、`datacache.enable` が `true` に設定されている場合にのみ利用可能です。

#### 高速スキーマ進化を設定する

`fast_schema_evolution`: テーブルの高速スキーマ進化を有効にするかどうか。 有効な値は `TRUE` または `FALSE` (デフォルト) です。 高速スキーマ進化を有効にすると、スキーマ変更の速度が向上し、列の追加または削除時のリソース使用量が削減されます。 現在、このプロパティはテーブル作成時にのみ有効にでき、テーブル作成後に [ALTER TABLE](./ALTER_TABLE.md) を使用して変更することはできません。このパラメータは v3.2.0 からサポートされています。
  > **NOTE**
  >
  > - StarRocks 共有データクラスタはこのパラメータをサポートしていません。
  > - クラスタレベルで高速スキーマ進化を設定する必要がある場合、たとえば StarRocks クラスタ内で高速スキーマ進化を無効にする場合、FE 動的パラメータ `enable_fast_schema_evolution` を設定できます。

#### ベースコンパクションを禁止する

`base_compaction_forbidden_time_ranges`: テーブルに対してベースコンパクションが禁止される時間範囲。このプロパティが設定されている場合、システムは指定された時間範囲外でのみ適格なタブレットに対してベースコンパクションを実行します。このプロパティは v3.2.13 からサポートされています。

> **NOTE**
>
> ベースコンパクションが禁止されている期間中にデータロードが 500 を超えないようにしてください。

`base_compaction_forbidden_time_ranges` の値は [Quartz cron 構文](https://productresources.collibra.com/docs/collibra/latest/Content/Cron/co_quartz-cron-syntax.htm) に従い、`<minute> <hour> <day-of-the-month> <month> <day-of-the-week>` のフィールドのみをサポートします。`<minute>` は `*` でなければなりません。

```Plain
crontab_param_value ::= [ "" | crontab ]

crontab ::= * <hour> <day-of-the-month> <month> <day-of-the-week>
```

- このプロパティが設定されていない場合、または `""`（空文字列）に設定されている場合、ベースコンパクションはいつでも禁止されません。
- このプロパティが `* * * * *` に設定されている場合、ベースコンパクションは常に禁止されます。
- その他の値は Quartz cron 構文に従います。
  - 独立した値はフィールドの単位時間を示します。たとえば、`<hour>` フィールドの `8` は 8:00-8:59 を意味します。
  - 値の範囲はフィールドの時間範囲を示します。たとえば、`<hour>` フィールドの `8-9` は 8:00-9:59 を意味します。
  - カンマで区切られた複数の値の範囲は、フィールドの複数の時間範囲を示します。
  - `<day of the week>` の開始値は `1` で日曜日を表し、`7` は土曜日を表します。

例:

```SQL
-- 毎日午前 8 時から午後 9 時までベースコンパクションを禁止します。
'base_compaction_forbidden_time_ranges' = '* 8-20 * * *'

-- 毎日午前 0 時から午前 5 時まで、および午後 9 時から午後 11 時までベースコンパクションを禁止します。
'base_compaction_forbidden_time_ranges' = '* 0-4,21-22 * * *'

-- 月曜日から金曜日までベースコンパクションを禁止します（土曜日と日曜日には許可されます）。
'base_compaction_forbidden_time_ranges' = '* * * * 2-6'

-- 平日の午前 8 時から午後 9 時までベースコンパクションを禁止します（月曜日から金曜日）。
'base_compaction_forbidden_time_ranges' = '* 8-20 * * 2-6'
```

## 例

### ハッシュバケット法と列指向（カラムナ）ストレージを使用する集計テーブルを作成する

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

### 集計テーブルを作成し、記憶媒体とクールダウン時間を設定する

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

または

```SQL
CREATE TABLE example_db.table_hash
(
    k1 BIGINT,
    k2 LARGEINT,
    v1 VARCHAR(2048) REPLACE,
    v2 SMALLINT SUM DEFAULT "10"
)
ENGINE=olap
PRIMARY KEY(k1, k2)
DISTRIBUTED BY HASH (k1, k2)
PROPERTIES(
    "storage_type"="column",
    "storage_medium" = "SSD",
    "storage_cooldown_time" = "2015-06-04 00:00:00"
);
```

### 範囲パーティション、ハッシュバケット法、列ベースストレージを使用し、記憶媒体とクールダウン時間を設定した重複キーテーブルを作成する

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

注意:

このステートメントは 3 つのデータパーティションを作成します:

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

### MySQL 外部テーブルを作成する

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

### HLL 列を含むテーブルを作成する

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

### BITMAP_UNION 集約タイプを含むテーブルを作成する

`v1` および `v2` 列の元のデータ型は TINYINT, SMALLINT, または INT でなければなりません。

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

### Colocate Join をサポートする 2 つのテーブルを作成する

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

### ビットマップインデックスを持つテーブルを作成する

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

### 動的パーティションテーブルを作成する

動的パーティション化機能は、FE 設定で有効にする必要があります ("dynamic_partition.enable" = "true")。詳細については、[動的パーティションの設定](#configure-dynamic-partitions) を参照してください。

この例では、次の 3 日間のパーティションを作成し、3 日前に作成されたパーティションを削除します。たとえば、今日が 2020-01-08 の場合、次の名前のパーティションが作成されます: p20200108, p20200109, p20200110, p20200111。これらの範囲は次のとおりです:

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

### 複数のパーティションが一括で作成され、整数型の列がパーティション列として指定されるテーブルを作成する

  次の例では、パーティション列 `datekey` は INT 型です。すべてのパーティションは、単一の簡単なパーティションクロース `START ("1") END ("5") EVERY (1)` によって作成されます。すべてのパーティションの範囲は `1` から始まり、`5` で終わり、パーティショングラニュラリティは `1` です:
  > **NOTE**
  >
  > **START()** および **END()** のパーティション列値は引用符で囲む必要がありますが、**EVERY()** のパーティショングラニュラリティは引用符で囲む必要はありません。

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

### Hive 外部テーブルを作成する

Hive 外部テーブルを作成する前に、Hive リソースとデータベースを作成している必要があります。詳細については、[外部テーブル](../../../data_source/External_table.md#deprecated-hive-external-table) を参照してください。

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

### 主キーテーブルを作成し、ソートキーを指定する

ユーザーの住所や最終アクティブ時間などの次元からリアルタイムでユーザーの行動を分析する必要があるとします。テーブルを作成する際、`user_id` 列を主キーとして定義し、`address` 列と `last_active` 列の組み合わせをソートキーとして定義できます。

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

## 参考資料

- [SHOW CREATE TABLE](SHOW_CREATE_TABLE.md)
- [SHOW TABLES](SHOW_TABLES.md)
- [USE](../Database/USE.md)
- [ALTER TABLE](ALTER_TABLE.md)
- [DROP TABLE](DROP_TABLE.md)