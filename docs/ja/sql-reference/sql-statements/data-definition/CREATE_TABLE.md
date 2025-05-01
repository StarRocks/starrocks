---
displayed_sidebar: docs
---

# CREATE TABLE

## 説明

StarRocks に新しいテーブルを作成します。

## 構文

```plaintext
CREATE [EXTERNAL] TABLE [IF NOT EXISTS] [database.]table_name
(column_definition1[, column_definition2, ...]
[, index_definition1[, index_definition12,]])
[ENGINE = [olap|mysql|elasticsearch|hive|hudi|iceberg|jdbc]]
[key_desc]
[COMMENT "table comment"]
[partition_desc]
distribution_desc
[rollup_index]
[PROPERTIES ("key"="value", ...)]
[BROKER PROPERTIES ("key"="value", ...)]
```

## パラメータ

### column_definition

構文:

```SQL
col_name col_type [agg_type] [NULL | NOT NULL] [DEFAULT "default_value"]
```

**col_name**: カラム名。

通常、`__op` または `__row` で始まる名前のカラムを作成することはできません。これらの名前形式は StarRocks で特別な目的のために予約されており、そのようなカラムを作成すると未定義の動作を引き起こす可能性があります。どうしてもそのようなカラムを作成する必要がある場合は、FE の動的パラメータ [`allow_system_reserved_names`](../../../administration/Configuration.md) を `TRUE` に設定してください。

**col_type**: カラムタイプ。特定のカラム情報、例えばタイプや範囲:

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
  - 整数部: precision - scale

    科学的記数法はサポートされていません。

- DATE (3 バイト): 0000-01-01 から 9999-12-31 までの範囲。
- DATETIME (8 バイト): 0000-01-01 00:00:00 から 9999-12-31 23:59:59 までの範囲。
- CHAR[(length)]: 固定長文字列。範囲: 1 ~ 255。デフォルト値: 1。
- VARCHAR[(length)]: 可変長文字列。デフォルト値は 1。単位: バイト。StarRocks 2.1 より前のバージョンでは、`length` の値の範囲は 1–65533。[プレビュー] StarRocks 2.1 以降のバージョンでは、`length` の値の範囲は 1–1048576。
- HLL (1~16385 バイト): HLL タイプの場合、長さやデフォルト値を指定する必要はありません。長さはデータ集約に応じてシステム内で制御されます。HLL カラムは [hll_union_agg](../../sql-functions/aggregate-functions/hll_union_agg.md)、[Hll_cardinality](../../sql-functions/scalar-functions/hll_cardinality.md)、および [hll_hash](../../sql-functions/aggregate-functions/hll_hash.md) によってのみクエリまたは使用されます。
- BITMAP: ビットマップタイプは指定された長さやデフォルト値を必要としません。これは符号なし bigint 数の集合を表します。最大の要素は最大で 2^64 - 1 まで可能です。

**agg_type**: 集約タイプ。指定されていない場合、このカラムはキーカラムです。指定されている場合、それは値カラムです。サポートされている集約タイプは以下の通りです:

- SUM, MAX, MIN, REPLACE
- HLL_UNION (HLL タイプのみ)
- BITMAP_UNION(BITMAP のみ)
- REPLACE_IF_NOT_NULL: インポートされたデータが非 null 値の場合にのみ置き換えられることを意味します。null 値の場合、StarRocks は元の値を保持します。

> 注意
>
> - 集約タイプ BITMAP_UNION のカラムがインポートされるとき、その元のデータタイプは TINYINT、SMALLINT、INT、および BIGINT でなければなりません。
> - テーブル作成時に REPLACE_IF_NOT_NULL カラムで NOT NULL が指定された場合、StarRocks はデータを NULL に変換してもユーザーにエラーレポートを送信しません。これにより、ユーザーは選択したカラムをインポートできます。

この集約タイプは、key_desc タイプが AGGREGATE KEY の集計テーブルにのみ適用されます。

**NULL | NOT NULL**: カラムが `NULL` を許可するかどうか。デフォルトでは、重複キーテーブル、集計テーブル、またはユニークキーテーブルを使用するテーブルのすべてのカラムに `NULL` が指定されます。主キーテーブルを使用するテーブルでは、デフォルトで値カラムには `NULL` が指定され、キーカラムには `NOT NULL` が指定されます。生データに `NULL` 値が含まれている場合、`\N` で表現します。StarRocks はデータロード中に `\N` を `NULL` として扱います。

**DEFAULT "default_value"**: カラムのデフォルト値。StarRocks にデータをロードする際、カラムにマッピングされたソースフィールドが空の場合、StarRocks は自動的にカラムにデフォルト値を埋めます。デフォルト値は次のいずれかの方法で指定できます:

- **DEFAULT current_timestamp**: 現在の時刻をデフォルト値として使用します。詳細は [current_timestamp()](../../sql-functions/date-time-functions/current_timestamp.md) を参照してください。
- **DEFAULT `<default_value>`**: カラムデータタイプの指定された値をデフォルト値として使用します。例えば、カラムのデータタイプが VARCHAR の場合、`DEFAULT "beijing"` のように、デフォルト値として beijing という VARCHAR 文字列を指定できます。デフォルト値は次のタイプのいずれかであってはなりません: ARRAY、BITMAP、JSON、HLL、および BOOLEAN。
- **DEFAULT (\<expr\>)**: 指定された関数によって返される結果をデフォルト値として使用します。サポートされているのは [uuid()](../../sql-functions/utility-functions/uuid.md) と [uuid_numeric()](../../sql-functions/utility-functions/uuid_numeric.md) のみです。

### index_definition

テーブルを作成する際にビットマップインデックスのみを作成できます。パラメータの説明と使用上の注意については、[ビットマップインデックス](../../../using_starrocks/Bitmap_index.md#create-a-bitmap-index) を参照してください。

```SQL
INDEX index_name (col_name[, col_name, ...]) [USING BITMAP] COMMENT 'xxxxxx'
```

### ENGINE type

デフォルト値: olap。このパラメータが指定されていない場合、デフォルトで OLAP テーブル (StarRocks 内部テーブル) が作成されます。

オプションの値: `mysql`, `elasticsearch`, `hive`, `jdbc` (2.3 以降), `iceberg`, および `hudi` (2.2 以降)。外部データソースをクエリするための外部テーブルを作成する場合は、`CREATE EXTERNAL TABLE` を指定し、`ENGINE` をこれらのいずれかの値に設定します。詳細は [外部テーブル](../../../data_source/External_table.md) を参照してください。

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

  - `hosts`: Elasticsearch クラスターに接続するために使用される URL。1 つ以上の URL を指定できます。
  - `user`: 基本認証が有効な Elasticsearch クラスターにログインするために使用されるルートユーザーのアカウント。
  - `password`: 前述のルートアカウントのパスワード。
  - `index`: Elasticsearch クラスター内の StarRocks テーブルのインデックス。インデックス名は StarRocks テーブル名と同じです。このパラメータを StarRocks テーブルのエイリアスに設定できます。
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

- AGGREGATE KEY: キー列の同一内容は、指定された集約タイプに従って値列に集約されます。通常、財務報告書や多次元分析などのビジネスシナリオに適用されます。
- UNIQUE KEY/PRIMARY KEY: キー列の同一内容は、インポート順序に従って値列に置き換えられます。キー列の追加、削除、変更、クエリを行うために適用できます。
- DUPLICATE KEY: StarRocks に同時に存在するキー列の同一内容。詳細データや集約属性のないデータを保存するために使用できます。**DUPLICATE KEY はデフォルトのタイプです。データはキー列に従って順序付けされます。**

> **注意**
>
> 値列は、AGGREGATE KEY を除いて、他の key_type を使用してテーブルを作成する際に集約タイプを指定する必要はありません。

### COMMENT

テーブルを作成する際にテーブルコメントを追加することができます（オプション）。COMMENT は `key_desc` の後に配置する必要があります。そうでない場合、テーブルは作成されません。

### partition_desc

パーティションの説明は次の方法で使用できます:

#### パーティションを動的に作成

[動的パーティション化](../../../table_design/dynamic_partitioning.md) は、パーティションのタイムトゥリブ（TTL）管理を提供します。StarRocks はデータの新鮮さを確保するために、事前に新しいパーティションを自動的に作成し、期限切れのパーティションを削除します。この機能を有効にするには、テーブル作成時に動的パーティション化関連のプロパティを設定します。

#### パーティションを一つずつ作成

**パーティションの上限のみを指定**

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

- パーティション名は [A-z0-9_] のみをサポートします
- 範囲パーティションの列は次のタイプのみをサポートします: TINYINT, SMALLINT, INT, BIGINT, LARGEINT, DATE, および DATETIME。
- パーティションは左閉じ右開きです。最初のパーティションの左境界は最小値です。
- NULL 値は最小値を含むパーティションにのみ保存されます。最小値を含むパーティションが削除されると、NULL 値はインポートできなくなります。
- パーティション列は単一列または複数列のいずれかです。パーティション値はデフォルトの最小値です。
- パーティション列として 1 つの列のみが指定されている場合、最新のパーティションのパーティション列の上限として `MAXVALUE` を設定できます。

  ```SQL
  PARTITION BY RANGE (pay_dt) (
    PARTITION p1 VALUES LESS THAN ("20210102"),
    PARTITION p2 VALUES LESS THAN ("20210103"),
    PARTITION p3 VALUES LESS THAN MAXVALUE
  )
  ```

注意してください:

- パーティションはしばしば時間に関連するデータを管理するために使用されます。
- データのバックトラッキングが必要な場合、後でパーティションを追加するために最初のパーティションを空にすることを検討するかもしれません。

**パーティションの下限と上限の両方を指定**

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

- 固定範囲は LESS THAN よりも柔軟です。左と右のパーティションをカスタマイズできます。
- 固定範囲は他の側面では LESS THAN と同じです。
- パーティション列として 1 つの列のみが指定されている場合、最新のパーティションのパーティション列の上限として `MAXVALUE` を設定できます。

```SQL
  PARTITION BY RANGE (pay_dt) (
    PARTITION p202101 VALUES [("20210101"), ("20210201")),
    PARTITION p202102 VALUES [("20210201"), ("20210301")),
    PARTITION p202103 VALUES [("20210301"), (MAXVALUE))
  )
  ```

#### 複数のパーティションを一括で作成

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

- パーティション列は日付型または整数型のいずれかです。
- パーティション列が日付型の場合、`INTERVAL` キーワードを使用して時間間隔を指定する必要があります。時間単位は時間（v3.0 以降）、日、週、月、または年として指定できます。パーティションの命名規則は動的パーティションと同じです。

詳細は [データ分布](../../../table_design/Data_distribution.md) を参照してください。

### distribution_desc

構文:

```SQL
DISTRIBUTED BY HASH (k1[,k2 ...]) [BUCKETS num]
```

パーティション内のデータは、バケッティング列のハッシュ値とバケット数に基づいて tablet に細分化できます。次の 2 つの要件を満たす列をバケッティング列として選択することをお勧めします。

- 高いカーディナリティを持つ列（例: ID）
- クエリでフィルタとしてよく使用される列

そのような列が存在しない場合、クエリの複雑さに応じてバケッティング列を決定できます。

- クエリが複雑な場合、バケッティング列として高いカーディナリティを持つ列を選択して、バケット間のデータ分布を均等にし、クラスタリソースの利用率を向上させることをお勧めします。
- クエリが比較的単純な場合、クエリ条件としてよく使用される列をバケッティング列として選択して、クエリ効率を向上させることをお勧めします。

1 つのバケッティング列を使用してパーティションデータを各 tablet に均等に分配できない場合、複数のバケッティング列（最大 3 つ）を選択できます。詳細は [バケッティング列の選択](../../../table_design/Data_distribution.md) を参照してください。

**注意事項**:

- **テーブルを作成する際、バケッティング列を指定する必要があります**。
- バケッティング列の値は更新できません。
- バケッティング列は指定後に変更できません。
- StarRocks 2.5 以降、テーブル作成時にバケット数を設定する必要はありません。StarRocks は自動的にバケット数を設定します。このパラメータを設定する場合は、[tablet 数の決定](../../../table_design/Data_distribution.md#determine-the-number-of-tablets) を参照してください。

### PROPERTIES

#### 記憶媒体、ストレージクールダウン時間、レプリカ数の指定

エンジンタイプが `olap` の場合、テーブル作成時に記憶媒体、ストレージクールダウン時間、レプリカ数を指定できます。

  > **注意**
  >
  > `storage_cooldown_time` は `storage_medium` が `SSD` に設定されている場合にのみ構成できます。`storage_medium` を SSD に設定する場合、クラスタが SSD ディスクを使用していることを確認してください。つまり、BEs によって報告された `storage_root_path` に SSD が含まれている必要があります。`storage_root_path` の詳細については、[設定](../../../administration/Configuration.md#configure-be-static-parameters) を参照してください。

```plaintext
PROPERTIES (
    "storage_medium" = "[SSD|HDD]",
    [ "storage_cooldown_time" = "yyyy-MM-dd HH:mm:ss", ]
    [ "replication_num" = "3" ]
)
```

**storage_medium**: 初期記憶媒体で、SSD または HDD に設定できます。

> **注意**
>
> - 2.5.1 以降、システムは BE ディスクタイプに基づいて記憶媒体を自動的に推測します。推測メカニズム: BEs によって報告された `storage_root_path` に SSD のみが含まれている場合、システムはこのパラメータを自動的に SSD に設定します。BEs によって報告された `storage_root_path` に HDD のみが含まれている場合、システムはこのパラメータを自動的に HDD に設定します。BEs によって報告された `storage_root_path` に SSD と HDD の両方が含まれている場合、システムはこのパラメータを自動的に SSD に設定します。2.5.4 以降、BEs によって報告された `storage_root_path` に SSD と HDD の両方が含まれており、プロパティ `storage_cooldown_time` が指定されている場合、`storage_medium` は SSD に設定されます。プロパティ `storage_cooldown_time` が指定されていない場合、`storage_medium` は HDD に設定されます。
> - FE 設定項目 `enable_strict_storage_medium_check` が `true` に設定されている場合、テーブル作成時にシステムは BE ディスクタイプを厳密にチェックします。CREATE TABLE で指定した記憶媒体が BE ディスクタイプと一致しない場合、エラー "Failed to find enough host in all backends with storage medium is SSD|HDD." が返され、テーブル作成に失敗します。`enable_strict_storage_medium_check` が `false` に設定されている場合、システムはこのエラーを無視し、強制的にテーブルを作成します。ただし、データがロードされた後、クラスタディスクスペースが不均等に分配される可能性があります。

**storage_cooldown_time**: パーティションのストレージクールダウン時間。このパラメータで指定された時間の後、ストレージ媒体が SSD の場合、SSD は HDD に切り替わります。形式: "yyyy-MM-dd HH:mm:ss"。指定された時間は現在の時間よりも後でなければなりません。このパラメータが明示的に指定されていない場合、デフォルトではストレージクールダウンは実行されません。

**replication_num**: 指定されたパーティションのレプリカ数。デフォルト数: 3。

テーブルにパーティションが 1 つしかない場合、プロパティはテーブルに属します。テーブルに 2 レベルのパーティションがある場合、プロパティは各パーティションに属します。ALTER TABLE ADD PARTITION または ALTER TABLE MODIFY PARTITION を使用して、異なるパーティションに異なるプロパティを指定することもできます。

#### カラムにブルームフィルターインデックスを追加

エンジンタイプが olap の場合、ブルームフィルターインデックスを採用するカラムを指定できます。

ブルームフィルターインデックスを使用する際の制限は次の通りです:

- 重複キーテーブルまたは主キーテーブルのすべてのカラムにブルームフィルターインデックスを作成できます。集計テーブルまたはユニークキーテーブルの場合、キー列にのみブルームフィルターインデックスを作成できます。
- TINYINT、FLOAT、DOUBLE、および DECIMAL カラムはブルームフィルターインデックスの作成をサポートしていません。
- ブルームフィルターインデックスは、`in` および `=` 演算子を含むクエリのパフォーマンスを向上させることができます。例: `Select xxx from table where x in {}` および `Select xxx from table where column = xxx`。このカラムにより多くの離散値があると、より正確なクエリが可能になります。

詳細は [ブルームフィルターインデックス](../../../using_starrocks/Bloomfilter_index.md) を参照してください。

```SQL
PROPERTIES (
    "bloom_filter_columns"="k1,k2,k3"
)
```

#### Colocate Join の使用

Colocate Join 属性を使用する場合は、`properties` に指定してください。

```SQL
PROPERTIES (
    "colocate_with"="table1"
)
```

#### 動的パーティションの設定

動的パーティション属性を使用する場合は、properties に指定してください。

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
| dynamic_partition.enable    | No       | 動的パーティション化を有効にするかどうか。 有効な値: `TRUE` および `FALSE`。 デフォルト値: `TRUE`。 |
| dynamic_partition.time_unit | Yes      | 動的に作成されるパーティションの時間粒度。必須パラメータです。 有効な値: `DAY`、`WEEK`、および `MONTH`。 時間粒度は動的に作成されるパーティションのサフィックス形式を決定します。<br/>  - 値が `DAY` の場合、動的に作成されるパーティションのサフィックス形式は `yyyyMMdd` です。 例: `20200321`。<br/>  - 値が `WEEK` の場合、動的に作成されるパーティションのサフィックス形式は `yyyy_ww` です。 例: `2020_13`（2020 年の第 13 週）。<br/>  - 値が `MONTH` の場合、動的に作成されるパーティションのサフィックス形式は `yyyyMM` です。 例: `202003`。 |
| dynamic_partition.start     | No       | 動的パーティション化の開始オフセット。このパラメータの値は負の整数でなければなりません。このオフセット前のパーティションは、`dynamic_partition.time_unit` によって決定される現在の日、週、または月に基づいて削除されます。デフォルト値は `Integer.MIN_VALUE`、すなわち -2147483648 で、これは履歴パーティションが削除されないことを意味します。 |
| dynamic_partition.end       | Yes      | 動的パーティション化の終了オフセット。このパラメータの値は正の整数でなければなりません。現在の日、週、または月から終了オフセットまでのパーティションが事前に作成されます。 |
| dynamic_partition.prefix    | No       | 動的パーティションの名前に追加されるプレフィックス。デフォルト値: `p`。 |
| dynamic_partition.buckets   | No       | 動的パーティションごとのバケット数。デフォルト値は、予約語 `BUCKETS` によって決定されるバケット数と同じか、StarRocks によって自動的に設定されます。 |

#### データ圧縮アルゴリズムの設定

テーブル作成時にプロパティ `compression` を追加することで、テーブルのデータ圧縮アルゴリズムを指定できます。

`compression` の有効な値は次の通りです:

- `LZ4`: LZ4 アルゴリズム。
- `ZSTD`: Zstandard アルゴリズム。
- `ZLIB`: zlib アルゴリズム。
- `SNAPPY`: Snappy アルゴリズム。

適切なデータ圧縮アルゴリズムの選択方法については、[データ圧縮](../../../table_design/data_compression.md) を参照してください。

#### データロードの書き込みクォーラムの設定

StarRocks クラスタに複数のデータレプリカがある場合、テーブルごとに異なる書き込みクォーラムを設定できます。つまり、StarRocks がロードタスクを成功と判断する前に、ロード成功を返す必要があるレプリカの数です。テーブル作成時にプロパティ `write_quorum` を追加して書き込みクォーラムを指定できます。このプロパティは v2.5 からサポートされています。

`write_quorum` の有効な値は次の通りです:

- `MAJORITY`: デフォルト値。データレプリカの**過半数**がロード成功を返すと、StarRocks はロードタスク成功を返します。それ以外の場合、StarRocks はロードタスク失敗を返します。
- `ONE`: データレプリカの**1 つ**がロード成功を返すと、StarRocks はロードタスク成功を返します。それ以外の場合、StarRocks はロードタスク失敗を返します。
- `ALL`: データレプリカの**すべて**がロード成功を返すと、StarRocks はロードタスク成功を返します。それ以外の場合、StarRocks はロードタスク失敗を返します。

> **注意**
>
> - ロードのために低い書き込みクォーラムを設定すると、データのアクセス不能や損失のリスクが増加します。例えば、StarRocks クラスタの 2 つのレプリカで 1 つの書き込みクォーラムを持つテーブルにデータをロードし、データが 1 つのレプリカにのみ正常にロードされた場合、StarRocks はロードタスクが成功したと判断しますが、データの生存レプリカは 1 つしかありません。ロードされたデータの tablet を保存するサーバーがダウンすると、これらの tablet のデータはアクセス不能になります。また、サーバーのディスクが損傷した場合、データは失われます。
> - StarRocks は、すべてのデータレプリカがステータスを返した後にのみロードタスクのステータスを返します。ロードステータスが不明なレプリカがある場合、StarRocks はロードタスクのステータスを返しません。レプリカでのロードタイムアウトもロード失敗と見なされます。

#### レプリカ間のデータ書き込みとレプリケーションモードの指定

StarRocks クラスタに複数のデータレプリカがある場合、`PROPERTIES` で `replicated_storage` パラメータを指定して、レプリカ間のデータ書き込みとレプリケーションモードを構成できます。

- `true` は「単一リーダーレプリケーション」を示し、データはプライマリレプリカにのみ書き込まれます。他のレプリカはプライマリレプリカからデータを同期します。このモードは、複数のレプリカへのデータ書き込みによって引き起こされる CPU コストを大幅に削減します。v2.5 からサポートされています。
- `false` (**v2.5 のデフォルト**) は「リーダーレスレプリケーション」を示し、データはプライマリおよびセカンダリレプリカを区別せずに直接複数のレプリカに書き込まれます。CPU コストはレプリカの数によって増加します。

ほとんどの場合、デフォルト値を使用することで、より良いデータ書き込みパフォーマンスが得られます。レプリカ間のデータ書き込みとレプリケーションモードを変更したい場合は、ALTER TABLE コマンドを実行してください。例:

```sql
    ALTER TABLE example_db.my_table
    SET ("replicated_storage" = "true");
```

#### 一括でロールアップを作成

テーブル作成時に一括でロールアップを作成できます。

構文:

```SQL
ROLLUP (rollup_name (column_name1, column_name2, ...)
[FROM from_index_name]
[PROPERTIES ("key"="value", ...)],...)
```

## 例

### ハッシュバケッティングと列指向（カラムナ）ストレージを使用する集計テーブルを作成

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
DISTRIBUTED BY HASH(k1) BUCKETS 10
PROPERTIES ("storage_type"="column");
```

### 集計テーブルを作成し、記憶媒体とクールダウン時間を設定

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
DISTRIBUTED BY HASH (k1, k2) BUCKETS 10
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
DISTRIBUTED BY HASH (k1, k2) BUCKETS 10
PROPERTIES(
    "storage_type"="column",
    "storage_medium" = "SSD",
    "storage_cooldown_time" = "2015-06-04 00:00:00"
);
```

### 範囲パーティション、ハッシュバケッティング、列指向（カラムナ）ストレージを使用し、記憶媒体とクールダウン時間を設定した重複キーテーブルを作成

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
DISTRIBUTED BY HASH(k2) BUCKETS 10
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
DISTRIBUTED BY HASH(k2) BUCKETS 10
PROPERTIES(
    "storage_medium" = "SSD"
);
```

### MySQL 外部テーブルを作成

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

### HLL カラムを含むテーブルを作成

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
DISTRIBUTED BY HASH(k1) BUCKETS 10
PROPERTIES ("storage_type"="column");
```

### BITMAP_UNION 集約タイプを含むテーブルを作成

`v1` および `v2` カラムの元のデータタイプは TINYINT、SMALLINT、または INT でなければなりません。

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
DISTRIBUTED BY HASH(k1) BUCKETS 10
PROPERTIES ("storage_type"="column");
```

### Colocate Join をサポートする 2 つのテーブルを作成

```SQL
CREATE TABLE `t1` 
(
     `id` int(11) COMMENT "",
    `value` varchar(8) COMMENT ""
) 
ENGINE=OLAP
DUPLICATE KEY(`id`)
DISTRIBUTED BY HASH(`id`) BUCKETS 10
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
DISTRIBUTED BY HASH(`id`) BUCKETS 10
PROPERTIES 
(
    "colocate_with" = "t1"
);
```

### ビットマップインデックスを持つテーブルを作成

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
DISTRIBUTED BY HASH(k1) BUCKETS 10
PROPERTIES ("storage_type"="column");
```

### 動的パーティションテーブルを作成

動的パーティション化機能は、FE 設定で有効にする必要があります ("dynamic_partition.enable" = "true")。詳細は [動的パーティションの設定](#configure-dynamic-partitions) を参照してください。

この例では、次の 3 日間のパーティションを作成し、3 日前に作成されたパーティションを削除します。例えば、今日が 2020-01-08 の場合、次の名前のパーティションが作成されます: p20200108, p20200109, p20200110, p20200111。それらの範囲は次の通りです:

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
DISTRIBUTED BY HASH(k2) BUCKETS 10
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

### 複数のパーティションを一括で作成し、整数型のカラムをパーティション列として指定

  次の例では、パーティション列 `datekey` は INT 型です。すべてのパーティションは、`START ("1") END ("5") EVERY (1)` という単純なパーティションクローズによって作成されます。すべてのパーティションの範囲は `1` から始まり、`5` で終わり、パーティショングラニュラリティは `1` です:
  > **注意**
  >
  > **START()** および **END()** 内のパーティション列の値は引用符で囲む必要がありますが、**EVERY()** 内のパーティショングラニュラリティは引用符で囲む必要はありません。

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

### Hive 外部テーブルを作成

Hive 外部テーブルを作成する前に、Hive リソースとデータベースを作成している必要があります。詳細は [外部テーブル](../../../data_source/External_table.md#hive-external-table) を参照してください。

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

## 参考文献

- [SHOW CREATE TABLE](../data-manipulation/SHOW_CREATE_TABLE.md)
- [SHOW TABLES](../data-manipulation/SHOW_TABLES.md)
- [USE](USE.md)
- [ALTER TABLE](ALTER_TABLE.md)
- [DROP TABLE](DROP_TABLE.md)
```