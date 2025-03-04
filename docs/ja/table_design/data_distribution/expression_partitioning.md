---
description: StarRocks でデータをパーティション分割する
displayed_sidebar: docs
---

# 式に基づくパーティション化（推奨）

v3.0以降、StarRocksは式に基づくパーティション化（以前は自動パーティション化として知られていました）をサポートしており、より柔軟でユーザーフレンドリーです。このパーティション化の手法は、連続した時間範囲やENUM値に基づいてデータをクエリおよび管理するようなほとんどのシナリオに適しています。

テーブル作成時にシンプルなパーティション式を指定するだけで、データロード中にStarRocksがデータとパーティション式で定義されたルールに基づいて自動的にパーティションを作成します。テーブル作成時に多数のパーティションを手動で作成する必要はなく、動的パーティションプロパティを設定する必要もありません。

v3.4以降、式に基づくパーティション化はさらに最適化され、すべてのパーティション化戦略を統一し、より複雑なソリューションをサポートします。ほとんどの場合に推奨されており、将来のリリースでは他のパーティション化戦略に取って代わる予定です。

## シンプルな時間関数式に基づくパーティション化

連続した時間範囲に基づいてデータを頻繁にクエリおよび管理する場合、日付型（DATEまたはDATETIME）の列をパーティション列として指定し、時間関数式で年、月、日、または時間をパーティショングラニュラリティとして指定するだけです。StarRocksはロードされたデータとパーティション式に基づいて自動的にパーティションを作成し、パーティションの開始日と終了日または日時を設定します。

ただし、特定のシナリオでは、たとえば履歴データを月ごとにパーティション化し、最近のデータを日ごとにパーティション化する場合、[レンジパーティション化](./Data_distribution.md#range-partitioning)を使用してパーティションを作成する必要があります。

### 構文

```sql
PARTITION BY expression
...
[ PROPERTIES( 'partition_live_number' = 'xxx' ) ]

expression ::=
    { date_trunc ( <time_unit> , <partition_column> ) |
      time_slice ( <partition_column> , INTERVAL <N> <time_unit> [ , boundary ] ) }
```

### パラメータ

#### `expression`

**必須**: YES<br/>
**説明**: [date_trunc](../../sql-reference/sql-functions/date-time-functions/date_trunc.md) または [time_slice](../../sql-reference/sql-functions/date-time-functions/time_slice.md) 関数を使用するシンプルな時間関数式です。`time_slice` 関数を使用する場合、`boundary` パラメータを渡す必要はありません。このシナリオでは、このパラメータのデフォルトかつ有効な値は `floor` であり、値は `ceil` にすることはできません。<br/>

#### `time_unit`

**必須**: YES<br/>
**説明**: パーティショングラニュラリティであり、`hour`、`day`、`month`、または `year` にすることができます。`week` パーティショングラニュラリティはサポートされていません。パーティショングラニュラリティが `hour` の場合、パーティション列は DATETIME データ型でなければならず、DATE データ型であってはなりません。<br/>

#### `partition_column` 

**必須**: YES<br/>
**説明**: パーティション列の名前です。<br/><ul><li>パーティション列は DATE または DATETIME データ型のみであることができます。パーティション列は `NULL` 値を許可します。</li><li>`date_trunc` 関数が使用される場合、パーティション列は DATE または DATETIME データ型であることができます。`time_slice` 関数が使用される場合、パーティション列は DATETIME データ型でなければなりません。</li><li>パーティション列が DATE データ型の場合、サポートされる範囲は [0000-01-01 ~ 9999-12-31] です。パーティション列が DATETIME データ型の場合、サポートされる範囲は [0000-01-01 01:01:01 ~ 9999-12-31 23:59:59] です。</li><li>現在、指定できるパーティション列は1つだけで、複数のパーティション列はサポートされていません。</li></ul> <br/>

#### `partition_live_number` 

**必須**: NO<br/>
**説明**: 保持する最新のパーティションの数です。パーティションは日付順に並べられ、**現在の日付を基準**として、`partition_live_number` を引いた日付より古いパーティションは削除されます。StarRocksはパーティションの数を管理するタスクをスケジュールし、スケジューリング間隔はFE動的パラメータ `dynamic_partition_check_interval_seconds` を通じて設定でき、デフォルトは600秒（10分）です。たとえば、現在の日付が2023年4月4日で、`partition_live_number` が `2` に設定されており、パーティションが `p20230401`、`p20230402`、`p20230403`、`p20230404` を含む場合、パーティション `p20230403` と `p20230404` が保持され、他のパーティションは削除されます。将来の日付である4月5日と4月6日のデータがロードされた場合、パーティションは `p20230401`、`p20230402`、`p20230403`、`p20230404`、`p20230405`、`p20230406` を含みます。その場合、パーティション `p20230403`、`p20230404`、`p20230405`、`p20230406` が保持され、他のパーティションは削除されます。<br/>

### 使用上の注意

- データロード中に、StarRocksはロードされたデータに基づいていくつかのパーティションを自動的に作成しますが、ロードジョブが何らかの理由で失敗した場合、StarRocksによって自動的に作成されたパーティションは自動的に削除されません。
- StarRocksは、1回のロードで自動的に作成されるパーティションの最大数を4096に設定しており、FEパラメータ `auto_partition_max_creation_number_per_load` によって設定できます。このパラメータは、誤って多くのパーティションを作成するのを防ぐことができます。
- パーティションの命名ルールは、動的パーティション化の命名ルールと一致しています。

### 例

例1: 日ごとにデータを頻繁にクエリする場合、パーティション式 `date_trunc()` を使用し、テーブル作成時にパーティション列を `event_day`、パーティショングラニュラリティを `day` と設定できます。ロード中にデータは日付に基づいて自動的にパーティション化されます。同じ日のデータは1つのパーティションに保存され、パーティションプルーニングを使用してクエリ効率を大幅に向上させることができます。

```SQL
CREATE TABLE site_access1 (
    event_day DATETIME NOT NULL,
    site_id INT DEFAULT '10',
    city_code VARCHAR(100),
    user_name VARCHAR(32) DEFAULT '',
    pv BIGINT DEFAULT '0'
)
DUPLICATE KEY(event_day, site_id, city_code, user_name)
PARTITION BY date_trunc('day', event_day)
DISTRIBUTED BY HASH(event_day, site_id);
```

たとえば、次の2つのデータ行がロードされると、StarRocksは自動的に2つのパーティション `p20230226` と `p20230227` を作成し、それぞれの範囲は [2023-02-26 00:00:00, 2023-02-27 00:00:00) および [2023-02-27 00:00:00, 2023-02-28 00:00:00) です。後続のロードされたデータがこれらの範囲内にある場合、それらは自動的に対応するパーティションにルーティングされます。

```SQL
-- 2つのデータ行を挿入
INSERT INTO site_access1  
    VALUES ("2023-02-26 20:12:04",002,"New York","Sam Smith",1),
           ("2023-02-27 21:06:54",001,"Los Angeles","Taylor Swift",1);

-- パーティションを表示
mysql > SHOW PARTITIONS FROM site_access1;
+-------------+---------------+----------------+---------------------+--------------------+--------+--------------+------------------------------------------------------------------------------------------------------+--------------------+---------+----------------+---------------+---------------------+--------------------------+----------+------------+----------+
| PartitionId | PartitionName | VisibleVersion | VisibleVersionTime  | VisibleVersionHash | State  | PartitionKey | Range                                                                                                | DistributionKey    | Buckets | ReplicationNum | StorageMedium | CooldownTime        | LastConsistencyCheckTime | DataSize | IsInMemory | RowCount |
+-------------+---------------+----------------+---------------------+--------------------+--------+--------------+------------------------------------------------------------------------------------------------------+--------------------+---------+----------------+---------------+---------------------+--------------------------+----------+------------+----------+
| 17138       | p20230226     | 2              | 2023-07-19 17:53:59 | 0                  | NORMAL | event_day    | [types: [DATETIME]; keys: [2023-02-26 00:00:00]; ..types: [DATETIME]; keys: [2023-02-27 00:00:00]; ) | event_day, site_id | 6       | 3              | HDD           | 9999-12-31 23:59:59 | NULL                     | 0B       | false      | 0        |
| 17113       | p20230227     | 2              | 2023-07-19 17:53:59 | 0                  | NORMAL | event_day    | [types: [DATETIME]; keys: [2023-02-27 00:00:00]; ..types: [DATETIME]; keys: [2023-02-28 00:00:00]; ) | event_day, site_id | 6       | 3              | HDD           | 9999-12-31 23:59:59 | NULL                     | 0B       | false      | 0        |
+-------------+---------------+----------------+---------------------+--------------------+--------+--------------+------------------------------------------------------------------------------------------------------+--------------------+---------+----------------+---------------+---------------------+--------------------------+----------+------------+----------+
2 rows in set (0.00 sec)
```

例2: パーティションのライフサイクル管理を実装したい場合、つまり、最近のパーティションのみを保持し、履歴パーティションを削除する場合、`partition_live_number` プロパティを使用して保持するパーティションの数を指定できます。

```SQL
CREATE TABLE site_access2 (
    event_day DATETIME NOT NULL,
    site_id INT DEFAULT '10',
    city_code VARCHAR(100),
    user_name VARCHAR(32) DEFAULT '',
    pv BIGINT DEFAULT '0'
) 
DUPLICATE KEY(event_day, site_id, city_code, user_name)
PARTITION BY date_trunc('month', event_day)
DISTRIBUTED BY HASH(event_day, site_id)
PROPERTIES(
    "partition_live_number" = "3" -- 最新の3つのパーティションのみを保持
);
```

例3: 週ごとにデータを頻繁にクエリする場合、パーティション式 `time_slice()` を使用し、テーブル作成時にパーティション列を `event_day`、パーティショングラニュラリティを7日間に設定できます。1週間のデータは1つのパーティションに保存され、パーティションプルーニングを使用してクエリ効率を大幅に向上させることができます。

```SQL
CREATE TABLE site_access(
    event_day DATETIME NOT NULL,
    site_id INT DEFAULT '10',
    city_code VARCHAR(100),
    user_name VARCHAR(32) DEFAULT '',
    pv BIGINT DEFAULT '0'
)
DUPLICATE KEY(event_day, site_id, city_code, user_name)
PARTITION BY time_slice(event_day, INTERVAL 7 day)
DISTRIBUTED BY HASH(event_day, site_id)
```

## 列式に基づくパーティション化（v3.1以降）

特定のタイプのデータを頻繁にクエリおよび管理する場合、タイプを表す列をパーティション列として指定するだけです。StarRocksはロードされたデータのパーティション列の値に基づいて自動的にパーティションを作成します。

ただし、特定のシナリオでは、たとえばテーブルに `city` 列が含まれており、国や都市に基づいてデータを頻繁にクエリおよび管理する場合、[リストパーティション化](./list_partitioning.md)を使用して、同じ国の複数の都市のデータを1つのパーティションに保存する必要があります。

### 構文

```sql
PARTITION BY expression
...

expression ::=
    partition_columns 
    
partition_columns ::=
    <column>, [ <column> [,...] ]
```

### パラメータ

#### `partition_columns`

**必須**: YES<br/>
**説明**: パーティション列の名前です。<br/> <ul><li>パーティション列の値は文字列（BINARYはサポートされていません）、日付または日時、整数、およびブール値であることができます。パーティション列は `NULL` 値を許可します。</li><li> 各パーティションには、パーティション列の同じ値を持つデータのみを含めることができます。パーティション列に異なる値を持つデータを1つのパーティションに含めるには、[リストパーティション化](./list_partitioning.md)を参照してください。</li></ul> <br/>

### 使用上の注意

- データロード中に、StarRocksはロードされたデータに基づいていくつかのパーティションを自動的に作成しますが、ロードジョブが何らかの理由で失敗した場合、StarRocksによって自動的に作成されたパーティションは自動的に削除されません。
- StarRocksは、1回のロードで自動的に作成されるパーティションの最大数を4096に設定しており、FEパラメータ `auto_partition_max_creation_number_per_load` によって設定できます。このパラメータは、誤って多くのパーティションを作成するのを防ぐことができます。
- パーティションの命名ルール: 複数のパーティション列が指定されている場合、異なるパーティション列の値はパーティション名でアンダースコア `_` で接続され、形式は `p<パーティション列1の値>_<パーティション列2の値>_...` です。たとえば、`dt` と `province` の2つの列がパーティション列として指定されており、どちらも文字列型であり、値が `2022-04-01` と `beijing` のデータ行がロードされた場合、対応する自動的に作成されたパーティションは `p20220401_beijing` と命名されます。

### 例

例1: データセンターの請求詳細を時間範囲と特定の都市に基づいて頻繁にクエリする場合、テーブル作成時に最初のパーティション列として `dt` と `city` を指定するパーティション式を使用できます。この方法では、同じ日付と都市に属するデータが同じパーティションにルーティングされ、パーティションプルーニングを使用してクエリ効率を大幅に向上させることができます。

```SQL
CREATE TABLE t_recharge_detail1 (
    id bigint,
    user_id bigint,
    recharge_money decimal(32,2), 
    city varchar(20) not null,
    dt varchar(20) not null
)
DUPLICATE KEY(id)
PARTITION BY dt,city
DISTRIBUTED BY HASH(`id`);
```

テーブルに単一のデータ行を挿入します。

```SQL
INSERT INTO t_recharge_detail1 
    VALUES (1, 1, 1, 'Houston', '2022-04-01');
```

パーティションを表示します。結果は、StarRocksがロードされたデータに基づいて自動的にパーティション `p20220401_Houston1` を作成することを示しています。後続のロードでは、パーティション列 `dt` と `city` に `2022-04-01` と `Houston` の値を持つデータがこのパーティションに保存されます。

:::tip
各パーティションには、指定されたパーティション列の1つの値を持つデータのみを含めることができます。パーティション内でパーティション列に複数の値を指定するには、[リストパーティション](./list_partitioning.md)を参照してください。
:::

```SQL
MySQL > SHOW PARTITIONS from t_recharge_detail1\G
*************************** 1. row ***************************
             PartitionId: 16890
           PartitionName: p20220401_Houston
          VisibleVersion: 2
      VisibleVersionTime: 2023-07-19 17:24:53
      VisibleVersionHash: 0
                   State: NORMAL
            PartitionKey: dt, city
                    List: (('2022-04-01', 'Houston'))
         DistributionKey: id
                 Buckets: 6
          ReplicationNum: 3
           StorageMedium: HDD
            CooldownTime: 9999-12-31 23:59:59
LastConsistencyCheckTime: NULL
                DataSize: 2.5KB
              IsInMemory: false
                RowCount: 1
1 row in set (0.00 sec)
```

## 複雑な時間関数式に基づくパーティション化（v3.4以降）

v3.4.0以降、式に基づくパーティション化は、DATEまたはDATETIME型を返す任意の式をサポートし、さらに複雑なパーティション化シナリオに対応します。

たとえば、Unixタイムスタンプ列を定義し、パーティション式でfrom_unixtime()を直接使用してパーティションキーを定義することができます。生成されたDATEまたはDATETIME列を関数で定義する必要はありません。使用方法の詳細については、[例](#examples-2)を参照してください。

### 例

例1: 各データ行にUnixタイムスタンプを割り当て、日ごとにデータを頻繁にクエリする場合、テーブル作成時にタイムスタンプ列をfrom_unixtime()関数とともに使用して、タイムスタンプをパーティション列として定義し、パーティショングラニュラリティを1日に設定できます。各日のデータは1つのパーティションに保存され、パーティションプルーニングを使用してクエリ効率を向上させることができます。

```SQL
CREATE TABLE orders (
    ts BIGINT NOT NULL,
    id BIGINT NOT NULL,
    city STRING NOT NULL
)
PARTITION BY from_unixtime(ts,'%Y%m%d');
```

例2: 各データ行に不規則なSTRING型のタイムスタンプを割り当て、日ごとにデータを頻繁にクエリする場合、テーブル作成時にタイムスタンプ列をcast()とdate_parse()関数とともに使用して、タイムスタンプをDATE型に変換し、パーティション列として設定し、パーティショングラニュラリティを1日に設定できます。各日のデータは1つのパーティションに保存され、パーティションプルーニングを使用してクエリ効率を向上させることができます。

```SQL
CREATE TABLE orders_new (
    ts STRING NOT NULL,
    id BIGINT NOT NULL,
    city STRING NOT NULL
)
PARTITION BY CAST(DATE_PARSE(CAST(ts AS VARCHAR(100)),'%Y%m%d') AS DATE);
```

### 使用上の注意

複雑な時間関数式に基づくパーティション化の場合、パーティションプルーニングが適用されます:

- パーティションクロースが `PARTITION BY from_unixtime(ts)` の場合、`ts>1727224687` 形式のフィルタを持つクエリは対応するパーティションにプルーニングされます。
- パーティションクロースが `PARTITION BY CAST(DATE_PARSE(CAST(ts AS VARCHAR(100)),'%Y%m%d') AS DATE)` の場合、`ts = "20240506"` 形式のフィルタを持つクエリはプルーニングされます。
- 上記のケースは、[混合式に基づくパーティション化](#partitioning-based-on-the-mixed-expression-since-v34)にも適用されます。

## 混合式に基づくパーティション化（v3.4以降）

v3.4.0以降、式に基づくパーティション化は、複数のパーティション列をサポートし、そのうちの1つが時間関数式であることをサポートします。

### 例

例1: 各データ行にUnixタイムスタンプを割り当て、日ごとおよび特定の都市でデータを頻繁にクエリする場合、テーブル作成時にタイムスタンプ列（from_unixtime()関数を使用）と都市列をパーティション列として使用できます。各日の各都市のデータは1つのパーティションに保存され、パーティションプルーニングを使用してクエリ効率を向上させることができます。

```SQL
CREATE TABLE orders (
    ts BIGINT NOT NULL,
    id BIGINT NOT NULL,
    city STRING NOT NULL
)
PARTITION BY from_unixtime(ts,'%Y%m%d'), city;
```

## パーティションの管理

### パーティションへのデータロード

データロード中に、StarRocksはロードされたデータとパーティション式で定義されたパーティションルールに基づいて自動的にパーティションを作成します。

テーブル作成時に式に基づくパーティション化を使用し、特定のパーティションのデータを上書きするために [INSERT OVERWRITE](../../loading/InsertInto.md#overwrite-data-via-insert-overwrite-select) を使用する必要がある場合、パーティションが作成されているかどうかにかかわらず、現在は `PARTITION()` で明示的にパーティション範囲を指定する必要があります。これは、[レンジパーティション化](./Data_distribution.md#range-partitioning) または [リストパーティション化](./list_partitioning.md) とは異なり、`PARTITION (<partition_name>)` でパーティション名のみを提供することができます。

テーブル作成時に時間関数式を使用し、特定のパーティションのデータを上書きしたい場合、そのパーティションの開始日または日時（テーブル作成時に設定されたパーティショングラニュラリティ）を提供する必要があります。パーティションが存在しない場合、データロード中に自動的に作成されることがあります。

```SQL
INSERT OVERWRITE site_access1 PARTITION(event_day='2022-06-08 20:12:04')
    SELECT * FROM site_access2 PARTITION(p20220608);
```

テーブル作成時に列式を使用し、特定のパーティションのデータを上書きしたい場合、そのパーティションに含まれるパーティション列の値を提供する必要があります。パーティションが存在しない場合、データロード中に自動的に作成されることがあります。

```SQL
INSERT OVERWRITE t_recharge_detail1 PARTITION(dt='2022-04-02',city='texas')
    SELECT * FROM t_recharge_detail2 PARTITION(p20220402_texas);
```

### パーティションの表示

自動的に作成されたパーティションの特定の情報を表示したい場合、`SHOW PARTITIONS FROM <table_name>` ステートメントを使用する必要があります。`SHOW CREATE TABLE <table_name>` ステートメントは、テーブル作成時に設定された式に基づくパーティション化の構文のみを返します。

```SQL
MySQL > SHOW PARTITIONS FROM t_recharge_detail1;
+-------------+-------------------+----------------+---------------------+--------------------+--------+--------------+-----------------------------+-----------------+---------+----------------+---------------+---------------------+--------------------------+----------+------------+----------+
| PartitionId | PartitionName     | VisibleVersion | VisibleVersionTime  | VisibleVersionHash | State  | PartitionKey | List                        | DistributionKey | Buckets | ReplicationNum | StorageMedium | CooldownTime        | LastConsistencyCheckTime | DataSize | IsInMemory | RowCount |
+-------------+-------------------+----------------+---------------------+--------------------+--------+--------------+-----------------------------+-----------------+---------+----------------+---------------+---------------------+--------------------------+----------+------------+----------+
| 16890       | p20220401_Houston | 2              | 2023-07-19 17:24:53 | 0                  | NORMAL | dt, city     | (('2022-04-01', 'Houston')) | id              | 6       | 3              | HDD           | 9999-12-31 23:59:59 | NULL                     | 2.5KB    | false      | 1        |
| 17056       | p20220402_texas   | 2              | 2023-07-19 17:27:42 | 0                  | NORMAL | dt, city     | (('2022-04-02', 'texas'))   | id              | 6       | 3              | HDD           | 9999-12-31 23:59:59 | NULL                     | 2.5KB    | false      | 1        |
+-------------+-------------------+----------------+---------------------+--------------------+--------+--------------+-----------------------------+-----------------+---------+----------------+---------------+---------------------+--------------------------+----------+------------+----------+
2 rows in set (0.00 sec)
```

## 制限

- v3.1.0以降、StarRocksの共有データモードは[時間関数式](#partitioning-based-on-a-simple-time-function-expression)をサポートしています。また、v3.1.1以降、StarRocksの共有データモードは[列式](#partitioning-based-on-the-column-expression-since-v31)もサポートしています。
- 現在、CTASを使用して式に基づくパーティション化が設定されたテーブルを作成することはサポートされていません。
- 現在、Spark Loadを使用して式に基づくパーティション化を使用するテーブルにデータをロードすることはサポートされていません。
- `ALTER TABLE <table_name> DROP PARTITION <partition_name>` ステートメントを使用して列式で作成されたパーティションを削除する場合、パーティション内のデータは直接削除され、復元できません。
- v3.4.0、v3.3.8、v3.2.13、およびv3.1.16以降、StarRocksは式に基づくパーティション化戦略で作成されたテーブルの[バックアップと復元](../../administration/management/Backup_and_restore.md)をサポートしています。