---
displayed_sidebar: docs
description: StarRocks でデータをパーティション分割する
sidebar_position: 10
---

# 式パーティショニング (推奨)

v3.0以降、StarRocksは式パーティショニング（以前は自動パーティショニングとして知られていました）をサポートしており、より柔軟でユーザーフレンドリーです。このパーティショニング方法は、連続する時間範囲やENUM値に基づいてデータをクエリおよび管理するなど、ほとんどのシナリオに適しています。

テーブル作成時にシンプルなパーティション式を指定するだけで済みます。データロード中、StarRocksはデータとパーティション式で定義されたルールに基づいて自動的にパーティションを作成します。テーブル作成時に多数のパーティションを手動で作成したり、動的パーティションプロパティを設定したりする必要はなくなります。

v3.4以降、式パーティショニングはすべてのパーティショニング戦略を統合し、より複雑なソリューションをサポートするようにさらに最適化されています。ほとんどのケースで推奨され、将来のリリースでは他のパーティショニング戦略に置き換わる予定です。

v3.5以降、StarRocksはストレージ効率とクエリパフォーマンスを最適化するために、時間関数に基づいた式パーティションのマージをサポートしています。詳細については、以下を参照してください。[式パーティションのマージ](#merge-expression-partitions)。

## シンプルな時間関数式に基づいたパーティショニング

連続する時間範囲に基づいてデータを頻繁にクエリおよび管理する場合、日付型（`DATE` または `DATETIME`）の列をパーティション列として指定し、時間関数式でパーティションの粒度として年、月、日、または時間を指定するだけで済みます。StarRocksは、ロードされたデータとパーティション式に基づいて、自動的にパーティションを作成し、パーティションの開始日と終了日または日時を設定します。

ただし、履歴データを月ごとにパーティション分割し、最近のデータを日ごとにパーティション分割するなど、一部の特殊なシナリオでは、以下を使用する必要があります。[範囲パーティショニング](./Data_distribution.md#range-partitioning) を使用してパーティションを作成します。

:::note
`PARTITION BY date_trunc(column)` と `PARTITION BY time_slice(column)` は、式パーティショニングの形式であるにもかかわらず、範囲パーティショニングと見なされます。したがって、`ALTER TABLE ... ADD PARTITION` ステートメントを範囲パーティションに使用して、そのようなパーティショニング戦略を使用するテーブルに新しいパーティションを追加できます。
:::

### 構文

```sql
PARTITION BY expression
...
[ PROPERTIES( { 'partition_live_number' = 'xxx' | 'partition_retention_condition' = 'expr' } ) ]

expression ::=
    { date_trunc ( <time_unit> , <partition_column> ) |
      time_slice ( <partition_column> , INTERVAL <N> <time_unit> [ , boundary ] ) }
```

### パラメータ

#### `expression`

**必須**: はい\
**説明**: を使用するシンプルな時間関数式[`date_trunc`](../../sql-reference/sql-functions/date-time-functions/date_trunc.md) または [`time_slice`](../../sql-reference/sql-functions/date-time-functions/time_slice.md) 関数。`time_slice` 関数を使用する場合、`boundary` パラメータを渡す必要はありません。このシナリオでは、このパラメータのデフォルトの有効な値は `floor` であり、値は `ceil` にはならないためです。

#### `time_unit`

**必須**: はい\
**説明**: パーティションの粒度。`hour`、`day`、`week`、`month`、または `year` のいずれかです。パーティションの粒度が `hour` の場合、パーティション列は `DATETIME` データ型である必要があり、`DATE` データ型であってはなりません。

#### `partition_column`

**必須**: はい\
**説明**: パーティション列の名前。

- パーティション列は `DATE` または `DATETIME` データ型のみにできます。パーティション列は `NULL` 値を許可します。
- `date_trunc` 関数が使用されている場合、パーティション列は `DATE` または `DATETIME` データ型にできます。`time_slice` 関数が使用されている場合、パーティション列は `DATETIME` データ型である必要があります。
- パーティション列が `DATE` データ型の場合、サポートされる範囲は `[0000-01-01 ~ 9999-12-31]` です。パーティション列が `DATETIME` データ型の場合、サポートされる範囲は `[0000-01-01 01:01:01 ~ 9999-12-31 23:59:59]` です。
- 現在、パーティション列は1つのみ指定できます。複数のパーティション列はサポートされていません。

#### `partition_live_number`

**必須**: いいえ\
**説明**: 保持する最新のパーティションの数。パーティションは時系列順にソートされ、**現在の日付を基準として**; 現在の日付から `partition_live_number` を引いた日付より古いパーティションは削除されます。StarRocksはパーティションの数を管理するタスクをスケジュールし、スケジューリング間隔はFE動的パラメータ `dynamic_partition_check_interval_seconds` を介して設定でき、デフォルトは600秒（10分）です。現在の日付が2023年4月4日で、`partition_live_number` が `2` に設定されており、パーティションに `p20230401`、`p20230402`、`p20230403`、`p20230404` が含まれているとします。パーティション `p20230403` と `p20230404` は保持され、他のパーティションは削除されます。将来の日付である4月5日と4月6日のデータなど、ダーティデータがロードされた場合、パーティションには `p20230401`、`p20230402`、`p20230403`、`p20230404`、`p20230405`、および `p20230406` が含まれます。その後、パーティション `p20230403`、`p20230404`、`p20230405`、および `p20230406` は保持され、他のパーティションは削除されます。

#### `partition_retention_condition`

v3.5.0以降、StarRocksネイティブテーブルは共通パーティション式TTLをサポートしています。

`partition_retention_condition`: 動的に保持するパーティションを宣言する式です。この式の条件を満たさないパーティションは定期的に削除されます。例: `'partition_retention_condition' = 'dt >= CURRENT_DATE() - INTERVAL 3 MONTH'`。

- 式にはパーティション列と定数のみを含めることができます。非パーティション列はサポートされていません。
- 共通パーティション式は、リストパーティションとレンジパーティションに異なる方法で適用されます。
  - リストパーティションを持つテーブルの場合、StarRocksは共通パーティション式によってフィルタリングされたパーティションの削除をサポートします。
  - レンジパーティションを持つテーブルの場合、StarRocksはFEのパーティションプルーニング機能を使用してのみパーティションをフィルタリングおよび削除できます。パーティションプルーニングでサポートされていない述語に対応するパーティションは、フィルタリングおよび削除できません。

### 使用上の注意

- データロード中、StarRocksはロードされたデータに基づいて一部のパーティションを自動的に作成しますが、何らかの理由でロードジョブが失敗した場合、StarRocksによって自動的に作成されたパーティションは自動的に削除されません。
- StarRocksは、1回のロードで自動的に作成されるパーティションのデフォルトの最大数を4096に設定します。これはFEパラメータ`auto_partition_max_creation_number_per_load`で設定できます。このパラメータは、誤って多くのパーティションを作成してしまうことを防ぎます。
- パーティションの命名規則は、動的パーティションの命名規則と一貫しています。

### 例

例1: 日ごとにデータを頻繁にクエリするとします。テーブル作成時に、パーティション式`date_trunc()`を使用し、パーティション列を`event_day`、パーティション粒度を`day`に設定できます。ロード中にデータは日付に基づいて自動的にパーティション分割されます。同じ日のデータは1つのパーティションに格納され、パーティションプルーニングを使用してクエリ効率を大幅に向上させることができます。

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

例えば、以下の2つのデータ行がロードされると、StarRocksは自動的に2つのパーティション`p20230226`と`p20230227`を作成し、それぞれ[2023-02-26 00:00:00, 2023-02-27 00:00:00)と[2023-02-27 00:00:00, 2023-02-28 00:00:00)の範囲を持ちます。その後のロードされたデータがこれらの範囲内に収まる場合、それらは自動的に対応するパーティションにルーティングされます。

```SQL
-- 2つのデータ行を挿入
INSERT INTO site_access1  
VALUES ("2023-02-26 20:12:04",002,"New York","Sam Smith",1),
("2023-02-27 21:06:54",001,"Los Angeles","Taylor Swift",1);

-- パーティションを表示
mysql > SHOW PARTITIONS FROM site_access1;
+-------------+---------------+----------------+---------------------+--------------------+--------+--------------+------------------------------------------------------------------------------------------------------+--------------------+---------+----------------+---------------+---------------------+--------------------------+----------+-------------+------------+----------+-------------+--------------------+----------------+----------------+
| PartitionId | PartitionName | VisibleVersion | VisibleVersionTime  | VisibleVersionHash | State  | PartitionKey | Range                                                                                                | DistributionKey    | Buckets | ReplicationNum | StorageMedium | CooldownTime        | LastConsistencyCheckTime | DataSize | StorageSize | IsInMemory | RowCount | DataVersion | VersionEpoch       | VersionTxnType | TabletBalanced |
+-------------+---------------+----------------+---------------------+--------------------+--------+--------------+------------------------------------------------------------------------------------------------------+--------------------+---------+----------------+---------------+---------------------+--------------------------+----------+-------------+------------+----------+-------------+--------------------+----------------+----------------+
| 17138       | p20230226     | 2              | 2023-07-19 17:53:59 | 0                  | NORMAL | event_day    | [types: [DATETIME]; keys: [2023-02-26 00:00:00]; ..types: [DATETIME]; keys: [2023-02-27 00:00:00]; ) | event_day, site_id | 6       | 3              | HDD           | 9999-12-31 23:59:59 | NULL                     | 0B       | 0B          | false      | 0        | 2           | 409742105974407168 | TXN_NORMAL     | true           |
| 17113       | p20230227     | 2              | 2023-07-19 17:53:59 | 0                  | NORMAL | event_day    | [types: [DATETIME]; keys: [2023-02-27 00:00:00]; ..types: [DATETIME]; keys: [2023-02-28 00:00:00]; ) | event_day, site_id | 6       | 3              | HDD           | 9999-12-31 23:59:59 | NULL                     | 0B       | 0B          | false      | 0        | 2           | 409742105974407169 | TXN_NORMAL     | true           |
+-------------+---------------+----------------+---------------------+--------------------+--------+--------------+------------------------------------------------------------------------------------------------------+--------------------+---------+----------------+---------------+---------------------+--------------------------+----------+-------------+------------+----------+-------------+--------------------+----------------+----------------+
2 rows in set (0.00 sec)
```

例2: パーティションのライフサイクル管理を実装したい場合、つまり、最近の一定数のパーティションのみを保持し、履歴パーティションを削除したい場合は、`partition_live_number`プロパティを使用して保持するパーティションの数を指定できます。

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
    "partition_live_number" = "3" -- only retains the most recent three partitions
);
```

例3: 週ごとにデータを頻繁にクエリするとします。テーブル作成時に、パーティション式`time_slice()`を使用し、パーティション列を`event_day`、パーティション粒度を1週間に設定できます。1週間分のデータは1つのパーティションに格納され、パーティションプルーニングを使用してクエリ効率を大幅に向上させることができます。

```SQL
CREATE TABLE site_access(
    event_day DATETIME NOT NULL,
    site_id INT DEFAULT '10',
    city_code VARCHAR(100),
    user_name VARCHAR(32) DEFAULT '',
    pv BIGINT DEFAULT '0'
)
DUPLICATE KEY(event_day, site_id, city_code, user_name)
PARTITION BY time_slice(event_day, INTERVAL 1 week)
DISTRIBUTED BY HASH(event_day, site_id);
```

## 列式に基づくパーティション分割 (v3.1以降)

特定の種類のデータを頻繁にクエリおよび管理する場合、その種類を表す列をパーティション列として指定するだけで済みます。StarRocksは、ロードされたデータのパーティション列の値に基づいてパーティションを自動的に作成します。

ただし、テーブルに`city`列が含まれており、国や都市に基づいてデータを頻繁にクエリおよび管理するなどの特殊なシナリオでは、[リストパーティション](./list_partitioning.md)同じ国内の複数の都市のデータを1つのパーティションに格納するために使用する必要があります。

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

**必須**: はい\
**説明**: パーティション列の名前。

- パーティション列の値は、文字列 (`BINARY`はサポートされていません)、`date`または`datetime`、`integer`、および`boolean`の値にすることができます。パーティション列は`NULL`値を許可します。
- 各パーティションには、パーティション列に同じ値を持つデータのみを含めることができます。パーティション列に異なる値を持つデータをパーティションに含めるには、[リストパーティション](./list_partitioning.md)を参照してください。

:::note

v3.4以降、パーティション列を囲む括弧を省略できます。例えば、`PARTITION BY (dt,city)`を`PARTITION BY dt,city`に置き換えることができます。

:::

### 使用上の注意

- データロード中、StarRocksはロードされたデータに基づいて一部のパーティションを自動的に作成しますが、何らかの理由でロードジョブが失敗した場合、StarRocksによって自動的に作成されたパーティションは自動的に削除されません。
- StarRocksは、1回のロードで自動的に作成されるパーティションのデフォルトの最大数を4096に設定します。これはFEパラメータ`auto_partition_max_creation_number_per_load`で設定できます。このパラメータは、誤って多くのパーティションを作成してしまうことを防ぎます。
- パーティションの命名規則: 複数のパーティション列が指定されている場合、異なるパーティション列の値はパーティション名でアンダースコア`_`で連結され、形式は`p<value in partition column 1>_<value in partition column 2>_...`です。例えば、`dt`と`province`の2つの列がパーティション列として指定され、両方とも文字列型であり、`2022-04-01`と`beijing`の値を持つデータ行がロードされた場合、対応する自動作成されたパーティションは`p20220401_beijing`と命名されます。

### 例

例1: 時間範囲と特定の都市に基づいてデータセンターの請求詳細を頻繁にクエリするとします。テーブル作成時に、パーティション式を使用して最初のパーティション列を`dt`と`city`として指定できます。これにより、同じ日付と都市に属するデータは同じパーティションにルーティングされ、パーティションプルーニングを使用してクエリ効率を大幅に向上させることができます。

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

パーティションを表示します。結果は、StarRocksがロードされたデータに基づいてパーティション`p20220401_Houston`を自動的に作成することを示しています。その後のロード中に、パーティション列`dt`と`city`に`2022-04-01`と`Houston`の値を持つデータがこのパーティションに格納されます。

:::tip
各パーティションには、パーティション列に指定された1つの値を持つデータのみを含めることができます。パーティション内のパーティション列に複数の値を指定するには、以下を参照してください。[リストパーティション](./list_partitioning.md)。
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
                    List: [["2022-04-01","Houston"]]
         DistributionKey: id
                 Buckets: 6
          ReplicationNum: 3
           StorageMedium: HDD
            CooldownTime: 9999-12-31 23:59:59
LastConsistencyCheckTime: NULL
                DataSize: 2.5KB
             StorageSize: 2.5KB
              IsInMemory: false
                RowCount: 1
             DataVersion: 2
            VersionEpoch: 409742188174376960
          VersionTxnType: TXN_NORMAL
          TabletBalanced: true
1 row in set (0.00 sec)
```

## 複雑な時間関数式に基づくパーティショニング (v3.4以降)

v3.4.0以降、式パーティショニングは、より複雑なパーティショニングシナリオに対応するため、`DATE`または`DATETIME`型を返すあらゆる式をサポートします。サポートされている時間関数については、以下を参照してください。[付録 - サポートされている時間関数](#supported-time-functions)。

例えば、Unixタイムスタンプ列を定義し、パーティション式でその列に対して`from_unixtime()`を直接使用してパーティションキーを定義できます。これにより、関数を持つ生成された`DATE`または`DATETIME`列を定義する必要がなくなります。使用方法の詳細については、以下を参照してください。[例](#examples-2)。

v3.4.4以降、ほとんどの`DATETIME`関連関数に基づくパーティションに対して、パーティションプルーニングがサポートされます。

### 例

例1: 各データ行にUnixタイムスタンプを割り当て、日ごとにデータを頻繁にクエリするとします。テーブル作成時に、式内の関数`from_unixtime()`を使用してタイムスタンプ列をパーティション列として定義し、パーティションの粒度を日単位に設定できます。各日のデータは1つのパーティションに格納され、パーティションプルーニングを使用してクエリ効率を向上させることができます。

```SQL
CREATE TABLE orders (
    ts BIGINT NOT NULL,
    id BIGINT NOT NULL,
    city STRING NOT NULL
)
PARTITION BY from_unixtime(ts,'%Y%m%d');
```

例2: 各データ行に`INT`型のタイムスタンプを割り当て、データを月単位で保存するとします。テーブル作成時に、式内の関数`cast()`と`str_to_date()`を使用してタイムスタンプを`DATE`型に変換し、それをパーティション列として設定し、`date_trunc()`を使用してパーティションの粒度を月単位に設定できます。各月のデータは1つのパーティションに格納され、パーティションプルーニングを使用してクエリ効率を向上させることができます。

```SQL
CREATE TABLE orders_new (
    ts INT NOT NULL,
    id BIGINT NOT NULL,
    city STRING NOT NULL
)
PARTITION BY date_trunc('month', str_to_date(CAST(ts as STRING),'%Y%m%d'));
```

### 使用上の注意

パーティションプルーニングは、複雑な時間関数式に基づくパーティショニングの場合に適用されます。

- パーティション句が`PARTITION BY from_unixtime(ts)`の場合、`ts>1727224687`形式のフィルターを持つクエリは、対応するパーティションにプルーニングできます。
- パーティション句が`PARTITION BY str2date(CAST(ts AS string),'%Y%m')`の場合、`ts = "20240506"`形式のフィルターを持つクエリはプルーニングできます。
- 上記のケースは、以下にも適用されます。[混合式に基づくパーティショニング](#partitioning-based-on-the-mixed-expression-since-v34)。

## 混合式に基づくパーティショニング (v3.4以降)

v3.4.0以降、式パーティショニングは、複数のパーティション列をサポートし、そのうちの1つが時間関数式である場合があります。

### 例

例1: 各データ行にUnixタイムスタンプを割り当て、日ごとおよび特定の都市ごとにデータを頻繁にクエリするとします。テーブル作成時に、タイムスタンプ列 (`from_unixtime()`関数を使用) と都市列をパーティション列として使用できます。各都市の各日のデータは1つのパーティションに格納され、パーティションプルーニングを使用してクエリ効率を向上させることができます。

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

データロード中、StarRocksはロードされたデータとパーティション式で定義されたパーティションルールに基づいて、パーティションを自動的に作成します。

式パーティショニングを使用する場合、以下を使用できます。[INSERT OVERWRITE](../../loading/InsertInto.md#overwrite-data-via-insert-overwrite-select)パーティション名を指定せずにデータを動的に上書きします。StarRocksは、対応するパーティション内のデータを自動的にルーティングして上書きします。

パーティション内のデータを上書きしたい場合、*特定の*パーティション内のデータを上書きしたい場合、`PARTITION()`でパーティション範囲を明示的に指定できます。式パーティションの場合、パーティション名だけでなく、そのパーティションの開始日または日時（テーブル作成時に設定されたパーティション粒度）、または特定の列値を指定する必要があることに注意してください。パーティションが存在しない場合、データロード中に自動的に作成されます。

```SQL
INSERT OVERWRITE site_access1 PARTITION(event_day='2022-06-08 20:12:04')
SELECT * FROM site_access2 PARTITION(p20220608);
```

テーブル作成時に列式を使用し、特定のパーティション内のデータを上書きしたい場合、そのパーティションに含まれるパーティション列の値を提供する必要があります。パーティションが存在しない場合、データロード中に自動的に作成できます。

```SQL
INSERT OVERWRITE t_recharge_detail1 PARTITION(dt='2022-04-02',city='texas')
SELECT * FROM t_recharge_detail2 PARTITION(p20220402_texas);
```

### パーティションの表示

自動的に作成されたパーティションに関する特定の情報を表示したい場合、`SHOW PARTITIONS FROM <table_name>`ステートメントを使用する必要があります。`SHOW CREATE TABLE <table_name>`ステートメントは、テーブル作成時に設定された式パーティショニングの構文のみを返します。

```SQL
MySQL > SHOW PARTITIONS FROM t_recharge_detail1;
+-------------+-------------------+----------------+---------------------+--------------------+--------+--------------+----------------------------+-----------------+---------+----------------+---------------+---------------------+--------------------------+----------+-------------+------------+----------+-------------+--------------------+----------------+----------------+
| PartitionId | PartitionName     | VisibleVersion | VisibleVersionTime  | VisibleVersionHash | State  | PartitionKey | List                       | DistributionKey | Buckets | ReplicationNum | StorageMedium | CooldownTime        | LastConsistencyCheckTime | DataSize | StorageSize | IsInMemory | RowCount | DataVersion | VersionEpoch       | VersionTxnType | TabletBalanced |
+-------------+-------------------+----------------+---------------------+--------------------+--------+--------------+----------------------------+-----------------+---------+----------------+---------------+---------------------+--------------------------+----------+-------------+------------+----------+-------------+--------------------+----------------+----------------+
| 11099       | p20220401_Houston | 2              | 2026-03-11 13:59:51 | 0                  | NORMAL | dt, city     | [["2022-04-01","Houston"]] | id              | 6       | 3              | HDD           | 9999-12-31 23:59:59 | NULL                     | 0B       | 0B          | false      | 0        | 2           | 409743636180238336 | TXN_NORMAL     | true           |
| 11116       | p20220402_texas   | 2              | 2026-03-11 13:59:52 | 0                  | NORMAL | dt, city     | [["2022-04-02","texas"]]   | id              | 6       | 3              | HDD           | 9999-12-31 23:59:59 | NULL                     | 0B       | 0B          | false      | 0        | 2           | 409743639174971392 | TXN_NORMAL     | true           |
+-------------+-------------------+----------------+---------------------+--------------------+--------+--------------+----------------------------+-----------------+---------+----------------+---------------+---------------------+--------------------------+----------+-------------+------------+----------+-------------+--------------------+----------------+----------------+
2 rows in set (0.01 sec)
```

### 式パーティションのマージ

データ管理において、異なる時間粒度に基づくパーティショニングは、クエリとストレージの最適化に不可欠です。ストレージ効率とクエリパフォーマンスを向上させるため、StarRocksは、より細かい時間粒度の複数の式パーティションを、より粗い時間粒度の1つのパーティションにマージすることをサポートしています。例えば、日ごとのパーティションを月ごとの1つのパーティションにマージするなどです。指定された条件（時間範囲）を満たすパーティションをマージすることで、StarRocksは異なる時間粒度でデータをパーティショニングすることを可能にします。

#### 構文

```SQL
ALTER TABLE [<db_name>.]<table_name>
PARTITION BY <time_expr>
[WHERE <time_range_column>] BETWEEN <start_time> AND <end_time>
```

#### パラメーター

##### `PARTITION BY <time_expr>`

**必須**: はい\
**説明**: パーティショニング戦略の新しい時間粒度を指定します。例: `PARTITION BY date_trunc('month', dt)`。

##### `WHERE <time_range_column> BETWEEN <start_time> AND <end_time>`

**必須**: はい\
**説明**: マージするパーティションの時間範囲を指定します。この範囲内のパーティションは、`PARTITION BY`句で定義されたルールに基づいてマージされます。

#### 例

テーブル`site_access1`のパーティションをマージし、パーティションの時間粒度を日単位から月単位に変更します。マージするパーティションの時間範囲は`2024-01-01`から`2024-03-31`までです。

```SQL
ALTER TABLE site_access1 PARTITION BY date_trunc('month', event_day)
BETWEEN '2024-01-01' AND '2024-03-31';
```

マージ後:

- 日レベルのパーティション`2024-01-01`から`2024-01-31`は、`2024-01`の月レベルのパーティションにマージされます。
- 日レベルのパーティション`2024-02-01`から`2024-02-29`は、`2024-02`の月レベルのパーティションにマージされます。
- 日レベルのパーティション`2024-03-01`から`2024-03-31`は、`2024-03`の月レベルのパーティションにマージされます。

#### 使用上の注意

- マージは、時間関数に基づく式パーティションでのみサポートされています。
- 複数のパーティション列を持つパーティションのマージはサポートされていません。
- マージとスキーマ変更/DML操作の並行実行はサポートされていません。

## 制限事項

- v3.1.0以降、StarRocksの共有データモードは[時間関数式](#partitioning-based-on-a-simple-time-function-expression)をサポートしています。そしてv3.1.1以降、StarRocksの共有データモードはさらに[列式](#partitioning-based-on-the-column-expression-since-v31)をサポートしています。
- 現在、CTASを使用して式パーティショニングが設定されたテーブルを作成することはサポートされていません。
- 現在、Spark Loadを使用して式パーティショニングを使用するテーブルにデータをロードすることはサポートされていません。
- `ALTER TABLE <table_name> DROP PARTITION <partition_name>`ステートメントを使用して列式で作成されたパーティションを削除すると、パーティション内のデータは直接削除され、回復できません。
- v3.4.0、v3.3.8、v3.2.13、およびv3.1.16以降、StarRocksは[バックアップと復元](../../administration/management/Backup_and_restore.md)式パーティショニング戦略で作成されたテーブルをサポートします。

## 付録

### サポートされている時間関数

式パーティショニングは、次の関数をサポートしています。

**時間関数**:

- `timediff`
- `datediff`
- `to_days`
- `years_add`/`sub`
- `quarters_add`/`sub`
- `months_add`/`sub`
- `weeks_add`/`sub`
- `date_add`/`sub`
- `days_add`/`sub`
- `hours_add`/`sub`
- `minutes_add`/`sub`
- `seconds_add`/`sub`
- `milliseconds_add`/`sub`
- `date_trunc`
- `date_format(YmdHiSf/YmdHisf)`
- `str2date(YmdHiSf/YmdHisf)`
- `str_to_date(YmdHiSf/YmdHisf)`
- `to_iso8601`
- `to_date`
- `unix_timestamp`
- `from_unixtime(YmdHiSf/YmdHisf)`
- `time_slice`

**その他の関数**:

- 追加
- 減算
- キャスト

:::note

- 複数の時間関数の組み合わせた使用がサポートされています。
- 上記にリストされているすべての時間関数には、システムのデフォルトタイムゾーンが使用されます。
- 時間関数 `YmdHiSf` の値の形式は、最も粗い時間粒度である `%Y` で始まる必要があります。例えば `%m-%d` のように、より細かい時間粒度で始まる形式は許可されません。

**例**

`PARTITION BY from_unixtime(cast(str as INT) + 3600, '%Y-%m-%d')`

:::
