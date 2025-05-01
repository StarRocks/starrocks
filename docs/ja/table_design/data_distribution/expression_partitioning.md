---
description: StarRocks でデータをパーティション分割する
displayed_sidebar: docs
---

# 式に基づくパーティション化 (推奨)

v3.0以降、StarRocksは式に基づくパーティション化（以前は自動パーティション化として知られていました）をサポートしており、より柔軟でユーザーフレンドリーです。このパーティション化の手法は、連続した時間範囲やENUM値に基づいてデータをクエリおよび管理するようなほとんどのシナリオに適しています。

テーブル作成時にシンプルなパーティション式（時間関数式またはカラム式のいずれか）を指定するだけで済みます。データロード中に、StarRocksはデータとパーティション式で定義されたルールに基づいて自動的にパーティションを作成します。テーブル作成時に多数のパーティションを手動で作成する必要はなく、動的パーティションプロパティを設定する必要もありません。

## 時間関数式に基づくパーティション化

連続した時間範囲に基づいてデータを頻繁にクエリおよび管理する場合、日付型（DATEまたはDATETIME）のカラムをパーティションカラムとして指定し、時間関数式で年、月、日、または時間をパーティショングラニュラリティとして指定するだけで済みます。StarRocksはロードされたデータとパーティション式に基づいて自動的にパーティションを作成し、パーティションの開始日と終了日または日時を設定します。

ただし、特定のシナリオでは、たとえば、履歴データを月ごとにパーティション化し、最近のデータを日ごとにパーティション化する場合、[レンジパーティション化](Data_distribution.md#range-partitioning)を使用してパーティションを作成する必要があります。

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
**説明**: 現在、[date_trunc](../../sql-reference/sql-functions/date-time-functions/date_trunc.md) および [time_slice](../../sql-reference/sql-functions/date-time-functions/time_slice.md) 関数のみがサポートされています。`time_slice` 関数を使用する場合、`boundary` パラメータを渡す必要はありません。このシナリオでは、このパラメータのデフォルトかつ有効な値は `floor` であり、値は `ceil` にはできません。<br/>

#### `time_unit`

**必須**: YES<br/>
**説明**: パーティショングラニュラリティであり、`hour`、`day`、`month`、`year` のいずれかです。`week` パーティショングラニュラリティはサポートされていません。パーティショングラニュラリティが `hour` の場合、パーティションカラムは DATETIME データ型でなければならず、DATE データ型ではいけません。<br/>

#### `partition_column` 

**必須**: YES<br/>
**説明**: パーティションカラムの名前。<br/><ul><li>パーティションカラムは DATE または DATETIME データ型のみであることができます。パーティションカラムは `NULL` 値を許可します。</li><li>`date_trunc` 関数が使用される場合、パーティションカラムは DATE または DATETIME データ型であることができます。`time_slice` 関数が使用される場合、パーティションカラムは DATETIME データ型でなければなりません。</li><li>パーティションカラムが DATE データ型の場合、サポートされる範囲は [0000-01-01 ~ 9999-12-31] です。パーティションカラムが DATETIME データ型の場合、サポートされる範囲は [0000-01-01 01:01:01 ~ 9999-12-31 23:59:59] です。</li><li>現在、指定できるパーティションカラムは1つだけで、複数のパーティションカラムはサポートされていません。</li></ul> <br/>

#### `partition_live_number` 

**必須**: NO<br/>
**説明**: 保持する最新のパーティションの数。パーティションは時系列順に並べられ、**現在の日付を基準**として、現在の日付から `partition_live_number` を引いた日付より古いパーティションは削除されます。StarRocksはパーティションの数を管理するタスクをスケジュールし、スケジューリング間隔はFE動的パラメータ `dynamic_partition_check_interval_seconds` で設定でき、デフォルトは600秒（10分）です。たとえば、現在の日付が2023年4月4日で、`partition_live_number` が `2` に設定され、パーティションが `p20230401`、`p20230402`、`p20230403`、`p20230404` を含む場合、`p20230403` と `p20230404` のパーティションが保持され、他のパーティションは削除されます。将来の日付のデータ（たとえば、4月5日と4月6日）がロードされた場合、パーティションは `p20230401`、`p20230402`、`p20230403`、`p20230404`、`p20230405`、`p20230406` を含みます。その場合、`p20230403`、`p20230404`、`p20230405`、`p20230406` のパーティションが保持され、他のパーティションは削除されます。<br/>

### 使用上の注意

- データロード中、StarRocksはロードされたデータに基づいていくつかのパーティションを自動的に作成しますが、ロードジョブが何らかの理由で失敗した場合、StarRocksによって自動的に作成されたパーティションは自動的に削除されません。
- StarRocksは自動的に作成されるパーティションの最大数を4096に設定しており、FEパラメータ `max_automatic_partition_number` で設定できます。このパラメータは、誤って多くのパーティションを作成するのを防ぐことができます。
- パーティションの命名ルールは、動的パーティション化の命名ルールと一致しています。

### **例**

例1: 日ごとにデータを頻繁にクエリする場合、パーティション式 `date_trunc()` を使用し、パーティションカラムを `event_day` として、パーティショングラニュラリティを `day` としてテーブルを作成できます。ロード中にデータは日付に基づいて自動的にパーティション化されます。同じ日のデータは1つのパーティションに格納され、パーティションプルーニングを使用してクエリ効率を大幅に向上させることができます。

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

たとえば、次の2つのデータ行がロードされると、StarRocksは自動的に2つのパーティション `p20230226` と `p20230227` を作成し、それぞれの範囲は [2023-02-26 00:00:00, 2023-02-27 00:00:00) と [2023-02-27 00:00:00, 2023-02-28 00:00:00) です。後続のロードされたデータがこれらの範囲内にある場合、それらは自動的に対応するパーティションにルーティングされます。

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

例2: パーティションのライフサイクル管理を実装し、最近のパーティションのみを保持し、履歴パーティションを削除したい場合、`partition_live_number` プロパティを使用して保持するパーティションの数を指定できます。

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

例3: 週ごとにデータを頻繁にクエリする場合、パーティション式 `time_slice()` を使用し、パーティションカラムを `event_day` として、パーティショングラニュラリティを7日間としてテーブルを作成できます。1週間のデータは1つのパーティションに格納され、パーティションプルーニングを使用してクエリ効率を大幅に向上させることができます。

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

## カラム式に基づくパーティション化 (v3.1以降)

特定のタイプのデータを頻繁にクエリおよび管理する場合、そのタイプを表すカラムをパーティションカラムとして指定するだけで済みます。StarRocksはロードされたデータのパーティションカラム値に基づいて自動的にパーティションを作成します。

ただし、特定のシナリオでは、たとえば、テーブルに `city` カラムが含まれており、国や都市に基づいてデータを頻繁にクエリおよび管理する場合、[リストパーティション化](list_partitioning.md)を使用して、同じ国の複数の都市のデータを1つのパーティションに格納する必要があります。

### 構文

```sql
PARTITION BY expression
...

expression ::=
    ( partition_columns )
    
partition_columns ::=
    <column>, [ <column> [,...] ]
```

### パラメータ

#### `partition_columns`

**必須**: YES<br/>
**説明**: パーティションカラムの名前。<br/> <ul><li>パーティションカラムの値は文字列（BINARYはサポートされていません）、日付または日時、整数、ブール値であることができます。パーティションカラムは `NULL` 値を許可します。</li><li> 各パーティションには、パーティションカラムの同じ値を持つデータのみを含めることができます。パーティションカラムに異なる値を持つデータをパーティションに含めるには、[リストパーティション化](list_partitioning.md)を参照してください。</li></ul> <br/>

### 使用上の注意

- データロード中、StarRocksはロードされたデータに基づいていくつかのパーティションを自動的に作成しますが、ロードジョブが何らかの理由で失敗した場合、StarRocksによって自動的に作成されたパーティションは自動的に削除されません。
- StarRocksは自動的に作成されるパーティションの最大数を4096に設定しており、FEパラメータ `max_automatic_partition_number` で設定できます。このパラメータは、誤って多くのパーティションを作成するのを防ぐことができます。
- パーティションの命名ルール: 複数のパーティションカラムが指定されている場合、異なるパーティションカラムの値はパーティション名でアンダースコア `_` で接続され、形式は `p<パーティションカラム1の値>_<パーティションカラム2の値>_...` です。たとえば、2つのカラム `dt` と `province` がパーティションカラムとして指定され、両方とも文字列型であり、データ行に `2022-04-01` と `beijing` の値がある場合、自動的に作成される対応するパーティションは `p20220401_beijing` と名付けられます。

### 例

例1: 時間範囲と特定の都市に基づいてデータセンターの請求詳細を頻繁にクエリする場合、テーブル作成時に最初のパーティションカラムを `dt` と `city` として指定するパーティション式を使用できます。この方法で、同じ日付と都市に属するデータは同じパーティションにルーティングされ、パーティションプルーニングを使用してクエリ効率を大幅に向上させることができます。

```SQL
CREATE TABLE t_recharge_detail1 (
    id bigint,
    user_id bigint,
    recharge_money decimal(32,2), 
    city varchar(20) not null,
    dt varchar(20) not null
)
DUPLICATE KEY(id)
PARTITION BY (dt,city)
DISTRIBUTED BY HASH(`id`);
```

テーブルに単一のデータ行を挿入します。

```SQL
INSERT INTO t_recharge_detail1 
    VALUES (1, 1, 1, 'Houston', '2022-04-01');
```

パーティションを表示します。結果は、StarRocksがロードされたデータに基づいて自動的に `p20220401_Houston1` というパーティションを作成することを示しています。後続のロード中に、パーティションカラム `dt` と `city` に `2022-04-01` と `Houston` の値を持つデータはこのパーティションに格納されます。

:::tip
各パーティションには、パーティションカラムに指定された1つの値のみを含めることができます。パーティションカラムに複数の値を指定するには、[リストパーティション](list_partitioning.md)を参照してください。
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

## パーティションの管理

### パーティションへのデータロード

データロード中、StarRocksはロードされたデータとパーティション式で定義されたパーティションルールに基づいて自動的にパーティションを作成します。

テーブル作成時に式に基づくパーティション化を使用し、特定のパーティションのデータを上書きするために [INSERT OVERWRITE](../../loading/InsertInto.md#overwrite-data-via-insert-overwrite-select) を使用する必要がある場合、パーティションが作成されているかどうかにかかわらず、現在は `PARTITION()` で明示的にパーティション範囲を提供する必要があります。これは、[レンジパーティション化](../Data_distribution.mdmd#range-partitioning) や [リストパーティション化](../list_partitioning.mdmd) とは異なり、`PARTITION (<partition_name>)` でパーティション名のみを提供することができます。

テーブル作成時に時間関数式を使用し、特定のパーティションのデータを上書きしたい場合、そのパーティションの開始日または日時（テーブル作成時に設定されたパーティショングラニュラリティ）を提供する必要があります。パーティションが存在しない場合、データロード中に自動的に作成されることがあります。

```SQL
INSERT OVERWRITE site_access1 PARTITION(event_day='2022-06-08 20:12:04')
    SELECT * FROM site_access2 PARTITION(p20220608);
```

テーブル作成時にカラム式を使用し、特定のパーティションのデータを上書きしたい場合、そのパーティションが含むパーティションカラムの値を提供する必要があります。パーティションが存在しない場合、データロード中に自動的に作成されることがあります。

```SQL
INSERT OVERWRITE t_recharge_detail1 PARTITION(dt='2022-04-02',city='texas')
    SELECT * FROM t_recharge_detail2 PARTITION(p20220402_texas);
```

### パーティションの表示

自動的に作成されたパーティションに関する特定の情報を表示したい場合、`SHOW PARTITIONS FROM <table_name>` ステートメントを使用する必要があります。`SHOW CREATE TABLE <table_name>` ステートメントは、テーブル作成時に設定された式に基づくパーティション化の構文のみを返します。

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

- v3.1.0以降、StarRocksの [共有データモード](../../deployment/shared_data/s3.md) は [時間関数式](#partitioning-based-on-a-time-function-expression) をサポートしています。そして、v3.1.1以降、StarRocksの [共有データモード](../../deployment/shared_data/s3.md) は [カラム式](#partitioning-based-on-the-column-expression-since-v31) もサポートしています。
- 現在、CTASを使用して式に基づくパーティション化を設定したテーブルを作成することはサポートされていません。
- 現在、Spark Loadを使用して式に基づくパーティション化を使用するテーブルにデータをロードすることはサポートされていません。
- `ALTER TABLE <table_name> DROP PARTITION <partition_name>` ステートメントを使用してカラム式で作成されたパーティションを削除する場合、パーティション内のデータは直接削除され、復元できません。
- 現在、式に基づくパーティション化で作成されたパーティションを [バックアップおよび復元](../../administration/management/Backup_and_restore.md) することはできません。
```