---
displayed_sidebar: docs
sidebar_position: 30
---

# レンジパーティション化 (レガシー)

レンジパーティション化は、時系列データや連続した数値データのような単純で連続したデータを保存するのに適しています。

レンジパーティション化に基づき、[動的パーティショニング戦略](#動的パーティション化)を使用してパーティションを作成できます。これにより、パーティションの有効期限（TTL）を管理することが可能です。

:::note

v3.4以降、[式に基づくパーティション化](./expression_partitioning.md)がさらに最適化され、すべてのパーティション化戦略を統一し、より複雑なソリューションをサポートしています。ほとんどの場合に推奨され、将来的にはレンジパーティション化戦略に取って代わる予定です。

:::

## 概要

**レンジパーティション化は、連続した日付/数値範囲に基づいて頻繁にクエリされるデータに適しています。また、過去のデータを月ごとにパーティション化し、最近のデータを日ごとにパーティション化する必要がある特別なケースにも適用できます。**

データのパーティション列を明示的に定義し、パーティションとパーティション列値の範囲との間のマッピング関係を確立する必要があります。データロード中に、StarRocksはデータのパーティション列値が属する範囲に基づいてデータを対応するパーティションに割り当てます。

パーティション列のデータ型については、v3.3.0以前は、レンジパーティション化は日付型と整数型のパーティション列のみをサポートしていました。v3.3.0以降、3つの特定の時間関数をパーティション列として使用できます。パーティションとパーティション列値の範囲とのマッピング関係を明示的に定義する際には、まず特定の時間関数を使用してタイムスタンプまたは文字列のパーティション列値を日付値に変換し、次に変換された日付値に基づいてパーティションを分割する必要があります。

:::info

- パーティション列の値がタイムスタンプの場合、パーティションを分割する際には from_unixtime または from_unixtime_ms 関数を使用してタイムスタンプを日付値に変換する必要があります。from_unixtime 関数を使用する場合、パーティション列は INT および BIGINT 型のみをサポートします。from_unixtime_ms 関数を使用する場合、パーティション列は BIGINT 型のみをサポートします。
- パーティション列の値が文字列 (STRING, VARCHAR, または CHAR 型) の場合、パーティションを分割する際には str2date 関数を使用して文字列を日付値に変換する必要があります。

:::

## 使用法

### 構文

```sql
PARTITION BY RANGE ( partition_columns | function_expression ) ( single_range_partition | multi_range_partitions )

partition_columns ::= 
    <column> [, ...]

function_expression ::= 
      from_unixtime(column) 
    | from_unixtime_ms(column) 
    | str2date(column) 

single_range_partition ::=
    PARTITION <partition_name> VALUES partition_key_desc

partition_key_desc ::=
          LESS THAN { MAXVALUE | value_list }
        | value_range

-- value_range は左閉右開区間です。例: `[202201, 202212)`。
-- 半開区間ではブラケット `[` を明示的に指定する必要があります。
value_range ::= 
    [value_list, value_list)

value_list ::=
    ( <value> [, ...] )


multi_range_partitions ::=
        { PARTITIONS START ("<start_date_value>") END ("<end_date_value>") EVERY ( INTERVAL <int_value> time_unit )
        | PARTITIONS START ("<start_integer_value>") END ("<end_integer_value>") EVERY ( <int_value> ) } -- START および END で指定されたパーティション列値が整数であっても、パーティション列値はダブルクォートで囲む必要があります。ただし、EVERY 句の間隔値はダブルクォートで囲む必要はありません。

time_unit ::=
    HOUR | DAY | WEEK | MONTH | YEAR 
```

### パラメータ

| **パラメータ**        | **説明**                                              |
| --------------------- | ------------------------------------------------------------ |
| `partition_columns`   | パーティション列の名前。パーティション列の値は文字列 (BINARY はサポートされていません)、日付または日時、または整数であることができます。   |
| `function_expression` | パーティション列を特定のデータ型に変換する関数式。サポートされている関数: from_unixtime, from_unixtime_ms, および str2date。<br />**注意**<br />レンジパーティション化は1つの関数式のみをサポートします。 |
| `partition_name`      | パーティション名。ビジネスシナリオに基づいて適切なパーティション名を設定し、異なるパーティション内のデータを区別することをお勧めします。 |

### 例

1. 日付型パーティション列に基づいて値の範囲を手動で定義してパーティションを作成します。

   ```SQL
   PARTITION BY RANGE(date_col)(
       PARTITION p1 VALUES LESS THAN ("2020-01-31"),
       PARTITION p2 VALUES LESS THAN ("2020-02-29"),
       PARTITION p3 VALUES LESS THAN ("2020-03-31")
   )
   ```

2. 整数型パーティション列に基づいて値の範囲を手動で定義してパーティションを作成します。

   ```SQL
   PARTITION BY RANGE (int_col) (
       PARTITION p1 VALUES LESS THAN ("20200131"),
       PARTITION p2 VALUES LESS THAN ("20200229"),
       PARTITION p3 VALUES LESS THAN ("20200331")
   )
   ```

3. 同じ日付間隔で複数のパーティションを作成します。

   ```SQL
   PARTITION BY RANGE (date_col) (
       START ("2021-01-01") END ("2021-01-04") EVERY (INTERVAL 1 DAY)
   )
   ```

4. 異なる日付間隔で複数のパーティションを作成します。

   ```SQL
   PARTITION BY RANGE (date_col) (
       START ("2019-01-01") END ("2021-01-01") EVERY (INTERVAL 1 YEAR),
       START ("2021-01-01") END ("2021-05-01") EVERY (INTERVAL 1 MONTH),
       START ("2021-05-01") END ("2021-05-04") EVERY (INTERVAL 1 DAY)
   )
   ```

5. 同じ整数間隔で複数のパーティションを作成します。

   ```SQL
   PARTITION BY RANGE (int_col) (
       START ("1") END ("10") EVERY (1)
   )
   ```

6. 異なる整数間隔で複数のパーティションを作成します。

   ```SQL
   PARTITION BY RANGE (int_col) (
       START ("1") END ("10") EVERY (1),
       START ("10") END ("100") EVERY (10)
   )
   ```

7. from_unixtime を使用してタイムスタンプ (文字列) 型のパーティション列を日付型に変換します。

   ```SQL
   PARTITION BY RANGE(from_unixtime(timestamp_col)) (
       PARTITION p1 VALUES LESS THAN ("2021-01-01"),
       PARTITION p2 VALUES LESS THAN ("2021-01-02"),
       PARTITION p3 VALUES LESS THAN ("2021-01-03")
   )
   ```

8. str2date を使用して文字列型のパーティション列を日付型に変換します。

   ```SQL
   PARTITION BY RANGE(str2date(string_col, '%Y-%m-%d'))(
       PARTITION p1 VALUES LESS THAN ("2021-01-01"),
       PARTITION p2 VALUES LESS THAN ("2021-01-02"),
       PARTITION p3 VALUES LESS THAN ("2021-01-03")
   )
   ```

## 動的パーティション化

StarRocks は動的パーティション化をサポートしており、テーブル内の新しい入力データをパーティション化したり、期限切れのパーティションを削除したりするなど、パーティションの生存時間 (TTL) を自動的に管理できます。この機能により、メンテナンスコストが大幅に削減されます。

### 動的パーティション化を有効にする

テーブル `site_access` を例にとります。動的パーティション化を有効にするには、PROPERTIES パラメータを設定する必要があります。設定項目の詳細については、[CREATE TABLE](../../sql-reference/sql-statements/table_bucket_part_index/CREATE_TABLE.md) を参照してください。

```SQL
CREATE TABLE site_access(
    event_day DATE,
    site_id INT DEFAULT '10',
    city_code VARCHAR(100),
    user_name VARCHAR(32) DEFAULT '',
    pv BIGINT DEFAULT '0'
)
DUPLICATE KEY(event_day, site_id, city_code, user_name)
PARTITION BY RANGE(event_day)(
    PARTITION p20200321 VALUES LESS THAN ("2020-03-22"),
    PARTITION p20200322 VALUES LESS THAN ("2020-03-23"),
    PARTITION p20200323 VALUES LESS THAN ("2020-03-24"),
    PARTITION p20200324 VALUES LESS THAN ("2020-03-25")
)
DISTRIBUTED BY HASH(event_day, site_id)
PROPERTIES(
    "dynamic_partition.enable" = "true",
    "dynamic_partition.time_unit" = "DAY",
    "dynamic_partition.start" = "-3",
    "dynamic_partition.end" = "3",
    "dynamic_partition.prefix" = "p",
    "dynamic_partition.buckets" = "32",
    "dynamic_partition.history_partition_num" = "0"
);
```

**`PROPERTIES`**:

| パラメータ                               | 必須 | 説明                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   |
|-----------------------------------------| -------- |-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| dynamic_partition.enable                | いいえ       | 動的パーティション化を有効にします。有効な値は `TRUE` と `FALSE` です。デフォルト値は `TRUE` です。                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                               |
| dynamic_partition.time_unit             | はい      | 動的に作成されるパーティションの時間粒度です。必須パラメータです。有効な値は `HOUR`、`DAY`、`WEEK`、`MONTH`、および `YEAR` です。時間粒度は、動的に作成されるパーティションのサフィックス形式を決定します。<ul><li>値が `HOUR` の場合、パーティション列は DATETIME 型のみをサポートし、DATE 型はサポートされません。動的に作成されるパーティションのサフィックス形式は yyyyMMddHH で、例として `2020032101` があります。</li><li>値が `DAY` の場合、動的に作成されるパーティションのサフィックス形式は yyyyMMdd です。例として `20200321` があります。</li><li>値が `WEEK` の場合、動的に作成されるパーティションのサフィックス形式は yyyy_ww で、例として `2020_13` (2020年の第13週) があります。</li><li>値が `MONTH` の場合、動的に作成されるパーティションのサフィックス形式は yyyyMM で、例として `202003` があります。</li><li>値が `YEAR` の場合、動的に作成されるパーティションのサフィックス形式は yyyy で、例として `2020` があります。</li></ul> |
| dynamic_partition.time_zone             | いいえ       | 動的パーティションのタイムゾーンで、デフォルトではシステムのタイムゾーンと同じです。                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   |
| dynamic_partition.start                 | いいえ       | 動的パーティション化の開始オフセットです。このパラメータの値は負の整数でなければなりません。このオフセットより前のパーティションは、`dynamic_partition.time_unit` パラメータの値によって決定される現在の日、週、または月に基づいて削除されます。デフォルト値は `Integer.MIN_VALUE`、すなわち -2147483648 で、これは履歴パーティションが削除されないことを意味します。                                                                                                                                                                                                                                                                                                                                                                  |
| dynamic_partition.end                   | はい      | 動的パーティション化の終了オフセットです。このパラメータの値は正の整数でなければなりません。現在の日、週、または月から終了オフセットまでのパーティションが事前に作成されます。                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                             |
| dynamic_partition.prefix                | いいえ       | 動的パーティションの名前に追加されるプレフィックスです。デフォルト値は `p` です。                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                |
| dynamic_partition.buckets               | いいえ       | 動的パーティションごとのバケット数です。デフォルト値は、予約語 BUCKETS によって決定されるバケット数と同じか、StarRocks によって自動的に設定されます。                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                |
| dynamic_partition.history_partition_num | いいえ       | 動的パーティション化メカニズムによって作成される履歴パーティションの数で、デフォルト値は `0` です。値が0より大きい場合、履歴パーティションが事前に作成されます。v2.5.2以降、StarRocks はこのパラメータをサポートしています。                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                 |
| dynamic_partition.start_day_of_week     | いいえ       | `dynamic_partition.time_unit` が `WEEK` の場合、このパラメータは各週の最初の日を指定するために使用されます。有効な値: `1` から `7`。`1` は月曜日を意味し、`7` は日曜日を意味します。デフォルト値は `1` で、これは毎週月曜日に始まることを意味します。                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                |
| dynamic_partition.start_day_of_month    | いいえ       | `dynamic_partition.time_unit` が `MONTH` の場合、このパラメータは各月の最初の日を指定するために使用されます。有効な値: `1` から `28`。`1` は毎月の1日を意味し、`28` は毎月の28日を意味します。デフォルト値は `1` で、これは毎月1日に始まることを意味します。最初の日は29日、30日、または31日にすることはできません。                                                                                                                                                                                                                                                                                                                                                                                                                                |
| dynamic_partition.replication_num       | いいえ       | 動的に作成されるパーティション内のタブレットのレプリカ数です。デフォルト値は、テーブル作成時に設定されたレプリカ数と同じです。  |

:::note

パーティション列が INT 型の場合、その形式はパーティション時間粒度に関係なく `yyyyMMdd` でなければなりません。

:::

**FE 設定:**

`dynamic_partition_check_interval_seconds`: 動的パーティション化をスケジュールするための間隔です。デフォルト値は600秒で、これは10分ごとにパーティションの状況を確認し、`PROPERTIES` で指定された動的パーティション化条件を満たしているかどうかを確認します。満たしていない場合、パーティションは自動的に作成および削除されます。

### 動的パーティションの表示

テーブルに対して動的パーティションを有効にした後、入力データは継続的かつ自動的にパーティション化されます。次のステートメントを使用して現在のパーティションを表示できます。たとえば、現在の日付が2020-03-25の場合、2020-03-25から2020-03-28の時間範囲内のパーティションのみが表示されます。

```SQL
SHOW PARTITIONS FROM site_access;

[types: [DATE]; keys: [2020-03-25]; ‥types: [DATE]; keys: [2020-03-26]; )
[types: [DATE]; keys: [2020-03-26]; ‥types: [DATE]; keys: [2020-03-27]; )
[types: [DATE]; keys: [2020-03-27]; ‥types: [DATE]; keys: [2020-03-28]; )
[types: [DATE]; keys: [2020-03-28]; ‥types: [DATE]; keys: [2020-03-29]; )
```

テーブル作成時に履歴パーティションを作成したい場合は、`dynamic_partition.history_partition_num` を指定して作成する履歴パーティションの数を定義する必要があります。たとえば、テーブル作成時に `dynamic_partition.history_partition_num` を `3` に設定し、現在の日付が2020-03-25の場合、2020-03-22から2020-03-28の時間範囲内のパーティションのみが表示されます。

```SQL
SHOW PARTITIONS FROM site_access;

[types: [DATE]; keys: [2020-03-22]; ‥types: [DATE]; keys: [2020-03-23]; )
[types: [DATE]; keys: [2020-03-23]; ‥types: [DATE]; keys: [2020-03-24]; )
[types: [DATE]; keys: [2020-03-24]; ‥types: [DATE]; keys: [2020-03-25]; )
[types: [DATE]; keys: [2020-03-25]; ‥types: [DATE]; keys: [2020-03-26]; )
[types: [DATE]; keys: [2020-03-26]; ‥types: [DATE]; keys: [2020-03-27]; )
[types: [DATE]; keys: [2020-03-27]; ‥types: [DATE]; keys: [2020-03-28]; )
[types: [DATE]; keys: [2020-03-28]; ‥types: [DATE]; keys: [2020-03-29]; )
```

### 動的パーティション化のプロパティを変更する

動的パーティション化のプロパティを変更するには、[ALTER TABLE](../../sql-reference/sql-statements/table_bucket_part_index/ALTER_TABLE.md) ステートメントを使用できます。たとえば、動的パーティション化を無効にするには、次のステートメントを使用します。

```SQL
ALTER TABLE site_access 
SET("dynamic_partition.enable"="false");
```

:::note
- テーブルの動的パーティション化のプロパティを確認するには、[SHOW CREATE TABLE](../../sql-reference/sql-statements/table_bucket_part_index/SHOW_CREATE_TABLE.md) ステートメントを実行します。
- ALTER TABLE ステートメントを使用して、テーブルの他のプロパティを変更することもできます。
:::
