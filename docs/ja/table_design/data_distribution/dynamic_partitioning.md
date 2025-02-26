---
displayed_sidebar: docs
sidebar_position: 30
---

# 動的パーティション化 (レガシー)

StarRocks は動的パーティション化をサポートしており、テーブル内の新しい入力データのパーティション化や期限切れのパーティションの削除など、パーティションの有効期限 (TTL) を自動的に管理できます。この機能により、メンテナンスコストが大幅に削減されます。

:::note

v3.4 以降、[式に基づくパーティション化](./expression_partitioning.md) がさらに最適化され、すべてのパーティション戦略を統一し、より複雑なソリューションをサポートしています。ほとんどの場合に推奨されており、将来のリリースでは動的パーティション化戦略に取って代わる予定です。

:::

## 動的パーティション化を有効にする

テーブル `site_access` を例にとります。動的パーティション化を有効にするには、PROPERTIES パラメータを設定する必要があります。設定項目については、[CREATE TABLE](../../sql-reference/sql-statements/table_bucket_part_index/CREATE_TABLE.md) を参照してください。

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
| dynamic_partition.enable                | No       | 動的パーティション化を有効にします。有効な値は `TRUE` と `FALSE` です。デフォルト値は `TRUE` です。                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                               |
| dynamic_partition.time_unit             | Yes      | 動的に作成されるパーティションの時間粒度です。必須のパラメータです。有効な値は `HOUR`、`DAY`、`WEEK`、`MONTH`、`YEAR` です。時間粒度は動的に作成されるパーティションのサフィックス形式を決定します。<ul><li>値が `HOUR` の場合、パーティション列は DATETIME 型のみ可能で、DATE 型は使用できません。動的に作成されるパーティションのサフィックス形式は yyyyMMddHH で、例として `2020032101` があります。</li><li>値が `DAY` の場合、動的に作成されるパーティションのサフィックス形式は yyyyMMdd です。例として `20200321` があります。</li><li>値が `WEEK` の場合、動的に作成されるパーティションのサフィックス形式は yyyy_ww で、2020 年の第 13 週の場合は `2020_13` です。</li><li>値が `MONTH` の場合、動的に作成されるパーティションのサフィックス形式は yyyyMM で、例として `202003` があります。</li><li>値が `YEAR` の場合、動的に作成されるパーティションのサフィックス形式は yyyy で、例として `2020` があります。</li></ul> |
| dynamic_partition.time_zone             | No       | 動的パーティションのタイムゾーンで、デフォルトではシステムのタイムゾーンと同じです。                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   |
| dynamic_partition.start                 | No       | 動的パーティション化の開始オフセットです。このパラメータの値は負の整数でなければなりません。このオフセットより前のパーティションは、`dynamic_partition.time_unit` パラメータの値によって決定される現在の日、週、または月に基づいて削除されます。デフォルト値は `Integer.MIN_VALUE`、すなわち -2147483648 で、履歴パーティションは削除されないことを意味します。                                                                                                                                                                                                                                                                                                                                                                  |
| dynamic_partition.end                   | Yes      | 動的パーティション化の終了オフセットです。このパラメータの値は正の整数でなければなりません。現在の日、週、または月から終了オフセットまでのパーティションが事前に作成されます。                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                             |
| dynamic_partition.prefix                | No       | 動的パーティションの名前に追加されるプレフィックスです。デフォルト値は `p` です。                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                |
| dynamic_partition.buckets               | No       | 動的パーティションごとのバケット数です。デフォルト値は、予約語 BUCKETS によって決定されるバケット数と同じか、StarRocks によって自動的に設定されます。                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                |
| dynamic_partition.history_partition_num | No       | 動的パーティション化メカニズムによって作成される履歴パーティションの数で、デフォルト値は `0` です。値が 0 より大きい場合、履歴パーティションが事前に作成されます。v2.5.2 以降、StarRocks はこのパラメータをサポートしています。                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                 |
| dynamic_partition.start_day_of_week     | No       | `dynamic_partition.time_unit` が `WEEK` の場合、このパラメータは各週の最初の日を指定するために使用されます。有効な値は `1` から `7` です。`1` は月曜日を意味し、`7` は日曜日を意味します。デフォルト値は `1` で、毎週月曜日から始まることを意味します。                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                |
| dynamic_partition.start_day_of_month    | No       | `dynamic_partition.time_unit` が `MONTH` の場合、このパラメータは各月の最初の日を指定するために使用されます。有効な値は `1` から `28` です。`1` は毎月の 1 日を意味し、`28` は毎月の 28 日を意味します。デフォルト値は `1` で、毎月 1 日から始まることを意味します。最初の日は 29 日、30 日、または 31 日にすることはできません。                                                                                                                                                                                                                                                                                                                                                                                                                                |
| dynamic_partition.replication_num       | No       | 動的に作成されるパーティション内のタブレットのレプリカ数です。デフォルト値はテーブル作成時に設定されたレプリカ数と同じです。  |

**FE の設定:**

`dynamic_partition_check_interval_seconds`: 動的パーティション化をスケジュールする間隔です。デフォルト値は 600 秒で、10 分ごとにパーティションの状況を確認し、`PROPERTIES` で指定された動的パーティション化条件を満たしているかどうかを確認します。満たしていない場合、パーティションは自動的に作成および削除されます。

## パーティションを表示する

テーブルに動的パーティションを有効にすると、入力データは継続的かつ自動的にパーティション化されます。現在のパーティションを表示するには、次のステートメントを使用します。たとえば、現在の日付が 2020-03-25 の場合、2020-03-25 から 2020-03-28 の時間範囲内のパーティションのみが表示されます。

```SQL
SHOW PARTITIONS FROM site_access;

[types: [DATE]; keys: [2020-03-25]; ‥types: [DATE]; keys: [2020-03-26]; )
[types: [DATE]; keys: [2020-03-26]; ‥types: [DATE]; keys: [2020-03-27]; )
[types: [DATE]; keys: [2020-03-27]; ‥types: [DATE]; keys: [2020-03-28]; )
[types: [DATE]; keys: [2020-03-28]; ‥types: [DATE]; keys: [2020-03-29]; )
```

テーブル作成時に履歴パーティションを作成したい場合は、`dynamic_partition.history_partition_num` を指定して作成する履歴パーティションの数を定義する必要があります。たとえば、テーブル作成時に `dynamic_partition.history_partition_num` を `3` に設定し、現在の日付が 2020-03-25 の場合、2020-03-22 から 2020-03-28 の時間範囲内のパーティションのみが表示されます。

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

## 動的パーティション化のプロパティを変更する

動的パーティション化のプロパティを変更するには、[ALTER TABLE](../../sql-reference/sql-statements/table_bucket_part_index/ALTER_TABLE.md) ステートメントを使用します。たとえば、動的パーティション化を無効にするには、次のステートメントを使用します。

```SQL
ALTER TABLE site_access 
SET("dynamic_partition.enable"="false");
```

> 注意:
>
> - テーブルの動的パーティション化のプロパティを確認するには、[SHOW CREATE TABLE](../../sql-reference/sql-statements/table_bucket_part_index/SHOW_CREATE_TABLE.md) ステートメントを実行します。
> - ALTER TABLE ステートメントを使用して、テーブルの他のプロパティを変更することもできます。