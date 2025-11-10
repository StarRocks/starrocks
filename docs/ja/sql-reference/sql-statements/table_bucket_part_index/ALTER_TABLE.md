---
displayed_sidebar: docs
---

# ALTER TABLE

import Beta from '../../../_assets/commonMarkdown/_beta.mdx'

## 説明

ALTER TABLE は既存のテーブルを修正します。以下を含みます:

- [テーブル、パーティション、ロールアップ、または列の名前変更](#rename)
- [テーブルコメントの修正](#alter-table-comment-from-v31)
- [パーティションの修正（パーティションの追加/削除とパーティション属性の修正）](#modify-partition)
- [バケッティング方法とバケット数の修正](#modify-the-bucketing-method-and-number-of-buckets-from-v32)
- [列の変更（列の追加/削除、列順の変更、列コメントの変更）](#modify-columns-adddelete-columns-change-the-order-of-columns)
- [ロールアップの作成/削除](#modify-rollup)
- [インデックスの作成/削除](#modify-indexes)
- [テーブルプロパティの修正](#modify-table-properties)
- [アトミックスワップ](#swap)
- [手動データバージョンコンパクション](#manual-compaction-from-31)
- [主キー永続性インデックスの削除](#drop-primary-key-persistent-index-from-339)

:::tip
この操作には、対象テーブルに対する ALTER 権限が必要です。
:::

## 構文

```SQL
ALTER TABLE [<db_name>.]<tbl_name>
alter_clause1[, alter_clause2, ...]
```

`alter_clause` には次の操作が含まれます: 名前変更、コメント、パーティション、バケット、列、ロールアップ、インデックス、テーブルプロパティ、スワップ、コンパクション。

- 名前変更: テーブル、ロールアップ、パーティション、または列の名前を変更します（**v3.3.2 以降でサポート**）。
- コメント: テーブルコメントを修正します（**v3.1 以降でサポート**）。
- パーティション: パーティションプロパティを修正、パーティションを削除、またはパーティションを追加します。
- バケット: バケッティング方法とバケット数を修正します。
- 列: 列の追加、削除、並び替え、列のタイプの変更、コメントの変更
- ロールアップ: ロールアップを作成または削除します。
- インデックス: インデックスを修正します。
- スワップ: 2つのテーブルをアトミックに交換します。
- コンパクション: ロードされたデータのバージョンをマージするために手動でコンパクションを実行します（**v3.1 以降でサポート**）。
- 永続性インデックスの削除: 共有データクラスタの主キーテーブルの永続性インデックスを削除します（**v3.3.9 以降でサポート**）。

## 制限と使用上の注意

- パーティション、列、ロールアップに対する操作は、1つの ALTER TABLE ステートメントで実行できません。
- 1つのテーブルには、同時に1つのスキーマ変更操作しか実行できません。1つのテーブルに対して同時に2つのスキーマ変更コマンドを実行することはできません。
- バケット、列、ロールアップに対する操作は非同期操作です。タスクが送信された後、成功メッセージが即座に返されます。進捗を確認するには [SHOW ALTER TABLE](SHOW_ALTER.md) コマンドを実行し、操作をキャンセルするには [CANCEL ALTER TABLE](CANCEL_ALTER_TABLE.md) コマンドを実行できます。
- 名前変更、コメント、パーティション、インデックス、スワップに対する操作は同期操作であり、コマンドの返り値は実行が完了したことを示します。

### 名前変更

名前変更は、テーブル名、ロールアップ、およびパーティション名の修正をサポートします。

#### テーブルの名前を変更する

```sql
ALTER TABLE <tbl_name> RENAME <new_tbl_name>
```

#### ロールアップの名前を変更する

```sql
ALTER TABLE [<db_name>.]<tbl_name>
RENAME ROLLUP <old_rollup_name> <new_rollup_name>
```

#### パーティションの名前を変更する

```sql
ALTER TABLE [<db_name>.]<tbl_name>
RENAME PARTITION <old_partition_name> <new_partition_name>
```

#### 列の名前を変更する

v3.3.2 以降、StarRocks は列の名前変更をサポートしています。

```sql
ALTER TABLE [<db_name>.]<tbl_name>
RENAME COLUMN <old_col_name> [ TO ] <new_col_name>
```

:::note

- 列 A を B に名前変更した後、新しい列 A を追加することはサポートされていません。
- 名前が変更された列に基づいて構築されたマテリアライズドビューは効果を持ちません。新しい名前の列に基づいて再構築する必要があります。

:::

### テーブルコメントの修正 (v3.1 以降)

構文:

```sql
ALTER TABLE [<db_name>.]<tbl_name> COMMENT = "<new table comment>";
```

### パーティションの変更

#### パーティションの追加

レンジパーティションまたはリストパーティションを追加する際は、必ずそれぞれの構文を厳格に遵守する必要があります。

:::note
- 式パーティションの追加はサポートされていません。
- 注意：`PARTITION BY date_trunc(column)` および `PARTITION BY time_slice(column)` は、表現形式が式パーティションであるにもかかわらず、レンジパーティションとして扱われます。したがって、このようなパーティション方法を使用するテーブルに新しいパーティションを追加する際は、以下のレンジパーティションの構文を使用できます。
:::

構文：

- レンジパーティション

    ```SQL
    ALTER TABLE
        ADD { single_range_partition | multi_range_partitions } [distribution_desc] ["key"="value"];

    single_range_partition ::=
        PARTITION [IF NOT EXISTS] <partition_name> VALUES partition_key_desc

    partition_key_desc ::=
        { LESS THAN { MAXVALUE | value_list }
        | [ value_list , value_list ) } -- [ は左閉区間を表します。

    value_list ::=
        ( <value> [, ...] )

    multi_range_partitions ::=
        { PARTITIONS START ("<start_date_value>") END ("<end_date_value>") EVERY ( INTERVAL <N> <time_unit> )
        | PARTITIONS START ("<start_integer_value>") END ("<end_integer_value>") EVERY ( <granularity> ) } -- START と END で指定されたパーティション列の値が整数であっても、パーティション列の値はダブルクォートで囲む必要があります。ただし、EVERY 節の間隔値はダブルクォートで囲む必要はありません。
    ```

- リストパーティション

    ```SQL
    ALTER TABLE
        ADD PARTITION <partition_name> VALUES IN (value_list) [distribution_desc] ["key"="value"];

    value_list ::=
        value_item [, ...]

    value_item ::=
        { <value> | ( <value> [, ...] ) }
    ```

パラメータ:

- パーティション関連のパラメータ:

  - レンジパーティションの場合、単一のレンジパーティション（`single_range_partition`）またはバッチで複数のレンジパーティション（`multi_range_partitions`）を追加できます。
  - リストパーティションの場合、単一のリストパーティションのみ追加できます。

- `distribution_desc`:

   新しいパーティションのバケット数を個別に設定できますが、バケッティング方法を個別に設定することはできません。

- `"key"="value"`:

   新しいパーティションのプロパティを設定できます。詳細は [CREATE TABLE](CREATE_TABLE.md#properties) を参照してください。

例:

- レンジパーティション

  - テーブル作成時にパーティション列が `event_day` と指定されている場合、例えば `PARTITION BY RANGE(event_day)`、テーブル作成後に新しいパーティションを追加する必要がある場合、次のように実行できます:

    ```sql
    ALTER TABLE site_access ADD PARTITION p4 VALUES LESS THAN ("2020-04-30");
    ```

  - テーブル作成時にパーティション列が `datekey` と指定されている場合、例えば `PARTITION BY RANGE (datekey)`、テーブル作成後にバッチで複数のパーティションを追加する必要がある場合、次のように実行できます:

    ```sql
    ALTER TABLE site_access
        ADD PARTITIONS START ("2021-01-05") END ("2021-01-10") EVERY (INTERVAL 1 DAY);
    ```

- リストパーティション

  - テーブル作成時に単一のパーティション列が指定されている場合、例えば `PARTITION BY LIST (city)`、テーブル作成後に新しいパーティションを追加する必要がある場合、次のように実行できます:

    ```sql
    ALTER TABLE t_recharge_detail2
    ADD PARTITION pCalifornia VALUES IN ("Los Angeles","San Francisco","San Diego");
    ```

  - テーブル作成時に複数のパーティション列が指定されている場合、例えば `PARTITION BY LIST (dt,city)`、テーブル作成後に新しいパーティションを追加する必要がある場合、次のように実行できます:

    ```sql
    ALTER TABLE t_recharge_detail4 
    ADD PARTITION p202204_California VALUES IN
    (
        ("2022-04-01", "Los Angeles"),
        ("2022-04-01", "San Francisco"),
        ("2022-04-02", "Los Angeles"),
        ("2022-04-02", "San Francisco")
    );
    ```

#### パーティションの削除

- 単一のパーティションを削除する:

```sql
ALTER TABLE [<db_name>.]<tbl_name>
DROP PARTITION [ IF EXISTS ] <partition_name> [ FORCE ]
```

- バッチでパーティションを削除する (v3.4.0 以降でサポート):

```sql
ALTER TABLE [<db_name>.]<tbl_name>
DROP PARTITIONS [ IF EXISTS ]  { partition_name_list | multi_range_partitions } [ FORCE ]

partition_name_list ::= ( <partition_name> [, ... ] )

multi_range_partitions ::=
    { START ("<start_date_value>") END ("<end_date_value>") EVERY ( INTERVAL <N> <time_unit> )
    | START ("<start_integer_value>") END ("<end_integer_value>") EVERY ( <granularity> ) } -- パーティション列の値が整数であっても、ダブルクォートで囲む必要があります。ただし、EVERY 節の間隔値はダブルクォートで囲む必要はありません。
```

  `multi_range_partitions` の注意事項:

  - これはレンジパーティション化にのみ適用されます。
  - 関与するパラメータは [ADD PARTITION(S)](#add-partitions) のものと一致しています。
  - 単一のパーティションキーを持つパーティションのみをサポートします。

- 共通パーティション式を使用してパーティションを削除する (v3.5.0 以降でサポート):

```sql
ALTER TABLE [<db_name>.]<tbl_name>
DROP PARTITIONS WHERE <expr>
```

v3.5.0 以降、StarRocks は共通パーティション式を使用したパーティションの削除をサポートしています。削除するパーティションをフィルタリングするために WHERE 句を指定できます。
- 式で宣言されたパーティションが削除されます。式の条件を満たすパーティションはバッチで削除されます。注意して進めてください。
- 式にはパーティション列と定数のみを含めることができます。非パーティション列はサポートされていません。
- 共通パーティション式はリストパーティションとレンジパーティションに異なる方法で適用されます:
  - リストパーティションを持つテーブルの場合、StarRocks は共通パーティション式でフィルタリングされたパーティションの削除をサポートします。
  - レンジパーティションを持つテーブルの場合、StarRocks は FE のパーティションプルーニング機能を使用してパーティションをフィルタリングおよび削除することしかできません。パーティションプルーニングによってサポートされていない述語に対応するパーティションはフィルタリングおよび削除できません。

例:

```sql
-- 過去3か月より前のデータを削除します。列 `dt` はテーブルのパーティション列です。
ALTER TABLE t1 DROP PARTITIONS WHERE dt < CURRENT_DATE() - INTERVAL 3 MONTH;
```

:::note

- パーティション化されたテーブルには少なくとも1つのパーティションを保持してください。
- FORCE が指定されていない場合、指定された期間内（デフォルトで1日）に [RECOVER](../backup_restore/RECOVER.md) コマンドを使用して削除されたパーティションを復元できます。
- FORCE が指定されている場合、パーティションは未完了の操作があるかどうかに関係なく直接削除され、復元できません。したがって、一般的にはこの操作は推奨されません。

:::

#### 一時パーティションの追加

構文:

```sql
ALTER TABLE [<db_name>.]<tbl_name> 
ADD TEMPORARY PARTITION [IF NOT EXISTS] <partition_name>
{ single_range_partition | multi_range_partitions | list_partitions }
[DISTRIBUTED BY HASH (k1[,k2 ...]) [BUCKETS num]]

-- single_range_partition および multi_range_partitions の詳細については、このページ内の「パーティションの追加」セクションを参照してください。

list_partitions::= 
    PARTITION <partition_name> VALUES IN (value_list)

value_list ::=
    value_item [, ...]

value_item ::=
    { <value> | ( <value> [, ...] ) }
```

#### 一時パーティションを使用して現在のパーティションを置き換える

構文:

```sql
ALTER TABLE [<db_name>.]<tbl_name>
REPLACE PARTITION <partition_name>
partition_desc ["key"="value"]
WITH TEMPORARY PARTITION
partition_desc ["key"="value"]
[PROPERTIES ("key"="value", ...)]
```

#### 一時パーティションの削除

構文:

```sql
ALTER TABLE [<db_name>.]<tbl_name>
DROP TEMPORARY PARTITION <partition_name>
```

#### パーティションプロパティの修正

**構文**

```sql
ALTER TABLE [<db_name>.]<tbl_name>
MODIFY PARTITION { <partition_name> | ( <partition1_name> [, <partition2_name> ...] ) | (*) }
SET ("key" = "value", ...);
```

**使用方法**

- パーティションの次のプロパティを修正できます:

  - 記憶媒体
  - storage_cooldown_ttl または storage_cooldown_time
  - replication_num

- テーブルにパーティションが1つしかない場合、パーティション名はテーブル名と同じです。テーブルが複数のパーティションに分割されている場合、`(*)` を使用してすべてのパーティションのプロパティを修正することができ、より便利です。

- 修正後のパーティションプロパティを表示するには、`SHOW PARTITIONS FROM <tbl_name>` を実行します。

### バケッティング方法とバケット数の修正 (v3.2 以降)

構文:

```SQL
ALTER TABLE [<db_name>.]<table_name>
[ partition_names ]
[ distribution_desc ]

partition_names ::= 
    (PARTITION | PARTITIONS) ( <partition_name> [, <partition_name> ...] )

distribution_desc ::=
    DISTRIBUTED BY RANDOM [ BUCKETS <num> ] |
    DISTRIBUTED BY HASH ( <column_name> [, <column_name> ...] ) [ [ DEFAULT ] BUCKETS <num> ]
```

例:

例えば、元のテーブルは重複キーテーブルで、ハッシュバケッティングが使用され、バケット数は StarRocks によって自動的に設定されています。

```SQL
CREATE TABLE IF NOT EXISTS details (
    event_time DATETIME NOT NULL COMMENT "datetime of event",
    event_type INT NOT NULL COMMENT "type of event",
    user_id INT COMMENT "id of user",
    device_code INT COMMENT "device code",
    channel INT COMMENT ""
)
DUPLICATE KEY(event_time, event_type)
PARTITION BY date_trunc('day', event_time)
DISTRIBUTED BY HASH(user_id);

-- 数日分のデータを挿入
INSERT INTO details (event_time, event_type, user_id, device_code, channel) VALUES
-- 11月26日のデータ
('2023-11-26 08:00:00', 1, 101, 12345, 2),
('2023-11-26 09:15:00', 2, 102, 54321, 3),
('2023-11-26 10:30:00', 1, 103, 98765, 1),
-- 11月27日のデータ
('2023-11-27 08:30:00', 1, 104, 11111, 2),
('2023-11-27 09:45:00', 2, 105, 22222, 3),
('2023-11-27 11:00:00', 1, 106, 33333, 1),
-- 11月28日のデータ
('2023-11-28 08:00:00', 1, 107, 44444, 2),
('2023-11-28 09:15:00', 2, 108, 55555, 3),
('2023-11-28 10:30:00', 1, 109, 66666, 1);
```

#### バケッティング方法のみを修正する

> **注意**
>
> - 修正はテーブル内のすべてのパーティションに適用され、特定のパーティションのみに適用することはできません。
> - バケッティング方法のみを修正する場合でも、コマンド内で `BUCKETS <num>` を使用してバケット数を指定する必要があります。`BUCKETS <num>` が指定されていない場合、バケット数は StarRocks によって自動的に決定されます。

- バケッティング方法をハッシュバケッティングからランダムバケッティングに変更し、バケット数は StarRocks によって自動的に設定されます。

  ```SQL
  ALTER TABLE details DISTRIBUTED BY RANDOM;
  ```

- ハッシュバケッティングのキーを `event_time, event_type` から `user_id, event_time` に変更します。バケット数は StarRocks によって自動的に設定されます。

  ```SQL
  ALTER TABLE details DISTRIBUTED BY HASH(user_id, event_time);
  ```

#### バケット数のみを修正する

> **注意**
>
> バケット数のみを修正する場合でも、バケッティング方法をコマンド内で指定する必要があります。例えば、`HASH(user_id)` です。

- すべてのパーティションのバケット数を StarRocks によって自動的に設定されるものから 10 に変更します。

  ```SQL
  ALTER TABLE details DISTRIBUTED BY HASH(user_id) BUCKETS 10;
  ```

- テーブルのデフォルトのバケット数を、**既存パーティションのバケット数を変更せずに**、StarRocksによる自動設定から 10 に変更します（v3.5.8 および v4.0.1 以降でサポート）。

  ```SQL
  ALTER TABLE details DISTRIBUTED BY HASH(user_id) DEFAULT BUCKETS 10;
  ```

  > **注意**
  >
  > `partition_names` と `DEFAULT` を同時に指定することはできません。


- 指定されたパーティションのバケット数を StarRocks によって自動的に設定されるものから 15 に変更します。

  ```SQL
  ALTER TABLE details PARTITIONS (p20231127, p20231128) DISTRIBUTED BY HASH(user_id) BUCKETS 15 ;
  ```

  > **注意**
  >
  > パーティション名は `SHOW PARTITIONS FROM <table_name>;` を実行して確認できます。

#### バケッティング方法とバケット数の両方を修正する

> **注意**
>
> 修正はテーブル内のすべてのパーティションに適用され、特定のパーティションのみに適用することはできません。

- バケッティング方法をハッシュバケッティングからランダムバケッティングに変更し、バケット数を StarRocks によって自動的に設定されるものから 10 に変更します。

   ```SQL
   ALTER TABLE details DISTRIBUTED BY RANDOM BUCKETS 10;
   ```

- ハッシュバケッティングのキーを変更し、バケット数を StarRocks によって自動的に設定されるものから 10 に変更します。ハッシュバケッティングに使用されるキーは、元の `event_time, event_type` から `user_id, event_time` に変更されます。バケット数は StarRocks によって自動的に設定されるものから 10 に変更されます。

  ```SQL
  ALTER TABLE details DISTRIBUTED BY HASH(user_id, event_time) BUCKETS 10;
  ```

### 列の変更（列の追加/削除、列順の変更、列コメントの変更）

#### 指定されたインデックスの指定された位置に列を追加する

構文:

```sql
ALTER TABLE [<db_name>.]<tbl_name>
ADD COLUMN column_name column_type [KEY | agg_type] [DEFAULT "default_value"]
[AFTER column_name|FIRST]
[TO rollup_index_name]
[PROPERTIES ("key"="value", ...)]
```

注意:

1. 集計テーブルに値列を追加する場合、`agg_type` を指定する必要があります。
2. 重複キーテーブルのような非集計テーブルにキー列を追加する場合、`KEY` キーワードを指定する必要があります。
3. 基本インデックスに既に存在する列をロールアップに追加することはできません。（必要に応じてロールアップを再作成できます。）

#### 指定されたインデックスに複数の列を追加する

構文:

- 複数の列を追加する

  ```sql
  ALTER TABLE [<db_name>.]<tbl_name>
  ADD COLUMN (column_name1 column_type [KEY | agg_type] DEFAULT "default_value", ...)
  [TO rollup_index_name]
  [PROPERTIES ("key"="value", ...)]
  ```

- 複数の列を追加し、追加された列の位置を `AFTER` を使用して指定する

  ```sql
  ALTER TABLE [<db_name>.]<tbl_name>
  ADD COLUMN column_name1 column_type [KEY | agg_type] DEFAULT "default_value" AFTER column_name,
  ADD COLUMN column_name2 column_type [KEY | agg_type] DEFAULT "default_value" AFTER column_name
  [, ...]
  [TO rollup_index_name]
  [PROPERTIES ("key"="value", ...)]
  ```

注意:

1. 集計テーブルに値列を追加する場合、`agg_type` を指定する必要があります。

2. 非集計テーブルにキー列を追加する場合、`KEY` キーワードを指定する必要があります。

3. 基本インデックスに既に存在する列をロールアップに追加することはできません。（必要に応じて別のロールアップを作成できます。）

#### 生成列を追加する (v3.1 以降)

構文:

```sql
ALTER TABLE [<db_name>.]<tbl_name>
ADD COLUMN col_name data_type [NULL] AS generation_expr [COMMENT 'string']
```

生成列を追加し、その式を指定できます。[生成列](../generated_columns.md) は、式の結果を事前に計算して保存するために使用でき、同じ複雑な式を持つクエリを大幅に高速化します。v3.1 以降、StarRocks は生成列をサポートしています。

#### 指定されたインデックスから列を削除する

構文:

```sql
ALTER TABLE [<db_name>.]<tbl_name>
DROP COLUMN column_name
[FROM rollup_index_name];
```

注意:

1. パーティション列を削除することはできません。
2. 列が基本インデックスから削除された場合、ロールアップに含まれている場合も削除されます。

#### 列の型、位置、コメント、その他のプロパティを変更する

構文:

```sql
ALTER TABLE [<db_name>.]<tbl_name>
MODIFY COLUMN <column_name> 
[ column_type [ KEY | agg_type ] ] [ NULL | NOT NULL ] 
[ DEFAULT "<default_value>"] [ COMMENT "<new_column_comment>" ]
[ AFTER <column_name> | FIRST ]
[ FROM rollup_index_name ]
[ PROPERTIES ("key"="value", ...) ]
```

注意:

1. 集計モデルで値列を修正する場合、`agg_type` を指定する必要があります。
2. 非集計モデルでキー列を修正する場合、`KEY` キーワードを指定する必要があります。
3. 列のタイプのみを修正できます。列の他のプロパティは現在のままです。（つまり、他のプロパティは元のプロパティに従ってステートメントに明示的に記述する必要があります。例8を参照してください。）
4. パーティション列は修正できません。
5. 現在サポートされている変換の種類（精度の損失はユーザーが保証します）:

   - TINYINT/SMALLINT/INT/BIGINT を TINYINT/SMALLINT/INT/BIGINT/DOUBLE に変換します。
   - TINYINT/SMALLINT/INT/BIGINT/LARGEINT/FLOAT/DOUBLE/DECIMAL を VARCHAR に変換します。VARCHAR は最大長の修正をサポートします。
   - VARCHAR を TINYINT/SMALLINT/INT/BIGINT/LARGEINT/FLOAT/DOUBLE に変換します。
   - VARCHAR を DATE に変換します（現在、6つの形式をサポートしています: "%Y-%m-%d", "%y-%m-%d", "%Y%m%d", "%y%m%d", "%Y/%m/%d", "%y/%m/%d"）。
   - DATETIME を DATE に変換します（年-月-日情報のみが保持されます。例: `2019-12-09 21:47:05` `<-->` `2019-12-09`）。
   - DATE を DATETIME に変換します（時、分、秒をゼロに設定します。例: `2019-12-09` `<-->` `2019-12-09 00:00:00`）。
   - FLOAT を DOUBLE に変換します。
   - INT を DATE に変換します（INT データの変換に失敗した場合、元のデータはそのままです）。

6. NULL から NOT NULL への変換はサポートされていません。
7. 単一のMODIFY COLUMN句で複数のプロパティを変更できます。ただし、一部のプロパティの組み合わせはサポートされていません。

#### 指定されたインデックスの列を再配置する

構文:

```sql
ALTER TABLE [<db_name>.]<tbl_name>
ORDER BY (column_name1, column_name2, ...)
[FROM rollup_index_name]
[PROPERTIES ("key"="value", ...)]
```

注意:

- インデックス内のすべての列を記述する必要があります。
- 値列はキー列の後にリストされます。

#### ソートキーの修正

v3.0 以降、主キーテーブルのソートキーを修正できます。v3.3 では、このサポートが重複キーテーブル、集計テーブル、およびユニークキーテーブルに拡張されました。

重複キーテーブルおよび主キーテーブルのソートキーは、任意のソート列の組み合わせにすることができます。集計テーブルおよびユニークキーテーブルのソートキーは、すべてのキー列を含む必要がありますが、列の順序はキー列と同じである必要はありません。

構文:

```SQL
ALTER TABLE [<db_name>.]<table_name>
[ order_desc ]

order_desc ::=
    ORDER BY <column_name> [, <column_name> ...]
```

例: 主キーテーブルのソートキーを修正する。

例えば、元のテーブルは主キーテーブルで、ソートキーと主キーが結合されており、`dt, order_id` です。

```SQL
create table orders (
    dt date NOT NULL,
    order_id bigint NOT NULL,
    user_id int NOT NULL,
    merchant_id int NOT NULL,
    good_id int NOT NULL,
    good_name string NOT NULL,
    price int NOT NULL,
    cnt int NOT NULL,
    revenue int NOT NULL,
    state tinyint NOT NULL
) PRIMARY KEY (dt, order_id)
PARTITION BY date_trunc('day', dt)
DISTRIBUTED BY HASH(order_id);
```

ソートキーを主キーから分離し、ソートキーを `dt, revenue, state` に修正します。

```SQL
ALTER TABLE orders ORDER BY (dt, revenue, state);
```

#### STRUCT 列を修正してフィールドを追加または削除する

<Beta />

v3.2.10 および v3.3.2 以降、StarRocks は STRUCT 列を修正してフィールドを追加または削除することをサポートしています。これはネストされたフィールドや ARRAY タイプ内のフィールドに適用されます。

構文:

```sql
-- フィールドを追加する
ALTER TABLE [<db_name>.]<tbl_name> MODIFY COLUMN <column_name>
ADD FIELD field_path field_desc

-- フィールドを削除する
ALTER TABLE [<db_name>.]<tbl_name> MODIFY COLUMN <column_name>
DROP FIELD field_path

field_path ::= [ { <field_name>. | [*]. } [ ... ] ]<field_name>

  -- ここで `[*]` は全体として事前定義されたシンボルであり、ARRAY タイプ内にネストされた STRUCT タイプのフィールドを追加または削除する際に、ARRAY フィールド内のすべての要素を表します。
  -- 詳細な情報は、`field_path` のパラメータ説明と例を参照してください。

field_desc ::= <field_type> [ AFTER <prior_field_name> | FIRST ]
```

パラメータ:

- `field_path`: 追加または削除するフィールド。これは単純なフィールド名であり、トップディメンションフィールドを示します。例えば、`new_field_name` や、ネストされたフィールドを表すカラムアクセスパスで、例えば `lv1_k1.lv2_k2.[*].new_field_name` です。
- `[*]`: STRUCT タイプが ARRAY タイプ内にネストされている場合、`[*]` は ARRAY フィールド内のすべての要素を表します。これは、ARRAY フィールドの下にネストされたすべての STRUCT 要素にフィールドを追加または削除するために使用されます。
- `prior_field_name`: 新しく追加されたフィールドの前にあるフィールド。AFTER キーワードと組み合わせて新しいフィールドの順序を指定するために使用されます。FIRST キーワードが使用される場合、このパラメータを指定する必要はありません。新しいフィールドが最初のフィールドであることを示します。`prior_field_name` のディメンションは `field_path`（具体的には、`new_field_name` の前の部分、つまり `level1_k1.level2_k2.[*]`）によって決定され、明示的に指定する必要はありません。

`field_path` の例:

- STRUCT 列内にネストされた STRUCT フィールドのサブフィールドを追加または削除する。

  例えば、`fx stuct<c1 int, c2 struct <v1 int, v2 int>>` という列があるとします。`c2` の下に `v3` フィールドを追加する構文は次のとおりです:

  ```SQL
  ALTER TABLE tbl MODIFY COLUMN fx ADD FIELD c2.v3 INT
  ```

  この操作の後、列は `fx stuct<c1 int, c2 struct <v1 int, v2 int, v3 int>>` になります。

- ARRAY フィールド内にネストされたすべての STRUCT フィールドにサブフィールドを追加または削除する。

  例えば、`fx struct<c1 int, c2 array<struct <v1 int, v2 int>>>` という列があるとします。フィールド `c2` は ARRAY タイプで、2つのフィールド `v1` と `v2` を持つ STRUCT を含んでいます。ネストされた STRUCT に `v3` フィールドを追加する構文は次のとおりです:

  ```SQL
  ALTER TABLE tbl MODIFY COLUMN fx ADD FIELD c2.[*].v3 INT
  ```

  この操作の後、列は `fx struct<c1 int, c2 array<struct <v1 int, v2 int, v3 int>>>` になります。

詳細な使用方法については、[Example - Column -14](#column) を参照してください。

:::note

- 現在、この機能は共有なしクラスタでのみサポートされています。
- テーブルには `fast_schema_evolution` プロパティが有効である必要があります。
- STRUCT タイプ内の MAP サブフィールドの値タイプを変更することはサポートされていません。値タイプが ARRAY、STRUCT、または MAP のいずれであっても同様です。
- 新しく追加されたフィールドにはデフォルト値や Nullable などの属性を指定することはできません。デフォルトで Nullable であり、デフォルト値は null です。
- この機能を使用した後、この機能をサポートしていないバージョンにクラスタを直接ダウングレードすることはできません。

:::

### ロールアップの修正

#### ロールアップの作成

構文:

```SQL
ALTER TABLE [<db_name>.]<tbl_name> 
ADD ROLLUP rollup_name (column_name1, column_name2, ...)
[FROM from_index_name]
[PROPERTIES ("key"="value", ...)]
```

PROPERTIES: タイムアウト時間を設定することをサポートしています。デフォルトのタイムアウト時間は1日です。

例:

```SQL
ALTER TABLE [<db_name>.]<tbl_name> 
ADD ROLLUP r1(col1,col2) from r0;
```

#### バッチでロールアップを作成する

構文:

```SQL
ALTER TABLE [<db_name>.]<tbl_name>
ADD ROLLUP [rollup_name (column_name1, column_name2, ...)
[FROM from_index_name]
[PROPERTIES ("key"="value", ...)],...];
```

例:

```sql
ALTER TABLE [<db_name>.]<tbl_name>
ADD ROLLUP r1(col1,col2) from r0, r2(col3,col4) from r0;
```

注意:

1. `from_index_name` が指定されていない場合、デフォルトでベースインデックスから作成されます。
2. ロールアップテーブルの列は、`from_index` に存在する列でなければなりません。
3. プロパティでは、ストレージ形式を指定できます。詳細は CREATE TABLE を参照してください。

#### ロールアップの削除

構文:

```sql
ALTER TABLE [<db_name>.]<tbl_name>
DROP ROLLUP rollup_name [PROPERTIES ("key"="value", ...)];
```

例:

```sql
ALTER TABLE [<db_name>.]<tbl_name> DROP ROLLUP r1;
```

#### バッチでロールアップを削除する

構文:

```sql
ALTER TABLE [<db_name>.]<tbl_name>
DROP ROLLUP [rollup_name [PROPERTIES ("key"="value", ...)],...];
```

例:

```sql
ALTER TABLE [<db_name>.]<tbl_name> DROP ROLLUP r1, r2;
```

注意: ベースインデックスを削除することはできません。

### インデックスの修正

次のインデックスを作成または削除できます:
- [Bitmap index](../../../table_design/indexes/Bitmap_index.md)
- [N-Gram bloom filter index](../../../table_design/indexes/Ngram_Bloom_Filter_Index.md)
- [Full-Text inverted index](../../../table_design/indexes/inverted_index.md)
- [Vector index](../../../table_design/indexes/vector_index.md)

#### インデックスの作成

構文:

```sql
ALTER TABLE [<db_name>.]<tbl_name>
ADD INDEX index_name (column) [USING { BITMAP | NGRAMBF | GIN | VECTOR } ] [COMMENT '<comment>']
```

これらのインデックスの作成に関する詳細な指示と例については、上記の対応するチュートリアルを参照してください。

#### インデックスの削除

構文:

```sql
ALTER TABLE [<db_name>.]<tbl_name>
DROP INDEX index_name;
```

### テーブルプロパティの修正

構文:

```sql
ALTER TABLE [<db_name>.]<tbl_name>
SET ("key" = "value")
```

現在、StarRocks は次のテーブルプロパティの修正をサポートしています:

- `replication_num`
- `default.replication_num`
- `default.storage_medium`
- 動的パーティション化関連のプロパティ
- `enable_persistent_index`
- `bloom_filter_columns`
- `colocate_with`
- `bucket_size` (v3.2 以降でサポート)
- `base_compaction_forbidden_time_ranges` (v3.2.13 以降でサポート)

:::note

- ほとんどの場合、一度に1つのプロパティのみを修正することが許可されています。これらのプロパティが同じプレフィックスを持つ場合にのみ、一度に複数のプロパティを修正することができます。現在、`dynamic_partition.` と `binlog.` のみがサポートされています。
- 上記の列に対する操作にマージすることによってプロパティを修正することもできます。詳細は [以下の例](#examples) を参照してください。

:::

### スワップ

スワップは2つのテーブルをアトミックに交換することをサポートします。

構文:

```sql
ALTER TABLE [<db_name>.]<tbl_name>
SWAP WITH <tbl_name>;
```

:::note
- OLAP テーブル間のユニークキーと外部キーの制約は、スワップ中に検証され、スワップされる2つのテーブルの制約が一致していることを確認します。不一致が検出された場合、エラーが返されます。不一致が検出されない場合、ユニークキーと外部キーの制約は自動的にスワップされます。
- スワップされるテーブルに依存するマテリアライズドビューは自動的に非アクティブに設定され、そのユニークキーと外部キーの制約は削除され、利用できなくなります。
:::

### 手動コンパクション (v3.1 以降)

StarRocks は、ロードされたデータの異なるバージョンをマージするためにコンパクションメカニズムを使用します。この機能は、小さなファイルを大きなファイルに結合することができ、クエリパフォーマンスを効果的に向上させます。

v3.1 以前は、コンパクションは2つの方法で実行されていました:

- システムによる自動コンパクション: コンパクションは BE レベルでバックグラウンドで実行されます。ユーザーはコンパクションのためにデータベースやテーブルを指定することはできません。
- ユーザーは HTTP インターフェースを呼び出してコンパクションを実行できます。

v3.1 以降、StarRocks はユーザーが SQL コマンドを実行して手動でコンパクションを実行できる SQL インターフェースを提供します。特定のテーブルまたはパーティションを選択してコンパクションを実行することができます。これにより、コンパクションプロセスに対する柔軟性と制御が向上します。

共有データクラスタは v3.3.0 以降でこの機能をサポートしています。

> **注意**
>
> v3.2.13 以降、プロパティ [`base_compaction_forbidden_time_ranges`](./CREATE_TABLE.md#forbid-base-compaction) を使用して、特定の時間範囲内でベースコンパクションを禁止することができます。

構文:

```SQL
ALTER TABLE <tbl_name> [ BASE | CUMULATIVE ] COMPACT [ <partition_name> | ( <partition1_name> [, <partition2_name> ...] ) ]
```

つまり:

```SQL
-- テーブル全体でコンパクションを実行します。
ALTER TABLE <tbl_name> COMPACT

-- 単一のパーティションでコンパクションを実行します。
ALTER TABLE <tbl_name> COMPACT <partition_name>

-- 複数のパーティションでコンパクションを実行します。
ALTER TABLE <tbl_name> COMPACT (<partition1_name>[,<partition2_name>,...])

-- 累積コンパクションを実行します。
ALTER TABLE <tbl_name> CUMULATIVE COMPACT (<partition1_name>[,<partition2_name>,...])

-- ベースコンパクションを実行します。
ALTER TABLE <tbl_name> BASE COMPACT (<partition1_name>[,<partition2_name>,...])
```

`information_schema` データベースの `be_compactions` テーブルはコンパクション結果を記録します。コンパクション後のデータバージョンをクエリするには、`SELECT * FROM information_schema.be_compactions;` を実行できます。

### 主キー永続性インデックスの削除 (v3.3.9 以降)

構文:

```sql
ALTER TABLE [<db_name>.]<tbl_name>
DROP PERSISTENT INDEX ON TABLETS(<tablet_id>[, <tablet_id>, ...]);
```
> **注意**
>
> StarRocks は、共有データクラスタのクラウドネイティブな主キーテーブルの永続性インデックスの削除のみをサポートしています。

## 例

### テーブル

1. テーブルのデフォルトのレプリカ数を修正します。これは新しく追加されたパーティションのデフォルトのレプリカ数として使用されます。

    ```sql
    ALTER TABLE example_db.my_table
    SET ("default.replication_num" = "2");
    ```

2. 単一パーティションテーブルの実際のレプリカ数を修正します。

    ```sql
    ALTER TABLE example_db.my_table
    SET ("replication_num" = "3");
    ```

3. レプリカ間のデータ書き込みとレプリケーションモードを修正します。

    ```sql
    ALTER TABLE example_db.my_table
    SET ("replicated_storage" = "false");
    ```

   この例では、レプリカ間のデータ書き込みとレプリケーションモードを「リーダーレスレプリケーション」に設定します。これは、データがプライマリとセカンダリのレプリカを区別せずに同時に複数のレプリカに書き込まれることを意味します。詳細については、[CREATE TABLE](CREATE_TABLE.md) の `replicated_storage` パラメータを参照してください。

### パーティション

1. パーティションを追加し、デフォルトのバケッティングモードを使用します。既存のパーティションは [MIN, 2013-01-01) です。追加されるパーティションは [2013-01-01, 2014-01-01) です。

    ```sql
    ALTER TABLE example_db.my_table
    ADD PARTITION p1 VALUES LESS THAN ("2014-01-01");
    ```

2. パーティションを追加し、新しいバケット数を使用します。

    ```sql
    ALTER TABLE example_db.my_table
    ADD PARTITION p1 VALUES LESS THAN ("2015-01-01")
    DISTRIBUTED BY HASH(k1);
    ```

3. パーティションを追加し、新しいレプリカ数を使用します。

    ```sql
    ALTER TABLE example_db.my_table
    ADD PARTITION p1 VALUES LESS THAN ("2015-01-01")
    ("replication_num"="1");
    ```

4. パーティションのレプリカ数を修正します。

    ```sql
    ALTER TABLE example_db.my_table
    MODIFY PARTITION p1 SET("replication_num"="1");
    ```

5. 指定されたパーティションのレプリカ数をバッチで修正します。

    ```sql
    ALTER TABLE example_db.my_table
    MODIFY PARTITION (p1, p2, p4) SET("replication_num"="1");
    ```

6. すべてのパーティションの記憶媒体をバッチで修正します。

    ```sql
    ALTER TABLE example_db.my_table
    MODIFY PARTITION (*) SET("storage_medium"="HDD");
    ```

7. パーティションを削除します。

    ```sql
    ALTER TABLE example_db.my_table
    DROP PARTITION p1;
    ```

8. 上限と下限を持つパーティションを追加します。

    ```sql
    ALTER TABLE example_db.my_table
    ADD PARTITION p1 VALUES [("2014-01-01"), ("2014-02-01"));
    ```

### ロールアップ

1. ベースインデックス (k1,k2,k3,v1,v2) に基づいてロールアップ `example_rollup_index` を作成します。カラムベースのストレージが使用されます。

    ```sql
    ALTER TABLE example_db.my_table
    ADD ROLLUP example_rollup_index(k1, k3, v1, v2)
    PROPERTIES("storage_type"="column");
    ```

2. `example_rollup_index(k1,k3,v1,v2)` に基づいてロールアップ `example_rollup_index2` を作成します。

    ```sql
    ALTER TABLE example_db.my_table
    ADD ROLLUP example_rollup_index2 (k1, v1)
    FROM example_rollup_index;
    ```

3. ベースインデックス (k1, k2, k3, v1) に基づいてロールアップ `example_rollup_index3` を作成します。ロールアップのタイムアウト時間は1時間に設定されています。

    ```sql
    ALTER TABLE example_db.my_table
    ADD ROLLUP example_rollup_index3(k1, k3, v1)
    PROPERTIES("storage_type"="column", "timeout" = "3600");
    ```

4. ロールアップ `example_rollup_index2` を削除します。

    ```sql
    ALTER TABLE example_db.my_table
    DROP ROLLUP example_rollup_index2;
    ```

### 列

1. `example_rollup_index` の `col1` 列の後にキー列 `new_col`（非集計列）を追加します。

    ```sql
    ALTER TABLE example_db.my_table
    ADD COLUMN new_col INT KEY DEFAULT "0" AFTER col1
    TO example_rollup_index;
    ```

2. `example_rollup_index` の `col1` 列の後に値列 `new_col`（非集計列）を追加します。

    ```sql
    ALTER TABLE example_db.my_table
    ADD COLUMN new_col INT DEFAULT "0" AFTER col1
    TO example_rollup_index;
    ```

3. `example_rollup_index` の `col1` 列の後にキー列 `new_col`（集計列）を追加します。

    ```sql
    ALTER TABLE example_db.my_table
    ADD COLUMN new_col INT DEFAULT "0" AFTER col1
    TO example_rollup_index;
    ```

4. `example_rollup_index` の `col1` 列の後に値列 `new_col SUM`（集計列）を追加します。

    ```sql
    ALTER TABLE example_db.my_table
    ADD COLUMN new_col INT SUM DEFAULT "0" AFTER col1
    TO example_rollup_index;
    ```

5. `example_rollup_index` に複数の列を追加します（集計）。

    ```sql
    ALTER TABLE example_db.my_table
    ADD COLUMN (col1 INT DEFAULT "1", col2 FLOAT SUM DEFAULT "2.3")
    TO example_rollup_index;
    ```

6. `example_rollup_index` に複数の列を追加し、`AFTER` を使用して追加された列の位置を指定します。

    ```sql
    ALTER TABLE example_db.my_table
    ADD COLUMN col1 INT DEFAULT "1" AFTER `k1`,
    ADD COLUMN col2 FLOAT SUM AFTER `v2`,
    TO example_rollup_index;
    ```

7. `example_rollup_index` から列を削除します。

    ```sql
    ALTER TABLE example_db.my_table
    DROP COLUMN col2
    FROM example_rollup_index;
    ```

8. ベースインデックスの `col1` の列タイプを BIGINT に修正し、`col2` の後に配置します。

    ```sql
    ALTER TABLE example_db.my_table
    MODIFY COLUMN col1 BIGINT DEFAULT "1" AFTER col2;
    ```

9. ベースインデックスの `val1` 列の最大長を 32 から 64 に修正します。

    ```sql
    ALTER TABLE example_db.my_table
    MODIFY COLUMN val1 VARCHAR(64) REPLACE DEFAULT "abc";
    ```

10. `example_rollup_index` の列を再配置します。元の列の順序は k1, k2, k3, v1, v2 です。

    ```sql
    ALTER TABLE example_db.my_table
    ORDER BY (k3,k1,k2,v2,v1)
    FROM example_rollup_index;
    ```

11. 一度に2つの操作（ADD COLUMN と ORDER BY）を実行します。

    ```sql
    ALTER TABLE example_db.my_table
    ADD COLUMN v2 INT MAX DEFAULT "0" AFTER k2 TO example_rollup_index,
    ORDER BY (k3,k1,k2,v2,v1) FROM example_rollup_index;
    ```

12. テーブルのブルームフィルター列を修正します。

     ```sql
     ALTER TABLE example_db.my_table
     SET ("bloom_filter_columns"="k1,k2,k3");
     ```

     この操作は、上記の列操作にマージすることもできます（複数の句の構文がわずかに異なることに注意してください）。

     ```sql
     ALTER TABLE example_db.my_table
     DROP COLUMN col2
     PROPERTIES ("bloom_filter_columns"="k1,k2,k3");
     ```

13. 単一のステートメントで複数の列のデータタイプを修正します。

    ```sql
    ALTER TABLE example_db.my_table
    MODIFY COLUMN k1 VARCHAR(100) KEY NOT NULL,
    MODIFY COLUMN v2 DOUBLE DEFAULT "1" AFTER v1;
    ```

14. STRUCT 型データのフィールドを追加および削除します。

    **前提条件**: テーブルを作成し、1行のデータを挿入します。

    ```sql
    CREATE TABLE struct_test(
        c0 INT,
        c1 STRUCT<v1 INT, v2 STRUCT<v4 INT, v5 INT>, v3 INT>,
        c2 STRUCT<v1 INT, v2 ARRAY<STRUCT<v3 INT, v4 STRUCT<v5 INT, v6 INT>>>>
    )
    DUPLICATE KEY(c0)
    DISTRIBUTED BY HASH(`c0`) BUCKETS 1
    PROPERTIES (
        "fast_schema_evolution" = "true"
    );
    INSERT INTO struct_test VALUES (
        1, 
        ROW(1, ROW(2, 3), 4), 
        ROW(5, [ROW(6, ROW(7, 8)), ROW(9, ROW(10, 11))])
    );
    ```

    ```plain
    mysql> SELECT * FROM struct_test\G
    *************************** 1. row ***************************
    c0: 1
    c1: {"v1":1,"v2":{"v4":2,"v5":3},"v3":4}
    c2: {"v1":5,"v2":[{"v3":6,"v4":{"v5":7,"v6":8}},{"v3":9,"v4":{"v5":10,"v6":11}}]}
    ```

    - STRUCT 型の列に新しいフィールドを追加します。

    ```sql
    ALTER TABLE struct_test MODIFY COLUMN c1 ADD FIELD v4 INT AFTER v2;
    ```

    ```plain
    mysql> SELECT * FROM struct_test\G
    *************************** 1. row ***************************
    c0: 1
    c1: {"v1":1,"v2":{"v4":2,"v5":3},"v4":null,"v3":4}
    c2: {"v1":5,"v2":[{"v3":6,"v4":{"v5":7,"v6":8}},{"v3":9,"v4":{"v5":10,"v6":11}}]}
    ```

    - ネストされた STRUCT 型に新しいフィールドを追加します。

```sql
ALTER TABLE struct_test MODIFY COLUMN c1 ADD FIELD v2.v6 INT FIRST;
```

```plain
mysql> SELECT * FROM struct_test\G
*************************** 1. row ***************************
c0: 1
c1: {"v1":1,"v2":{"v6":null,"v4":2,"v5":3},"v4":null,"v3":4}
c2: {"v1":5,"v2":[{"v3":6,"v4":{"v5":7,"v6":8}},{"v3":9,"v4":{"v5":10,"v6":11}}]}
```

- 配列内の STRUCT 型に新しいフィールドを追加します。

```sql
ALTER TABLE struct_test MODIFY COLUMN c2 ADD FIELD v2.[*].v7 INT AFTER v3;
```

```plain
mysql> SELECT * FROM struct_test\G
*************************** 1. row ***************************
c0: 1
c1: {"v1":1,"v2":{"v6":null,"v4":2,"v5":3},"v4":null,"v3":4}
c2: {"v1":5,"v2":[{"v3":6,"v7":null,"v4":{"v5":7,"v6":8}},{"v3":9,"v7":null,"v4":{"v5":10,"v6":11}}]}
```

- STRUCT 型の列からフィールドを削除します。

```sql
ALTER TABLE struct_test MODIFY COLUMN c1 DROP FIELD v3;
```

```plain
mysql> SELECT * FROM struct_test\G
*************************** 1. row ***************************
c0: 1
c1: {"v1":1,"v2":{"v6":null,"v4":2,"v5":3},"v4":null}
c2: {"v1":5,"v2":[{"v3":6,"v7":null,"v4":{"v5":7,"v6":8}},{"v3":9,"v7":null,"v4":{"v5":10,"v6":11}}]}
```

- ネストされた STRUCT 型からフィールドを削除します。

```sql
ALTER TABLE struct_test MODIFY COLUMN c1 DROP FIELD v2.v4;
```

```plain
mysql> SELECT * FROM struct_test\G
*************************** 1. row ***************************
c0: 1
c1: {"v1":1,"v2":{"v6":null,"v5":3},"v4":null}
c2: {"v1":5,"v2":[{"v3":6,"v7":null,"v4":{"v5":7,"v6":8}},{"v3":9,"v7":null,"v4":{"v5":10,"v6":11}}]}
```

- 配列内の STRUCT 型からフィールドを削除します。

```sql
ALTER TABLE struct_test MODIFY COLUMN c2 DROP FIELD v2.[*].v3;
```

```plain
mysql> SELECT * FROM struct_test\G
*************************** 1. row ***************************
c0: 1
c1: {"v1":1,"v2":{"v6":null,"v5":3},"v4":null}
c2: {"v1":5,"v2":[{"v7":null,"v4":{"v5":7,"v6":8}},{"v7":null,"v4":{"v5":10,"v6":11}}]}
```

### テーブルプロパティ

1. テーブルのコロケートプロパティを修正します。

     ```sql
     ALTER TABLE example_db.my_table
     SET ("colocate_with" = "t1");
     ```

2. テーブルの動的パーティションプロパティを修正します。

     ```sql
     ALTER TABLE example_db.my_table
     SET ("dynamic_partition.enable" = "false");
     ```

     動的パーティションプロパティが設定されていないテーブルに動的パーティションプロパティを追加する必要がある場合、すべての動的パーティションプロパティを指定する必要があります。

     ```sql
     ALTER TABLE example_db.my_table
     SET (
         "dynamic_partition.enable" = "true",
         "dynamic_partition.time_unit" = "DAY",
         "dynamic_partition.end" = "3",
         "dynamic_partition.prefix" = "p",
         "dynamic_partition.buckets" = "32"
         );
     ```

3. テーブルの記憶媒体プロパティを修正します。

     ```sql
     ALTER TABLE example_db.my_table SET("default.storage_medium"="SSD");
     ```

### 名前変更

1. `table1` を `table2` に名前変更します。

    ```sql
    ALTER TABLE table1 RENAME table2;
    ```

2. `example_table` のロールアップ `rollup1` を `rollup2` に名前変更します。

    ```sql
    ALTER TABLE example_table RENAME ROLLUP rollup1 rollup2;
    ```

3. `example_table` のパーティション `p1` を `p2` に名前変更します。

    ```sql
    ALTER TABLE example_table RENAME PARTITION p1 p2;
    ```

### インデックス

1. `table1` の `siteid` 列にビットマップインデックスを作成します。

    ```sql
    ALTER TABLE table1
    ADD INDEX index_1 (siteid) USING BITMAP COMMENT 'site_id_bitmap';
    ```

2. `table1` の `siteid` 列のビットマップインデックスを削除します。

    ```sql
    ALTER TABLE table1
    DROP INDEX index_1;
    ```

### スワップ

`table1` と `table2` のアトミックスワップ。

```sql
ALTER TABLE table1 SWAP WITH table2
```

### 手動コンパクション

```sql
CREATE TABLE compaction_test( 
    event_day DATE,
    pv BIGINT)
DUPLICATE KEY(event_day)
PARTITION BY date_trunc('month', event_day)
DISTRIBUTED BY HASH(event_day) BUCKETS 8
PROPERTIES("replication_num" = "3");

INSERT INTO compaction_test VALUES
('2023-02-14', 2),
('2033-03-01',2);
{'label':'insert_734648fa-c878-11ed-90d6-00163e0dcbfc', 'status':'VISIBLE', 'txnId':'5008'}

INSERT INTO compaction_test VALUES
('2023-02-14', 2),('2033-03-01',2);
{'label':'insert_85c95c1b-c878-11ed-90d6-00163e0dcbfc', 'status':'VISIBLE', 'txnId':'5009'}

ALTER TABLE compaction_test COMPACT;

ALTER TABLE compaction_test COMPACT p203303;

ALTER TABLE compaction_test COMPACT (p202302,p203303);

ALTER TABLE compaction_test CUMULATIVE COMPACT (p202302,p203303);

ALTER TABLE compaction_test BASE COMPACT (p202302,p203303);
```

### 主キー永続性インデックスの削除

共有データクラスタの主キーテーブル `db1.test_tbl` のタブレット `100` と `101` の永続性インデックスを削除します。

```sql
ALTER TABLE db1.test_tbl DROP PERSISTENT INDEX ON TABLETS (100, 101);
```

## 参考文献

- [CREATE TABLE](CREATE_TABLE.md)
- [SHOW CREATE TABLE](SHOW_CREATE_TABLE.md)
- [SHOW TABLES](SHOW_TABLES.md)
- [SHOW ALTER TABLE](SHOW_ALTER.md)
- [DROP TABLE](DROP_TABLE.md)
```