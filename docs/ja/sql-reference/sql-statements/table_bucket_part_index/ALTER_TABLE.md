---
displayed_sidebar: docs
---

# ALTER TABLE

## 説明

既存のテーブルを修正します。以下を含みます:

- [テーブル、パーティション、インデックスの名前変更](#rename)
- [テーブルコメントの修正](#alter-table-comment-from-v31)
- [パーティションの修正（パーティションの追加/削除およびパーティション属性の修正）](#modify-partition)
- [バケット法とバケット数の修正](#modify-the-bucketing-method-and-number-of-buckets-from-v32)
- [列の修正（列の追加/削除および列の順序変更）](#modify-columns-adddelete-columns-change-the-order-of-columns)
- [ロールアップインデックスの作成/削除](#modify-rollup-index)
- [ビットマップインデックスの修正](#modify-bitmap-indexes)
- [テーブルプロパティの修正](#modify-table-properties)
- [アトミックスワップ](#swap)
- [手動データバージョンのコンパクション](#manual-compaction-from-31)

:::tip
この操作には、対象テーブルに対する ALTER 権限が必要です。
:::

## 構文

```SQL
ALTER TABLE [<db_name>.]<tbl_name>
alter_clause1[, alter_clause2, ...]
```

`alter_clause` には次の操作が含まれます: 名前変更、コメント、パーティション、バケット、列、ロールアップインデックス、ビットマップインデックス、テーブルプロパティ、スワップ、コンパクション。

- 名前変更: テーブル、ロールアップインデックス、またはパーティションの名前を変更します。
- コメント: テーブルコメントを修正します（**v3.1以降**でサポート）。
- パーティション: パーティションプロパティを修正、パーティションを削除、またはパーティションを追加します。
- バケット: バケット法とバケット数を修正します。
- 列: 列を追加、削除、または再配置、または列タイプを修正します。
- ロールアップインデックス: ロールアップインデックスを作成または削除します。
- ビットマップインデックス: インデックスを修正します（ビットマップインデックスのみ修正可能）。
- スワップ: 2つのテーブルのアトミック交換。
- コンパクション: ロードされたデータのバージョンをマージするための手動コンパクションを実行します（**v3.1以降**でサポート）。

## 制限と使用上の注意

- パーティション、列、ロールアップインデックスに対する操作は、1つの ALTER TABLE 文で実行できません。
- 列名は修正できません。
- 列コメントは修正できません。
- 1つのテーブルには、同時に1つのスキーマ変更操作しか実行できません。同時に2つのスキーマ変更コマンドを実行することはできません。
- バケット、列、ロールアップインデックスに対する操作は非同期操作です。タスクが送信されるとすぐに成功メッセージが返されます。進行状況を確認するには [SHOW ALTER TABLE](SHOW_ALTER.md) コマンドを実行し、操作をキャンセルするには [CANCEL ALTER TABLE](CANCEL_ALTER_TABLE.md) コマンドを実行できます。
- 名前変更、コメント、パーティション、ビットマップインデックス、スワップに対する操作は同期操作であり、コマンドが返されると実行が完了したことを示します。

### 名前変更

名前変更は、テーブル名、ロールアップインデックス、およびパーティション名の修正をサポートします。

#### テーブルの名前変更

```sql
ALTER TABLE <tbl_name> RENAME <new_tbl_name>
```

#### ロールアップインデックスの名前変更

```sql
ALTER TABLE [<db_name>.]<tbl_name>
RENAME ROLLUP <old_rollup_name> <new_rollup_name>
```

#### パーティションの名前変更

```sql
ALTER TABLE [<db_name>.]<tbl_name>
RENAME PARTITION <old_partition_name> <new_partition_name>
```

### テーブルコメントの修正 (v3.1以降)

構文:

```sql
ALTER TABLE [<db_name>.]<tbl_name> COMMENT = "<new table comment>";
```

:::tip
現在、列コメントは修正できません。
:::

### パーティションの修正

#### パーティションの追加

範囲パーティションまたはリストパーティションを追加できます。式パーティションの追加はサポートされていません。

構文：

- 範囲パーティション

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
        | PARTITIONS START ("<start_integer_value>") END ("<end_integer_value>") EVERY ( <granularity> ) } -- START および END で指定されたパーティション列の値が整数であっても、パーティション列の値はダブルクォートで囲む必要があります。ただし、EVERY 句の間隔値はダブルクォートで囲む必要はありません。
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

  - 範囲パーティションの場合、単一範囲パーティション（`single_range_partition`）またはバッチで複数の範囲パーティション（`multi_range_partitions`）を追加できます。
  - リストパーティションの場合、単一のリストパーティションのみ追加できます。

- `distribution_desc`:

   新しいパーティションのバケット数を個別に設定できますが、バケット法を個別に設定することはできません。

- `"key"="value"`:

   新しいパーティションのプロパティを設定できます。詳細は [CREATE TABLE](CREATE_TABLE.md#properties) を参照してください。

例:

- 範囲パーティション

  - パーティション列がテーブル作成時に `event_day` と指定されている場合、例えば `PARTITION BY RANGE(event_day)`、テーブル作成後に新しいパーティションを追加する必要がある場合、次のように実行できます:

    ```sql
    ALTER TABLE site_access ADD PARTITION p4 VALUES LESS THAN ("2020-04-30");
    ```

  - パーティション列がテーブル作成時に `datekey` と指定されている場合、例えば `PARTITION BY RANGE (datekey)`、テーブル作成後にバッチで複数のパーティションを追加する必要がある場合、次のように実行できます:

    ```sql
    ALTER TABLE site_access
        ADD PARTITIONS START ("2021-01-05") END ("2021-01-10") EVERY (INTERVAL 1 DAY);
    ```

- リストパーティション

  - 単一のパーティション列がテーブル作成時に指定されている場合、例えば `PARTITION BY LIST (city)`、テーブル作成後に新しいパーティションを追加する必要がある場合、次のように実行できます:

    ```sql
    ALTER TABLE t_recharge_detail2
    ADD PARTITION pCalifornia VALUES IN ("Los Angeles","San Francisco","San Diego");
    ```

  - 複数のパーティション列がテーブル作成時に指定されている場合、例えば `PARTITION BY LIST (dt,city)`、テーブル作成後に新しいパーティションを追加する必要がある場合、次のように実行できます:

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

構文:

```sql
-- 2.0以前
ALTER TABLE [<db_name>.]<tbl_name>
DROP PARTITION [IF EXISTS | FORCE] <partition_name>
-- 2.0以降
ALTER TABLE [<db_name>.]<tbl_name>
DROP PARTITION [IF EXISTS] <partition_name> [FORCE]
```

注意:

1. パーティション化されたテーブルには少なくとも1つのパーティションを保持してください。
2. DROP PARTITION を実行した後、指定された期間内（デフォルトで1日）に [RECOVER](../backup_restore/RECOVER.md) コマンドを使用して削除されたパーティションを復元できます。
3. DROP PARTITION FORCE を実行すると、パーティションは直接削除され、パーティション上の未完了のアクティビティがあるかどうかを確認せずに復元できなくなります。したがって、一般的にこの操作は推奨されません。

#### 一時パーティションの追加

構文:

```sql
ALTER TABLE [<db_name>.]<tbl_name> 
ADD TEMPORARY PARTITION [IF NOT EXISTS] <partition_name>
partition_desc ["key"="value"]
[DISTRIBUTED BY HASH (k1[,k2 ...]) [BUCKETS num]]
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

**使用法**

- パーティションの次のプロパティを修正できます:

  - 記憶媒体
  - storage_cooldown_ttl または storage_cooldown_time
  - replication_num

- テーブルに1つのパーティションしかない場合、パーティション名はテーブル名と同じです。複数のパーティションに分割されている場合、`(*)` を使用してすべてのパーティションのプロパティを修正することができ、より便利です。

- 修正後のパーティションプロパティを表示するには `SHOW PARTITIONS FROM <tbl_name>` を実行します。

### バケット法とバケット数の修正 (v3.2以降)

構文:

```SQL
ALTER TABLE [<db_name>.]<table_name>
[ partition_names ]
[ distribution_desc ]

partition_names ::= 
    (PARTITION | PARTITIONS) ( <partition_name> [, <partition_name> ...] )

distribution_desc ::=
    DISTRIBUTED BY RANDOM [ BUCKETS <num> ] |
    DISTRIBUTED BY HASH ( <column_name> [, <column_name> ...] ) [ BUCKETS <num> ]
```

例:

例えば、元のテーブルはハッシュバケット法を使用し、バケット数が StarRocks によって自動的に設定される重複キーテーブルです。

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

#### バケット法のみを修正

> **注意**
>
> - 修正はテーブル内のすべてのパーティションに適用され、特定のパーティションのみに適用することはできません。
> - バケット法のみを修正する必要がある場合でも、`BUCKETS <num>` を使用してコマンドでバケット数を指定する必要があります。`BUCKETS <num>` が指定されていない場合、バケット数は StarRocks によって自動的に決定されます。

- バケット法をハッシュバケット法からランダムバケット法に修正し、バケット数は StarRocks によって自動的に設定されます。

  ```SQL
  ALTER TABLE details DISTRIBUTED BY RANDOM;
  ```

- ハッシュバケット法のキーを `event_time, event_type` から `user_id, event_time` に修正します。そして、バケット数は StarRocks によって自動的に設定されます。

  ```SQL
  ALTER TABLE details DISTRIBUTED BY HASH(user_id, event_time);
  ```

#### バケット数のみを修正

> **注意**
>
> バケット数のみを修正する必要がある場合でも、バケット法をコマンドで指定する必要があります。例えば、`HASH(user_id)`。

- すべてのパーティションのバケット数を StarRocks によって自動的に設定されるものから10に修正します。

  ```SQL
  ALTER TABLE details DISTRIBUTED BY HASH(user_id) BUCKETS 10;
  ```

- 指定されたパーティションのバケット数を StarRocks によって自動的に設定されるものから15に修正します。

  ```SQL
  ALTER TABLE details PARTITIONS (p20231127, p20231128) DISTRIBUTED BY HASH(user_id) BUCKETS 15 ;
  ```

  > **注意**
  >
  > パーティション名は `SHOW PARTITIONS FROM <table_name>;` を実行して確認できます。

#### バケット法とバケット数の両方を修正

> **注意**
>
> 修正はテーブル内のすべてのパーティションに適用され、特定のパーティションのみに適用することはできません。

- バケット法をハッシュバケット法からランダムバケット法に修正し、バケット数を StarRocks によって自動的に設定されるものから10に変更します。

   ```SQL
   ALTER TABLE details DISTRIBUTED BY RANDOM BUCKETS 10;
   ```

- ハッシュバケット法のキーを修正し、バケット数を StarRocks によって自動的に設定されるものから10に変更します。ハッシュバケット法に使用されるキーは、元の `event_time, event_type` から `user_id, event_time` に修正されます。バケット数は StarRocks によって自動的に設定されるものから10に修正されます。

  ```SQL
  ALTER TABLE details DISTRIBUTED BY HASH(user_id, event_time) BUCKETS 10;
  ```

### 列の修正（列の追加/削除、列の順序変更）

#### 指定されたインデックスの指定された位置に列を追加

構文:

```sql
ALTER TABLE [<db_name>.]<tbl_name>
ADD COLUMN column_name column_type [KEY | agg_type] [DEFAULT "default_value"]
[AFTER column_name|FIRST]
[TO rollup_index_name]
[PROPERTIES ("key"="value", ...)]
```

注意:

1. 集計テーブルに値の列を追加する場合、agg_type を指定する必要があります。
2. 非集計テーブル（例えば重複キーテーブル）にキー列を追加する場合、KEY キーワードを指定する必要があります。
3. 基本インデックスに既に存在する列をロールアップインデックスに追加することはできません。（必要に応じてロールアップインデックスを再作成できます。）

#### 指定されたインデックスに複数の列を追加

構文:

- 複数の列を追加

  ```sql
  ALTER TABLE [<db_name>.]<tbl_name>
  ADD COLUMN (column_name1 column_type [KEY | agg_type] DEFAULT "default_value", ...)
  [TO rollup_index_name]
  [PROPERTIES ("key"="value", ...)]
  ```

- 複数の列を追加し、AFTER を使用して追加された列の位置を指定

  ```sql
  ALTER TABLE [<db_name>.]<tbl_name>
  ADD COLUMN column_name1 column_type [KEY | agg_type] DEFAULT "default_value" AFTER column_name,
  ADD COLUMN column_name2 column_type [KEY | agg_type] DEFAULT "default_value" AFTER column_name
  [, ...]
  [TO rollup_index_name]
  [PROPERTIES ("key"="value", ...)]
  ```

注意:

1. 集計テーブルに値の列を追加する場合、agg_type を指定する必要があります。

2. 非集計テーブルにキー列を追加する場合、KEY キーワードを指定する必要があります。

3. 基本インデックスに既に存在する列をロールアップインデックスに追加することはできません。（必要に応じて別のロールアップインデックスを作成できます。）

#### 生成列の追加 (v3.1以降)

構文:

```sql
ALTER TABLE [<db_name>.]<tbl_name>
ADD COLUMN col_name data_type [NULL] AS generation_expr [COMMENT 'string']
```

生成列を追加し、その式を指定できます。[生成列](../generated_columns.md) は、式の結果を事前に計算して保存するために使用でき、同じ複雑な式を持つクエリを大幅に高速化します。v3.1以降、StarRocks は生成列をサポートしています。

#### 指定されたインデックスから列を削除

構文:

```sql
ALTER TABLE [<db_name>.]<tbl_name>
DROP COLUMN column_name
[FROM rollup_index_name];
```

注意:

1. パーティション列を削除することはできません。
2. 基本インデックスから列が削除されると、ロールアップインデックスに含まれている場合も削除されます。

#### 指定されたインデックスの列タイプと列位置を修正

構文:

```sql
ALTER TABLE [<db_name>.]<tbl_name>
MODIFY COLUMN column_name column_type [KEY | agg_type] [NULL | NOT NULL] [DEFAULT "default_value"]
[AFTER column_name|FIRST]
[FROM rollup_index_name]
[PROPERTIES ("key"="value", ...)]
```

注意:

1. 集計モデルで値の列を修正する場合、agg_type を指定する必要があります。
2. 非集計モデルでキー列を修正する場合、KEY キーワードを指定する必要があります。
3. 列のタイプのみを修正できます。列の他のプロパティは現在のままです。（つまり、他のプロパティは元のプロパティに従ってステートメントに明示的に記述する必要があります。例8を参照してください。）
4. パーティション列は修正できません。
5. 現在サポートされている変換の種類（精度の損失はユーザーが保証します）:

   - TINYINT/SMALLINT/INT/BIGINT を TINYINT/SMALLINT/INT/BIGINT/DOUBLE に変換。
   - TINYINT/SMALLINT/INT/BIGINT/LARGEINT/FLOAT/DOUBLE/DECIMAL を VARCHAR に変換。VARCHAR は最大長の修正をサポートします。
   - VARCHAR を TINYINT/SMALLINT/INT/BIGINT/LARGEINT/FLOAT/DOUBLE に変換。
   - VARCHAR を DATE に変換（現在6つの形式をサポート: "%Y-%m-%d", "%y-%m-%d", "%Y%m%d", "%y%m%d", "%Y/%m/%d, "%y/%m/%d"）
   - DATETIME を DATE に変換（年-月-日情報のみが保持されます。例: `2019-12-09 21:47:05` `<-->` `2019-12-09`）
   - DATE を DATETIME に変換（時、分、秒をゼロに設定。例: `2019-12-09` `<-->` `2019-12-09 00:00:00`）
   - FLOAT を DOUBLE に変換
   - INT を DATE に変換（INT データの変換に失敗した場合、元のデータはそのままです）

6. NULL から NOT NULL への変換はサポートされていません。

#### 指定されたインデックスの列を再配置

構文:

```sql
ALTER TABLE [<db_name>.]<tbl_name>
ORDER BY (column_name1, column_name2, ...)
[FROM rollup_index_name]
[PROPERTIES ("key"="value", ...)]
```

注意:

- インデックス内のすべての列を記述する必要があります。
- 値の列はキー列の後にリストされます。

#### 主キーテーブルのソートキーの列を修正

構文:

```SQL
ALTER TABLE [<db_name>.]<table_name>
[ order_desc ]

order_desc ::=
    ORDER BY <column_name> [, <column_name> ...]
```

例:

例えば、元のテーブルはソートキーと主キーが結合された主キーテーブルで、`dt, order_id` です。

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

#### STRUCT 列を修正してフィールドを追加または削除

v3.2.10 および v3.3.2 以降、StarRocks は STRUCT 列を修正してフィールドを追加または削除することをサポートしています。これはネストされたものや ARRAY 型内でも可能です。

構文:

```sql
-- フィールドを追加
ALTER TABLE [<db_name>.]<tbl_name> MODIFY COLUMN <column_name>
ADD FIELD field_path field_desc

-- フィールドを削除
ALTER TABLE [<db_name>.]<tbl_name> MODIFY COLUMN <column_name>
DROP FIELD field_path

field_path ::= [ { <field_name>. | [*]. } [ ... ] ]<field_name>

  -- ここで `[*]` は全体として事前定義されたシンボルであり、ARRAY 型内にネストされた STRUCT 型のフィールドを追加または削除する際に ARRAY フィールド内のすべての要素を表します。
  -- 詳細な情報は `field_path` のパラメータ説明と例を参照してください。

field_desc ::= <field_type> [ AFTER <prior_field_name> | FIRST ]
```

パラメータ:

- `field_path`: 追加または削除するフィールド。これは単純なフィールド名で、トップディメンションのフィールドを示します。例えば、`new_field_name`、またはネストされたフィールドを表すカラムアクセスパスで、例えば、`lv1_k1.lv2_k2.[*].new_field_name`。
- `[*]`: STRUCT 型が ARRAY 型内にネストされている場合、`[*]` は ARRAY フィールド内のすべての要素を表します。これは、ARRAY フィールドの下にネストされたすべての STRUCT 要素にフィールドを追加または削除するために使用されます。
- `prior_field_name`: 新しく追加されたフィールドの前にあるフィールド。AFTER キーワードと組み合わせて新しいフィールドの順序を指定するために使用されます。FIRST キーワードを使用する場合、このパラメータを指定する必要はありません。新しいフィールドが最初のフィールドであることを示します。`prior_field_name` の次元は `field_path`（具体的には `new_field_name` の前の部分、つまり `level1_k1.level2_k2.[*]`）によって決定され、明示的に指定する必要はありません。

`field_path` の例:

- STRUCT 列内にネストされた STRUCT フィールドのサブフィールドを追加または削除。

  例えば、`fx stuct<c1 int, c2 struct <v1 int, v2 int>>` という列があるとします。`c2` の下に `v3` フィールドを追加する構文は次のとおりです:

  ```SQL
  ALTER TABLE tbl MODIFY COLUMN fx ADD FIELD c2.v3 INT
  ```

  操作後、列は `fx stuct<c1 int, c2 struct <v1 int, v2 int, v3 int>>` になります。

- ARRAY フィールド内にネストされたすべての STRUCT フィールドにサブフィールドを追加または削除。

  例えば、`fx struct<c1 int, c2 array<struct <v1 int, v2 int>>>` という列があるとします。フィールド `c2` は ARRAY 型で、2つのフィールド `v1` と `v2` を持つ STRUCT を含んでいます。ネストされた STRUCT に `v3` フィールドを追加する構文は次のとおりです:

  ```SQL
  ALTER TABLE tbl MODIFY COLUMN fx ADD FIELD c2.[*].v3 INT
  ```

  操作後、列は `fx struct<c1 int, c2 array<struct <v1 int, v2 int, v3 int>>>` になります。

詳細な使用方法については、[Example - Column -14](#column) を参照してください。

:::note

- 現在、この機能は共有なしクラスタでのみサポートされています。
- テーブルには `fast_schema_evolution` プロパティが有効である必要があります。
- MAP 型内の STRUCT 型でフィールドを追加または削除することはサポートされていません。
- 新しく追加されたフィールドにはデフォルト値や Nullable などの属性を指定することはできません。デフォルトで Nullable であり、デフォルト値は null です。
- この機能を使用した後、この機能をサポートしていないバージョンにクラスタを直接ダウングレードすることはできません。

:::

### ロールアップインデックスの修正

#### ロールアップインデックスの作成

構文:

```SQL
ALTER TABLE [<db_name>.]<tbl_name> 
ADD ROLLUP rollup_name (column_name1, column_name2, ...)
[FROM from_index_name]
[PROPERTIES ("key"="value", ...)]
```

PROPERTIES: タイムアウト時間の設定をサポートし、デフォルトのタイムアウト時間は1日です。

例:

```SQL
ALTER TABLE [<db_name>.]<tbl_name> 
ADD ROLLUP r1(col1,col2) from r0;
```

#### バッチでロールアップインデックスを作成

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

1. from_index_name が指定されていない場合、デフォルトで基本インデックスから作成されます。
2. ロールアップテーブルの列は from_index に存在する列でなければなりません。
3. プロパティでストレージ形式を指定できます。詳細は CREATE TABLE を参照してください。

#### ロールアップインデックスの削除

構文:

```sql
ALTER TABLE [<db_name>.]<tbl_name>
DROP ROLLUP rollup_name [PROPERTIES ("key"="value", ...)];
```

例:

```sql
ALTER TABLE [<db_name>.]<tbl_name> DROP ROLLUP r1;
```

#### バッチでロールアップインデックスを削除

構文:

```sql
ALTER TABLE [<db_name>.]<tbl_name>
DROP ROLLUP [rollup_name [PROPERTIES ("key"="value", ...)],...];
```

例:

```sql
ALTER TABLE [<db_name>.]<tbl_name> DROP ROLLUP r1, r2;
```

注意: 基本インデックスを削除することはできません。

### ビットマップインデックスの修正

ビットマップインデックスは次の修正をサポートします:

#### ビットマップインデックスの作成

構文:

```sql
ALTER TABLE [<db_name>.]<tbl_name>
ADD INDEX index_name (column [, ...],) [USING BITMAP] [COMMENT 'balabala'];
```

注意:

```plain text
1. ビットマップインデックスは現在のバージョンでのみサポートされています。
2. ビットマップインデックスは単一の列にのみ作成されます。
```

#### ビットマップインデックスの削除

構文:

```sql
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
- `storage_cooldown_ttl`
- `storage_cooldown_time`
- 動的パーティション化関連のプロパティ
- `enable_persistent_index`
- `bloom_filter_columns`
- `colocate_with`
- `bucket_size` (3.2以降でサポート)
- `base_compaction_forbidden_time_ranges` (v3.2.13以降でサポート)

:::note

- ほとんどの場合、一度に1つのプロパティのみを修正することが許可されています。これらのプロパティが同じプレフィックスを持つ場合にのみ、複数のプロパティを一度に修正できます。現在、`dynamic_partition.` と `binlog.` のみがサポートされています。
- 列に対する操作にマージしてプロパティを修正することもできます。詳細は[以下の例](#examples)を参照してください。

:::

### スワップ

スワップは2つのテーブルのアトミック交換をサポートします。

構文:

```sql
ALTER TABLE [<db_name>.]<tbl_name>
SWAP WITH <tbl_name>;
```

### 手動コンパクション (3.1以降)

StarRocks はロードされたデータの異なるバージョンをマージするためにコンパクションメカニズムを使用します。この機能は、小さなファイルを大きなファイルに結合することができ、クエリパフォーマンスを効果的に向上させます。

v3.1以前は、コンパクションは2つの方法で実行されます:

- システムによる自動コンパクション: コンパクションはバックグラウンドで BE レベルで実行されます。ユーザーはコンパクションのためにデータベースやテーブルを指定できません。
- ユーザーは HTTP インターフェースを呼び出してコンパクションを実行できます。

v3.1以降、StarRocks はユーザーが SQL コマンドを実行して手動でコンパクションを実行するための SQL インターフェースを提供します。ユーザーは特定のテーブルまたはパーティションを選択してコンパクションを実行できます。これにより、コンパクションプロセスに対する柔軟性と制御が向上します。

> **注意**
>
> v3.2.13以降、プロパティ [`base_compaction_forbidden_time_ranges`](./CREATE_TABLE.md#forbid-base-compaction) を使用して、特定の時間範囲内でベースコンパクションを禁止することができます。

構文:

```SQL
ALTER TABLE <tbl_name> [ BASE | CUMULATIVE ] COMPACT [ <partition_name> | ( <partition1_name> [, <partition2_name> ...] ) ]
```

つまり:

```SQL
-- テーブル全体に対してコンパクションを実行します。
ALTER TABLE <tbl_name> COMPACT

-- 単一のパーティションに対してコンパクションを実行します。
ALTER TABLE <tbl_name> COMPACT <partition_name>

-- 複数のパーティションに対してコンパクションを実行します。
ALTER TABLE <tbl_name> COMPACT (<partition1_name>[,<partition2_name>,...])

-- 累積コンパクションを実行します。
ALTER TABLE <tbl_name> CUMULATIVE COMPACT (<partition1_name>[,<partition2_name>,...])

-- ベースコンパクションを実行します。
ALTER TABLE <tbl_name> BASE COMPACT (<partition1_name>[,<partition2_name>,...])
```

`information_schema` データベースの `be_compactions` テーブルにはコンパクション結果が記録されます。コンパクション後のデータバージョンをクエリするには、`SELECT * FROM information_schema.be_compactions;` を実行できます。

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

3. レプリカ間のデータ書き込みおよびレプリケーションモードを修正します。

    ```sql
    ALTER TABLE example_db.my_table
    SET ("replicated_storage" = "false");
    ```

   この例では、レプリカ間のデータ書き込みおよびレプリケーションモードを「リーダーレスレプリケーション」に設定します。これは、データがプライマリおよびセカンダリレプリカを区別せずに同時に複数のレプリカに書き込まれることを意味します。詳細は [CREATE TABLE](CREATE_TABLE.md) の `replicated_storage` パラメータを参照してください。

### パーティション

1. パーティションを追加し、デフォルトのバケットモードを使用します。既存のパーティションは [MIN, 2013-01-01) です。追加されたパーティションは [2013-01-01, 2014-01-01) です。

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

### ロールアップインデックス

1. 基本インデックス (k1,k2,k3,v1,v2) に基づいてロールアップインデックス `example_rollup_index` を作成します。カラムベースのストレージが使用されます。

    ```sql
    ALTER TABLE example_db.my_table
    ADD ROLLUP example_rollup_index(k1, k3, v1, v2)
    PROPERTIES("storage_type"="column");
    ```

2. `example_rollup_index(k1,k3,v1,v2)` に基づいてインデックス `example_rollup_index2` を作成します。

    ```sql
    ALTER TABLE example_db.my_table
    ADD ROLLUP example_rollup_index2 (k1, v1)
    FROM example_rollup_index;
    ```

3. 基本インデックス (k1, k2, k3, v1) に基づいてインデックス `example_rollup_index3` を作成します。ロールアップのタイムアウト時間は1時間に設定されます。

    ```sql
    ALTER TABLE example_db.my_table
    ADD ROLLUP example_rollup_index3(k1, k3, v1)
    PROPERTIES("storage_type"="column", "timeout" = "3600");
    ```

4. インデックス `example_rollup_index2` を削除します。

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

2. `example_rollup_index` の `col1` 列の後に値の列 `new_col`（非集計列）を追加します。

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

4. `example_rollup_index` の `col1` 列の後に値の列 `new_col SUM`（集計列）を追加します。

    ```sql
    ALTER TABLE example_db.my_table
    ADD COLUMN new_col INT SUM DEFAULT "0" AFTER col1
    TO example_rollup_index;
    ```

5. `example_rollup_index`（集計）に複数の列を追加します。

    ```sql
    ALTER TABLE example_db.my_table
    ADD COLUMN (col1 INT DEFAULT "1", col2 FLOAT SUM DEFAULT "2.3")
    TO example_rollup_index;
    ```

6. `example_rollup_index`（集計）に複数の列を追加し、`AFTER` を使用して追加された列の位置を指定します。

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

8. 基本インデックスの col1 の列タイプを BIGINT に修正し、`col2` の後に配置します。

    ```sql
    ALTER TABLE example_db.my_table
    MODIFY COLUMN col1 BIGINT DEFAULT "1" AFTER col2;
    ```

9. 基本インデックスの `val1` 列の最大長を64に修正します。元の長さは32です。

    ```sql
    ALTER TABLE example_db.my_table
    MODIFY COLUMN val1 VARCHAR(64) REPLACE DEFAULT "abc";
    ```

10. `example_rollup_index` の列を再配置します。元の列順序は k1, k2, k3, v1, v2 です。

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

1. テーブルの Colocate プロパティを修正します。

     ```sql
     ALTER TABLE example_db.my_table
     SET ("colocate_with" = "t1");
     ```

2. テーブルの動的パーティションプロパティを修正します。

     ```sql
     ALTER TABLE example_db.my_table
     SET ("dynamic_partition.enable" = "false");
     ```

     動的パーティションプロパティが構成されていないテーブルに動的パーティションプロパティを追加する必要がある場合、すべての動的パーティションプロパティを指定する必要があります。

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

### 名前変更

1. `table1` を `table2` に名前変更します。

    ```sql
    ALTER TABLE table1 RENAME table2;
    ```

2. `example_table` のロールアップインデックス `rollup1` を `rollup2` に名前変更します。

    ```sql
    ALTER TABLE example_table RENAME ROLLUP rollup1 rollup2;
    ```

3. `example_table` のパーティション `p1` を `p2` に名前変更します。

    ```sql
    ALTER TABLE example_table RENAME PARTITION p1 p2;
    ```

### ビットマップインデックス

1. `table1` の列 `siteid` にビットマップインデックスを作成します。

    ```sql
    ALTER TABLE table1
    ADD INDEX index_1 (siteid) [USING BITMAP] COMMENT 'balabala';
    ```

2. `table1` の列 `siteid` のビットマップインデックスを削除します。

    ```sql
    ALTER TABLE table1
    DROP INDEX index_1;
    ```

### スワップ

`table1` と `table2` の間でアトミックスワップを行います。

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

## 参考文献

- [CREATE TABLE](CREATE_TABLE.md)
- [SHOW CREATE TABLE](SHOW_CREATE_TABLE.md)
- [SHOW TABLES](SHOW_TABLES.md)
- [SHOW ALTER TABLE](SHOW_ALTER.md)
- [DROP TABLE](DROP_TABLE.md)