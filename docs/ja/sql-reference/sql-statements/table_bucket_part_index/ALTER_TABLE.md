---
displayed_sidebar: docs
---

# ALTER TABLE

## 説明

既存のテーブルを変更します。以下を含みます：

- [テーブル、パーティション、インデックスの名前変更](#rename)
- [テーブルコメントの変更](#alter-table-comment-from-v31)
- [アトミックスワップ](#swap)
- [パーティションの追加/削除とパーティション属性の変更](#modify-partition)
- [スキーマ変更](#schema-change)
- [ロールアップインデックスの作成/削除](#modify-rollup-index)
- [ビットマップインデックスの変更](#modify-bitmap-indexes)
- [手動データバージョンのコンパクション](#manual-compaction-from-31)

:::tip
この操作には、対象テーブルに対する ALTER 権限が必要です。
:::

## 構文

```SQL
ALTER TABLE [<db_name>.]<tbl_name>
alter_clause1[, alter_clause2, ...]
```

`alter_clause` は6つの操作に分類されます：パーティション、ロールアップ、スキーマ変更、名前変更、インデックス、スワップ、コメント、コンパクト。

- rename: テーブル、ロールアップインデックス、またはパーティションの名前を変更します。
- comment: テーブルコメントを変更します（**v3.1以降**でサポート）。
- partition: パーティションのプロパティを変更、パーティションを削除、またはパーティションを追加します。
- bucket: バケッティング方法とバケット数を変更します。
- column: 列を追加、削除、または並べ替え、または列タイプを変更します。
- rollup index: ロールアップインデックスを作成または削除します。
- bitmap index: インデックスを変更します（ビットマップインデックスのみ変更可能）。
- swap: 2つのテーブルをアトミックに交換します。
- schema change: 列を追加、削除、または並べ替え、または列タイプを変更します。
- compact: ロードされたデータのバージョンをマージするための手動コンパクションを実行します（**v3.1以降**でサポート）。

## 制限と使用上の注意

- パーティション、列、ロールアップインデックスに対する操作は、1つの ALTER TABLE ステートメントで実行できません。
- 列名は変更できません。
- 列コメントは変更できません。
- 1つのテーブルには、同時に1つのスキーマ変更操作しか実行できません。1つのテーブルに対して2つのスキーマ変更コマンドを同時に実行することはできません。
- バケット、列、ロールアップインデックスに対する操作は非同期操作です。タスクが送信された後、すぐに成功メッセージが返されます。進行状況を確認するには [SHOW ALTER TABLE](SHOW_ALTER.md) コマンドを実行し、操作をキャンセルするには [CANCEL ALTER TABLE](CANCEL_ALTER_TABLE.md) コマンドを実行します。
- 名前変更、コメント、パーティション、ビットマップインデックス、スワップに対する操作は同期操作であり、コマンドの返り値が実行の完了を示します。

### 名前変更

名前変更は、テーブル名、ロールアップインデックス、およびパーティション名の変更をサポートします。

#### テーブルの名前を変更する

```sql
ALTER TABLE <tbl_name> RENAME <new_tbl_name>
```

#### ロールアップインデックスの名前を変更する

```sql
ALTER TABLE [<db_name>.]<tbl_name>
RENAME ROLLUP <old_rollup_name> <new_rollup_name>
```

#### パーティションの名前を変更する

```sql
ALTER TABLE [<db_name>.]<tbl_name>
RENAME PARTITION <old_partition_name> <new_partition_name>
```

### テーブルコメントの変更 (v3.1以降)

構文:

```sql
ALTER TABLE [<db_name>.]<tbl_name> COMMENT = "<new table comment>";
```

:::tip
現在、列コメントは変更できません。
:::

### パーティションの変更

#### パーティションを追加する

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
        | PARTITIONS START ("<start_integer_value>") END ("<end_integer_value>") EVERY ( <granularity> ) } -- START および END で指定されたパーティション列の値が整数であっても、パーティション列の値はダブルクォートで囲む必要があります。ただし、EVERY 節の間隔値はダブルクォートで囲む必要はありません。
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

  - 範囲パーティションの場合、単一の範囲パーティション (`single_range_partition`) または複数の範囲パーティションを一括で追加できます (`multi_range_partitions`)。
  - リストパーティションの場合、単一のリストパーティションのみ追加できます。

- `distribution_desc`:

   新しいパーティションのバケット数を個別に設定できますが、バケッティング方法を個別に設定することはできません。

- `"key"="value"`:

   新しいパーティションのプロパティを設定できます。詳細は [CREATE TABLE](CREATE_TABLE.md#properties) を参照してください。

例:

- 範囲パーティション

  - テーブル作成時にパーティション列が `event_day` と指定されている場合、例えば `PARTITION BY RANGE(event_day)`、テーブル作成後に新しいパーティションを追加する必要がある場合、以下を実行できます：

    ```sql
    ALTER TABLE site_access ADD PARTITION p4 VALUES LESS THAN ("2020-04-30");
    ```

  - テーブル作成時にパーティション列が `datekey` と指定されている場合、例えば `PARTITION BY RANGE (datekey)`、テーブル作成後に複数のパーティションを一括で追加する必要がある場合、以下を実行できます：

    ```sql
    ALTER TABLE site_access
        ADD PARTITIONS START ("2021-01-05") END ("2021-01-10") EVERY (INTERVAL 1 DAY);
    ```

- リストパーティション

  - テーブル作成時に単一のパーティション列が指定されている場合、例えば `PARTITION BY LIST (city)`、テーブル作成後に新しいパーティションを追加する必要がある場合、以下を実行できます：

    ```sql
    ALTER TABLE t_recharge_detail2
    ADD PARTITION pCalifornia VALUES IN ("Los Angeles","San Francisco","San Diego");
    ```

  - テーブル作成時に複数のパーティション列が指定されている場合、例えば `PARTITION BY LIST (dt,city)`、テーブル作成後に新しいパーティションを追加する必要がある場合、以下を実行できます：

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

#### パーティションを削除する

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
2. DROP PARTITION を実行した後、指定された期間内（デフォルトで1日）に [RECOVER](./RECOVER.md) コマンドを使用して削除されたパーティションを復元できます。
3. DROP PARTITION FORCE を実行すると、パーティションは直接削除され、パーティション上の未完了のアクティビティがあるかどうかを確認せずに復元できなくなります。したがって、一般的にこの操作は推奨されません。

#### 一時パーティションを追加する

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

#### 一時パーティションを削除する

構文:

```sql
ALTER TABLE [<db_name>.]<tbl_name>
DROP TEMPORARY PARTITION <partition_name>
```

#### パーティションプロパティを変更する

**構文**

```sql
ALTER TABLE [<db_name>.]<tbl_name>
MODIFY PARTITION { <partition_name> | ( <partition1_name> [, <partition2_name> ...] ) | (*) }
SET ("key" = "value", ...);
```

**使用法**

- パーティションの以下のプロパティを変更できます：

  - 記憶媒体
  - storage_cooldown_ttl または storage_cooldown_time
  - replication_num

- パーティションが1つしかないテーブルの場合、パーティション名はテーブル名と同じです。複数のパーティションに分割されている場合、`(*)` を使用してすべてのパーティションのプロパティを変更することができ、より便利です。

- `SHOW PARTITIONS FROM <tbl_name>` を実行して、変更後のパーティションプロパティを確認します。

### スキーマ変更

スキーマ変更は以下の変更をサポートします。

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

1. 集計テーブルに値列を追加する場合、agg_type を指定する必要があります。
2. 重複キーテーブルなどの非集計テーブルにキー列を追加する場合、KEY キーワードを指定する必要があります。
3. ベースインデックスに既に存在する列をロールアップインデックスに追加することはできません。（必要に応じてロールアップインデックスを再作成できます。）

#### 指定されたインデックスに複数の列を追加する

構文:

- 複数の列を追加する

  ```sql
  ALTER TABLE [<db_name>.]<tbl_name>
  ADD COLUMN (column_name1 column_type [KEY | agg_type] DEFAULT "default_value", ...)
  [TO rollup_index_name]
  [PROPERTIES ("key"="value", ...)]
  ```

- 複数の列を追加し、AFTER を使用して追加された列の位置を指定する

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

2. 非集計テーブルにキー列を追加する場合、KEY キーワードを指定する必要があります。

3. ベースインデックスに既に存在する列をロールアップインデックスに追加することはできません。（必要に応じて別のロールアップインデックスを作成できます。）

#### 指定されたインデックスから列を削除する

構文:

```sql
ALTER TABLE [<db_name>.]<tbl_name>
DROP COLUMN column_name
[FROM rollup_index_name];
```

注意:

1. パーティション列を削除することはできません。
2. ベースインデックスから列を削除すると、ロールアップインデックスに含まれている場合も削除されます。

#### 指定されたインデックスの列タイプと列位置を変更する

構文:

```sql
ALTER TABLE [<db_name>.]<tbl_name>
MODIFY COLUMN column_name column_type [KEY | agg_type] [NULL | NOT NULL] [DEFAULT "default_value"]
[AFTER column_name|FIRST]
[FROM rollup_index_name]
[PROPERTIES ("key"="value", ...)]
```

注意:

1. 集計モデルの値列を変更する場合、agg_type を指定する必要があります。
2. 非集計モデルのキー列を変更する場合、KEY キーワードを指定する必要があります。
3. 列のタイプのみを変更できます。列の他のプロパティは現在のままです。（つまり、他のプロパティは元のプロパティに従ってステートメントに明示的に記述する必要があります。例8を参照）
4. パーティション列は変更できません。
5. 現在サポートされている変換タイプは以下の通りです（精度の損失はユーザーが保証します）。

   - TINYINT/SMALLINT/INT/BIGINT を TINYINT/SMALLINT/INT/BIGINT/DOUBLE に変換します。
   - TINYINT/SMALLINT/INT/BIGINT/LARGEINT/FLOAT/DOUBLE/DECIMAL を VARCHAR に変換します。VARCHAR は最大長の変更をサポートします。
   - VARCHAR を TINYINT/SMALLINT/INT/BIGINT/LARGEINT/FLOAT/DOUBLE に変換します。
   - VARCHAR を DATE に変換します（現在6つの形式をサポートしています: "%Y-%m-%d", "%y-%m-%d", "%Y%m%d", "%y%m%d", "%Y/%m/%d, "%y/%m/%d"）。
   - DATETIME を DATE に変換します（年-月-日の情報のみが保持されます。例：`2019-12-09 21:47:05` `<-->` `2019-12-09`）。
   - DATE を DATETIME に変換します（時、分、秒をゼロに設定します。例：`2019-12-09` `<-->` `2019-12-09 00:00:00`）。
   - FLOAT を DOUBLE に変換します。
   - INT を DATE に変換します（INT データの変換に失敗した場合、元のデータはそのままです）。

6. NULL から NOT NULL への変換はサポートされていません。

#### 指定されたインデックスの列を並べ替える

構文:

```sql
ALTER TABLE [<db_name>.]<tbl_name>
ORDER BY (column_name1, column_name2, ...)
[FROM rollup_index_name]
[PROPERTIES ("key"="value", ...)]
```

注意:

1. インデックス内のすべての列を記述する必要があります。
2. 値列はキー列の後にリストされます。

#### 生成列を追加する (v3.1以降)

構文:

```sql
ALTER TABLE [<db_name>.]<tbl_name>
ADD COLUMN col_name data_type [NULL] AS generation_expr [COMMENT 'string']
```

生成列を追加し、その式を指定できます。[生成列](../generated_columns.md) は、式の結果を事前に計算して保存するために使用でき、同じ複雑な式を持つクエリを大幅に高速化します。v3.1以降、StarRocks は生成列をサポートしています。

#### テーブルプロパティを変更する

現在、StarRocks は以下のテーブルプロパティの変更をサポートしています：

- `replication_num`
- `default.replication_num`
- `storage_cooldown_ttl`
- `storage_cooldown_time`
- 動的パーティション化関連のプロパティ
- `enable_persistent_index`
- `bloom_filter_columns`
- `colocate_with`

構文:

```sql
ALTER TABLE [<db_name>.]<tbl_name>
SET ("key" = "value",...)
```

注意:
上記のスキーマ変更操作にマージしてプロパティを変更することもできます。以下の例を参照してください。

### ロールアップインデックスの変更

#### ロールアップインデックスを作成する

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

#### バッチでロールアップインデックスを作成する

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

1. from_index_name が指定されていない場合、デフォルトでベースインデックスから作成されます。
2. ロールアップテーブルの列は、from_index に存在する列でなければなりません。
3. プロパティでは、ストレージ形式を指定できます。詳細は CREATE TABLE を参照してください。

#### ロールアップインデックスを削除する

構文:

```sql
ALTER TABLE [<db_name>.]<tbl_name>
DROP ROLLUP rollup_name [PROPERTIES ("key"="value", ...)];
```

例:

```sql
ALTER TABLE [<db_name>.]<tbl_name> DROP ROLLUP r1;
```

#### バッチでロールアップインデックスを削除する

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

### ビットマップインデックスの変更

ビットマップインデックスは以下の変更をサポートします：

#### ビットマップインデックスを作成する

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

#### インデックスを削除する

構文:

```sql
DROP INDEX index_name;
```

### スワップ

スワップは2つのテーブルをアトミックに交換することをサポートします。

構文:

```sql
ALTER TABLE [<db_name>.]<tbl_name>
SWAP WITH <tbl_name>;
```

### 手動コンパクション (3.1以降)

StarRocks は、ロードされたデータの異なるバージョンをマージするためのコンパクションメカニズムを使用しています。この機能は、小さなファイルを大きなファイルに結合することができ、クエリパフォーマンスを効果的に向上させます。

v3.1以前では、コンパクションは2つの方法で実行されます：

- システムによる自動コンパクション: コンパクションは BE レベルでバックグラウンドで実行されます。ユーザーはコンパクションのためにデータベースやテーブルを指定できません。
- ユーザーは HTTP インターフェースを呼び出してコンパクションを実行できます。

v3.1以降、StarRocks はユーザーが SQL コマンドを実行して手動でコンパクションを実行できる SQL インターフェースを提供しています。ユーザーは特定のテーブルまたはパーティションを選択してコンパクションを実行できます。これにより、コンパクションプロセスに対する柔軟性と制御が向上します。

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

`information_schema` データベースの `be_compactions` テーブルにはコンパクション結果が記録されています。コンパクション後のデータバージョンをクエリするには、`SELECT * FROM information_schema.be_compactions;` を実行します。

## 例

### テーブル

1. テーブルのデフォルトのレプリカ数を変更します。これは新しく追加されたパーティションのデフォルトのレプリカ数として使用されます。

    ```sql
    ALTER TABLE example_db.my_table
    SET ("default.replication_num" = "2");
    ```

2. 単一パーティションテーブルの実際のレプリカ数を変更します。

    ```sql
    ALTER TABLE example_db.my_table
    SET ("replication_num" = "3");
    ```

3. レプリカ間のデータ書き込みとレプリケーションモードを変更します。

    ```sql
    ALTER TABLE example_db.my_table
    SET ("replicated_storage" = "false");
    ```

   この例では、レプリカ間のデータ書き込みとレプリケーションモードを「リーダーレスレプリケーション」に設定します。これは、データがプライマリとセカンダリのレプリカを区別せずに同時に複数のレプリカに書き込まれることを意味します。詳細は、[CREATE TABLE](CREATE_TABLE.md) の `replicated_storage` パラメータを参照してください。

### パーティション

1. パーティションを追加し、デフォルトのバケッティングモードを使用します。既存のパーティションは [MIN, 2013-01-01) です。追加されたパーティションは [2013-01-01, 2014-01-01) です。

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

4. パーティションのレプリカ数を変更します。

    ```sql
    ALTER TABLE example_db.my_table
    MODIFY PARTITION p1 SET("replication_num"="1");
    ```

5. 指定されたパーティションのレプリカ数を一括で変更します。

    ```sql
    ALTER TABLE example_db.my_table
    MODIFY PARTITION (p1, p2, p4) SET("replication_num"="1");
    ```

6. すべてのパーティションの記憶媒体を一括で変更します。

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

1. ベースインデックス (k1,k2,k3,v1,v2) に基づいてインデックス `example_rollup_index` を作成します。カラムベースのストレージが使用されます。

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

3. ベースインデックス (k1, k2, k3, v1) に基づいてインデックス `example_rollup_index3` を作成します。ロールアップのタイムアウト時間は1時間に設定されています。

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

### スキーマ変更

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

8. ベースインデックスの col1 の列タイプを BIGINT に変更し、`col2` の後に配置します。

    ```sql
    ALTER TABLE example_db.my_table
    MODIFY COLUMN col1 BIGINT DEFAULT "1" AFTER col2;
    ```

9. ベースインデックスの `val1` 列の最大長を64に変更します。元の長さは32です。

    ```sql
    ALTER TABLE example_db.my_table
    MODIFY COLUMN val1 VARCHAR(64) REPLACE DEFAULT "abc";
    ```

10. `example_rollup_index` の列を並べ替えます。元の列順序は k1, k2, k3, v1, v2 です。

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

12. テーブルのブルームフィルター列を変更します。

     ```sql
     ALTER TABLE example_db.my_table
     SET ("bloom_filter_columns"="k1,k2,k3");
     ```

     この操作は上記のスキーマ変更操作にマージすることもできます（複数の句の構文がわずかに異なることに注意してください）。

     ```sql
     ALTER TABLE example_db.my_table
     DROP COLUMN col2
     PROPERTIES ("bloom_filter_columns"="k1,k2,k3");
     ```

13. テーブルの Colocate プロパティを変更します。

     ```sql
     ALTER TABLE example_db.my_table
     SET ("colocate_with" = "t1");
     ```

14. テーブルのバケッティングモードをランダム分散からハッシュ分散に変更します。

     ```sql
     ALTER TABLE example_db.my_table
     SET ("distribution_type" = "hash");
     ```

15. テーブルの動的パーティションプロパティを変更します。

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

16. 単一のステートメントで複数の列のデータ型を変更します。

    ```sql
    ALTER TABLE example_db.my_table
    MODIFY COLUMN k1 VARCHAR(100) KEY NOT NULL,
    MODIFY COLUMN v2 DOUBLE DEFAULT "1" AFTER v1;
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

### インデックス

1. `table1` の `siteid` 列にビットマップインデックスを作成します。

    ```sql
    ALTER TABLE table1
    ADD INDEX index_1 (siteid) [USING BITMAP] COMMENT 'balabala';
    ```

2. `table1` の `siteid` 列のビットマップインデックスを削除します。

    ```sql
    ALTER TABLE table1
    DROP INDEX index_1;
    ```

### スワップ

`table1` と `table2` の間でアトミックスワップを行います。

```sql
ALTER TABLE table1 SWAP WITH table2
```

### 手動コンパクションの例

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

```sql
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
```