---
displayed_sidebar: docs
---

# ALTER TABLE

## 説明

既存のテーブルを修正します。以下を含みます:

- [テーブル、パーティション、インデックスの名前変更](#rename)
- [アトミックスワップ](#swap)
- [パーティションの追加/削除とパーティション属性の修正](#modify-partition)
- [Schema change](#schema-change)
- [ロールアップインデックスの作成/削除](#modify-rollup-index)
- [ビットマップインデックスの修正](#modify-bitmap-indexes)

## 構文

```SQL
ALTER TABLE [database.]table
alter_clause1[, alter_clause2, ...]
```

`alter_clause` は6つの操作に分類されます: パーティション、ロールアップ、schema change、名前変更、インデックス、スワップ。

- rename: テーブル、ロールアップインデックス、またはパーティションの名前を変更します。
- comment: テーブルコメントを修正します (**v3.1以降でサポート**されます)。
- partition: パーティションのプロパティを修正、パーティションを削除、またはパーティションを追加します。
- bucket: バケット方式とバケット数を修正します。
- column: カラムを追加、削除、または並べ替え、またはカラムタイプを修正します。
- rollup index: ロールアップインデックスを作成または削除します。
- bitmap index: インデックスを修正します (ビットマップインデックスのみが修正可能です)。
- swap: 2つのテーブルのアトミック交換。

## 制限と使用上の注意

- パーティション、カラム、およびロールアップインデックスの操作は、1つの ALTER TABLE ステートメントで実行できません。
- カラム名は修正できません。
- カラムコメントは修正できません。
- 1つのテーブルには、同時に1つの schema change 操作しか実行できません。1つのテーブルに対して2つの schema change コマンドを同時に実行することはできません。
- バケット、カラム、およびロールアップインデックスの操作は非同期操作です。タスクが送信されるとすぐに成功メッセージが返されます。進行状況を確認するには [SHOW ALTER TABLE](../data-manipulation/SHOW_ALTER.md) コマンドを実行し、操作をキャンセルするには [CANCEL ALTER TABLE](../data-definition/CANCEL_ALTER_TABLE.md) コマンドを実行します。
- 名前変更、コメント、パーティション、ビットマップインデックス、およびスワップの操作は同期操作であり、コマンドの戻り値は実行が完了したことを示します。

### 名前変更

名前変更は、テーブル名、ロールアップインデックス、およびパーティション名の修正をサポートします。

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

### パーティションの修正

#### パーティションを追加する

構文:

```SQL
ALTER TABLE [database.]table 
ADD PARTITION [IF NOT EXISTS] <partition_name>
partition_desc ["key"="value"]
[DISTRIBUTED BY HASH (k1[,k2 ...]) [BUCKETS num]];
```

注意:

1. Partition_desc は以下の2つの式をサポートします:

    ```plain
    VALUES LESS THAN [MAXVALUE|("value1", ...)]
    VALUES ("value1", ...), ("value1", ...)
    ```

2. パーティションは左閉右開区間です。ユーザーが右の境界のみを指定した場合、システムは自動的に左の境界を決定します。
3. バケットモードが指定されていない場合、組み込みテーブルで使用されるバケット方式が自動的に使用されます。
4. バケットモードが指定されている場合、バケット数のみが修正可能であり、バケットモードまたはバケットカラムは修正できません。
5. ユーザーは ["key"="value"] でパーティションのいくつかのプロパティを設定できます。詳細は [CREATE TABLE](CREATE_TABLE.md) を参照してください。

#### パーティションを削除する

構文:

```sql
-- 2.0以前
ALTER TABLE [database.]table
DROP PARTITION [IF EXISTS | FORCE] <partition_name>
-- 2.0以降
ALTER TABLE [database.]table
DROP PARTITION [IF EXISTS] <partition_name> [FORCE]
```

注意:

1. パーティション化されたテーブルには少なくとも1つのパーティションを保持してください。
2. DROP PARTITION を実行してしばらくすると、削除されたパーティションは RECOVER ステートメントで復元できます。詳細は RECOVER ステートメントを参照してください。
3. DROP PARTITION FORCE を実行すると、パーティションは直接削除され、パーティション上の未完了のアクティビティがあるかどうかを確認せずに復元できなくなります。したがって、一般的にこの操作は推奨されません。

#### 一時パーティションを追加する

構文:

```sql
ALTER TABLE [database.]table 
ADD TEMPORARY PARTITION [IF NOT EXISTS] <partition_name>
partition_desc ["key"="value"]
[DISTRIBUTED BY HASH (k1[,k2 ...]) [BUCKETS num]]
```

#### 一時パーティションを使用して現在のパーティションを置き換える

構文:

```sql
ALTER TABLE [database.]table
REPLACE PARTITION <partition_name>
partition_desc ["key"="value"]
WITH TEMPORARY PARTITION
partition_desc ["key"="value"]
[PROPERTIES ("key"="value", ...)]
```

#### 一時パーティションを削除する

構文:

```sql
ALTER TABLE [database.]table
DROP TEMPORARY PARTITION <partition_name>
```

#### パーティションプロパティを修正する

構文:

```sql
ALTER TABLE [database.]table
MODIFY PARTITION p1|(p1[, p2, ...]) SET ("key" = "value", ...)
```

注意:

1. パーティションの以下のプロパティを修正できます:

   - storage_medium
   - storage_cooldown_time
   - replication_num
   - in_memory

2. 単一パーティションテーブルの場合、パーティション名はテーブル名と同じです。

### Schema change

Schema change は以下の修正をサポートします。

#### 指定されたインデックスの指定された位置にカラムを追加する

構文:

```sql
ALTER TABLE [database.]table
ADD COLUMN column_name column_type [KEY | agg_type] [DEFAULT "default_value"]
[AFTER column_name|FIRST]
[TO rollup_index_name]
[PROPERTIES ("key"="value", ...)]
```

注意:

```plain text
1. 集計テーブルに値カラムを追加する場合、agg_type を指定する必要があります。
2. 非集計テーブル (例えば重複キーテーブル) にキー カラムを追加する場合、KEY キーワードを指定する必要があります。
3. ベースインデックスに既に存在するカラムをロールアップインデックスに追加することはできません。(必要に応じてロールアップインデックスを再作成できます。)
```

#### 指定されたインデックスに複数のカラムを追加する

構文:

```sql
ALTER TABLE [database.]table
ADD COLUMN (column_name1 column_type [KEY | agg_type] DEFAULT "default_value", ...)
[TO rollup_index_name]
[PROPERTIES ("key"="value", ...)]
```

注意:

1. 集計テーブルに値カラムを追加する場合、`agg_type` を指定する必要があります。

2. 非集計テーブルにキー カラムを追加する場合、KEY キーワードを指定する必要があります。

3. ベースインデックスに既に存在するカラムをロールアップインデックスに追加することはできません。(必要に応じて別のロールアップインデックスを作成できます。)

#### 指定されたインデックスからカラムを削除する

構文:

```sql
ALTER TABLE [database.]table
DROP COLUMN column_name
[FROM rollup_index_name];
```

注意:

1. パーティションカラムは削除できません。
2. カラムがベースインデックスから削除された場合、ロールアップインデックスに含まれている場合も削除されます。

#### 指定されたインデックスのカラムタイプとカラム位置を修正する

構文:

```sql
ALTER TABLE [database.]table
MODIFY COLUMN column_name column_type [KEY | agg_type] [NULL | NOT NULL] [DEFAULT "default_value"]
[AFTER column_name|FIRST]
[FROM rollup_index_name]
[PROPERTIES ("key"="value", ...)]
```

注意:

1. 集計モデルで値カラムを修正する場合、agg_type を指定する必要があります。
2. 非集計モデルでキー カラムを修正する場合、KEY キーワードを指定する必要があります。
3. カラムのタイプのみが修正可能です。カラムの他のプロパティは現在のままです。(つまり、他のプロパティは元のプロパティに従ってステートメントに明示的に記述する必要があります。例8を参照してください)。
4. パーティションカラムは修正できません。
5. 現在サポートされている変換の種類は次のとおりです (精度の損失はユーザーによって保証されます)。

   - TINYINT/SMALLINT/INT/BIGINT を TINYINT/SMALLINT/INT/BIGINT/DOUBLE に変換します。
   - TINTINT/SMALLINT/INT/BIGINT/LARGEINT/FLOAT/DOUBLE/DECIMAL を VARCHAR に変換します。VARCHAR は最大長の修正をサポートします。
   - VARCHAR を TINTINT/SMALLINT/INT/BIGINT/LARGEINT/FLOAT/DOUBLE に変換します。
   - VARCHAR を DATE に変換します (現在6つの形式をサポート: "%Y-%m-%d", "%y-%m-%d", "%Y%m%d", "%y%m%d", "%Y/%m/%d, "%y/%m/%d")
   - DATETIME を DATE に変換します (年-月-日情報のみが保持されます。例: `2019-12-09 21:47:05` `<-->` `2019-12-09`)
   - DATE を DATETIME に変換します (時、分、秒をゼロに設定します。例: `2019-12-09` `<-->` `2019-12-09 00:00:00`)
   - FLOAT を DOUBLE に変換します
   - INT を DATE に変換します (INT データの変換に失敗した場合、元のデータはそのまま残ります)

6. NULL から NOT NULL への変換はサポートされていません。

#### 指定されたインデックスのカラムを並べ替える

構文:

```sql
ALTER TABLE [database.]table
ORDER BY (column_name1, column_name2, ...)
[FROM rollup_index_name]
[PROPERTIES ("key"="value", ...)]
```

注意:

1. インデックス内のすべてのカラムを記述する必要があります。
2. 値カラムはキー カラムの後にリストされます。

#### テーブルプロパティを修正する

現在、StarRocks は以下のテーブルプロパティの修正をサポートしています:

- `replication_num`
- `default.replication_num`
- `storage_cooldown_ttl`
- `storage_cooldown_time`
- 動的パーティション化関連プロパティ
- `enable_persistent_index`
- `bloom_filter_columns`
- `colocate_with`

構文:

```sql
ALTER TABLE [<db_name>.]<tbl_name>
SET ("key" = "value",...)
```

注意:
上記の schema change 操作にマージしてプロパティを修正することもできます。以下の例を参照してください。

### ロールアップインデックスの修正

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

#### ロールアップインデックスをバッチで作成する

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
2. ロールアップテーブル内のカラムは from_index に存在するカラムでなければなりません。
3. プロパティでは、ユーザーがストレージ形式を指定できます。詳細は CREATE TABLE を参照してください。

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

#### ロールアップインデックスをバッチで削除する

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

### ビットマップインデックスの修正

ビットマップインデックスは以下の修正をサポートします:

#### ビットマップインデックスを作成する

構文:

```sql
 ALTER TABLE [database.]table
ADD INDEX index_name (column [, ...],) [USING BITMAP] [COMMENT 'balabala'];
```

注意:

```plain text
1. ビットマップインデックスは現在のバージョンでのみサポートされています。
2. ビットマップインデックスは単一のカラムでのみ作成されます。
```

#### インデックスを削除する

構文:

```sql
DROP INDEX index_name;
```

### スワップ

スワップは2つのテーブルのアトミック交換をサポートします。

構文:

```sql
ALTER TABLE [database.]table
SWAP WITH table_name;
```

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

   この例では、レプリカ間のデータ書き込みとレプリケーションモードを「リーダーレスレプリケーション」に設定します。これは、データがプライマリとセカンダリのレプリカを区別せずに同時に複数のレプリカに書き込まれることを意味します。詳細は [CREATE TABLE](CREATE_TABLE.md) の `replicated_storage` パラメータを参照してください。

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
    DISTRIBUTED BY HASH(k1) BUCKETS 20;
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

5. 指定されたパーティションの `in_memory` プロパティをバッチで変更します。

    ```sql
    ALTER TABLE example_db.my_table
    MODIFY PARTITION (p1, p2, p4) SET("in_memory"="true");
    ```

6. すべてのパーティションの記憶媒体をバッチで変更します。

    ```sql
    ALTER TABLE example_db.my_table
    MODIFY PARTITION (*) SET("storage_medium"="HDD");
    ```

7. パーティションを削除します。

    ```sql
    ALTER TABLE example_db.my_table
    DROP PARTITION p1;
    ```

8. 上下の境界を持つパーティションを追加します。

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

### Schema Change

1. `example_rollup_index` の `col1` カラムの後にキー カラム `new_col` (非集計カラム) を追加します。

    ```sql
    ALTER TABLE example_db.my_table
    ADD COLUMN new_col INT KEY DEFAULT "0" AFTER col1
    TO example_rollup_index;
    ```

2. `example_rollup_index` の `col1` カラムの後に値カラム `new_col` (非集計カラム) を追加します。

    ```sql
    ALTER TABLE example_db.my_table
    ADD COLUMN new_col INT DEFAULT "0" AFTER col1
    TO example_rollup_index;
    ```

3. `example_rollup_index` の `col1` カラムの後にキー カラム `new_col` (集計カラム) を追加します。

    ```sql
    ALTER TABLE example_db.my_table
    ADD COLUMN new_col INT DEFAULT "0" AFTER col1
    TO example_rollup_index;
    ```

4. `example_rollup_index` の `col1` カラムの後に値カラム `new_col SUM` (集計カラム) を追加します。

    ```sql
    ALTER TABLE example_db.my_table
    ADD COLUMN new_col INT SUM DEFAULT "0" AFTER col1
    TO example_rollup_index;
    ```

5. `example_rollup_index` に複数のカラムを追加します (集計)。

    ```sql
    ALTER TABLE example_db.my_table
    ADD COLUMN (col1 INT DEFAULT "1", col2 FLOAT SUM DEFAULT "2.3")
    TO example_rollup_index;
    ```

6. `example_rollup_index` からカラムを削除します。

    ```sql
    ALTER TABLE example_db.my_table
    DROP COLUMN col2
    FROM example_rollup_index;
    ```

7. ベースインデックスの col1 のカラムタイプを BIGINT に変更し、`col2` の後に配置します。

    ```sql
    ALTER TABLE example_db.my_table
    MODIFY COLUMN col1 BIGINT DEFAULT "1" AFTER col2;
    ```

8. ベースインデックスの `val1` カラムの最大長を 64 に変更します。元の長さは 32 です。

    ```sql
    ALTER TABLE example_db.my_table
    MODIFY COLUMN val1 VARCHAR(64) REPLACE DEFAULT "abc";
    ```

9. `example_rollup_index` のカラムを並べ替えます。元のカラム順序は k1, k2, k3, v1, v2 です。

    ```sql
    ALTER TABLE example_db.my_table
    ORDER BY (k3,k1,k2,v2,v1)
    FROM example_rollup_index;
    ```

10. 一度に2つの操作 (ADD COLUMN と ORDER BY) を実行します。

    ```sql
    ALTER TABLE example_db.my_table
    ADD COLUMN v2 INT MAX DEFAULT "0" AFTER k2 TO example_rollup_index,
    ORDER BY (k3,k1,k2,v2,v1) FROM example_rollup_index;
    ```

11. テーブルのブルームフィルターカラムを変更します。

     ```sql
     ALTER TABLE example_db.my_table
     SET ("bloom_filter_columns"="k1,k2,k3");
     ```

     この操作は上記の schema change 操作にマージすることもできます (複数の句の構文がわずかに異なることに注意してください)。

     ```sql
     ALTER TABLE example_db.my_table
     DROP COLUMN col2
     PROPERTIES ("bloom_filter_columns"="k1,k2,k3");
     ```

12. テーブルの Colocate プロパティを変更します。

     ```sql
     ALTER TABLE example_db.my_table
     SET ("colocate_with" = "t1");
     ```

13. テーブルのバケット方式をランダム分散からハッシュ分散に変更します。

     ```sql
     ALTER TABLE example_db.my_table
     SET ("distribution_type" = "hash");
     ```

14. テーブルの動的パーティションプロパティを変更します。

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

15. テーブルの `in_memory` プロパティを変更します。

    ```sql
    ALTER TABLE example_db.my_table
    SET ("in_memory" = "true");
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

1. `table1` のカラム `siteid` にビットマップインデックスを作成します。

    ```sql
    ALTER TABLE table1
    ADD INDEX index_1 (siteid) [USING BITMAP] COMMENT 'balabala';
    ```

2. `table1` のカラム `siteid` のビットマップインデックスを削除します。

    ```sql
    ALTER TABLE table1
    DROP INDEX index_1;
    ```

### スワップ

1. `table1` と `table2` の間でアトミックスワップを行います。

    ```sql
    ALTER TABLE table1 SWAP WITH table2
    ```

## 参考文献

- [CREATE TABLE](CREATE_TABLE.md)
- [SHOW CREATE TABLE](../data-manipulation/SHOW_CREATE_TABLE.md)
- [SHOW ALTER TABLE](../data-manipulation/SHOW_ALTER.md)
```