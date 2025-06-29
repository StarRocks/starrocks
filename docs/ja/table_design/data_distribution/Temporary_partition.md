---
displayed_sidebar: docs
sidebar_position: 40
---

# 一時パーティション

このトピックでは、一時パーティション機能の使用方法について説明します。

既にパーティションルールが定義されているパーティションテーブルに一時パーティションを作成し、これらの一時パーティションに対して新しいデータ分散戦略を定義できます。一時パーティションは、パーティション内のデータをアトミックに上書きする場合や、パーティションおよびバケッティング戦略を調整する場合に、一時的なデータキャリアとして機能します。一時パーティションでは、特定の要件を満たすために、パーティション範囲、バケット数、レプリカ数、記憶媒体などのデータ分散戦略をリセットできます。

一時パーティション機能は、次のシナリオで使用できます。

- アトミック上書き操作

  パーティション内のデータを再書き込みする必要があり、その過程でデータをクエリできるようにする場合、まず元の正式なパーティションに基づいて一時パーティションを作成し、新しいデータを一時パーティションにロードします。その後、置換操作を使用して、元の正式なパーティションを一時パーティションでアトミックに置き換えることができます。非パーティションテーブルでのアトミック上書き操作については、[ALTER TABLE - SWAP](../../sql-reference/sql-statements/table_bucket_part_index/ALTER_TABLE.md#swap)を参照してください。

- パーティションデータクエリの同時実行性の調整

  パーティションのバケット数を変更する必要がある場合、元の正式なパーティションと同じパーティション範囲を持つ一時パーティションを作成し、新しいバケット数を指定します。その後、`INSERT INTO` コマンドを使用して、元の正式なパーティションのデータを一時パーティションにロードします。最後に、置換操作を使用して、元の正式なパーティションを一時パーティションでアトミックに置き換えることができます。

- パーティションルールの変更

  パーティション戦略を変更したい場合、例えばパーティションをマージしたり、大きなパーティションを複数の小さなパーティションに分割したりする場合、まず期待されるマージまたは分割範囲を持つ一時パーティションを作成します。その後、`INSERT INTO` コマンドを使用して、元の正式なパーティションのデータを一時パーティションにロードします。最後に、置換操作を使用して、元の正式なパーティションを一時パーティションでアトミックに置き換えることができます。

## 一時パーティションの作成

[ALTER TABLE](../../sql-reference/sql-statements/table_bucket_part_index/ALTER_TABLE.md) コマンドを使用して、一度に1つ以上のパーティションを作成できます。

### 構文

#### 単一の一時パーティションを作成する

```SQL
ALTER TABLE <table_name>
ADD TEMPORARY PARTITION <temporary_partition_name> VALUES [("value1"), {MAXVALUE|("value2")})]
[(partition_desc)]
[DISTRIBUTED BY HASH(<bucket_key>)];
ALTER TABLE <table_name> 
ADD TEMPORARY PARTITION <temporary_partition_name> VALUES LESS THAN {MAXVALUE|(<"value">)}
[(partition_desc)]
[DISTRIBUTED BY HASH(<bucket_key>)];
```

#### 一度に複数のパーティションを作成する

```SQL
ALTER TABLE <table_name>
ADD TEMPORARY PARTITIONS START ("value1") END ("value2") EVERY {(INTERVAL <num> <time_unit>)|<num>}
[(partition_desc)]
[DISTRIBUTED BY HASH(<bucket_key>)];
```

### パラメータ

`partition_desc`: 一時パーティションのバケット数やプロパティ（レプリカ数や記憶媒体など）を指定します。

### 例

テーブル `site_access` に一時パーティション `tp1` を作成し、その範囲を `[2020-01-01, 2020-02-01)` と指定します。

```SQL
ALTER TABLE site_access
ADD TEMPORARY PARTITION tp1 VALUES [("2020-01-01"), ("2020-02-01"));
```

テーブル `site_access` に一時パーティション `tp2` を作成し、その上限を `2020-03-01` と指定します。StarRocks は、前の一時パーティションの上限をこの一時パーティションの下限として使用し、左閉右開範囲 `[2020-02-01, 2020-03-01)` の一時パーティションを生成します。

```SQL
ALTER TABLE site_access
ADD TEMPORARY PARTITION tp2 VALUES LESS THAN ("2020-03-01");
```

テーブル `site_access` に一時パーティション `tp3` を作成し、その上限を `2020-04-01` と指定し、レプリカ数を `1` と指定します。

```SQL
ALTER TABLE site_access
ADD TEMPORARY PARTITION tp3 VALUES LESS THAN ("2020-04-01")
 ("replication_num" = "1")
DISTRIBUTED BY HASH (site_id);
```

テーブル `site_access` に一度に複数のパーティションを作成し、これらのパーティションの範囲を `[2020-04-01, 2021-01-01)` とし、月ごとのパーティショングラニュラリティを指定します。

```SQL
ALTER TABLE site_access 
ADD TEMPORARY PARTITIONS START ("2020-04-01") END ("2021-01-01") EVERY (INTERVAL 1 MONTH);
```

### 使用上の注意

- 一時パーティションのパーティション列は、元の正式なパーティションのパーティション列と同じでなければならず、変更できません。
- 一時パーティションの名前は、他の正式なパーティションや一時パーティションの名前と同じにすることはできません。
- テーブル内のすべての一時パーティションの範囲は重複してはなりませんが、一時パーティションと正式なパーティションの範囲は重複することができます。

## 一時パーティションの表示

[SHOW TEMPORARY PARTITIONS](../../sql-reference/sql-statements/table_bucket_part_index/SHOW_PARTITIONS.md) コマンドを使用して、一時パーティションを表示できます。

```SQL
SHOW TEMPORARY PARTITIONS FROM [db_name.]table_name [WHERE] [ORDER BY] [LIMIT]
```

## 一時パーティションへのデータのロード

`INSERT INTO` コマンド、STREAM LOAD、または BROKER LOAD を使用して、1つ以上の一時パーティションにデータをロードできます。

### `INSERT INTO` コマンドを使用したデータのロード

例:

```SQL
INSERT INTO site_access TEMPORARY PARTITION (tp1) VALUES ("2020-01-01",1,"ca","lily",4);
INSERT INTO site_access TEMPORARY PARTITION (tp2) SELECT * FROM site_access_copy PARTITION p2;
INSERT INTO site_access TEMPORARY PARTITION (tp3, tp4,...) SELECT * FROM site_access_copy PARTITION (p3, p4,...);
```

詳細な構文とパラメータの説明については、[INSERT INTO](../../sql-reference/sql-statements/loading_unloading/INSERT.md) を参照してください。

### STREAM LOAD を使用したデータのロード

例:

```bash
curl --location-trusted -u root: -H "label:123" -H "Expect:100-continue" -H "temporary_partitions: tp1, tp2, ..." -T testData \
    http://host:port/api/example_db/site_access/_stream_load    
```

詳細な構文とパラメータの説明については、[STREAM LOAD](../../sql-reference/sql-statements/loading_unloading/STREAM_LOAD.md) を参照してください。

### BROKER LOAD を使用したデータのロード

例:

```SQL
LOAD LABEL example_db.label1
(
    DATA INFILE("hdfs://hdfs_host:hdfs_port/user/starrocks/data/input/file")
    INTO TABLE my_table
    TEMPORARY PARTITION (tp1, tp2, ...)
    ...
)
WITH BROKER
(
    StorageCredentialParams
);
```

`StorageCredentialParams` は、選択した認証方法に応じて異なる一連の認証パラメータを表します。詳細な構文とパラメータの説明については、[Broker Load](../../sql-reference/sql-statements/loading_unloading/BROKER_LOAD.md) を参照してください。

### ROUTINE LOAD を使用したデータのロード

例:

```SQL
CREATE ROUTINE LOAD example_db.site_access ON example_tbl
COLUMNS(col, col2,...),
TEMPORARY PARTITIONS(tp1, tp2, ...)
FROM KAFKA
(
    "kafka_broker_list" ="<kafka_broker1_ip>:<kafka_broker1_port>,<kafka_broker2_ip>:<kafka_broker2_port>",
    "kafka_topic" = "ordertest"
);
```

詳細な構文とパラメータの説明については、[CREATE ROUTINE LOAD](../../sql-reference/sql-statements/loading_unloading/routine_load/CREATE_ROUTINE_LOAD.md) を参照してください。

## 一時パーティション内のデータのクエリ

指定された一時パーティション内のデータをクエリするには、[SELECT](../../sql-reference/sql-statements/table_bucket_part_index/SELECT.md) ステートメントを使用できます。

```SQL
SELECT * FROM
site_access TEMPORARY PARTITION (tp1);

SELECT * FROM
site_access TEMPORARY PARTITION (tp1, tp2, ...);

SELECT event_day,site_id,pv FROM
site_access TEMPORARY PARTITION (tp1, tp2, ...);
```

2つのテーブルの一時パーティション内のデータをクエリするには、JOIN 句を使用できます。

```SQL
SELECT * FROM
site_access TEMPORARY PARTITION (tp1, tp2, ...)
JOIN
site_access_copy TEMPORARY PARTITION (tp1, tp2, ...)
ON site_access.site_id=site_access1.site_id and site_access.event_day=site_access1.event_day;
```

## 元の正式なパーティションを一時パーティションで置き換える

[ALTER TABLE](../../sql-reference/sql-statements/table_bucket_part_index/ALTER_TABLE.md) ステートメントを使用して、元の正式なパーティションを一時パーティションで置き換え、新しい正式なパーティションを作成できます。

> **注意**
>
> ALTER TABLE ステートメントで操作した元の正式なパーティションと一時パーティションは削除され、復元できません。

### 構文

```SQL
ALTER TABLE table_name REPLACE PARTITION (partition_name) WITH TEMPORARY PARTITION (temporary_partition_name1, ...)
PROPERTIES ("key" = "value");
```

### パラメータ

- **strict_range**
  
  デフォルト値: `true`

  このパラメータが `true` に設定されている場合、すべての元の正式なパーティションの範囲の結合は、置換に使用される一時パーティションの範囲の結合と正確に同じでなければなりません。このパラメータが `false` に設定されている場合、新しい正式なパーティションの範囲が置換後に他の正式なパーティションと重ならないことを確認するだけで済みます。

  - 例 1:
  
    次の例では、元の正式なパーティション `p1`、`p2`、`p3` の範囲の結合は、一時パーティション `tp1` と `tp2` の範囲の結合と同じであり、`tp1` と `tp2` を使用して `p1`、`p2`、`p3` を置き換えることができます。

      ```plaintext
      # 元の正式なパーティション p1、p2、p3 の範囲 => これらの範囲の結合
      [10, 20), [20, 30), [40, 50) => [10, 30), [40, 50)
      
      # 一時パーティション tp1 と tp2 の範囲 => これらの範囲の結合
      [10, 30), [40, 45), [45, 50) => [10, 30), [40, 50)
      ```

  - 例 2:

    次の例では、元の正式なパーティションの範囲の結合が、一時パーティションの範囲の結合と異なります。パラメータ `strict_range` の値が `true` に設定されている場合、一時パーティション `tp1` と `tp2` は元の正式なパーティション `p1` を置き換えることができません。値が `false` に設定されており、一時パーティションの範囲 [10, 30) と [40, 50) が他の正式なパーティションと重ならない場合、一時パーティションは元の正式なパーティションを置き換えることができます。

      ```plaintext
      # 元の正式なパーティション p1 の範囲 => 範囲の結合
      [10, 50) => [10, 50)
      
      # 一時パーティション tp1 と tp2 の範囲 => これらの範囲の結合
      [10, 30), [40, 50) => [10, 30), [40, 50)
      ```

- **use_temp_partition_name**
  
  デフォルト値: `false`

  置換に使用される一時パーティションの数が元の正式なパーティションの数と同じ場合、このパラメータが `false` に設定されていると、置換後も新しい正式なパーティションの名前は変更されません。このパラメータが `true` に設定されている場合、置換後の新しい正式なパーティションの名前は一時パーティションの名前になります。

  次の例では、このパラメータが `false` に設定されている場合、置換後も新しい正式なパーティションの名前は `p1` のままです。ただし、その関連データとプロパティは一時パーティション `tp1` のデータとプロパティに置き換えられます。このパラメータが `true` に設定されている場合、置換後の新しい正式なパーティションの名前は `tp1` に変更されます。元の正式なパーティション `p1` は存在しなくなります。

    ```sql
    ALTER TABLE tbl1 REPLACE PARTITION (p1) WITH TEMPORARY PARTITION (tp1);
    ```

  置換される正式なパーティションの数が置換に使用される一時パーティションの数と異なる場合、このパラメータがデフォルト値 `false` のままであると、このパラメータの値 `false` は無効です。

  次の例では、置換後、新しい正式なパーティションの名前は `tp1` に変更され、元の正式なパーティション `p1` と `p2` は存在しなくなります。

    ```SQL
    ALTER TABLE site_access REPLACE PARTITION (p1, p2) WITH TEMPORARY PARTITION (tp1);
    ```

### 例

元の正式なパーティション `p1` を一時パーティション `tp1` で置き換えます。

```SQL
ALTER TABLE site_access REPLACE PARTITION (p1) WITH TEMPORARY PARTITION (tp1);
```

元の正式なパーティション `p2` と `p3` を一時パーティション `tp2` と `tp3` で置き換えます。

```SQL
ALTER TABLE site_access REPLACE PARTITION (p2, p3) WITH TEMPORARY PARTITION (tp2, tp3);
```

元の正式なパーティション `p4` と `p5` を一時パーティション `tp4` と `tp5` で置き換え、パラメータ `strict_range` を `false`、`use_temp_partition_name` を `true` に指定します。

```SQL
ALTER TABLE site_access REPLACE PARTITION (p4, p5) WITH TEMPORARY PARTITION (tp4, tp5)
PROPERTIES (
    "strict_range" = "false",
    "use_temp_partition_name" = "true"
);
```

### 使用上の注意

- テーブルに一時パーティションがある場合、`ALTER` コマンドを使用して Schema Change 操作をテーブルに対して実行することはできません。
- テーブルに対して Schema Change 操作を実行する際に、一時パーティションをテーブルに追加することはできません。

## 一時パーティションの削除

次のコマンドを使用して、一時パーティション `tp1` を削除します。

```SQL
ALTER TABLE site_access DROP TEMPORARY PARTITION tp1;
```

次の制限に注意してください:

- `DROP` コマンドを使用してデータベースやテーブルを直接削除した場合、`RECOVER` コマンドを使用して制限時間内にデータベースやテーブルを復元できますが、一時パーティションは復元できません。
- `ALTER` コマンドを使用して正式なパーティションを削除した後、`RECOVER` コマンドを使用して制限時間内に復元できます。一時パーティションは正式なパーティションと結びついていないため、一時パーティションに対する操作は正式なパーティションに影響を与えません。
- `ALTER` コマンドを使用して一時パーティションを削除した後、`RECOVER` コマンドを使用して復元することはできません。
- `TRUNCATE` コマンドを使用してテーブル内のデータを削除すると、そのテーブルの一時パーティションも削除され、復元できません。
- `TRUNCATE` コマンドを使用して正式なパーティション内のデータを削除しても、一時パーティションには影響しません。
- `TRUNCATE` コマンドは、一時パーティション内のデータを削除するためには使用できません。