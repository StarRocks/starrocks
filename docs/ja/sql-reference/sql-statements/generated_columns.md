---
displayed_sidebar: docs
---

# 生成列

バージョン v3.1 以降、StarRocks は生成列をサポートしています。生成列は、複雑な式を含むクエリを高速化するために使用できます。この機能は、式の結果を事前に計算して保存し、[クエリの書き換え](#query-rewrites)をサポートすることで、同じ複雑な式を含むクエリを大幅に高速化します。

テーブル作成時に、式の結果を保存するための生成列を1つ以上定義できます。そのため、定義した生成列に結果が保存されている式を含むクエリを実行する際、CBO はクエリを生成列から直接データを読み取るように書き換えます。あるいは、生成列のデータを直接クエリすることもできます。

また、**生成列がロードパフォーマンスに与える影響を評価することをお勧めします。なぜなら、式の計算には時間がかかるからです**。さらに、**テーブル作成時に生成列を作成することをお勧めします**。なぜなら、テーブル作成後に生成列を追加または変更するのは時間とコストがかかるからです。

ただし、生成列を持つテーブルにデータをロードする際、StarRocks は式に基づいて結果を計算し、生成列に結果を書き込むため、時間とオーバーヘッドが増加する可能性があることに注意してください。

現在、StarRocks の共有データモードは生成列をサポートしていません。

## 基本操作

### 生成列の作成

#### 構文

```SQL
<col_name> <data_type> [NULL] AS <expr> [COMMENT 'string']
```

#### テーブル作成時に生成列を作成

`test_tbl1` という名前のテーブルを作成し、5つの列を持ち、そのうち `newcol1` と `newcol2` は生成列であり、指定された式を使用して通常の列 `data_array` と `data_json` の値を参照して計算されます。

```SQL
CREATE TABLE test_tbl1
(
    id INT NOT NULL,
    data_array ARRAY<int> NOT NULL,
    data_json JSON NOT NULL,
    newcol1 DOUBLE AS array_avg(data_array),
    newcol2 String AS json_string(json_query(data_json, "$.a"))
)
PRIMARY KEY (id)
DISTRIBUTED BY HASH(id);
```

**注意**:

- 生成列は通常の列の後に定義する必要があります。
- 集計関数は生成列の式で使用できません。
- 生成列の式は他の生成列や[自動インクリメント列](table_bucket_part_index/auto_increment.md)を参照できませんが、複数の通常の列を参照することはできます。
- 生成列のデータ型は、生成列の式によって生成される結果のデータ型と一致している必要があります。
- 集計テーブルに生成列を作成することはできません。

#### テーブル作成後に生成列を追加

:::tip

ほとんどの場合、クエリ時に頻繁に使用される式はテーブル作成後に決定されるため、生成列はテーブル作成後に追加されることが多いです。パフォーマンスの観点から、StarRocks がテーブル作成後に生成列を追加するための基盤となるロジックは最適化されています。そのため、生成列を追加する際、StarRocks はすべてのデータを書き換える必要はありません。代わりに、StarRocks は新たに追加された生成列のデータを書き込み、そのデータを既存の物理データファイルと関連付けるだけで済みます。これにより、テーブル作成後に生成列を追加する効率が大幅に向上します。

:::

1. `id`、`data_array`、`data_json` の3つの通常の列を持つ `test_tbl2` という名前のテーブルを作成し、データ行を挿入します。

    ```SQL
    -- テーブルを作成
    CREATE TABLE test_tbl2
    (
        id INT NOT NULL,
        data_array ARRAY<int> NOT NULL,
        data_json JSON NOT NULL
    )
    PRIMARY KEY (id)
    DISTRIBUTED BY HASH(id);

    -- データ行を挿入
    INSERT INTO test_tbl2 VALUES (1, [1,2], parse_json('{"a" : 1, "b" : 2}'));

    -- テーブルをクエリ
    MySQL [example_db]> select * from test_tbl2;
    +------+------------+------------------+
    | id   | data_array | data_json        |
    +------+------------+------------------+
    |    1 | [1,2]      | {"a": 1, "b": 2} |
    +------+------------+------------------+
    1 row in set (0.04 sec)
    ```

2. `ALTER TABLE ... ADD COLUMN ...` を実行して、通常の列 `data_array` と `data_json` の値に基づいて評価された式によって作成された生成列 `newcol1` と `newcol2` を追加します。

    ```SQL
    ALTER TABLE test_tbl2
    ADD COLUMN newcol1 DOUBLE AS array_avg(data_array);

    ALTER TABLE test_tbl2
    ADD COLUMN newcol2 String AS json_string(json_query(data_json, "$.a"));
    ```

    **注意**:

    - 集計テーブルに生成列を追加することはサポートされていません。
    - 通常の列は生成列の前に定義する必要があります。`ALTER TABLE ... ADD COLUMN ...` ステートメントを使用して通常の列を追加する際に、新しい通常の列の位置を指定しない場合、システムは自動的にそれを生成列の前に配置します。さらに、AFTER を使用して通常の列を生成列の後に明示的に配置することはできません。

3. テーブルデータをクエリします。

    ```SQL
    MySQL [example_db]> SELECT * FROM test_tbl2;
    +------+------------+------------------+---------+---------+
    | id   | data_array | data_json        | newcol1 | newcol2 |
    +------+------------+------------------+---------+---------+
    |    1 | [1,2]      | {"a": 1, "b": 2} |     1.5 | 1       |
    +------+------------+------------------+---------+---------+
    1 row in set (0.04 sec)
    ```

    結果は、生成列 `newcol1` と `newcol2` がテーブルに追加され、StarRocks が式に基づいてその値を自動的に計算することを示しています。

### 生成列へのデータロード

データロード中、StarRocks は式に基づいて生成列の値を自動的に計算します。生成列の値を指定することはできません。以下の例では、[INSERT INTO](../../loading/InsertInto.md) ステートメントを使用してデータをロードします。

1. `INSERT INTO` を使用して `test_tbl1` テーブルにレコードを挿入します。`VALUES ()` 句内で生成列の値を指定することはできません。

    ```SQL
    INSERT INTO test_tbl1 (id, data_array, data_json)
    VALUES (1, [1,2], parse_json('{"a" : 1, "b" : 2}'));
    ```

2. テーブルデータをクエリします。

    ```SQL
    MySQL [example_db]> SELECT * FROM test_tbl1;
    +------+------------+------------------+---------+---------+
    | id   | data_array | data_json        | newcol1 | newcol2 |
    +------+------------+------------------+---------+---------+
    |    1 | [1,2]      | {"a": 1, "b": 2} |     1.5 | 1       |
    +------+------------+------------------+---------+---------+
    1 row in set (0.01 sec)
    ```

    結果は、StarRocks が式に基づいて生成列 `newcol1` と `newcol2` の値を自動的に計算することを示しています。

    **注意**:

    データロード中に生成列の値を指定すると、次のエラーが返されます。

    ```SQL
    MySQL [example_db]> INSERT INTO test_tbl1 (id, data_array, data_json, newcol1, newcol2) 
    VALUES (2, [3,4], parse_json('{"a" : 3, "b" : 4}'), 3.5, "3");
    ERROR 1064 (HY000): Getting analyzing error. Detail message: materialized column 'newcol1' can not be specified.

    MySQL [example_db]> INSERT INTO test_tbl1 VALUES (2, [3,4], parse_json('{"a" : 3, "b" : 4}'), 3.5, "3");
    ERROR 1064 (HY000): Getting analyzing error. Detail message: Column count doesn't match value count.
    ```

### 生成列の変更

:::tip

生成列を変更する際、StarRocks はすべてのデータを書き換える必要があり、時間とリソースがかかります。生成列を変更するために ALTER TABLE を使用することが避けられない場合は、事前にコストと時間を評価することをお勧めします。

:::

生成列のデータ型と式を変更できます。

1. `id`、`data_array`、`data_json` の値を参照して指定された式を使用して計算された値を持つ生成列 `newcol1` と `newcol2` を含む5つの列を持つ `test_tbl3` テーブルを作成し、データ行を挿入します。

    ```SQL
    -- テーブルを作成
    MySQL [example_db]> CREATE TABLE test_tbl3
    (
        id INT NOT NULL,
        data_array ARRAY<int> NOT NULL,
        data_json JSON NOT NULL,
        -- 生成列のデータ型と式は次のように指定されます:
        newcol1 DOUBLE AS array_avg(data_array),
        newcol2 String AS json_string(json_query(data_json, "$.a"))
    )
    PRIMARY KEY (id)
    DISTRIBUTED BY HASH(id);

    -- データ行を挿入
    INSERT INTO test_tbl3 (id, data_array, data_json)
    VALUES (1, [1,2], parse_json('{"a" : 1, "b" : 2}'));

    -- テーブルをクエリ
    MySQL [example_db]> select * from test_tbl3;
    +------+------------+------------------+---------+---------+
    | id   | data_array | data_json        | newcol1 | newcol2 |
    +------+------------+------------------+---------+---------+
    |    1 | [1,2]      | {"a": 1, "b": 2} |     1.5 | 1       |
    +------+------------+------------------+---------+---------+
    1 row in set (0.01 sec)
    ```

2. 生成列 `newcol1` と `newcol2` を変更します:

    - 生成列 `newcol1` のデータ型を `ARRAY<INT>` に変更し、その式を `data_array` に変更します。

        ```SQL
        ALTER TABLE test_tbl3 
        MODIFY COLUMN newcol1 ARRAY<INT> AS data_array;
        ```

    - 生成列 `newcol2` の式を通常の列 `data_json` のフィールド `b` の値を抽出するように変更します。

        ```SQL
        ALTER TABLE test_tbl3
        MODIFY COLUMN newcol2 String AS json_string(json_query(data_json, "$.b"));
        ```

3. 変更後のスキーマとテーブル内のデータを表示します。

    - 変更後のスキーマを表示します。

        ```SQL
        MySQL [example_db]> show create table test_tbl3\G
        **** 1. row ****
            Table: test_tbl3
        Create Table: CREATE TABLE test_tbl3 (
        id int(11) NOT NULL COMMENT "",
        data_array array<int(11)> NOT NULL COMMENT "",
        data_json json NOT NULL COMMENT "",
        -- 変更後、生成列のデータ型と式は次のようになります:
        newcol1 array<int(11)> NULL AS example_db.test_tbl3.data_array COMMENT "",
        newcol2 varchar(65533) NULL AS json_string(json_query(example_db.test_tbl3.data_json, '$.b')) COMMENT ""
        ) ENGINE=OLAP 
        PRIMARY KEY(id)
        DISTRIBUTED BY HASH(id)
        PROPERTIES (...);
        1 row in set (0.00 sec)
        ```

    - 変更後のテーブルデータをクエリします。結果は、StarRocks が変更された式に基づいて生成列 `newcol1` と `newcol2` の値を再計算することを示しています。

        ```SQL
        MySQL [example_db]> select * from test_tbl3;
        +------+------------+------------------+---------+---------+
        | id   | data_array | data_json        | newcol1 | newcol2 |
        +------+------------+------------------+---------+---------+
        |    1 | [1,2]      | {"a": 1, "b": 2} | [1,2]   | 2       |
        +------+------------+------------------+---------+---------+
        1 row in set (0.01 sec)
        ```

### 生成列の削除

`test_tbl3` テーブルから列 `newcol1` を削除します。

```SQL
ALTER TABLE test_tbl3 DROP COLUMN newcol1;
```

> **注意**:
>
> 生成列が式で通常の列を参照している場合、その通常の列を直接削除または変更することはできません。代わりに、最初に生成列を削除し、その後で通常の列を削除または変更する必要があります。

### クエリの書き換え

クエリ内の式が生成列の式と一致する場合、オプティマイザはクエリを自動的に書き換えて生成列の値を直接読み取るようにします。

1. 次のスキーマを持つ `test_tbl4` テーブルを作成したとします:

    ```SQL
    CREATE TABLE test_tbl4
    (
        id INT NOT NULL,
        data_array ARRAY<int> NOT NULL,
        data_json JSON NOT NULL,
        newcol1 DOUBLE AS array_avg(data_array),
        newcol2 String AS json_string(json_query(data_json, "$.a"))
    )
    PRIMARY KEY (id) DISTRIBUTED BY HASH(id);
    ```

2. `SELECT array_avg(data_array), json_string(json_query(data_json, "$.a")) FROM test_tbl4;` ステートメントを使用して `test_tbl4` テーブルのデータをクエリする場合、クエリは通常の列 `data_array` と `data_json` のみを含みます。しかし、クエリ内の式は生成列 `newcol1` と `newcol2` の式と一致します。この場合、実行計画は CBO がクエリを自動的に書き換えて生成列 `newcol1` と `newcol2` の値を読み取ることを示しています。

    ```SQL
    MySQL [example_db]> EXPLAIN SELECT array_avg(data_array), json_string(json_query(data_json, "$.a")) FROM test_tbl4;
    +---------------------------------------+
    | Explain String                        |
    +---------------------------------------+
    | PLAN FRAGMENT 0                       |
    |  OUTPUT EXPRS:4: newcol1 | 5: newcol2 | -- クエリは生成列 newcol1 と newcol2 からデータを読み取るように書き換えられます。
    |   PARTITION: RANDOM                   |
    |                                       |
    |   RESULT SINK                         |
    |                                       |
    |   0:OlapScanNode                      |
    |      TABLE: test_tbl4                 |
    |      PREAGGREGATION: ON               |
    |      partitions=0/1                   |
    |      rollup: test_tbl4                |
    |      tabletRatio=0/0                  |
    |      tabletList=                      |
    |      cardinality=1                    |
    |      avgRowSize=2.0                   |
    +---------------------------------------+
    15 rows in set (0.00 sec)
    ```

### 部分更新と生成列

主キーテーブルで部分更新を行うには、生成列が参照するすべての通常の列を `columns` パラメータで指定する必要があります。以下の例では、Stream Load を使用して部分更新を行います。

1. `id`、`data_array`、`data_json` の値を参照して指定された式を使用して計算された値を持つ生成列 `newcol1` と `newcol2` を含む5つの列を持つ `test_tbl5` テーブルを作成し、データ行を挿入します。

    ```SQL
    -- テーブルを作成
    CREATE TABLE test_tbl5
    (
        id INT NOT NULL,
        data_array ARRAY<int> NOT NULL,
        data_json JSON NULL,
        newcol1 DOUBLE AS array_avg(data_array),
        newcol2 String AS json_string(json_query(data_json, "$.a"))
    )
    PRIMARY KEY (id)
    DISTRIBUTED BY HASH(id);

    -- データ行を挿入
    INSERT INTO test_tbl5 (id, data_array, data_json)
    VALUES (1, [1,2], parse_json('{"a" : 1, "b" : 2}'));

    -- テーブルをクエリ
    MySQL [example_db]> select * from test_tbl5;
    +------+------------+------------------+---------+---------+
    | id   | data_array | data_json        | newcol1 | newcol2 |
    +------+------------+------------------+---------+---------+
    |    1 | [1,2]      | {"a": 1, "b": 2} |     1.5 | 1       |
    +------+------------+------------------+---------+---------+
    1 row in set (0.01 sec)
    ```

2. `my_data1.csv` という CSV ファイルを準備して、`test_tbl5` テーブルのいくつかの列を更新します。

    ```SQL
    1|[3,4]|{"a": 3, "b": 4}
    2|[3,4]|{"a": 3, "b": 4} 
    ```

3. [Stream Load](../../loading/StreamLoad.md) を使用して `my_data1.csv` ファイルで `test_tbl5` テーブルのいくつかの列を更新します。`partial_update:true` を設定し、生成列が参照するすべての通常の列を `columns` パラメータで指定する必要があります。

    ```Bash
    curl --location-trusted -u <username>:<password> -H "label:1" \
        -H "column_separator:|" \
        -H "partial_update:true" \
        -H "columns:id,data_array,data_json" \ 
        -T my_data1.csv -XPUT \
        http://<fe_host>:<fe_http_port>/api/example_db/test_tbl5/_stream_load
    ```

4. テーブルデータをクエリします。

    ```SQL
    [example_db]> select * from test_tbl5;
    +------+------------+------------------+---------+---------+
    | id   | data_array | data_json        | newcol1 | newcol2 |
    +------+------------+------------------+---------+---------+
    |    1 | [3,4]      | {"a": 3, "b": 4} |     3.5 | 3       |
    |    2 | [3,4]      | {"a": 3, "b": 4} |     3.5 | 3       |
    +------+------------+------------------+---------+---------+
    2 rows in set (0.01 sec)
    ```

生成列が参照するすべての通常の列を指定せずに部分更新を行うと、Stream Load によってエラーが返されます。

1. `my_data2.csv` という CSV ファイルを準備します。

      ```csv
      1|[3,4]
      2|[3,4]
      ```

2. `my_data2.csv` ファイルを使用して [Stream Load](../../loading/StreamLoad.md) で部分列更新を行う場合、`my_data2.csv` に `data_json` 列の値が提供されておらず、Stream Load ジョブの `columns` パラメータに `data_json` 列が含まれていない場合、たとえ `data_json` 列が null 値を許可していても、生成列 `newcol2` が `data_json` 列を参照しているため、Stream Load によってエラーが返されます。