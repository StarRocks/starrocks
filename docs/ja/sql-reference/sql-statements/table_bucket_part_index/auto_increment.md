---
displayed_sidebar: docs
---

# AUTO_INCREMENT

バージョン 3.0 以降、StarRocks は `AUTO_INCREMENT` 列属性をサポートしており、データ管理を簡素化できます。このトピックでは、`AUTO_INCREMENT` 列属性の適用シナリオ、使用法、および機能について紹介します。

## 概要

新しいデータ行がテーブルにロードされ、`AUTO_INCREMENT` 列の値が指定されていない場合、StarRocks はその行の `AUTO_INCREMENT` 列に対してテーブル全体で一意の ID として整数値を自動的に割り当てます。`AUTO_INCREMENT` 列の後続の値は、その行の ID から特定のステップで自動的に増加します。`AUTO_INCREMENT` 列はデータ管理を簡素化し、一部のクエリを高速化するために使用できます。以下は、`AUTO_INCREMENT` 列の適用シナリオです：

- 主キーとして使用: `AUTO_INCREMENT` 列を主キーとして使用することで、各行に一意の ID を持たせ、データのクエリと管理を容易にします。
- テーブルのジョイン: 複数のテーブルをジョインする際に、`AUTO_INCREMENT` 列をジョインキーとして使用することで、例えば UUID のような STRING 型の列を使用するよりもクエリを迅速化できます。
- 高カーディナリティ列の異なる値の数を数える: `AUTO_INCREMENT` 列を辞書内の一意の値列として使用できます。直接 STRING の異なる値を数えるのに比べて、`AUTO_INCREMENT` 列の異なる整数値を数えることで、クエリ速度が数倍、あるいは数十倍向上することがあります。

CREATE TABLE ステートメントで `AUTO_INCREMENT` 列を指定する必要があります。`AUTO_INCREMENT` 列のデータ型は BIGINT でなければなりません。AUTO_INCREMENT 列の値は [暗黙的に割り当てるか明示的に指定する](#assign-values-for-auto_increment-column) ことができます。1 から始まり、新しい行ごとに 1 ずつ増加します。

## 基本操作

### テーブル作成時に `AUTO_INCREMENT` 列を指定する

`id` と `number` の 2 つの列を持つ `test_tbl1` という名前のテーブルを作成します。列 `number` を `AUTO_INCREMENT` 列として指定します。

```SQL
CREATE TABLE test_tbl1
(
    id BIGINT NOT NULL, 
    number BIGINT NOT NULL AUTO_INCREMENT
) 
PRIMARY KEY (id) 
DISTRIBUTED BY HASH(id)
PROPERTIES("replicated_storage" = "true");
```

### `AUTO_INCREMENT` 列に値を割り当てる

#### 暗黙的に値を割り当てる

StarRocks テーブルにデータをロードする際、`AUTO_INCREMENT` 列の値を指定する必要はありません。StarRocks はその列に一意の整数値を自動的に割り当て、テーブルに挿入します。

```SQL
INSERT INTO test_tbl1 (id) VALUES (1);
INSERT INTO test_tbl1 (id) VALUES (2);
INSERT INTO test_tbl1 (id) VALUES (3),(4),(5);
```

テーブル内のデータを表示します。

```SQL
mysql > SELECT * FROM test_tbl1 ORDER BY id;
+------+--------+
| id   | number |
+------+--------+
|    1 |      1 |
|    2 |      2 |
|    3 |      3 |
|    4 |      4 |
|    5 |      5 |
+------+--------+
5 rows in set (0.02 sec)
```

StarRocks テーブルにデータをロードする際、`AUTO_INCREMENT` 列の値を `DEFAULT` として指定することもできます。StarRocks はその列に一意の整数値を自動的に割り当て、テーブルに挿入します。

```SQL
INSERT INTO test_tbl1 (id, number) VALUES (6, DEFAULT);
```

テーブル内のデータを表示します。

```SQL
mysql > SELECT * FROM test_tbl1 ORDER BY id;
+------+--------+
| id   | number |
+------+--------+
|    1 |      1 |
|    2 |      2 |
|    3 |      3 |
|    4 |      4 |
|    5 |      5 |
|    6 |      6 |
+------+--------+
6 rows in set (0.02 sec)
```

実際の使用では、テーブル内のデータを表示したときに次の結果が返されることがあります。これは、StarRocks が `AUTO_INCREMENT` 列の値が厳密に単調であることを保証できないためです。しかし、StarRocks は値が大まかに時系列順に増加することを保証できます。詳細については、[Monotonicity](#monotonicity) を参照してください。

```SQL
mysql > SELECT * FROM test_tbl1 ORDER BY id;
+------+--------+
| id   | number |
+------+--------+
|    1 |      1 |
|    2 | 100001 |
|    3 | 200001 |
|    4 | 200002 |
|    5 | 200003 |
|    6 | 200004 |
+------+--------+
6 rows in set (0.01 sec)
```

#### 明示的に値を指定する

`AUTO_INCREMENT` 列の値を明示的に指定し、テーブルに挿入することもできます。

```SQL
INSERT INTO test_tbl1 (id, number) VALUES (7, 100);

-- テーブル内のデータを表示します。

mysql > SELECT * FROM test_tbl1 ORDER BY id;
+------+--------+
| id   | number |
+------+--------+
|    1 |      1 |
|    2 | 100001 |
|    3 | 200001 |
|    4 | 200002 |
|    5 | 200003 |
|    6 | 200004 |
|    7 |    100 |
+------+--------+
7 rows in set (0.01 sec)
```

さらに、明示的に値を指定しても、新しく挿入されたデータ行に対して StarRocks が生成する後続の値には影響しません。

```SQL
INSERT INTO test_tbl1 (id) VALUES (8);

-- テーブル内のデータを表示します。

mysql > SELECT * FROM test_tbl1 ORDER BY id;
+------+--------+
| id   | number |
+------+--------+
|    1 |      1 |
|    2 | 100001 |
|    3 | 200001 |
|    4 | 200002 |
|    5 | 200003 |
|    6 | 200004 |
|    7 |    100 |
|    8 |      2 |
+------+--------+
8 rows in set (0.01 sec)
```

**注意**

`AUTO_INCREMENT` 列に対して暗黙的に割り当てられた値と明示的に指定された値を同時に使用しないことをお勧めします。指定された値が StarRocks によって生成された値と同じになる可能性があり、[自動増分 ID のグローバルな一意性](#uniqueness) を破る可能性があるためです。

## 基本機能

### 一意性

一般に、StarRocks は `AUTO_INCREMENT` 列の値がテーブル全体でグローバルに一意であることを保証します。`AUTO_INCREMENT` 列に対して暗黙的に割り当てられた値と明示的に指定された値を同時に使用しないことをお勧めします。そうすることで、自動増分 ID のグローバルな一意性が破られる可能性があります。以下は簡単な例です：`id` と `number` の 2 つの列を持つ `test_tbl2` という名前のテーブルを作成します。列 `number` を `AUTO_INCREMENT` 列として指定します。

```SQL
CREATE TABLE test_tbl2
(
    id BIGINT NOT NULL,
    number BIGINT NOT NULL AUTO_INCREMENT
 ) 
PRIMARY KEY (id) 
DISTRIBUTED BY HASH(id)
PROPERTIES("replicated_storage" = "true");
```

テーブル `test_tbl2` の `AUTO_INCREMENT` 列 `number` に対して暗黙的に割り当てられた値と明示的に指定された値を使用します。

```SQL
INSERT INTO test_tbl2 (id, number) VALUES (1, DEFAULT);
INSERT INTO test_tbl2 (id, number) VALUES (2, 2);
INSERT INTO test_tbl2 (id) VALUES (3);
```

テーブル `test_tbl2` をクエリします。

```SQL
mysql > SELECT * FROM test_tbl2 ORDER BY id;
+------+--------+
| id   | number |
+------+--------+
|    1 |      1 |
|    2 |      2 |
|    3 | 100001 |
+------+--------+
3 rows in set (0.08 sec)
```

### 単調性

自動増分 ID の割り当てパフォーマンスを向上させるために、BEs は一部の自動増分 ID をローカルにキャッシュします。この状況では、StarRocks は `AUTO_INCREMENT` 列の値が厳密に単調であることを保証できません。値が大まかに時系列順に増加することのみが保証されます。

> **注意**
>
> BEs によってキャッシュされる自動増分 ID の数は、FE の動的パラメータ `auto_increment_cache_size` によって決定され、デフォルトは `100,000` です。この値は `ADMIN SET FRONTEND CONFIG ("auto_increment_cache_size" = "xxx");` を使用して変更できます。

例えば、StarRocks クラスターには 1 つの FE ノードと 2 つの BE ノードがあります。`test_tbl3` という名前のテーブルを作成し、次のように 5 行のデータを挿入します：

```SQL
CREATE TABLE test_tbl3
(
    id BIGINT NOT NULL,
    number BIGINT NOT NULL AUTO_INCREMENT
) 
PRIMARY KEY (id)
DISTRIBUTED BY HASH(id)
PROPERTIES("replicated_storage" = "true");

INSERT INTO test_tbl3 VALUES (1, DEFAULT);
INSERT INTO test_tbl3 VALUES (2, DEFAULT);
INSERT INTO test_tbl3 VALUES (3, DEFAULT);
INSERT INTO test_tbl3 VALUES (4, DEFAULT);
INSERT INTO test_tbl3 VALUES (5, DEFAULT);
```

テーブル `test_tbl3` の自動増分 ID は単調に増加しません。これは、2 つの BE ノードがそれぞれ [1, 100000] と [100001, 200000] の自動増分 ID をキャッシュしているためです。複数の INSERT ステートメントを使用してデータがロードされると、データは異なる BE ノードに送信され、それぞれが独立して自動増分 ID を割り当てます。したがって、自動増分 ID が厳密に単調であることは保証できません。

```SQL
mysql > SELECT * FROM test_tbl3 ORDER BY id;
+------+--------+
| id   | number |
+------+--------+
|    1 |      1 |
|    2 | 100001 |
|    3 | 200001 |
|    4 |      2 |
|    5 | 100002 |
+------+--------+
5 rows in set (0.07 sec)
```

## 部分更新と `AUTO_INCREMENT` 列

このセクションでは、`AUTO_INCREMENT` 列を含むテーブルで特定の列のみを更新する方法について説明します。

> **注意**
>
> 現在、部分更新をサポートしているのは主キーテーブルのみです。

### `AUTO_INCREMENT` 列が主キーの場合

部分更新中に主キーを指定する必要があります。したがって、`AUTO_INCREMENT` 列が主キーまたは主キーの一部である場合、部分更新のユーザーの動作は `AUTO_INCREMENT` 列が定義されていない場合とまったく同じです。

1. データベース `example_db` にテーブル `test_tbl4` を作成し、1 行のデータを挿入します。

    ```SQL
    -- テーブルを作成します。
    CREATE TABLE test_tbl4
    (
        id BIGINT AUTO_INCREMENT,
        name BIGINT NOT NULL,
        job1 BIGINT NOT NULL,
        job2 BIGINT NOT NULL
    ) 
    PRIMARY KEY (id, name)
    DISTRIBUTED BY HASH(id)
    PROPERTIES("replicated_storage" = "true");

    -- データを準備します。
    mysql > INSERT INTO test_tbl4 (id, name, job1, job2) VALUES (0, 0, 1, 1);
    Query OK, 1 row affected (0.04 sec)
    {'label':'insert_6af28e77-7d2b-11ed-af6e-02424283676b', 'status':'VISIBLE', 'txnId':'152'}

    -- テーブルをクエリします。
    mysql > SELECT * FROM test_tbl4 ORDER BY id;
    +------+------+------+------+
    | id   | name | job1 | job2 |
    +------+------+------+------+
    |    0 |    0 |    1 |    1 |
    +------+------+------+------+
    1 row in set (0.01 sec)
    ```

2. テーブル `test_tbl4` を更新するための CSV ファイル **my_data4.csv** を準備します。CSV ファイルには `AUTO_INCREMENT` 列の値が含まれており、列 `job1` の値は含まれていません。最初の行の主キーは既にテーブル `test_tbl4` に存在し、2 番目の行の主キーはテーブルに存在しません。

    ```Plaintext
    0,0,99
    1,1,99
    ```

3. [Stream Load](../loading_unloading/STREAM_LOAD.md) ジョブを実行し、CSV ファイルを使用してテーブル `test_tbl4` を更新します。

    ```Bash
    curl --location-trusted -u <username>:<password> -H "label:1" \
        -H "column_separator:," \
        -H "partial_update:true" \
        -H "columns:id,name,job2" \
        -T my_data4.csv -XPUT \
        http://<fe_host>:<fe_http_port>/api/example_db/test_tbl4/_stream_load
    ```

4. 更新されたテーブルをクエリします。最初の行のデータは既にテーブル `test_tbl4` に存在しており、列 `job1` の値は変更されません。2 番目の行のデータは新しく挿入され、列 `job1` のデフォルト値が指定されていないため、部分更新フレームワークはこの列の値を直接 `0` に設定します。

    ```SQL
    mysql > SELECT * FROM test_tbl4 ORDER BY id;
    +------+------+------+------+
    | id   | name | job1 | job2 |
    +------+------+------+------+
    |    0 |    0 |    1 |   99 |
    |    1 |    1 |    0 |   99 |
    +------+------+------+------+
    2 rows in set (0.01 sec)
    ```

### `AUTO_INCREMENT` 列が主キーでない場合

`AUTO_INCREMENT` 列が主キーまたは主キーの一部でない場合、Stream Load ジョブで自動増分 ID が提供されていないと、次の状況が発生します：

- 行がテーブルに既に存在する場合、StarRocks は自動増分 ID を更新しません。
- 行が新たにテーブルにロードされる場合、StarRocks は新しい自動増分 ID を生成します。

この機能は、異なる STRING 値を迅速に計算するための辞書テーブルを構築するために使用できます。

1. データベース `example_db` にテーブル `test_tbl5` を作成し、列 `job1` を `AUTO_INCREMENT` 列として指定し、テーブル `test_tbl5` にデータ行を挿入します。

    ```SQL
    -- テーブルを作成します。
    CREATE TABLE test_tbl5
    (
        id BIGINT NOT NULL,
        name BIGINT NOT NULL,
        job1 BIGINT NOT NULL AUTO_INCREMENT,
        job2 BIGINT NOT NULL
    )
    PRIMARY KEY (id, name)
    DISTRIBUTED BY HASH(id)
    PROPERTIES("replicated_storage" = "true");

    -- データを準備します。
    mysql > INSERT INTO test_tbl5 VALUES (0, 0, -1, -1);
    Query OK, 1 row affected (0.04 sec)
    {'label':'insert_458d9487-80f6-11ed-ae56-aa528ccd0ebf', 'status':'VISIBLE', 'txnId':'94'}

    -- テーブルをクエリします。
    mysql > SELECT * FROM test_tbl5 ORDER BY id;
    +------+------+------+------+
    | id   | name | job1 | job2 |
    +------+------+------+------+
    |    0 |    0 |   -1 |   -1 |
    +------+------+------+------+
    1 row in set (0.01 sec)
    ```

2. テーブル `test_tbl5` を更新するための CSV ファイル **my_data5.csv** を準備します。CSV ファイルには `AUTO_INCREMENT` 列 `job1` の値が含まれていません。最初の行の主キーはテーブルに既に存在していますが、2 番目と 3 番目の行の主キーは存在しません。

    ```Plaintext
    0,0,99
    1,1,99
    2,2,99
    ```

3. [Stream Load](../loading_unloading/STREAM_LOAD.md) ジョブを実行して、CSV ファイルからテーブル `test_tbl5` にデータをロードします。

    ```Bash
    curl --location-trusted -u <username>:<password> -H "label:2" \
        -H "column_separator:," \
        -H "partial_update:true" \
        -H "columns: id,name,job2" \
        -T my_data5.csv -XPUT \
        http://<fe_host>:<fe_http_port>/api/example_db/test_tbl5/_stream_load
    ```

4. 更新されたテーブルをクエリします。最初の行のデータは既にテーブル `test_tbl5` に存在しているため、`AUTO_INCREMENT` 列 `job1` は元の値を保持します。2 番目と 3 番目の行のデータは新しく挿入されるため、StarRocks は `AUTO_INCREMENT` 列 `job1` に新しい値を生成します。

    ```SQL
    mysql > SELECT * FROM test_tbl5 ORDER BY id;
    +------+------+--------+------+
    | id   | name | job1   | job2 |
    +------+------+--------+------+
    |    0 |    0 |     -1 |   99 |
    |    1 |    1 |      1 |   99 |
    |    2 |    2 | 100001 |   99 |
    +------+------+--------+------+
    3 rows in set (0.01 sec)
    ```

## 制限事項

- `AUTO_INCREMENT` 列を持つテーブルを作成する際には、すべてのレプリカが同じ自動増分 ID を持つことを保証するために `'replicated_storage' = 'true'` を設定する必要があります。
- 各テーブルには 1 つの `AUTO_INCREMENT` 列しか持てません。
- `AUTO_INCREMENT` 列のデータ型は BIGINT でなければなりません。
- `AUTO_INCREMENT` 列は `NOT NULL` であり、デフォルト値を持たない必要があります。
- `AUTO_INCREMENT` 列を持つ主キーテーブルからデータを削除することができます。ただし、`AUTO_INCREMENT` 列が主キーまたは主キーの一部でない場合、以下のシナリオでデータを削除する際には次の制限事項に注意する必要があります：

  - DELETE 操作中に、UPSERT 操作のみを含む部分更新のロードジョブがある場合。UPSERT 操作と DELETE 操作が同じデータ行にヒットし、UPSERT 操作が DELETE 操作の後に実行される場合、UPSERT 操作が効果を発揮しない可能性があります。
  - 部分更新のロードジョブがあり、同じデータ行に対して複数の UPSERT および DELETE 操作が含まれている場合。特定の UPSERT 操作が DELETE 操作の後に実行される場合、UPSERT 操作が効果を発揮しない可能性があります。

- ALTER TABLE を使用して `AUTO_INCREMENT` 属性を追加することはサポートされていません。
- バージョン 3.1 以降、StarRocks の共有データモードは `AUTO_INCREMENT` 属性をサポートしています。
- StarRocks は `AUTO_INCREMENT` 列の開始値とステップサイズを指定することをサポートしていません。

## キーワード

AUTO_INCREMENT, AUTO INCREMENT