---
displayed_sidebar: docs
---

# ロードによるデータ変更

import InsertPrivNote from '../_assets/commonMarkdown/insertPrivNote.md'

StarRocks が提供する[主キーテーブル](../table_design/table_types/primary_key_table.md)を使用すると、[Stream Load](../sql-reference/sql-statements/loading_unloading/STREAM_LOAD.md)、[Broker Load](../sql-reference/sql-statements/loading_unloading/BROKER_LOAD.md)、または[Routine Load](../sql-reference/sql-statements/loading_unloading/routine_load/CREATE_ROUTINE_LOAD.md)ジョブを実行して StarRocks テーブルにデータ変更を加えることができます。これらのデータ変更には、挿入、更新、削除が含まれます。ただし、主キーテーブルは、[Spark Load](../sql-reference/sql-statements/loading_unloading/SPARK_LOAD.md)や[INSERT](../sql-reference/sql-statements/loading_unloading/INSERT.md)を使用したデータ変更をサポートしていません。

StarRocks は部分更新と条件付き更新もサポートしています。

<InsertPrivNote />

このトピックでは、CSV データを例にして、ロードを通じて StarRocks テーブルにデータ変更を加える方法を説明します。サポートされるデータファイル形式は、選択したロード方法によって異なります。

> **注意**
>
> CSV データの場合、UTF-8 文字列（カンマ（,）、タブ、パイプ（|）など）をテキスト区切り文字として使用できますが、その長さは 50 バイトを超えないようにしてください。

## 実装

StarRocks が提供する主キーテーブルは、UPSERT および DELETE 操作をサポートしており、INSERT 操作と UPDATE 操作を区別しません。

ロードジョブを作成する際に、StarRocks はジョブ作成ステートメントまたはコマンドに `__op` という名前のフィールドを追加することをサポートしています。`__op` フィールドは、実行したい操作の種類を指定するために使用されます。

> **注意**
>
> テーブルを作成する際に、そのテーブルに `__op` という名前の列を追加する必要はありません。

`__op` フィールドの定義方法は、選択したロード方法によって異なります。

- Stream Load を選択した場合、`columns` パラメータを使用して `__op` フィールドを定義します。

- Broker Load を選択した場合、SET 句を使用して `__op` フィールドを定義します。

- Routine Load を選択した場合、`COLUMNS` 列を使用して `__op` フィールドを定義します。

データ変更に基づいて `__op` フィールドを追加するかどうかを決定できます。`__op` フィールドを追加しない場合、操作の種類はデフォルトで UPSERT になります。主なデータ変更シナリオは次のとおりです。

- ロードしたいデータファイルが UPSERT 操作のみを含む場合、`__op` フィールドを追加する必要はありません。

- ロードしたいデータファイルが DELETE 操作のみを含む場合、`__op` フィールドを追加し、操作の種類を DELETE として指定する必要があります。

- ロードしたいデータファイルが UPSERT および DELETE 操作の両方を含む場合、`__op` フィールドを追加し、データファイルに `0` または `1` の値を持つ列が含まれていることを確認する必要があります。`0` の値は UPSERT 操作を示し、`1` の値は DELETE 操作を示します。

## 使用上の注意

- データファイルの各行が同じ数の列を持っていることを確認してください。

- データ変更に関与する列には、主キー列が含まれている必要があります。

## 基本操作

このセクションでは、ロードを通じて StarRocks テーブルにデータ変更を加える方法の例を示します。詳細な構文とパラメータの説明については、[STREAM LOAD](../sql-reference/sql-statements/loading_unloading/STREAM_LOAD.md)、[BROKER LOAD](../sql-reference/sql-statements/loading_unloading/BROKER_LOAD.md)、および[CREATE ROUTINE LOAD](../sql-reference/sql-statements/loading_unloading/routine_load/CREATE_ROUTINE_LOAD.md)を参照してください。

### UPSERT

ロードしたいデータファイルが UPSERT 操作のみを含む場合、`__op` フィールドを追加する必要はありません。

> **注意**
>
> `__op` フィールドを追加する場合:
>
> - 操作の種類を UPSERT として指定できます。
> - `__op` フィールドを空のままにしておくことができます。操作の種類はデフォルトで UPSERT になります。

#### データ例

1. データファイルを準備します。

   a. ローカルファイルシステムに `example1.csv` という名前の CSV ファイルを作成します。このファイルは、ユーザー ID、ユーザー名、ユーザースコアを順に表す 3 つの列で構成されています。

      ```Plain
      101,Lily,100
      102,Rose,100
      ```

   b. `example1.csv` のデータを Kafka クラスターの `topic1` に公開します。

2. StarRocks テーブルを準備します。

   a. StarRocks データベース `test_db` に `table1` という名前の主キーテーブルを作成します。このテーブルは、`id`、`name`、`score` の 3 つの列で構成されており、`id` が主キーです。

      ```SQL
      CREATE TABLE `table1`
      (
          `id` int(11) NOT NULL COMMENT "user ID",
          `name` varchar(65533) NOT NULL COMMENT "user name",
          `score` int(11) NOT NULL COMMENT "user score"
      )
      ENGINE=OLAP
      PRIMARY KEY(`id`)
      DISTRIBUTED BY HASH(`id`);
      ```

      > **注意**
      >
      > バージョン v2.5.7 以降、StarRocks はテーブルを作成する際やパーティションを追加する際に、バケット数 (BUCKETS) を自動的に設定できます。バケット数を手動で設定する必要はありません。詳細情報については、[バケット数の設定](../table_design/data_distribution/Data_distribution.md#set-the-number-of-buckets)を参照してください。

   b. `table1` にレコードを挿入します。

      ```SQL
      INSERT INTO table1 VALUES
          (101, 'Lily',80);
      ```

#### データのロード

`example1.csv` の `id` が `101` のレコードを `table1` に更新し、`example1.csv` の `id` が `102` のレコードを `table1` に挿入するロードジョブを実行します。

- Stream Load ジョブを実行します。
  
  - `__op` フィールドを含めたくない場合、次のコマンドを実行します。

    ```Bash
    curl --location-trusted -u <username>:<password> \
        -H "Expect:100-continue" \
        -H "label:label1" \
        -H "column_separator:," \
        -T example1.csv -XPUT \
        http://<fe_host>:<fe_http_port>/api/test_db/table1/_stream_load
    ```

  - `__op` フィールドを含めたい場合、次のコマンドを実行します。

    ```Bash
    curl --location-trusted -u <username>:<password> \
        -H "Expect:100-continue" \
        -H "label:label2" \
        -H "column_separator:," \
        -H "columns:__op ='upsert'" \
        -T example1.csv -XPUT \
        http://<fe_host>:<fe_http_port>/api/test_db/table1/_stream_load
    ```

- Broker Load ジョブを実行します。

  - `__op` フィールドを含めたくない場合、次のコマンドを実行します。

    ```SQL
    LOAD LABEL test_db.label1
    (
        data infile("hdfs://<hdfs_host>:<hdfs_port>/example1.csv")
        into table table1
        columns terminated by ","
        format as "csv"
    )
    WITH BROKER;
    ```

  - `__op` フィールドを含めたい場合、次のコマンドを実行します。

    ```SQL
    LOAD LABEL test_db.label2
    (
        data infile("hdfs://<hdfs_host>:<hdfs_port>/example1.csv")
        into table table1
        columns terminated by ","
        format as "csv"
        set (__op = 'upsert')
    )
    WITH BROKER;
    ```

- Routine Load ジョブを実行します。

  - `__op` フィールドを含めたくない場合、次のコマンドを実行します。

    ```SQL
    CREATE ROUTINE LOAD test_db.table1 ON table1
    COLUMNS TERMINATED BY ",",
    COLUMNS (id, name, score)
    PROPERTIES
    (
        "desired_concurrent_number" = "3",
        "max_batch_interval" = "20",
        "max_batch_rows"= "250000",
        "max_error_number" = "1000"
    )
    FROM KAFKA
    (
        "kafka_broker_list" ="<kafka_broker_host>:<kafka_broker_port>",
        "kafka_topic" = "test1",
        "property.kafka_default_offsets" ="OFFSET_BEGINNING"
    );
    ```

  - `__op` フィールドを含めたい場合、次のコマンドを実行します。

    ```SQL
    CREATE ROUTINE LOAD test_db.table1 ON table1
    COLUMNS TERMINATED BY ",",
    COLUMNS (id, name, score, __op ='upsert')
    PROPERTIES
    (
        "desired_concurrent_number" = "3",
        "max_batch_interval" = "20",
        "max_batch_rows"= "250000",
        "max_error_number" = "1000"
    )
    FROM KAFKA
    (
        "kafka_broker_list" ="<kafka_broker_host>:<kafka_broker_port>",
        "kafka_topic" = "test1",
        "property.kafka_default_offsets" ="OFFSET_BEGINNING"
    );
    ```

#### データのクエリ

ロードが完了したら、`table1` のデータをクエリして、ロードが成功したことを確認します。

```SQL
SELECT * FROM table1;
+------+------+-------+
| id   | name | score |
+------+------+-------+
|  101 | Lily |   100 |
|  102 | Rose |   100 |
+------+------+-------+
2 rows in set (0.02 sec)
```

上記のクエリ結果に示されているように、`example1.csv` の `id` が `101` のレコードは `table1` に更新され、`example1.csv` の `id` が `102` のレコードは `table1` に挿入されました。

### DELETE

ロードしたいデータファイルが DELETE 操作のみを含む場合、`__op` フィールドを追加し、操作の種類を DELETE として指定する必要があります。

#### データ例

1. データファイルを準備します。

   a. ローカルファイルシステムに `example2.csv` という名前の CSV ファイルを作成します。このファイルは、ユーザー ID、ユーザー名、ユーザースコアを順に表す 3 つの列で構成されています。

      ```Plain
      101,Jack,100
      ```

   b. `example2.csv` のデータを Kafka クラスターの `topic2` に公開します。

2. StarRocks テーブルを準備します。

   a. StarRocks テーブル `test_db` に `table2` という名前の主キーテーブルを作成します。このテーブルは、`id`、`name`、`score` の 3 つの列で構成されており、`id` が主キーです。

      ```SQL
      CREATE TABLE `table2`
            (
          `id` int(11) NOT NULL COMMENT "user ID",
          `name` varchar(65533) NOT NULL COMMENT "user name",
          `score` int(11) NOT NULL COMMENT "user score"
      )
      ENGINE=OLAP
      PRIMARY KEY(`id`)
      DISTRIBUTED BY HASH(`id`);
      ```

      > **注意**
      >
      > バージョン v2.5.7 以降、StarRocks はテーブルを作成する際やパーティションを追加する際に、バケット数 (BUCKETS) を自動的に設定できます。バケット数を手動で設定する必要はありません。詳細情報については、[バケット数の設定](../table_design/data_distribution/Data_distribution.md#set-the-number-of-buckets)を参照してください。

   b. `table2` に 2 つのレコードを挿入します。

      ```SQL
      INSERT INTO table2 VALUES
      (101, 'Jack', 100),
      (102, 'Bob', 90);
      ```

#### データのロード

`example2.csv` の `id` が `101` のレコードを `table2` から削除するロードジョブを実行します。

- Stream Load ジョブを実行します。

  ```Bash
  curl --location-trusted -u <username>:<password> \
      -H "Expect:100-continue" \
      -H "label:label3" \
      -H "column_separator:," \
      -H "columns:__op='delete'" \
      -T example2.csv -XPUT \
      http://<fe_host>:<fe_http_port>/api/test_db/table2/_stream_load
  ```

- Broker Load ジョブを実行します。

  ```SQL
  LOAD LABEL test_db.label3
  (
      data infile("hdfs://<hdfs_host>:<hdfs_port>/example2.csv")
      into table table2
      columns terminated by ","
      format as "csv"
      set (__op = 'delete')
  )
  WITH BROKER;  
  ```

- Routine Load ジョブを実行します。

  ```SQL
  CREATE ROUTINE LOAD test_db.table2 ON table2
  COLUMNS(id, name, score, __op = 'delete')
  PROPERTIES
  (
      "desired_concurrent_number" = "3",
      "max_batch_interval" = "20",
      "max_batch_rows"= "250000",
      "max_error_number" = "1000"
  )
  FROM KAFKA
  (
      "kafka_broker_list" ="<kafka_broker_host>:<kafka_broker_port>",
      "kafka_topic" = "test2",
      "property.kafka_default_offsets" ="OFFSET_BEGINNING"
  );
  ```

#### データのクエリ

ロードが完了したら、`table2` のデータをクエリして、ロードが成功したことを確認します。

```SQL
SELECT * FROM table2;
+------+------+-------+
| id   | name | score |
+------+------+-------+
|  102 | Bob  |    90 |
+------+------+-------+
1 row in set (0.00 sec)
```

上記のクエリ結果に示されているように、`example2.csv` の `id` が `101` のレコードは `table2` から削除されました。

### UPSERT と DELETE

ロードしたいデータファイルが UPSERT および DELETE 操作の両方を含む場合、`__op` フィールドを追加し、データファイルに `0` または `1` の値を持つ列が含まれていることを確認する必要があります。`0` の値は UPSERT 操作を示し、`1` の値は DELETE 操作を示します。

#### データ例

1. データファイルを準備します。

   a. ローカルファイルシステムに `example3.csv` という名前の CSV ファイルを作成します。このファイルは、ユーザー ID、ユーザー名、ユーザースコア、操作の種類を順に表す 4 つの列で構成されています。

      ```Plain
      101,Tom,100,1
      102,Sam,70,0
      103,Stan,80,0
      ```

   b. `example3.csv` のデータを Kafka クラスターの `topic3` に公開します。

2. StarRocks テーブルを準備します。

   a. StarRocks データベース `test_db` に `table3` という名前の主キーテーブルを作成します。このテーブルは、`id`、`name`、`score` の 3 つの列で構成されており、`id` が主キーです。

      ```SQL
      CREATE TABLE `table3`
      (
          `id` int(11) NOT NULL COMMENT "user ID",
          `name` varchar(65533) NOT NULL COMMENT "user name",
          `score` int(11) NOT NULL COMMENT "user score"
      )
      ENGINE=OLAP
      PRIMARY KEY(`id`)
      DISTRIBUTED BY HASH(`id`);
      ```

> **注意**
      >
      > バージョン v2.5.7 以降、StarRocks はテーブルを作成する際やパーティションを追加する際に、バケット数 (BUCKETS) を自動的に設定できます。バケット数を手動で設定する必要はありません。詳細情報については、[バケット数の設定](../table_design/data_distribution/Data_distribution.md#set-the-number-of-buckets)を参照してください。

   b. `table3` に 2 つのレコードを挿入します。

      ```SQL
      INSERT INTO table3 VALUES
          (101, 'Tom', 100),
          (102, 'Sam', 90);
      ```

#### データのロード

`example3.csv` の `id` が `101` のレコードを `table3` から削除し、`id` が `102` のレコードを `table3` に更新し、`id` が `103` のレコードを `table3` に挿入するロードジョブを実行します。

- Stream Load ジョブを実行します。

  ```Bash
  curl --location-trusted -u <username>:<password> \
      -H "Expect:100-continue" \
      -H "label:label4" \
      -H "column_separator:," \
      -H "columns: id, name, score, temp, __op = temp" \
      -T example3.csv -XPUT \
      http://<fe_host>:<fe_http_port>/api/test_db/table3/_stream_load
  ```

  > **注意**
  >
  > 上記の例では、`example3.csv` の操作タイプを表す第 4 列が一時的に `temp` として命名され、`columns` パラメータを使用して `__op` フィールドが `temp` 列にマッピングされています。このようにして、StarRocks は `example3.csv` の第 4 列の値が `0` または `1` であるかに応じて、UPSERT または DELETE 操作を実行するかどうかを決定できます。

- Broker Load ジョブを実行します。

  ```Bash
  LOAD LABEL test_db.label4
  (
      data infile("hdfs://<hdfs_host>:<hdfs_port>/example1.csv")
      into table table1
      columns terminated by ","
      format as "csv"
      (id, name, score, temp)
      set (__op=temp)
  )
  WITH BROKER;
  ```

- Routine Load ジョブを実行します。

  ```SQL
  CREATE ROUTINE LOAD test_db.table3 ON table3
  COLUMNS(id, name, score, temp, __op = temp)
  PROPERTIES
  (
      "desired_concurrent_number" = "3",
      "max_batch_interval" = "20",
      "max_batch_rows"= "250000",
      "max_error_number" = "1000"
  )
  FROM KAFKA
  (
      "kafka_broker_list" = "<kafka_broker_host>:<kafka_broker_port>",
      "kafka_topic" = "test3",
      "property.kafka_default_offsets" = "OFFSET_BEGINNING"
  );
  ```

#### データのクエリ

ロードが完了したら、`table3` のデータをクエリして、ロードが成功したことを確認します。

```SQL
SELECT * FROM table3;
+------+------+-------+
| id   | name | score |
+------+------+-------+
|  102 | Sam  |    70 |
|  103 | Stan |    80 |
+------+------+-------+
2 rows in set (0.01 sec)
```

上記のクエリ結果に示されているように、`example3.csv` の `id` が `101` のレコードは `table3` から削除され、`example3.csv` の `id` が `102` のレコードは `table3` に更新され、`example3.csv` の `id` が `103` のレコードは `table3` に挿入されました。

## 部分更新

主キーテーブルは部分更新もサポートしており、異なるデータ更新シナリオに対応するために、行モードと列モードの 2 つの部分更新モードを提供します。これらの 2 つの部分更新モードは、クエリパフォーマンスを保証しながら、可能な限り部分更新のオーバーヘッドを最小限に抑え、リアルタイム更新を実現します。行モードは、多くの列と小さなバッチを含むリアルタイム更新シナリオにより適しています。列モードは、少数の列と多数の行を含むバッチ処理更新シナリオに適しています。

> **注意**
>
> 部分更新を実行する際に、更新対象の行が存在しない場合、StarRocks は新しい行を挿入し、データ更新が挿入されていないフィールドにはデフォルト値を埋め込みます。

このセクションでは、CSV を例にして部分更新を実行する方法を説明します。

### データ例

1. データファイルを準備します。

   a. ローカルファイルシステムに `example4.csv` という名前の CSV ファイルを作成します。このファイルは、ユーザー ID とユーザー名を順に表す 2 つの列で構成されています。

      ```Plain
      101,Lily
      102,Rose
      103,Alice
      ```

   b. `example4.csv` のデータを Kafka クラスターの `topic4` に公開します。

2. StarRocks テーブルを準備します。

   a. StarRocks データベース `test_db` に `table4` という名前の主キーテーブルを作成します。このテーブルは、`id`、`name`、`score` の 3 つの列で構成されており、`id` が主キーです。

      ```SQL
      CREATE TABLE `table4`
      (
          `id` int(11) NOT NULL COMMENT "user ID",
          `name` varchar(65533) NOT NULL COMMENT "user name",
          `score` int(11) NOT NULL COMMENT "user score"
      )
      ENGINE=OLAP
      PRIMARY KEY(`id`)
      DISTRIBUTED BY HASH(`id`);
      ```

      > **注意**
      >
      > バージョン v2.5.7 以降、StarRocks はテーブルを作成する際やパーティションを追加する際に、バケット数 (BUCKETS) を自動的に設定できます。バケット数を手動で設定する必要はありません。詳細情報については、[バケット数の設定](../table_design/data_distribution/Data_distribution.md#set-the-number-of-buckets)を参照してください。

   b. `table4` にレコードを挿入します。

      ```SQL
      INSERT INTO table4 VALUES
          (101, 'Tom',80);
      ```

### データのロード

`example4.csv` の 2 つの列のデータを `table4` の `id` 列と `name` 列に更新するロードを実行します。

- Stream Load ジョブを実行します。

  ```Bash
  curl --location-trusted -u <username>:<password> \
      -H "Expect:100-continue" \
      -H "label:label7" -H "column_separator:," \
      -H "partial_update:true" \
      -H "columns:id,name" \
      -T example4.csv -XPUT \
      http://<fe_host>:<fe_http_port>/api/test_db/table4/_stream_load
  ```

  > **注意**
  >
  > Stream Load を選択する場合、部分更新機能を有効にするために `partial_update` パラメータを `true` に設定する必要があります。デフォルトは行モードでの部分更新です。列モードで部分更新を実行する必要がある場合は、`partial_update_mode` を `column` に設定する必要があります。さらに、更新したい列を指定するために `columns` パラメータを使用する必要があります。

- Broker Load ジョブを実行します。

  ```SQL
  LOAD LABEL test_db.table4
  (
      data infile("hdfs://<hdfs_host>:<hdfs_port>/example4.csv")
      into table table4
      format as "csv"
      (id, name)
  )
  WITH BROKER
  PROPERTIES
  (
      "partial_update" = "true"
  );
  ```

  > **注意**
  >
  > Broker Load を選択する場合、部分更新機能を有効にするために `partial_update` パラメータを `true` に設定する必要があります。デフォルトは行モードでの部分更新です。列モードで部分更新を実行する必要がある場合は、`partial_update_mode` を `column` に設定する必要があります。さらに、更新したい列を指定するために `column_list` パラメータを使用する必要があります。

- Routine Load ジョブを実行します。

  ```SQL
  CREATE ROUTINE LOAD test_db.table4 on table4
  COLUMNS (id, name),
  COLUMNS TERMINATED BY ','
  PROPERTIES
  (
      "partial_update" = "true"
  )
  FROM KAFKA
  (
      "kafka_broker_list" ="<kafka_broker_host>:<kafka_broker_port>",
      "kafka_topic" = "test4",
      "property.kafka_default_offsets" ="OFFSET_BEGINNING"
  );
  ```

  > **注意**
  >
  > - Routine Load を選択する場合、部分更新機能を有効にするために `partial_update` パラメータを `true` に設定する必要があります。さらに、更新したい列を指定するために `COLUMNS` パラメータを使用する必要があります。
  > - Routine Load は行モードでの部分更新のみをサポートし、列モードでの部分更新はサポートしていません。

### データのクエリ

ロードが完了したら、`table4` のデータをクエリして、ロードが成功したことを確認します。

```SQL
SELECT * FROM table4;
+------+-------+-------+
| id   | name  | score |
+------+-------+-------+
|  102 | Rose  |     0 |
|  101 | Lily  |    80 |
|  103 | Alice |     0 |
+------+-------+-------+
3 rows in set (0.01 sec)
```

上記のクエリ結果に示されているように、`example4.csv` の `id` が `101` のレコードは `table4` に更新され、`example4.csv` の `id` が `102` と `103` のレコードは `table4` に挿入されました。

## 条件付き更新

StarRocks v2.5 以降、主キーテーブルは条件付き更新をサポートしています。非主キー列を条件として指定し、更新が有効になるかどうかを決定できます。このようにして、ソースレコードから宛先レコードへの更新は、指定された列でソースデータレコードが宛先データレコードよりも大きいか等しい値を持つ場合にのみ有効になります。

条件付き更新機能は、データの順序が乱れている問題を解決するために設計されています。ソースデータが無秩序である場合、この機能を使用して、新しいデータが古いデータによって上書きされないようにすることができます。

> **注意**
>
> - 同じバッチのデータに対して異なる列を更新条件として指定することはできません。
> - DELETE 操作は条件付き更新をサポートしていません。
> - バージョン v3.1.3 より前のバージョンでは、部分更新と条件付き更新を同時に使用することはできません。v3.1.3 以降、StarRocks は部分更新と条件付き更新の同時使用をサポートしています。

### データ例

1. データファイルを準備します。

   a. ローカルファイルシステムに `example5.csv` という名前の CSV ファイルを作成します。このファイルは、ユーザー ID、バージョン、ユーザースコアを順に表す 3 つの列で構成されています。

      ```Plain
      101,1,100
      102,3,100
      ```

   b. `example5.csv` のデータを Kafka クラスターの `topic5` に公開します。

2. StarRocks テーブルを準備します。

   a. StarRocks データベース `test_db` に `table5` という名前の主キーテーブルを作成します。このテーブルは、`id`、`version`、`score` の 3 つの列で構成されており、`id` が主キーです。

      ```SQL
      CREATE TABLE `table5`
      (
          `id` int(11) NOT NULL COMMENT "user ID", 
          `version` int NOT NULL COMMENT "version",
          `score` int(11) NOT NULL COMMENT "user score"
      )
      ENGINE=OLAP
      PRIMARY KEY(`id`) DISTRIBUTED BY HASH(`id`);
      ```

      > **注意**
      >
      > バージョン v2.5.7 以降、StarRocks はテーブルを作成する際やパーティションを追加する際に、バケット数 (BUCKETS) を自動的に設定できます。バケット数を手動で設定する必要はありません。詳細情報については、[バケット数の設定](../table_design/data_distribution/Data_distribution.md#set-the-number-of-buckets)を参照してください。

   b. `table5` にレコードを挿入します。

      ```SQL
      INSERT INTO table5 VALUES
          (101, 2, 80),
          (102, 2, 90);
      ```

### データのロード

`example5.csv` の `id` が `101` と `102` のレコードをそれぞれ `table5` に更新し、更新が有効になるのは、2 つのレコードの `version` 値がそれぞれの現在の `version` 値以上である場合のみと指定します。

- Stream Load ジョブを実行します。

  ```Bash
  curl --location-trusted -u <username>:<password> \
      -H "Expect:100-continue" \
      -H "label:label10" \
      -H "column_separator:," \
      -H "merge_condition:version" \
      -T example5.csv -XPUT \
      http://<fe_host>:<fe_http_port>/api/test_db/table5/_stream_load
  ```

- Routine Load ジョブを実行します。

  ```SQL
  CREATE ROUTINE LOAD test_db.table5 on table5
  COLUMNS (id, version, score),
  COLUMNS TERMINATED BY ','
  PROPERTIES
  (
      "merge_condition" = "version"
  )
  FROM KAFKA
  (
      "kafka_broker_list" ="<kafka_broker_host>:<kafka_broker_port>",
      "kafka_topic" = "topic5",
      "property.kafka_default_offsets" ="OFFSET_BEGINNING"
  );
  ```

- Broker Load ジョブを実行します。

  ```SQL
  LOAD LABEL test_db.table5
  ( DATA INFILE ("s3://xxx.csv")
    INTO TABLE table5 COLUMNS TERMINATED BY "," FORMAT AS "CSV"
  )
  WITH BROKER
  PROPERTIES
  (
      "merge_condition" = "version"
  );
  ```

### データのクエリ

ロードが完了したら、`table5` のデータをクエリして、ロードが成功したことを確認します。

```SQL
SELECT * FROM table5;
+------+------+-------+
| id   | version | score |
+------+------+-------+
|  101 |       2 |   80 |
|  102 |       3 |  100 |
+------+------+-------+
2 rows in set (0.02 sec)
```

上記のクエリ結果に示されているように、`example5.csv` の `id` が `101` のレコードは `table5` に更新されず、`example5.csv` の `id` が `102` のレコードは `table5` に挿入されました。