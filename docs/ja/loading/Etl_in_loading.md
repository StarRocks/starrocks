---
displayed_sidebar: docs
---

# ロード時のデータ変換

import InsertPrivNote from '../_assets/commonMarkdown/insertPrivNote.md'

StarRocks はロード時のデータ変換をサポートしています。

この機能は [Stream Load](../sql-reference/sql-statements/loading_unloading/STREAM_LOAD.md)、[Broker Load](../sql-reference/sql-statements/loading_unloading/BROKER_LOAD.md)、および [Routine Load](../sql-reference/sql-statements/loading_unloading/routine_load/CREATE_ROUTINE_LOAD.md) をサポートしていますが、[Spark Load](../sql-reference/sql-statements/loading_unloading/SPARK_LOAD.md) はサポートしていません。

<InsertPrivNote />

このトピックでは、CSV データを例にとり、ロード時にデータを抽出および変換する方法を説明します。サポートされるデータファイル形式は、選択したロード方法によって異なります。

> **注意**
>
> CSV データの場合、UTF-8 文字列（カンマ (,)、タブ、パイプ (|) など）をテキスト区切り文字として使用できますが、その長さは 50 バイトを超えないようにしてください。

## シナリオ

データファイルを StarRocks テーブルにロードする際、データファイルのデータが StarRocks テーブルのデータに完全にマッピングされない場合があります。このような場合、データを StarRocks テーブルにロードする前に抽出または変換する必要はありません。StarRocks はロード中にデータを抽出および変換するのを支援します。

- ロードする必要のない列をスキップする。
  
  ロードする必要のない列をスキップできます。また、データファイルの列が StarRocks テーブルの列と異なる順序である場合、データファイルと StarRocks テーブルの間で列マッピングを作成できます。

- ロードしたくない行をフィルタリングする。
  
  StarRocks がロードしたくない行をフィルタリングする条件を指定できます。

- 元の列から新しい列を生成する。
  
  生成列は、データファイルの元の列から計算される特別な列です。生成列を StarRocks テーブルの列にマッピングできます。

- ファイルパスからパーティションフィールドの値を抽出する。
  
  データファイルが Apache Hive™ から生成された場合、ファイルパスからパーティションフィールドの値を抽出できます。

## データ例

1. ローカルファイルシステムにデータファイルを作成します。

   a. `file1.csv` という名前のデータファイルを作成します。このファイルは、ユーザー ID、ユーザーの性別、イベント日付、イベントタイプを順に表す 4 つの列で構成されています。

      ```Plain
      354,female,2020-05-20,1
      465,male,2020-05-21,2
      576,female,2020-05-22,1
      687,male,2020-05-23,2
      ```

   b. `file2.csv` という名前のデータファイルを作成します。このファイルは日付を表す 1 つの列のみで構成されています。

      ```Plain
      2020-05-20
      2020-05-21
      2020-05-22
      2020-05-23
      ```

2. StarRocks データベース `test_db` にテーブルを作成します。

   > **注意**
   >
   > バージョン 2.5.7 以降、StarRocks はテーブルを作成する際やパーティションを追加する際に、バケット数 (BUCKETS) を自動的に設定できます。バケット数を手動で設定する必要はありません。詳細については、[バケット数を設定する](../table_design/data_distribution/Data_distribution.md#set-the-number-of-buckets) を参照してください。

   a. `table1` という名前のテーブルを作成します。このテーブルは、`event_date`、`event_type`、`user_id` の 3 つの列で構成されています。

      ```SQL
      MySQL [test_db]> CREATE TABLE table1
      (
          `event_date` DATE COMMENT "event date",
          `event_type` TINYINT COMMENT "event type",
          `user_id` BIGINT COMMENT "user ID"
      )
      DISTRIBUTED BY HASH(user_id);
      ```

   b. `table2` という名前のテーブルを作成します。このテーブルは、`date`、`year`、`month`、`day` の 4 つの列で構成されています。

      ```SQL
      MySQL [test_db]> CREATE TABLE table2
      (
          `date` DATE COMMENT "date",
          `year` INT COMMENT "year",
          `month` TINYINT COMMENT "month",
          `day` TINYINT COMMENT "day"
      )
      DISTRIBUTED BY HASH(date);
      ```

3. `file1.csv` と `file2.csv` を HDFS クラスターの `/user/starrocks/data/input/` パスにアップロードし、`file1.csv` のデータを Kafka クラスターの `topic1` に、`file2.csv` のデータを `topic2` に公開します。

## ロードする必要のない列をスキップする

StarRocks テーブルにロードしたいデータファイルには、StarRocks テーブルの列にマッピングできない列が含まれている場合があります。このような場合、StarRocks はデータファイルから StarRocks テーブルの列にマッピングできる列のみをロードすることをサポートしています。

この機能は、以下のデータソースからのデータロードをサポートしています。

- ローカルファイルシステム

- HDFS およびクラウドストレージ
  
  > **注意**
  >
  > このセクションでは HDFS を例として使用します。

- Kafka

ほとんどの場合、CSV ファイルの列には名前が付けられていません。一部の CSV ファイルでは、最初の行が列名で構成されていますが、StarRocks は最初の行の内容を一般的なデータとして処理し、列名としては扱いません。したがって、CSV ファイルをロードする際には、ジョブ作成ステートメントまたはコマンドで CSV ファイルの列を**順番に**一時的に命名する必要があります。これらの一時的に命名された列は、StarRocks テーブルの列に**名前で**マッピングされます。データファイルの列については、以下の点に注意してください。

- StarRocks テーブルの列にマッピングでき、StarRocks テーブルの列名を使用して一時的に命名された列のデータは、直接ロードされます。

- StarRocks テーブルの列にマッピングできない列は無視され、これらの列のデータはロードされません。

- StarRocks テーブルの列にマッピングできるが、ジョブ作成ステートメントまたはコマンドで一時的に命名されていない列がある場合、ロードジョブはエラーを報告します。

このセクションでは、`file1.csv` と `table1` を例として使用します。`file1.csv` の 4 つの列は、順番に `user_id`、`user_gender`、`event_date`、`event_type` として一時的に命名されます。`file1.csv` の一時的に命名された列の中で、`user_id`、`event_date`、`event_type` は `table1` の特定の列にマッピングできますが、`user_gender` は `table1` のどの列にもマッピングできません。したがって、`user_id`、`event_date`、`event_type` は `table1` にロードされますが、`user_gender` はロードされません。

### データのロード

#### ローカルファイルシステムからデータをロードする

`file1.csv` がローカルファイルシステムに保存されている場合、次のコマンドを実行して [Stream Load](../loading/StreamLoad.md) ジョブを作成します。

```Bash
curl --location-trusted -u <username>:<password> \
    -H "Expect:100-continue" \
    -H "column_separator:," \
    -H "columns: user_id, user_gender, event_date, event_type" \
    -T file1.csv -XPUT \
    http://<fe_host>:<fe_http_port>/api/test_db/table1/_stream_load
```

> **注意**
>
> Stream Load を選択する場合、データファイルと StarRocks テーブルの間で列マッピングを作成するために、`columns` パラメーターを使用してデータファイルの列を一時的に命名する必要があります。

詳細な構文とパラメーターの説明については、[STREAM LOAD](../sql-reference/sql-statements/loading_unloading/STREAM_LOAD.md) を参照してください。

#### HDFS クラスターからデータをロードする

`file1.csv` が HDFS クラスターに保存されている場合、次のステートメントを実行して [Broker Load](../loading/hdfs_load.md) ジョブを作成します。

```SQL
LOAD LABEL test_db.label1
(
    DATA INFILE("hdfs://<hdfs_host>:<hdfs_port>/user/starrocks/data/input/file1.csv")
    INTO TABLE `table1`
    FORMAT AS "csv"
    COLUMNS TERMINATED BY ","
    (user_id, user_gender, event_date, event_type)
)
WITH BROKER;
```

> **注意**
>
> Broker Load を選択する場合、データファイルと StarRocks テーブルの間で列マッピングを作成するために、`column_list` パラメーターを使用してデータファイルの列を一時的に命名する必要があります。

詳細な構文とパラメーターの説明については、[BROKER LOAD](../sql-reference/sql-statements/loading_unloading/BROKER_LOAD.md) を参照してください。

#### Kafka クラスターからデータをロードする

`file1.csv` のデータが Kafka クラスターの `topic1` に公開されている場合、次のステートメントを実行して [Routine Load](../loading/RoutineLoad.md) ジョブを作成します。

```SQL
CREATE ROUTINE LOAD test_db.table101 ON table1
    COLUMNS TERMINATED BY ",",
    COLUMNS(user_id, user_gender, event_date, event_type)
FROM KAFKA
(
    "kafka_broker_list" = "<kafka_broker_host>:<kafka_broker_port>",
    "kafka_topic" = "topic1",
    "property.kafka_default_offsets" = "OFFSET_BEGINNING"
);
```

> **注意**
>
> Routine Load を選択する場合、データファイルと StarRocks テーブルの間で列マッピングを作成するために、`COLUMNS` パラメーターを使用してデータファイルの列を一時的に命名する必要があります。

詳細な構文とパラメーターの説明については、[CREATE ROUTINE LOAD](../sql-reference/sql-statements/loading_unloading/routine_load/CREATE_ROUTINE_LOAD.md) を参照してください。

### データのクエリ

ローカルファイルシステム、HDFS クラスター、または Kafka クラスターからのデータのロードが完了したら、`table1` のデータをクエリしてロードが成功したことを確認します。

```SQL
MySQL [test_db]> SELECT * FROM table1;
+------------+------------+---------+
| event_date | event_type | user_id |
+------------+------------+---------+
| 2020-05-22 |          1 |     576 |
| 2020-05-20 |          1 |     354 |
| 2020-05-21 |          2 |     465 |
| 2020-05-23 |          2 |     687 |
+------------+------------+---------+
4 rows in set (0.01 sec)
```

## ロードしたくない行をフィルタリングする

データファイルを StarRocks テーブルにロードする際、特定の行をロードしたくない場合があります。このような場合、WHERE 句を使用してロードしたい行を指定できます。StarRocks は、WHERE 句で指定されたフィルター条件を満たさないすべての行をフィルタリングします。

この機能は、以下のデータソースからのデータロードをサポートしています。

- ローカルファイルシステム

- HDFS およびクラウドストレージ
  > **注意**
  >
  > このセクションでは HDFS を例として使用します。

- Kafka

このセクションでは、`file1.csv` と `table1` を例として使用します。`file1.csv` から `table1` にロードする際に、イベントタイプが `1` の行のみをロードしたい場合、WHERE 句を使用してフィルター条件 `event_type = 1` を指定できます。

### データのロード

#### ローカルファイルシステムからデータをロードする

`file1.csv` がローカルファイルシステムに保存されている場合、次のコマンドを実行して [Stream Load](../loading/StreamLoad.md) ジョブを作成します。

```Bash
curl --location-trusted -u <username>:<password> \
    -H "Expect:100-continue" \
    -H "column_separator:," \
    -H "columns: user_id, user_gender, event_date, event_type" \
    -H "where: event_type=1" \
    -T file1.csv -XPUT \
    http://<fe_host>:<fe_http_port>/api/test_db/table1/_stream_load
```

詳細な構文とパラメーターの説明については、[STREAM LOAD](../sql-reference/sql-statements/loading_unloading/STREAM_LOAD.md) を参照してください。

#### HDFS クラスターからデータをロードする

`file1.csv` が HDFS クラスターに保存されている場合、次のステートメントを実行して [Broker Load](../loading/hdfs_load.md) ジョブを作成します。

```SQL
LOAD LABEL test_db.label2
(
    DATA INFILE("hdfs://<hdfs_host>:<hdfs_port>/user/starrocks/data/input/file1.csv")
    INTO TABLE `table1`
    FORMAT AS "csv"
    COLUMNS TERMINATED BY ","
    (user_id, user_gender, event_date, event_type)
    WHERE event_type = 1
)
WITH BROKER;
```

詳細な構文とパラメーターの説明については、[BROKER LOAD](../sql-reference/sql-statements/loading_unloading/BROKER_LOAD.md) を参照してください。

#### Kafka クラスターからデータをロードする

`file1.csv` のデータが Kafka クラスターの `topic1` に公開されている場合、次のステートメントを実行して [Routine Load](../loading/RoutineLoad.md) ジョブを作成します。

```SQL
CREATE ROUTINE LOAD test_db.table102 ON table1
COLUMNS TERMINATED BY ",",
COLUMNS (user_id, user_gender, event_date, event_type),
WHERE event_type = 1
FROM KAFKA
(
    "kafka_broker_list" = "<kafka_broker_host>:<kafka_broker_port>",
    "kafka_topic" = "topic1",
    "property.kafka_default_offsets" = "OFFSET_BEGINNING"
);
```

詳細な構文とパラメーターの説明については、[CREATE ROUTINE LOAD](../sql-reference/sql-statements/loading_unloading/routine_load/CREATE_ROUTINE_LOAD.md) を参照してください。

### データのクエリ

ローカルファイルシステム、HDFS クラスター、または Kafka クラスターからのデータのロードが完了したら、`table1` のデータをクエリしてロードが成功したことを確認します。

```SQL
MySQL [test_db]> SELECT * FROM table1;
+------------+------------+---------+
| event_date | event_type | user_id |
+------------+------------+---------+
| 2020-05-20 |          1 |     354 |
| 2020-05-22 |          1 |     576 |
+------------+------------+---------+
2 rows in set (0.01 sec)
```

## 元の列から新しい列を生成する

データファイルを StarRocks テーブルにロードする際、データファイルの一部のデータは、StarRocks テーブルにロードする前に変換が必要な場合があります。このような場合、ジョブ作成コマンドまたはステートメントで関数や式を使用してデータ変換を実装できます。

この機能は、以下のデータソースからのデータロードをサポートしています。

- ローカルファイルシステム

- HDFS およびクラウドストレージ
  > **注意**
  >
  > このセクションでは HDFS を例として使用します。

- Kafka

このセクションでは、`file2.csv` と `table2` を例として使用します。`file2.csv` は日付を表す 1 つの列のみで構成されています。`file2.csv` から各日付の年、月、日を抽出し、抽出したデータを `table2` の `year`、`month`、`day` 列にロードするために、[year](../sql-reference/sql-functions/date-time-functions/year.md)、[month](../sql-reference/sql-functions/date-time-functions/month.md)、および [day](../sql-reference/sql-functions/date-time-functions/day.md) 関数を使用できます。

### データのロード

#### ローカルファイルシステムからデータをロードする

`file2.csv` がローカルファイルシステムに保存されている場合、次のコマンドを実行して [Stream Load](../loading/StreamLoad.md) ジョブを作成します。

```Bash
curl --location-trusted -u <username>:<password> \
    -H "Expect:100-continue" \
    -H "column_separator:," \
    -H "columns:date,year=year(date),month=month(date),day=day(date)" \
    -T file2.csv -XPUT \
    http://<fe_host>:<fe_http_port>/api/test_db/table2/_stream_load
```

> **注意**
>
> - `columns` パラメーターでは、まずデータファイルの**すべての列**を一時的に命名し、次にデータファイルの元の列から生成したい新しい列を一時的に命名する必要があります。前述の例では、`file2.csv` の唯一の列が `date` として一時的に命名され、その後 `year=year(date)`, `month=month(date)`, `day=day(date)` 関数が呼び出され、`year`、`month`、`day` として一時的に命名された 3 つの新しい列が生成されます。
>
> - Stream Load は `column_name = function(column_name)` をサポートしていませんが、`column_name = function(column_name)` をサポートしています。

詳細な構文とパラメーターの説明については、[STREAM LOAD](../sql-reference/sql-statements/loading_unloading/STREAM_LOAD.md) を参照してください。

#### HDFS クラスターからデータをロードする

`file2.csv` が HDFS クラスターに保存されている場合、次のステートメントを実行して [Broker Load](../loading/hdfs_load.md) ジョブを作成します。

```SQL
LOAD LABEL test_db.label3
(
    DATA INFILE("hdfs://<hdfs_host>:<hdfs_port>/user/starrocks/data/input/file2.csv")
    INTO TABLE `table2`
    FORMAT AS "csv"
    COLUMNS TERMINATED BY ","
    (date)
    SET(year=year(date), month=month(date), day=day(date))
)
WITH BROKER;
```

> **注意**
>
> まず `column_list` パラメーターを使用してデータファイルの**すべての列**を一時的に命名し、次に SET 句を使用してデータファイルの元の列から生成したい新しい列を一時的に命名する必要があります。前述の例では、`file2.csv` の唯一の列が `column_list` パラメーターで `date` として一時的に命名され、その後 SET 句で `year=year(date)`, `month=month(date)`, `day=day(date)` 関数が呼び出され、`year`、`month`、`day` として一時的に命名された 3 つの新しい列が生成されます。

詳細な構文とパラメーターの説明については、[BROKER LOAD](../sql-reference/sql-statements/loading_unloading/BROKER_LOAD.md) を参照してください。

#### Kafka クラスターからデータをロードする

`file2.csv` のデータが Kafka クラスターの `topic2` に公開されている場合、次のステートメントを実行して [Routine Load](../loading/RoutineLoad.md) ジョブを作成します。

```SQL
CREATE ROUTINE LOAD test_db.table201 ON table2
    COLUMNS TERMINATED BY ",",
    COLUMNS(date,year=year(date),month=month(date),day=day(date))
FROM KAFKA
(
    "kafka_broker_list" = "<kafka_broker_host>:<kafka_broker_port>",
    "kafka_topic" = "topic2",
    "property.kafka_default_offsets" = "OFFSET_BEGINNING"
);
```

> **注意**
>
> `COLUMNS` パラメーターでは、まずデータファイルの**すべての列**を一時的に命名し、次にデータファイルの元の列から生成したい新しい列を一時的に命名する必要があります。前述の例では、`file2.csv` の唯一の列が `date` として一時的に命名され、その後 `year=year(date)`, `month=month(date)`, `day=day(date)` 関数が呼び出され、`year`、`month`、`day` として一時的に命名された 3 つの新しい列が生成されます。

詳細な構文とパラメーターの説明については、[CREATE ROUTINE LOAD](../sql-reference/sql-statements/loading_unloading/routine_load/CREATE_ROUTINE_LOAD.md) を参照してください。

### データのクエリ

ローカルファイルシステム、HDFS クラスター、または Kafka クラスターからのデータのロードが完了したら、`table2` のデータをクエリしてロードが成功したことを確認します。

```SQL
MySQL [test_db]> SELECT * FROM table2;
+------------+------+-------+------+
| date       | year | month | day  |
+------------+------+-------+------+
| 2020-05-20 | 2020 |  5    | 20   |
| 2020-05-21 | 2020 |  5    | 21   |
| 2020-05-22 | 2020 |  5    | 22   |
| 2020-05-23 | 2020 |  5    | 23   |
+------------+------+-------+------+
4 rows in set (0.01 sec)
```

## ファイルパスからパーティションフィールドの値を抽出する

指定したファイルパスにパーティションフィールドが含まれている場合、`COLUMNS FROM PATH AS` パラメーターを使用して、ファイルパスから抽出したいパーティションフィールドを指定できます。ファイルパス内のパーティションフィールドは、データファイル内の列と同等です。`COLUMNS FROM PATH AS` パラメーターは、HDFS クラスターからデータをロードする場合にのみサポートされています。

たとえば、Hive から生成された次の 4 つのデータファイルをロードしたいとします。

```Plain
/user/starrocks/data/input/date=2020-05-20/data
1,354
/user/starrocks/data/input/date=2020-05-21/data
2,465
/user/starrocks/data/input/date=2020-05-22/data
1,576
/user/starrocks/data/input/date=2020-05-23/data
2,687
```

これらの 4 つのデータファイルは、HDFS クラスターの `/user/starrocks/data/input/` パスに保存されています。各データファイルは、パーティションフィールド `date` でパーティション化され、イベントタイプとユーザー ID を順に表す 2 つの列で構成されています。

### HDFS クラスターからデータをロードする

次のステートメントを実行して [Broker Load](../loading/hdfs_load.md) ジョブを作成します。このジョブでは、`/user/starrocks/data/input/` ファイルパスから `date` パーティションフィールドの値を抽出し、ワイルドカード (*) を使用してファイルパス内のすべてのデータファイルを `table1` にロードすることを指定します。

```SQL
LOAD LABEL test_db.label4
(
    DATA INFILE("hdfs://<fe_host>:<fe_http_port>/user/starrocks/data/input/date=*/*")
    INTO TABLE `table1`
    FORMAT AS "csv"
    COLUMNS TERMINATED BY ","
    (event_type, user_id)
    COLUMNS FROM PATH AS (date)
    SET(event_date = date)
)
WITH BROKER;
```

> **注意**
>
> 前述の例では、指定されたファイルパス内の `date` パーティションフィールドは、`table1` の `event_date` 列と同等です。したがって、SET 句を使用して `date` パーティションフィールドを `event_date` 列にマッピングする必要があります。指定されたファイルパス内のパーティションフィールドが StarRocks テーブルの列と同じ名前を持つ場合、マッピングを作成するために SET 句を使用する必要はありません。

詳細な構文とパラメーターの説明については、[BROKER LOAD](../sql-reference/sql-statements/loading_unloading/BROKER_LOAD.md) を参照してください。

### データのクエリ

HDFS クラスターからのデータのロードが完了したら、`table1` のデータをクエリしてロードが成功したことを確認します。

```SQL
MySQL [test_db]> SELECT * FROM table1;
+------------+------------+---------+
| event_date | event_type | user_id |
+------------+------------+---------+
| 2020-05-22 |          1 |     576 |
| 2020-05-20 |          1 |     354 |
| 2020-05-21 |          2 |     465 |
| 2020-05-23 |          2 |     687 |
+------------+------------+---------+
4 rows in set (0.01 sec)
```