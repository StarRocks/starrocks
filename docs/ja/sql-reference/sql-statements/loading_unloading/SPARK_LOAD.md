---
displayed_sidebar: docs
---

# SPARK LOAD

## 説明

Spark Load は、外部の Spark リソースを通じてインポートされたデータを前処理し、大量の StarRocks データのインポート性能を向上させ、StarRocks クラスターの計算リソースを節約します。主に初期移行や大量のデータを StarRocks にインポートするシナリオで使用されます。

Spark Load は非同期のインポート方法です。ユーザーは MySQL プロトコルを通じて Spark タイプのインポートタスクを作成し、`SHOW LOAD` を通じてインポート結果を確認する必要があります。

> **注意**
>
> - StarRocks テーブルにデータをロードするには、その StarRocks テーブルに対して INSERT 権限を持つユーザーである必要があります。INSERT 権限がない場合は、[GRANT](../account-management/GRANT.md) の指示に従って、使用するユーザーに INSERT 権限を付与してください。
> - Spark Load を使用して StarRocks テーブルにデータをロードする場合、StarRocks テーブルのバケット列は DATE、DATETIME、または DECIMAL 型であってはなりません。

構文

```sql
LOAD LABEL load_label
(
data_desc1[, data_desc2, ...]
)
WITH RESOURCE resource_name
[resource_properties]
[opt_properties]
```

1.load_label

現在インポートされているバッチのラベル。データベース内で一意です。

構文:

```sql
[database_name.]your_label
```

2.data_desc

インポートされたデータのバッチを説明するために使用されます。

構文:

```sql
DATA INFILE
(
"file_path1"[, file_path2, ...]
)
[NEGATIVE]
INTO TABLE `table_name`
[PARTITION (p1, p2)]
[COLUMNS TERMINATED BY "column_separator"]
[FORMAT AS "file_type"]
[(column_list)]
[COLUMNS FROM PATH AS (col2, ...)]
[SET (k1 = func(k2))]
[WHERE predicate]

DATA FROM TABLE hive_external_tbl
[NEGATIVE]
INTO TABLE tbl_name
[PARTITION (p1, p2)]
[SET (k1=f1(xx), k2=f2(xx))]
[WHERE predicate]
```

注意

```plain text
file_path:

ファイルパスは1つのファイルを指定することも、* ワイルドカードを使用してディレクトリ内のすべてのファイルを指定することもできます。ワイルドカードはファイルに一致しなければならず、ディレクトリには一致しません。

hive_external_tbl:

Hive 外部テーブル名。
インポートされた StarRocks テーブルの列は、Hive 外部テーブルに存在する必要があります。
各ロードタスクは1つの Hive 外部テーブルからのロードのみをサポートします。
file_path モードと同時に使用することはできません。

PARTITION:

このパラメータが指定されている場合、指定されたパーティションのみがインポートされ、インポートされたパーティション外のデータはフィルタリングされます。
指定されていない場合、テーブルのすべてのパーティションがデフォルトでインポートされます。

NEGATIVE:

このパラメータが指定されている場合、以前にインポートされた同じバッチのデータを相殺するために使用される「ネガティブ」データのバッチをロードするのと同等です。
このパラメータは、値列が存在し、値列の集計タイプが SUM のみの場合にのみ適用されます。

column_separator:

インポートファイル内の列セパレータを指定します。デフォルトは \t です。
不可視文字の場合、\ \ x をプレフィックスとして付け、16進数でセパレータを表現する必要があります。
例えば、Hive ファイルのセパレータ \x01 は "\ \ x01" と指定されます。

file_type:

インポートされたファイルのタイプを指定するために使用されます。現在サポートされているファイルタイプは csv、orc、parquet です。

column_list:

インポートファイル内の列とテーブル内の列の対応を指定するために使用されます。
インポートファイル内の列をスキップする必要がある場合、テーブルに存在しない列名としてその列を指定します。

構文:
(col_name1, col_name2, ...)

SET:

このパラメータを指定すると、ソースファイルの列を関数に従って変換し、変換された結果をテーブルにインポートできます。構文は column_name = expression です。
Spark SQL の組み込み関数のみがサポートされています。詳細は https://spark.apache.org/docs/2.4.6/api/sql/index.html を参照してください。
理解を助けるためにいくつかの例を示します。
例1: テーブルに3つの列 "c1, c2, c3" があり、ソースファイルの最初の2列が (c1, c2) に対応し、最後の2列の合計が C3 に対応する場合、columns (c1, c2, tmp_c3, tmp_c4) set (c3 = tmp_c3 + tmp_c4) を指定する必要があります。
例2: テーブルに3つの列 "year, month, day" があり、ソースファイルには "2018-06-01 01:02:03" の形式で1つの時間列しかない場合、columns (tmp_time) set (year = year (tmp_time), month = month (tmp_time), day = day (tmp_time)) を指定してインポートを完了できます。

WHERE:

変換されたデータをフィルタリングし、WHERE 条件を満たすデータのみがインポートされます。WHERE 文ではテーブル内の列名のみを参照できます。
```

3.resource_name

使用される Spark リソースの名前は `SHOW RESOURCES` コマンドで確認できます。

4.resource_properties

一時的なニーズがある場合、例えば Spark や HDFS の設定を変更する場合、ここでパラメータを設定できます。これにより、この特定の Spark ロードジョブでのみ有効になり、StarRocks クラスターの既存の設定には影響を与えません。

5.opt_properties

特別なパラメータを指定するために使用されます。

構文:

```sql
[PROPERTIES ("key"="value", ...)]
```

以下のパラメータを指定できます:
timeout:         インポート操作のタイムアウトを指定します。デフォルトのタイムアウトは4時間です。秒単位。
max_filter_ratio:フィルタリング可能なデータの最大許容割合（非標準データなどの理由で）。デフォルトはゼロトレランスです。
strict mode:     データを厳密に制限するかどうか。デフォルトは false です。
timezone:         タイムゾーンに影響を受けるいくつかの関数のタイムゾーンを指定します。strftime / alignment_timestamp/from_unixtime など。詳細は [time zone] ドキュメントを参照してください。指定されていない場合、"Asia / Shanghai" タイムゾーンが使用されます。

6.インポートデータ形式の例

int (TINYINT/SMALLINT/INT/BIGINT/LARGEINT): 1, 1000, 1234
float (FLOAT/DOUBLE/DECIMAL): 1.1, 0.23, .356
date (DATE/DATETIME) :2017-10-03, 2017-06-13 12:34:03.
（注: 他の日付形式については、インポートコマンドで strftime または time_format 関数を使用して変換できます） string クラス (CHAR/VARCHAR): "I am a student", "a"

NULL 値: \ N

## 例

1. HDFS からデータのバッチをインポートし、タイムアウト時間とフィルタリング比率を指定します。Spark の名前として my_spark リソースを使用します。

    ```sql
    LOAD LABEL example_db.label1
    (
    DATA INFILE("hdfs://hdfs_host:hdfs_port/user/starRocks/data/input/file")
    INTO TABLE `my_table`
    )
    WITH RESOURCE 'my_spark'
    PROPERTIES
    (
        "timeout" = "3600",
        "max_filter_ratio" = "0.1"
    );
    ```

    ここで、hdfs_host は namenode のホスト、hdfs_port は fs.defaultfs ポート（デフォルト 9000）です。

2. HDFS から「ネガティブ」データのバッチをインポートし、セパレータをカンマとして指定し、ワイルドカード * を使用してディレクトリ内のすべてのファイルを指定し、Spark リソースの一時パラメータを指定します。

    ```sql
    LOAD LABEL example_db.label3
    (
    DATA INFILE("hdfs://hdfs_host:hdfs_port/user/starRocks/data/input/*")
    NEGATIVE
    INTO TABLE `my_table`
    COLUMNS TERMINATED BY ","
    )
    WITH RESOURCE 'my_spark'
    (
        "spark.executor.memory" = "3g",
        "broker.username" = "hdfs_user",
        "broker.password" = "hdfs_passwd"
    );
    ```

3. HDFS からデータのバッチをインポートし、パーティションを指定し、インポートファイルの列にいくつかの変換を行います。以下のようにします。

    ```plain text
    テーブル構造は次のとおりです:
    k1 varchar(20)
    k2 int
    
    データファイルには1行のデータしかないと仮定します:
    
    Adele,1,1
    
    データファイル内の各列はインポート文で指定された各列に対応します:
    k1,tmp_k2,tmp_k3
    
    変換は次のとおりです:
    
    1. k1: 変換なし
    2. k2: tmp_k2 と tmp_k3 の合計
    
    LOAD LABEL example_db.label6
    (
    DATA INFILE("hdfs://hdfs_host:hdfs_port/user/starRocks/data/input/file")
    INTO TABLE `my_table`
    PARTITION (p1, p2)
    COLUMNS TERMINATED BY ","
    (k1, tmp_k2, tmp_k3)
    SET (
    k2 = tmp_k2 + tmp_k3
    )
    )
    WITH RESOURCE 'my_spark';
    ```

4. ファイルパス内のパーティションフィールドを抽出します

    必要に応じて、ファイルパス内のパーティションフィールドはテーブルで定義されたフィールドタイプに従って解決され、Spark の Partition Discovery 機能に似ています。

    ```sql
    LOAD LABEL example_db.label10
    (
    DATA INFILE("hdfs://hdfs_host:hdfs_port/user/starRocks/data/input/dir/city=beijing/*/*")
    INTO TABLE `my_table`
    (k1, k2, k3)
    COLUMNS FROM PATH AS (city, utc_date)
    SET (uniq_id = md5sum(k1, city))
    )
    WITH RESOURCE 'my_spark';
    ```

    `hdfs://hdfs_host:hdfs_port/user/starRocks/data/input/dir/city=beijing` ディレクトリには次のファイルが含まれています:

    `[hdfs://hdfs_host:hdfs_port/user/starRocks/data/input/dir/city=beijing/utc_date=2019-06-26/0000.csv, hdfs://hdfs_host:hdfs_port/user/starRocks/data/input/dir/city=beijing/utc_date=2019-06-26/0001.csv, ...]`

    ファイルパス内の city と utc_date フィールドが抽出されます

5. インポートするデータをフィルタリングします。k1 値が 10 より大きい列のみがインポートされます。

    ```sql
    LOAD LABEL example_db.label10
    (
    DATA INFILE("hdfs://hdfs_host:hdfs_port/user/starRocks/data/input/file")
    INTO TABLE `my_table`
    WHERE k1 > 10
    )
    WITH RESOURCE 'my_spark';
    ```

6. Hive 外部テーブルからインポートし、ソーステーブルの uuid 列をグローバル辞書を通じてビットマップ型に変換します。

    ```sql
    LOAD LABEL db1.label1
    (
    DATA FROM TABLE hive_t1
    INTO TABLE tbl1
    SET
    (
    uuid=bitmap_dict(uuid)
    )
    )
    WITH RESOURCE 'my_spark';
    ```