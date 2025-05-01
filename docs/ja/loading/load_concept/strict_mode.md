---
displayed_sidebar: docs
---

# Strict モード

Strict モードは、データロードのために設定できるオプションのプロパティです。これは、ロードの動作と最終的にロードされるデータに影響を与えます。

このトピックでは、Strict モードとは何か、そしてどのように設定するかを紹介します。

## Strict モードを理解する

データロード中、ソースカラムのデータ型が、宛先カラムのデータ型と完全に一致しない場合があります。そのような場合、StarRocks はデータ型が一致しないソースカラムの値に対して変換を行います。データ変換は、フィールドのデータ型が一致しないことやフィールドの長さがオーバーフローするなどの様々な問題で失敗することがあります。適切に変換できなかったソースカラムの値は不適格なカラム値とされ、不適格なカラム値を含むソース行は「不適格な行」と呼ばれます。Strict モードは、データロード中に不適格な行をフィルタリングするかどうかを制御するために使用されます。

Strict モードは次のように動作します：

- Strict モードが有効な場合、StarRocks は適格な行のみをロードします。不適格な行をフィルタリングし、不適格な行の詳細を返します。
- Strict モードが無効な場合、StarRocks は不適格なカラム値を `NULL` に変換し、これらの `NULL` 値を含む不適格な行を適格な行と一緒にロードします。

次の点に注意してください：

- 実際のビジネスシナリオでは、適格な行と不適格な行の両方が `NULL` 値を含む場合があります。宛先カラムが `NULL` 値を許可しない場合、StarRocks はエラーを報告し、`NULL` 値を含む行をフィルタリングします。

- [Stream Load](../../sql-reference/sql-statements/loading_unloading/STREAM_LOAD.md), [Broker Load](../../sql-reference/sql-statements/loading_unloading/BROKER_LOAD.md), [Routine Load](../../sql-reference/sql-statements/loading_unloading/routine_load/CREATE_ROUTINE_LOAD.md), または [Spark Load](../../sql-reference/sql-statements/loading_unloading/SPARK_LOAD.md) ジョブでフィルタリングできる不適格な行の最大割合は、オプションのジョブプロパティ `max_filter_ratio` によって制御されます。[INSERT](../../sql-reference/sql-statements/loading_unloading/INSERT.md) は `max_filter_ratio` プロパティの設定をサポートしていません。

例えば、CSV 形式のデータファイルから StarRocks テーブルのカラムに `\N` (`\N` は `NULL` 値を示します)、`abc`、`2000`、および `1` の値を持つ4行をロードしたいとします。宛先の StarRocks テーブルカラムのデータ型は TINYINT [-128, 127] です。

- ソースカラム値 `\N` は TINYINT に変換される際に `NULL` として処理されます。

  > **NOTE**
  >
  > `\N` は、宛先データ型に関係なく、常に変換時に `NULL` として処理されます。

- ソースカラム値 `abc` は TINYINT ではないため、変換に失敗し、`NULL` として処理されます。

- ソースカラム値 `2000` は TINYINT がサポートする範囲を超えているため、変換に失敗し、`NULL` として処理されます。

- ソースカラム値 `1` は TINYINT 型の値 `1` に適切に変換できます。

Strict モードが無効な場合、StarRocks は4行すべてをロードします。

Strict モードが有効な場合、StarRocks は `\N` または `1` を持つ行のみをロードし、`abc` または `2000` を持つ行をフィルタリングします。フィルタリングされた行は、`max_filter_ratio` パラメータで指定された不十分なデータ品質のためにフィルタリングできる行の最大割合に対してカウントされます。

### Strict モードが無効な場合の最終ロードデータ

| ソースカラム値 | TINYINT への変換時のカラム値 | 宛先カラムが NULL 値を許可する場合のロード結果 | 宛先カラムが NULL 値を許可しない場合のロード結果 |
| ------------------- | --------------------------------------- | ------------------------------------------------------ | ------------------------------------------------------------ |
| \N                 | NULL                                    | 値 `NULL` がロードされます。                            | エラーが報告されます。                                        |
| abc                 | NULL                                    | 値 `NULL` がロードされます。                            | エラーが報告されます。                                        |
| 2000                | NULL                                    | 値 `NULL` がロードされます。                            | エラーが報告されます。                                        |
| 1                   | 1                                       | 値 `1` がロードされます。                               | 値 `1` がロードされます。                                     |

### Strict モードが有効な場合の最終ロードデータ

| ソースカラム値 | TINYINT への変換時のカラム値 | 宛先カラムが NULL 値を許可する場合のロード結果       | 宛先カラムが NULL 値を許可しない場合のロード結果 |
| ------------------- | --------------------------------------- | ------------------------------------------------------------ | ------------------------------------------------------------ |
| \N                 | NULL                                    | 値 `NULL` がロードされます。                                  | エラーが報告されます。                                        |
| abc                 | NULL                                    | 値 `NULL` は許可されず、フィルタリングされます。 | エラーが報告されます。                                        |
| 2000                | NULL                                    | 値 `NULL` は許可されず、フィルタリングされます。 | エラーが報告されます。                                        |
| 1                   | 1                                       | 値 `1` がロードされます。                                     | 値 `1` がロードされます。                                     |

## Strict モードを設定する

[Stream Load](../../sql-reference/sql-statements/loading_unloading/STREAM_LOAD.md), [Broker Load](../../sql-reference/sql-statements/loading_unloading/BROKER_LOAD.md), [Routine Load](../../sql-reference/sql-statements/loading_unloading/routine_load/CREATE_ROUTINE_LOAD.md), または [Spark Load](../../sql-reference/sql-statements/loading_unloading/SPARK_LOAD.md) ジョブを実行してデータをロードする場合、`strict_mode` パラメータを使用してロードジョブのために Strict モードを設定します。有効な値は `true` と `false` です。デフォルト値は `false` です。値 `true` は Strict モードを有効にし、値 `false` は Strict モードを無効にします。

[INSERT](../../sql-reference/sql-statements/loading_unloading/INSERT.md) を実行してデータをロードする場合、`enable_insert_strict` セッション変数を使用して Strict モードを設定します。有効な値は `true` と `false` です。デフォルト値は `true` です。値 `true` は Strict モードを有効にし、値 `false` は Strict モードを無効にします。

以下に例を示します：

### Stream Load

```Bash
curl --location-trusted -u <username>:<password> \
    -H "strict_mode: {true | false}" \
    -T <file_name> -XPUT \
    http://<fe_host>:<fe_http_port>/api/<database_name>/<table_name>/_stream_load
```

Stream Load の詳細な構文とパラメータについては、[STREAM LOAD](../../sql-reference/sql-statements/loading_unloading/STREAM_LOAD.md) を参照してください。

### Broker Load

```SQL
LOAD LABEL [<database_name>.]<label_name>
(
    DATA INFILE ("<file_path>"[, "<file_path>" ...])
    INTO TABLE <table_name>
)
WITH BROKER
(
    "username" = "<hdfs_username>",
    "password" = "<hdfs_password>"
)
PROPERTIES
(
    "strict_mode" = "{true | false}"
)
```

上記のコードスニペットは HDFS を例として使用しています。Broker Load の詳細な構文とパラメータについては、[BROKER LOAD](../../sql-reference/sql-statements/loading_unloading/BROKER_LOAD.md) を参照してください。

### Routine Load

```SQL
CREATE ROUTINE LOAD [<database_name>.]<job_name> ON <table_name>
PROPERTIES
(
    "strict_mode" = "{true | false}"
) 
FROM KAFKA
(
    "kafka_broker_list" ="<kafka_broker1_ip>:<kafka_broker1_port>[,<kafka_broker2_ip>:<kafka_broker2_port>...]",
    "kafka_topic" = "<topic_name>"
)
```

上記のコードスニペットは Apache Kafka® を例として使用しています。Routine Load の詳細な構文とパラメータについては、[CREATE ROUTINE LOAD](../../sql-reference/sql-statements/loading_unloading/routine_load/CREATE_ROUTINE_LOAD.md) を参照してください。

### Spark Load

```SQL
LOAD LABEL [<database_name>.]<label_name>
(
    DATA INFILE ("<file_path>"[, "<file_path>" ...])
    INTO TABLE <table_name>
)
WITH RESOURCE <resource_name>
(
    "spark.executor.memory" = "3g",
    "broker.username" = "<hdfs_username>",
    "broker.password" = "<hdfs_password>"
)
PROPERTIES
(
    "strict_mode" = "{true | false}"   
)
```

上記のコードスニペットは HDFS を例として使用しています。Spark Load の詳細な構文とパラメータについては、[SPARK LOAD](../../sql-reference/sql-statements/loading_unloading/SPARK_LOAD.md) を参照してください。

### INSERT

```SQL
SET enable_insert_strict = {true | false};
INSERT INTO <table_name> ...
```

INSERT の詳細な構文とパラメータについては、[INSERT](../../sql-reference/sql-statements/loading_unloading/INSERT.md) を参照してください。