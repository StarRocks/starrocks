---
displayed_sidebar: docs
---

# EXPORT

## 説明

テーブルのデータを指定された場所にエクスポートします。

これは非同期操作です。エクスポートタスクを送信した後にエクスポート結果が返されます。[SHOW EXPORT](SHOW_EXPORT.md) を使用して、エクスポートタスクの進行状況を確認できます。

> **注意**
>
> StarRocks テーブルからデータをエクスポートするには、その StarRocks テーブルに対して EXPORT 権限を持つユーザーである必要があります。EXPORT 権限がない場合は、[GRANT](../../account-management/GRANT.md) に従って、StarRocks クラスターに接続するために使用するユーザーに EXPORT 権限を付与してください。

## 構文

```SQL
EXPORT TABLE <table_name>
[PARTITION (<partition_name>[, ...])]
[(<column_name>[, ...])]
TO <export_path>
[opt_properties]
WITH BROKER
[broker_properties]
```

## パラメーター

- `table_name`

  テーブルの名前です。StarRocks は、`engine` が `olap` または `mysql` のテーブルのデータをエクスポートすることをサポートしています。

- `partition_name`

  データをエクスポートしたいパーティションです。このパラメーターを指定しない場合、デフォルトで StarRocks はテーブルのすべてのパーティションからデータをエクスポートします。

- `column_name`

  データをエクスポートしたいカラムです。このパラメーターを使用して指定するカラムの順序は、テーブルのスキーマと異なる場合があります。このパラメーターを指定しない場合、デフォルトで StarRocks はテーブルのすべてのカラムからデータをエクスポートします。

- `export_path`

  テーブルのデータをエクスポートしたい場所です。場所にパスが含まれている場合は、パスがスラッシュ（/）で終わることを確認してください。そうでない場合、パス内の最後のスラッシュ（/）以降の部分がエクスポートされたファイルの名前のプレフィックスとして使用されます。デフォルトでは、ファイル名のプレフィックスが指定されていない場合、`data_` がファイル名のプレフィックスとして使用されます。

- `opt_properties`

  エクスポートタスクのために設定できるオプションのプロパティです。

  構文:

  ```SQL
  [PROPERTIES ("<key>"="<value>", ...)]
  ```

  | **プロパティ**     | **説明**                                              |
  | ---------------- | ------------------------------------------------------------ |
  | column_separator | エクスポートされたファイルで使用したいカラムセパレーターです。デフォルト値: `\t`. |
  | line_delimiter   | エクスポートされたファイルで使用したい行セパレーターです。デフォルト値: `\n`. |
  | load_mem_limit   | 各個別の BE でエクスポートタスクに許可される最大メモリです。単位: バイト。デフォルトの最大メモリは 2 GB です。 |
  | timeout          | エクスポートタスクがタイムアウトするまでの時間です。単位: 秒。デフォルト値: `86400`、つまり 1 日です。 |
  | include_query_id | エクスポートされたファイル名に `query_id` が含まれるかどうかを指定します。有効な値: `true` と `false`。値 `true` はファイル名に `query_id` が含まれることを指定し、値 `false` はファイル名に `query_id` が含まれないことを指定します。 |

- `WITH BROKER`

  v2.4 以前では、使用したいブローカーを指定するために `WITH BROKER "<broker_name>"` を入力します。v2.5 以降では、ブローカーを指定する必要はありませんが、`WITH BROKER` キーワードは保持する必要があります。

- `broker_properties`

  ソースデータを認証するために使用される情報です。認証情報はデータソースによって異なります。詳細については、[BROKER LOAD](../BROKER_LOAD.md) を参照してください。

## 例

### テーブルのすべてのデータを HDFS にエクスポート

次の例は、`testTbl` テーブルのすべてのデータを HDFS クラスターの `hdfs://<hdfs_host>:<hdfs_port>/a/b/c/` パスにエクスポートします。

```SQL
EXPORT TABLE testTbl 
TO "hdfs://<hdfs_host>:<hdfs_port>/a/b/c/" 
WITH BROKER
(
        "username"="xxx",
        "password"="yyy"
);
```

### テーブルの指定されたパーティションのデータを HDFS にエクスポート

次の例は、`testTbl` テーブルの 2 つのパーティション `p1` と `p2` のデータを HDFS クラスターの `hdfs://<hdfs_host>:<hdfs_port>/a/b/c/` パスにエクスポートします。

```SQL
EXPORT TABLE testTbl
PARTITION (p1,p2) 
TO "hdfs://<hdfs_host>:<hdfs_port>/a/b/c/" 
WITH BROKER
(
        "username"="xxx",
        "password"="yyy"
);
```

### カラムセパレーターを指定してテーブルのすべてのデータを HDFS にエクスポート

次の例は、`testTbl` テーブルのすべてのデータを HDFS クラスターの `hdfs://<hdfs_host>:<hdfs_port>/a/b/c/` パスにエクスポートし、カラムセパレーターとしてカンマ（`,`）を使用することを指定します。

```SQL
EXPORT TABLE testTbl 
TO "hdfs://<hdfs_host>:<hdfs_port>/a/b/c/" 
PROPERTIES
(
    "column_separator"=","
) 
WITH BROKER
(
    "username"="xxx",
    "password"="yyy"
);
```

次の例は、`testTbl` テーブルのすべてのデータを HDFS クラスターの `hdfs://<hdfs_host>:<hdfs_port>/a/b/c/` パスにエクスポートし、カラムセパレーターとして `\x01`（Hive がサポートするデフォルトのカラムセパレーター）を使用することを指定します。

```SQL
EXPORT TABLE testTbl 
TO "hdfs://<hdfs_host>:<hdfs_port>/a/b/c/" 
PROPERTIES
(
    "column_separator"="\\x01"
) 
WITH BROKER;
```

### ファイル名のプレフィックスを指定してテーブルのすべてのデータを HDFS にエクスポート

次の例は、`testTbl` テーブルのすべてのデータを HDFS クラスターの `hdfs://<hdfs_host>:<hdfs_port>/a/b/c/` パスにエクスポートし、エクスポートされたファイルの名前のプレフィックスとして `testTbl_` を使用することを指定します。

```SQL
EXPORT TABLE testTbl 
TO "hdfs://<hdfs_host>:<hdfs_port>/a/b/c/testTbl_" 
WITH BROKER;
```

### AWS S3 にデータをエクスポート

次の例は、`testTbl` テーブルのすべてのデータを AWS S3 バケットの `s3-package/export/` パスにエクスポートします。

```SQL
EXPORT TABLE testTbl 
TO "s3a://s3-package/export/"
WITH BROKER
(
    "aws.s3.access_key" = "xxx",
    "aws.s3.secret_key" = "yyy",
    "aws.s3.region" = "zzz"
);
```