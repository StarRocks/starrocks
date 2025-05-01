---
displayed_sidebar: docs
---

# CREATE RESOURCE

## 説明

このステートメントはリソースを作成するために使用されます。リソースを作成できるのは、ユーザー root または admin のみです。現在、サポートされているのは Spark と Hive のリソースのみです。将来的には、Spark/GPU をクエリに、HDFS/S3 を外部ストレージに、MapReduce を ETL に使用するなど、他の外部リソースが StarRocks に追加される可能性があります。

構文:

```sql
CREATE [EXTERNAL] RESOURCE "resource_name"
PROPERTIES ("key"="value", ...)
```

注意:  

1. PROPERTIES はリソースの種類を指定します。現在、サポートされているのは Spark と Hive のみです。
2. PROPERTIES はリソースの種類によって異なります。詳細は例を参照してください。

## 例

1. yarn クラスター モードで spark0 という名前の Spark リソースを作成します。

    ```sql
    CREATE EXTERNAL RESOURCE "spark0"
    PROPERTIES
    (
        "type" = "spark",
        "spark.master" = "yarn",
        "spark.submit.deployMode" = "cluster",
        "spark.jars" = "xxx.jar,yyy.jar",
        "spark.files" = "/tmp/aaa,/tmp/bbb",
        "spark.executor.memory" = "1g",
        "spark.yarn.queue" = "queue0",
        "spark.hadoop.yarn.resourcemanager.address" = "127.0.0.1:9999",
        "spark.hadoop.fs.defaultFS" = "hdfs://127.0.0.1:10000",
        "working_dir" = "hdfs://127.0.0.1:10000/tmp/starrocks",
        "broker" = "broker0",
        "broker.username" = "user0",
        "broker.password" = "password0"
    );
    ```

    Spark に関連するパラメータは以下の通りです:

    ```plain text
    1. spark.master: 必須です。現在、yarn と spark://host:port がサポートされています。
    2. spark.submit.deployMode: Spark プログラムのデプロイメント モードが必要です。クラスターとクライアントをサポートします。
    3. spark.hadoop.yarn.resourcemanager.address: master が yarn の場合に必須です。
    4. spark.hadoop.fs.defaultFS: master が yarn の場合に必須です。
    5. その他のパラメータは任意です。詳細は http://spark.apache.org/docs/latest/configuration.html を参照してください。
    ```

    Spark が ETL に使用される場合、working_dir と broker を指定する必要があります。指示は以下の通りです:

    ```plain text
    working_dir: ETL に使用されるディレクトリです。spark が ETL リソースとして使用される場合に必須です。例: hdfs://host:port/tmp/starrocks。
    broker: ブローカーの名前です。spark が ETL リソースとして使用され、事前に `ALTER SYSTEM ADD BROKER` コマンドで設定する必要がある場合に必須です。
    broker.property_key: ブローカーが ETL によって作成された中間ファイルを読み取る際に指定する必要があるプロパティ情報です。
    ```

2. hive0 という名前の Hive リソースを作成します。

    ```sql
    CREATE EXTERNAL RESOURCE "hive0"
    PROPERTIES
    (
        "type" = "hive",
        "hive.metastore.uris" = "thrift://10.10.44.98:9083"
    );
    ```