---
displayed_sidebar: docs
---

# CREATE RESOURCE

## 説明

このステートメントはリソースを作成するために使用されます。リソースを作成できるのは、ユーザ root または admin のみです。現在、サポートされているのは Spark と Hive のリソースのみです。将来的には、Spark/GPU をクエリに、HDFS/S3 を外部ストレージに、MapReduce を ETL に使用するなど、他の外部リソースが StarRocks に追加される可能性があります。

構文:

```sql
CREATE [EXTERNAL] RESOURCE "resource_name"
PROPERTIES ("key"="value", ...)
```

注意:  

1. PROPERTIES はリソースのタイプを指定します。現在、サポートされているのは Spark と Hive のみです。
2. PROPERTIES はリソースのタイプによって異なります。詳細は例を参照してください。

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
    2. spark.submit.deployMode: Spark プログラムのデプロイ モードは必須です。cluster と client をサポートします。
    3. spark.hadoop.yarn.resourcemanager.address: master が yarn の場合に必須です。
    4. spark.hadoop.fs.defaultFS: master が yarn の場合に必須です。
    5. その他のパラメータは任意です。詳細は http://spark.apache.org/docs/latest/configuration.html を参照してください。
    ```

    Spark を ETL に使用する場合、working_DIR と broker を指定する必要があります。指示は以下の通りです:

    ```plain text
    working_dir: ETL が使用するディレクトリです。spark が ETL リソースとして使用される場合に必須です。例: hdfs://host:port/tmp/starrocks。
    broker: broker の名前です。spark が ETL リソースとして使用される場合に必須で、事前に `ALTER SYSTEM ADD BROKER` コマンドを使用して設定する必要があります。
    broker.property_key: broker が ETL によって作成された中間ファイルを読み取る際に指定する必要があるプロパティ情報です。
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