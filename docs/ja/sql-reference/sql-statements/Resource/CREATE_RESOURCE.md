---
displayed_sidebar: docs
---

# CREATE RESOURCE

## 説明

リソースを作成します。作成できるリソースのタイプは、Apache Spark™、Apache Hive™、Apache Iceberg、Apache Hudi、および JDBC です。Spark リソースは、Spark Load で YARN の設定、中間データの保存パス、Broker の設定などのロード情報を管理するために使用されます。Hive、Iceberg、Hudi、および JDBC リソースは、[外部テーブル](../../../data_source/External_table.md) のクエリに関与するデータソースアクセス情報を管理するために使用されます。

:::tip

- SYSTEM レベルの CREATE RESOURCE 権限を持つユーザーのみがこの操作を実行できます。
- JDBC リソースは StarRocks v2.3 以降でのみ作成できます。

:::

## 構文

```sql
CREATE [EXTERNAL] RESOURCE "resource_name"
PROPERTIES ("key"="value", ...)
```

## パラメータ

- `resource_name`: 作成するリソースの名前。命名規則については、[System limits](../../System_limit.md) を参照してください。

- `PROPERTIES`: リソースタイプのプロパティを指定します。PROPERTIES はリソースタイプによって異なります。詳細は例を参照してください。

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
    1. spark.master: 必須です。現在、yarn と spark ://host:port がサポートされています。
    2. spark.submit.deployMode: Spark プログラムのデプロイメントモードは必須です。クラスターとクライアントをサポートします。
    3. spark.hadoop.yarn.resourcemanager.address: master が yarn の場合に必須です。
    4. spark.hadoop.fs.defaultFS: master が yarn の場合に必須です。
    5. その他のパラメータは任意です。詳細は http://spark.apache.org/docs/latest/configuration.html を参照してください。
    ```

    Spark が ETL に使用される場合、working_DIR と broker を指定する必要があります。指示は以下の通りです:

    ```plain text
    working_dir: ETL に使用されるディレクトリです。spark が ETL リソースとして使用される場合に必須です。例: hdfs://host:port/tmp/starrocks。
    broker: Broker の名前です。spark が ETL リソースとして使用され、事前に `ALTER SYSTEM ADD BROKER` コマンドで設定する必要がある場合に必須です。
    broker.property_key: Broker が ETL によって作成された中間ファイルを読み取る際に指定する必要があるプロパティ情報です。
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