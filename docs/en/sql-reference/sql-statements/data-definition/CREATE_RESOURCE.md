# CREATE RESOURCE

## description

This statement is used to create resources. Only user root or admin can create resources. Currently, only Spark and Hive resources are supported. Other external resources may be added to StarRocks in the future, such as Spark/GPU for query, HDFS/S3 for external storage, and MapReduce for ETL.

Syntax:

```sql
CREATE [EXTERNAL] RESOURCE "resource_name"
PROPERTIES ("key"="value", ...);
```

Note:  

1. PROPERTIES specifies the resource type. Currently, only Spark and Hive are supported.
2. The PROPERTIES vary depending on the resource type. See examples for details.

## example

1. Create a Spark resource named spark0 in yarn Cluster mode.

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

    Parameters related to Spark are as follows:

    ```plain text
    1. spark.master: required. Currently, yarn and spark ://host:port. are supported. 
    2. spark.submit.deployMode: deployment mode of the Spark program is required. Support cluster and client.
    3. spark.hadoop.yarn.resourcemanager.address: required when master is yarn.
    4. spark.hadoop.fs.defaultFS: required when master is yarn.
    5. Other parameters are optional. Please refer to http://spark.apache.org/docs/latest/configuration.html
    ```

    If Spark is used for ETL, working_DIR and broker need to be specified. The instructions are as follows:

    ```plain text
    working_dir: Directory used by ETL. It is required when spark is used as ETL resource. For example：hdfs://host:port/tmp/starrocks。
    broker: Name of broker. It is required when spark is used as ETL resource and needs to be configured beforehand by using `ALTER SYSTEM ADD BROKER` command. 
    broker.property_key: It is the property information needed to be specified when broker reads the intermediate files created by ETL. 
    ```

2. Create a Hive resource named hive0.

    ```sql
    CREATE EXTERNAL RESOURCE "hive0"
    PROPERTIES
    (
        "type" = "hive",
        "hive.metastore.uris" = "thrift://10.10.44.98:9083"
    );
    ```

## keyword

CREATE RESOURCE
