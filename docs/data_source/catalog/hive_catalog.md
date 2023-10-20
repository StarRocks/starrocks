# Hive catalog

This topic describes how to create a Hive catalog, and how to configure your StarRocks cluster for querying data from Apache Hive™.

A Hive catalog is an external catalog supported in StarRocks 2.3 and later versions. It enables you to query data from Hive without loading data into StarRocks or creating external tables.

## Usage notes

- StarRocks supports querying data files of Hive in the following formats: Parquet, ORC, and CSV.
- StarRocks supports querying Hive data of the following types: TINYINT, SMALLINT, DATE, BOOLEAN, INTEGER, BIGINT, TIMESTAMP, STRING, VARCHAR, CHAR, DOUBLE, FLOAT, DECIMAL, and ARRAY. Note that an error occurs if you query Hive data of unsupported data types, including INTERVAL, BINARY, MAP, STRUCT, and UNION.

- You can use the [DESC](../../sql-reference/sql-statements/Utility/DESCRIBE.md) statement to view the schema of a Hive table in StarRocks 2.4 and later versions.

## Before you begin

Before you create a Hive catalog, configure your StarRocks cluster so that StarRocks can access the data storage system and metadata service of your Hive cluster. StarRocks supports two data storage systems for Hive: HDFS and Amazon S3. StarRocks supports one metadata service for Hive: Hive metastore.

### HDFS

If you use HDFS as the data storage system, configure your StarRocks cluster as follows:

- (Optional) Set the username that is used to access your HDFS and Hive metastore. By default, StarRocks uses the username of the FE and BE processes to access your HDFS and Hive metastore. You can also set the username via the `HADOOP_USERNAME` parameter in the **fe/conf/hadoop_env.sh** file of each FE and the **be/conf/hadoop_env.sh** file of each BE. Then restart each FE and BE to make the parameter settings take effect. You can set only one username for a StarRocks cluster.

- When you query Hive data, the FEs and BEs use the HDFS client to access HDFS. In general, StarRocks starts the HDFS client using the default configurations. However, in the following cases, you need to configure your StarRocks cluster:
  - If your HDFS cluster runs in HA mode, add the **hdfs-site.xml** file of your HA cluster to the **$FE_HOME/conf path** of each FE and the **$BE_HOME/conf** path of each BE.
  - If you configure View File System (ViewFs) to your HDFS cluster, add the **core-site.xml** file of your HDFS cluster to the **$FE_HOME/conf** path of each FE and the **$BE_HOME/conf** path of each BE.

> **Note**
>
> If an error (unknown host) occurs when you send a query, configure the mapping between the host names and IP addresses of HDFS nodes under the **/etc/hosts** path.

### Kerberos authentication

If  Kerberos authentication is enabled for your HDFS cluster or Hive metastore, configure your StarRocks cluster as follows:

- Run the `kinit -kt keytab_path principal` command on each FE and each BE to obtain Ticket Granting Ticket (TGT) from Key Distribution Center (KDC). To run this command, you must have the permissions to access your HDFS cluster and Hive metastore. Note that accessing KDC with this command is time-sensitive. Therefore, you need to use the cron to run this command periodically.
- Add `JAVA_OPTS="-Djava.security.krb5.conf=/etc/krb5.conf"` to the **$FE_HOME/conf/fe.conf** file of each FE and the **$BE_HOME/conf/be.conf** file of each BE. `/etc/krb5.conf` indicates the path of the **krb5.conf** file. You can modify the path based on your needs.

### Amazon S3

If you use Amazon S3 as the data storage system, configure your StarRocks cluster as follows:

1. Add the following configuration items to the **$FE_HOME/conf/core-site.xml** file of each FE.

      ```XML
      <configuration>
            <property>
                <name>fs.s3a.impl</name>
                <value>org.apache.hadoop.fs.s3a.S3AFileSystem</value>
            </property>
            <property>
                <name>fs.AbstractFileSystem.s3a.impl</name>
                <value>org.apache.hadoop.fs.s3a.S3A</value>
            </property>
            <property>
                <name>fs.s3a.access.key</name>
                <value>******</value>
            </property>
            <property>
                <name>fs.s3a.secret.key</name>
                <value>******</value>
            </property>
            <property>
                <name>fs.s3a.endpoint</name>
                <value>******</value>
            </property>
            <property>
                <name>fs.s3a.connection.maximum</name>
                <value>500</value>
            </property>
      </configuration>
      ```

     The following table describes the configuration items.

      | **Configuration item**    |**Description**                                              |
      | ------------------------- | ------------------------------------------------------------ |
      | fs.s3a.access.key         | The access key ID of the root user or an Identity and Access Management (IAM) user. For information about how to obtain the access key ID, see [Understanding and getting your AWS credentials](https://docs.aws.amazon.com/general/latest/gr/aws-sec-cred-types.html). |
      | fs.s3a.secret.key         | The secret access key of the root user or an IAM user. For information about how to obtain the secret access key, see [Understanding and getting your AWS credentials](https://docs.aws.amazon.com/general/latest/gr/aws-sec-cred-types.html). |
      | fs.s3a.endpoint           | The regional endpoint of your Amazon S3 service. For example, `s3.us-west-2.amazonaws.com` is the endpoint of US West (Oregon). For information about how to obtain your regional endpoint, see [Amazon Simple Storage Service endpoints and quotas](https://docs.aws.amazon.com/general/latest/gr/s3.html). |
      | fs.s3a.connection.maximum | The maximum number of concurrent connections that are allowed by your Amazon S3 service. This parameter defaults to `500`.  If an error (`Timeout waiting for connection from poll`) occurs when you query Hive data, increase the value of this parameter. |

2. Add the following configuration items to the **$BE_HOME/conf/be.conf** file of each BE.

      | **Configuration item**           | **Description**                                              |
      | -------------------------------- | ------------------------------------------------------------ |
      | object_storage_access_key_id     | The access key ID of the root user or an IAM user. This parameter value is the same as the `fs.s3a.access.key` parameter. |
      | object_storage_secret_access_key | The secret access key of the root user or an IAM user. The value of the parameter is the same as the value of the `fs.s3a.secret.key` parameter. |
      | object_storage_endpoint          | The regional endpoint of your Amazon S3 service. The value of the parameter is the same as the value of the `fs.s3a.endpoint` parameter. |

3. Restart all BEs and FEs.

## Create a Hive catalog

After you complete the preceding configurations, you can create a Hive catalog using the following syntax:

```SQL
CREATE EXTERNAL CATALOG catalog_name 
PROPERTIES ("key"="value", ...);
```

The parameter description is as follows:

- `catalog_name`: the name of the Hive catalog. This parameter is required.<br/>The naming conventions are as follows:

  - The name can contain letters, digits (0-9), and underscores (_). It must start with a letter.
  - The name cannot exceed 64 characters in length.

- `PROPERTIES`: the properties of the Hive catalog. This parameter is required.<br/>You can configure the following properties:

    | **Property**        | **Required** | **Description**                                              |
    | ------------------- | ------------ | ------------------------------------------------------------ |
    | type                | Yes          | The type of the data source. Set the value to `hive`.        |
    | hive.metastore.uris | Yes          | The URI of the Hive metastore. The parameter value is in the following format: `thrift://<IP address of Hive metastore>:<port number>`. The port number defaults to 9083. |

> **Note**
>
> Before querying Hive data, you must add the mapping between the domain name and IP address of the Hive metastore node to the **/etc/hosts** path. Otherwise, StarRocks may fail to access Hive metastore when you start a query.

## Use catalog to query Hive data

After the catalog is created, you can use the Hive catalog to query Hive data. For more information, see [Query external data](../catalog/query_external_data.md).

## Caching strategy of Hive metadata

StarRocks develops a query execution plan based on the metadata of Hive tables. Therefore, the response time of Hive metastore directly affects the time consumed by a query. To reduce the impact, StarRocks provides caching strategies, based on which StarRocks can cache and update the metadata of Hive tables, such as partition statistics and file information of partitions. Currently, the following two strategies are available:

- **Asynchronous update**: This is the default strategy that is used to cache the metadata of Hive tables in StarRocks and requires no further configurations on your StarRocks cluster or Hive metastore. With this strategy, caching is automatically triggered when two conditions are met (see [How it works](#how-it-works) for more information). This means that the metadata cached in StarRocks cannot always stay up-to-date with the metadata in Hive. In some cases, you need to manually update the metadata cached in StarRocks.
- **Automatic incremental update**: To enable this strategy, you need to add additional configurations to your StarRocks cluster and Hive metastore. After this strategy is enabled, the metadata cached in StarRocks always stay up-to-date with the metadata in Hive, and no manual updates are required.

### Asynchronous update

#### How it works

If a query hits a partition of a Hive table, StarRocks asynchronously caches the metadata of the partition. If another query hits the partition again and the time interval from the last update exceeds the default time interval, StarRocks asynchronously updates the metadata cached in StarRocks. Otherwise, the cached metadata will not be updated. This process of update is called lazy update.

You can set the default time interval by the `hive_meta_cache_refresh_interval_s` parameter. The parameter value defaults to `7200`. Unit: seconds. You can set this parameter in the **fe.conf** file of each FE, and then restart each FE to make the parameter value take effect.

If a query hits a partition and the time interval from the last update exceeds the default time interval, but the metadata cached in StarRocks is not updated, that means the cached metadata is invalid and will be cached again at the next query. You can set the time period during which the cached metadata is valid by the `hive_meta_cache_ttl_s` parameter. The parameter value defaults to `86400`. Unit: Seconds. You can set this parameter in the **fe.conf** file of each FE, and then restart each FE to make the parameter value take effect.

#### Examples

For example, there is a Hive table named `table1`, which has four partitions: `p1`, `p2`, `p3`, and `p4`. A query hit `p1`, and StarRocks cached the metadata of `p1`. If the default time interval to update the metadata cached in StarRocks is 1 hour, there are the following two situations for subsequent updates:

- If another query hits `p1` again and the current time from the last update is more than 1 hour, StarRocks asynchronously updates the cached metadata of `p1`.
- If another query hits `p1` again and the current time from the last update is less than 1 hour, StarRocks does not asynchronously update the cached metadata of `p1`.

#### Manual update

To query the latest Hive data, make sure that the metadata cached in StarRocks is updated to the latest. If the time interval from the last update does not exceed the default time interval, you can manually update the cached metadata before sending a query.

- Execute the following statement to synchronize the schema changes (such as adding columns or removing partitions) of a Hive table to StarRocks.

    ```SQL
    REFRESH EXTERNAL TABLE [external_catalog.][db_name.]table_name;
    ```

- Execute the following statement to synchronize the data changes (such as data ingestion) of a Hive table to StarRocks.

    ```SQL
    REFRESH EXTERNAL TABLE [external_catalog.][db_name.]table_name
    [PARTITION ('partition_name', ...)];
    ```

For more information about the parameter descriptions and examples of using the REFRESH EXTERNAL TABEL statement, see [REFRESH EXTERNAL TABEL](../../sql-reference/sql-statements/data-definition/REFRESH_EXTERNAL_TABLE.md).

### Automatic incremental update

This strategy enables FEs to read the events from Hive metastore, such as adding columns, removing partitions, or updating data. Then StarRocks automatically updates the metadata cached in StarRocks based on these events. Follow the steps below to enable this strategy.

> **Note**
>
> After you enable this strategy, the asynchronous update strategy is disabled.

#### Step 1: Configure the event listener for your Hive metastore

Both Hive metastore 2.x and 3.x support the configuration of the event listener. The following configuration is for Hive metastore 3.1.2. Add the following configuration items to the file **$HiveMetastore/conf/hive-site.xml** and restart Hive metastore.

```XML
<property>
    <name>hive.metastore.event.db.notification.api.auth</name>
    <value>false</value>
</property>
<property>
    <name>hive.metastore.notifications.add.thrift.objects</name>
    <value>true</value>
</property>
<property>
    <name>hive.metastore.alter.notifications.basic</name>
    <value>false</value>
</property>
<property>
    <name>hive.metastore.dml.events</name>
    <value>true</value>
</property>
<property>
    <name>hive.metastore.transactional.event.listeners</name>
    <value>org.apache.hive.hcatalog.listener.DbNotificationListener</value>
</property>
<property>
    <name>hive.metastore.event.db.listener.timetolive</name>
    <value>172800s</value>
</property>
<property>
    <name>hive.metastore.server.max.message.size</name>
    <value>858993459</value>
</property>
```

You can search for the `event id` parameter in the FE log to check whether the event listener is successfully configured. If not, the `event id` parameter is `0`.

#### Step 2: Enable automatically incremental update of StarRocks

Configure the following parameters in the **$FE_HOME/conf/fe.conf** file of each FE to read the events from Hive metastore.

| **Parameter**                      | **Description**                                              |
| ---------------------------------- | ------------------------------------------------------------ |
| enable_hms_events_incremental_sync | Whether the automatic incremental update strategy is enabled. Valid values are:<ul><li>`TRUE`: means enabled. The value of the parameter defaults to `TRUE`.</li><li>`FALSE`: means disabled. </li></ul>|
| hms_events_polling_interval_ms     | The time interval for StarRocks to read events from Hive metastore. The parameter defaults to `5000`. Unit: milliseconds. |
| hms_events_batch_size_per_rpc      | The maximum number of events that StarRocks can read at a time. The parameter value defaults to `500`. |
| enable_hms_parallel_process_evens  | Whether the read events are processed in parallel. Valid values are:<ul><li>`TRUE`: means the events are processed in parallel. The value of the parameter defaults to `TRUE`.</li><li>`FALSE`: means the events are not processed in parallel.</li></ul> |
| hms_process_events_parallel_num    | The maximum number of events that can be processed in parallel. This parameter defaults to `4`. |

## References

- To view examples of creating an external catalog, see [CREATE EXTERNAL CATALOG](../../sql-reference/sql-statements/data-definition/CREATE_EXTERNAL_CATALOG.md).
- To view all catalogs in the current StarRocks cluster, see [SHOW CATALOGS](../../sql-reference/sql-statements/data-manipulation/SHOW_CATALOGS.md).
- To delete an external catalog, see [DROP CATALOG](../../sql-reference/sql-statements/data-definition/DROP_CATALOG.md).
