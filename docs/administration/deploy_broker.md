# Deploy Broker

This topic describes how to deploy Broker. With Broker, StarRocks can read data from source such as HDFS and S3, and pre-process, load, and backup the data with its own computing resources.

## Download and decompress the installer

[Download](https://www.starrocks.io/download/community) and decompress StarRocks installer.

```bash
tar -xzvf StarRocks-x.x.x.tar.gz
```

> **CAUTION**
>
> Replace the file name in the command as the real file name you downloaded.

## Configure a Broker

Navigate to **StarRocks-x.x.x/apache_hdfs_broker**.

```bash
cd StarRocks-x.x.x/apache_hdfs_broker
```

> **CAUTION**
>
> Replace the file name in the command as the real file name you downloaded.
Specify the Broker configuration file **conf/apache_hdfs_broker.conf**. Because the default configuration can be used directly, no configuration item is changed in the following example. You can copy your own HDFS cluster configuration file and paste it under **conf** directory.

## Start a Broker

Run the following command to start Broker.

```bash
./apache_hdfs_broker/bin/start_broker.sh --daemon
```

## Add a Broker to cluster

Run the following command to add the Broker to cluster.

```sql
ALTER SYSTEM ADD BROKER broker_name "broker_ip:broker_port";
```

> **NOTE**
>
> - By default, the port for Broker is `8000`.
> - When multiple Brokers are added to a cluster at the same time, they share the same `broker_name`.

## verify if Broker starts

You can verify if the BE node is started properly via MySQL client.

```sql
SHOW PROC "/brokers"\G
```

Example:

```plain text
MySQL [(none)]> SHOW PROC "/brokers"\G
*************************** 1. row ***************************
          Name: broker1
            IP: x.x.x.x
          Port: 8000
         Alive: true
 LastStartTime: 2022-05-19 11:21:36
LastUpdateTime: 2022-05-19 11:28:31
        ErrMsg:
1 row in set (0.00 sec)
```

When the field `Alive` is `true`, the Broker is properly started and added to the cluster.

## Stop a Broker

Run the following command to stop a Broker.

```bash
./bin/stop_broker.sh --daemon
```

## Configure a broker

You can only set the configuration items of a broker by changing them in the corresponding configuration file **broker.conf**, and restart the broker to allow the change to take effect.

| Configuration item | Default | Unit | Description |
| ------------------------- | ------------------ | ------ | ------------------------------------------------------------ |
| hdfs_read_buffer_size_kb | 8192 | KB | Size of the buffer that is used to read data from HDFS. |
| hdfs_write_buffer_size_kb | 1024 | KB | Size of the buffer that is used to write data into HDFS. |
| client_expire_seconds | 300 | Second | Client sessions will be deleted if they do not receive any ping after the specified time. |
| broker_ipc_port | 8000 | N/A | The HDFS thrift RPC port. |
| disable_broker_client_expiration_checking | false | N/A | Whether to disable the checking and clearing of the expired OSS file descriptors, which, in some cases, causes the broker to stuck when OSS is close. To avoid this situation, you can set this parameter to `true` to disable the checking. |
| sys_log_dir | ${BROKER_HOME}/log | N/A | The directory used to store system logs (including INFO, WARNING, ERROR, and FATAL). |
| sys_log_level | INFO | N/A | The log level. Valid values include INFO, WARNING, ERROR, and FATAL. |
| sys_log_roll_mode | SIZE-MB-1024 | N/A | The mode how system logs are segmented into log rolls. Valid values include TIME-DAY, TIME-HOUR, and SIZE-MB-nnn. The default value indicates that logs are segmented into rolls which are 1 GB each. |
| sys_log_roll_num | 30 | N/A | The number of log rolls to reserve. |
| audit_log_dir | ${BROKER_HOME}/log | N/A | The directory that stores audit log files. |
| audit_log_modules | Empty string | N/A | The modules for which StarRocks generates audit log entries. By default, StarRocks generates audit logs for the slow_query module and the query module. You can specify multiple modules, whose names must be separated by a comma (,) and a space. |
| audit_log_roll_mode | TIME-DAY | N/A | Valid values include `TIME-DAY`, `TIME-HOUR`, and `SIZE-MB-<size>`. |
| audit_log_roll_num | 10 | N/A | This configuration does not work if the audit_log_roll_mode is set to `SIZE-MB-<size>`. |
| sys_log_verbose_modules | com.starrocks | N/A | The modules for which StarRocks generates system logs. Valid values are namespaces in BE, including `starrocks`, `starrocks::debug`, `starrocks::fs`, `starrocks::io`, `starrocks::lake`, `starrocks::pipeline`, `starrocks::query_cache`, `starrocks::stream`, and `starrocks::workgroup`. |
