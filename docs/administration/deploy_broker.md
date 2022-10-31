# Deploy Broker

This topic describes how to deploy Broker. With Broker, StarRocks can read data from source such as HDFS and S3, and pre-process, load, and backup the data with its own computing resources.

## Download and decompress the installer

[Download](https://download.starrocks.com/en-US/download/community) and decompress StarRocks installer.

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
