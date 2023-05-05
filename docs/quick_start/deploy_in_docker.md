# Quick start: Deploy StarRocks with Docker

This quickstart provides a guide to:

- Use [Docker](https://docs.docker.com/engine/install/) to deploy StarRocks with one FE and one BE.
- Connect to StarRocks with MySQL client.
- Create a table, insert some data, and query the data.

## Prerequisites

1. Docker
2. MySQL client

## Step 1: Deploy

To choose a StarRocks version, go to the [StarRocks Dockerhub repository](https://hub.docker.com/r/starrocks/allin1-ubuntu/tags) and choose a version based on the version tag.

For example, to deploy StarRocks v2.5.4, run the following command:

```sh
docker run -p 9030:9030 -p 8030:8030 -p 8040:8040 -itd starrocks/allin1-ubuntu:2.5.4
```

Then you can check the container status using the following command:

```sh
docker ps
```

## Step 2: Connect to StarRocks

StarRocks needs some time to get ready. We recommend that you wait at least 30 seconds before connecting.

```sh
mysql -P9030 -h127.0.0.1 -uroot --prompt="StarRocks > "
```

## Step 3: Use StarRocks

Use the following commands to check the status of FE and BE. If `Alive` shows `true` for both FE and BE, StarRocks is healthy and ready to go.

FE

```SQL
SHOW PROC '/frontends'\G
```

```plaintext
StarRocks > SHOW PROC '/frontends'\G
*************************** 1. row ***************************
             Name: be552e5f0de9_9010_1680659932444
               IP: be552e5f0de9
      EditLogPort: 9010
         HttpPort: 8030
        QueryPort: 9030
          RpcPort: 9020
             Role: LEADER
        ClusterId: 1556630880
             Join: true
            Alive: true
ReplayedJournalId: 944
    LastHeartbeat: 2023-04-05 02:49:36
         IsHelper: true
           ErrMsg: 
        StartTime: 2023-04-05 01:59:00
          Version: 2.5.4-1021a9299
1 row in set (0.05 sec)

```

BE

```SQL
SHOW PROC '/backends'\G
```

```plaintext
StarRocks > SHOW PROC '/backends'\G
*************************** 1. row ***************************
            BackendId: 10004
                   IP: be552e5f0de9
        HeartbeatPort: 9050
               BePort: 9060
             HttpPort: 8040
             BrpcPort: 8060
        LastStartTime: 2023-04-05 01:59:13
        LastHeartbeat: 2023-04-05 02:50:06
                Alive: true
 SystemDecommissioned: false
ClusterDecommissioned: false
            TabletNum: 32
     DataUsedCapacity: 4.474 KB
        AvailCapacity: 197.683 GB
        TotalCapacity: 235.983 GB
              UsedPct: 16.23 %
       MaxDiskUsedPct: 16.23 %
               ErrMsg: 
              Version: 2.5.4-1021a9299
               Status: {"lastSuccessReportTabletsTime":"2023-04-05 02:49:14"}
    DataTotalCapacity: 197.683 GB
          DataUsedPct: 0.00 %
             CpuCores: 7
    NumRunningQueries: 0
           MemUsedPct: 0.24 %
           CpuUsedPct: 0.5 %
1 row in set (0.03 sec)
```

Now you can create a table and insert some data.

**_NOTE:_** This quickstart deploys one BE, you need to add `properties ("replication_num" = "1")` in the CREATE TABLE clause, so only one replica of data is persisted in the BE.

```SQL
CREATE DATABASE test;

USE test;

CREATE TABLE tbl(c1 int, c2 int) distributed by hash(c1) properties ("replication_num" = "1");

INSERT INTO tbl VALUES (1, 1), (2, 2), (3, 3);
```

Query the data

```plaintext
StarRocks > SELECT * FROM tbl;
+------+------+
| c1   | c2   |
+------+------+
|    3 |    3 |
|    1 |    1 |
|    2 |    2 |
+------+------+
3 rows in set (0.03 sec)
```
