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

```sh
docker run -p 9030:9030 -p 8030:8030 -p 8040:8040 -itd starrocks/allin1-ubuntu:3.0.0-rc01
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

Use the following commands to check the status of FE and BE. If `Alive` shows true for both FE and BE, StarRocks is healthy and ready to go.

FE

```SQL
SHOW PROC '/frontends'\G
```

```plaintext
StarRocks > SHOW PROC '/frontends'\G
*************************** 1. row ***************************
             Name: 47ecfdef4bd0_9010_1680695755143
               IP: 47ecfdef4bd0
      EditLogPort: 9010
         HttpPort: 8030
        QueryPort: 9030
          RpcPort: 9020
             Role: LEADER
        ClusterId: 293521004
             Join: true
            Alive: true
ReplayedJournalId: 115
    LastHeartbeat: 2023-04-05 12:01:32
         IsHelper: true
           ErrMsg:
        StartTime: 2023-04-05 11:56:02
          Version: 3.0.0-RC01-a99fb8571
1 row in set (0.17 sec)
```

BE

```SQL
SHOW PROC '/backends'\G
```

```plaintext
StarRocks > SHOW PROC '/backends'\G
*************************** 1. row ***************************
            BackendId: 10004
                   IP: 47ecfdef4bd0
        HeartbeatPort: 9050
               BePort: 9060
             HttpPort: 8040
             BrpcPort: 8060
        LastStartTime: 2023-04-05 11:56:26
        LastHeartbeat: 2023-04-05 12:00:22
                Alive: true
 SystemDecommissioned: false
ClusterDecommissioned: false
            TabletNum: 30
     DataUsedCapacity: 0.000
        AvailCapacity: 192.752 GB
        TotalCapacity: 235.983 GB
              UsedPct: 18.32 %
       MaxDiskUsedPct: 18.32 %
               ErrMsg:
              Version: 3.0.0-RC01-a99fb8571
               Status: {"lastSuccessReportTabletsTime":"2023-04-05 11:59:27"}
    DataTotalCapacity: 192.752 GB
          DataUsedPct: 0.00 %
             CpuCores: 7
    NumRunningQueries: 0
           MemUsedPct: 0.08 %
           CpuUsedPct: 0.4 %
1 row in set (0.02 sec)
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
