---
displayed_sidebar: "English"
sidebar_position: 1
---

# Deploy StarRocks with Docker

This QuickStart tutorial guides you through the procedures to deploy StarRocks on your local machine with Docker. Before getting started, you can read [StarRocks Architecture](../introduction/Architecture.md) for more conceptual details.

By following these steps, you can deploy a simple StarRocks cluster with **one FE node** and **one BE node**. It can help you complete the upcoming QuickStart tutorials on [creating a table](../quick_start/Create_table.md) and [loading and querying data](../quick_start/Import_and_query.md), and thereby acquaints you with the basic operations of StarRocks.

> **CAUTION**
>
> Deploying StarRocks with the Docker image used in this tutorial merely applies to the situation when you need to verify a DEMO with a small dataset. It is not recommended for a massive testing or production environment. To deploy a high-availability StarRocks cluster, see [Deployment overview](../deployment/deployment_overview.md) for other options that suit your scenarios.

## Prerequisites

Before deploying StarRocks in Docker, make sure the following requirements are satisfied:

- **Hardware**

  We recommend deploying StarRocks on a machine with 8 CPU cores and 16 GB of memory or more.

- **Software**

  You must have the following software installed on your machine:

  - [Docker Engine](https://docs.docker.com/engine/install/) (17.06.0 or later) AND you must have at least 5GB of free space in the disk partitions of the meta directory.  See https://github.com/StarRocks/starrocks/issues/35608 for details. 
  - MySQL client (5.5 or later)

## Step 1: Download the StarRocks Docker image

Download a StarRocks Docker image from [StarRocks Docker Hub](https://hub.docker.com/r/starrocks/allin1-ubuntu/tags). You can choose a specific version based on the tag of the image.

```Bash
sudo docker run -p 9030:9030 -p 8030:8030 -p 8040:8040 \
    -itd starrocks/allin1-ubuntu
```

> **TROUBLESHOOTING**
>
> If any of the above ports on the host machine is occupied, the system prints "docker: Error response from daemon: driver failed programming external connectivity on endpoint tender_torvalds (): Bind for 0.0.0.0:xxxx failed: port is already allocated.". You can allocate available ports on the host machine by changing the ports preceding the colons (:) in the command.

You can check if the container is created and running properly by running the following command:

```Bash
sudo docker ps
```

As shown below, if the `STATUS` of your StarRocks containers is `Up`, you have successfully deployed StarRocks in the Docker container.

```Plain
CONTAINER ID   IMAGE                                          COMMAND                  CREATED         STATUS                 PORTS                                                                                                                             NAMES
8962368f9208   starrocks/allin1-ubuntu:branch-3.0-0afb97bbf   "/bin/sh -c ./start_…"   4 minutes ago   Up 4 minutes           0.0.0.0:8037->8030/tcp, :::8037->8030/tcp, 0.0.0.0:8047->8040/tcp, :::8047->8040/tcp, 0.0.0.0:9037->9030/tcp, :::9037->9030/tcp   xxxxx
```

## Step 2: Connect to StarRocks

After StarRocks is deployed properly, you can connect to it via a MySQL client.

```Bash
mysql -P9030 -h127.0.0.1 -uroot --prompt="StarRocks > "
```

> **CAUTION**
>
> If you have allocated a different port for `9030` in the `docker run` command, you must replace `9030` in the above command with the port you have allocated.

You can check the status of the FE node by executing the following SQL:

```SQL
SHOW PROC '/frontends'\G
```

Example:

```Plain
StarRocks > SHOW PROC '/frontends'\G
*************************** 1. row ***************************
             Name: 8962368f9208_9010_1681370634632
               IP: 8962368f9208
      EditLogPort: 9010
         HttpPort: 8030
        QueryPort: 9030
          RpcPort: 9020
             Role: LEADER
        ClusterId: 555505802
             Join: true
            Alive: true
ReplayedJournalId: 99
    LastHeartbeat: 2023-04-13 07:28:50
         IsHelper: true
           ErrMsg: 
        StartTime: 2023-04-13 07:24:11
          Version: BRANCH-3.0-0afb97bbf
1 row in set (0.02 sec)
```

- If the field `Alive` is `true`, this FE node is properly started and added to the cluster.
- If the field `Role` is `FOLLOWER`, this FE node is eligible to be elected as the Leader FE node.
- If the field `Role` is `LEADER`, this FE node is the Leader FE node.

You can check the status of the BE node by executing the following SQL:

```SQL
SHOW PROC '/backends'\G
```

Example:

```Plain
StarRocks > SHOW PROC '/backends'\G
*************************** 1. row ***************************
            BackendId: 10004
                   IP: 8962368f9208
        HeartbeatPort: 9050
               BePort: 9060
             HttpPort: 8040
             BrpcPort: 8060
        LastStartTime: 2023-04-13 07:24:25
        LastHeartbeat: 2023-04-13 07:29:05
                Alive: true
 SystemDecommissioned: false
ClusterDecommissioned: false
            TabletNum: 30
     DataUsedCapacity: 0.000 
        AvailCapacity: 527.437 GB
        TotalCapacity: 1.968 TB
              UsedPct: 73.83 %
       MaxDiskUsedPct: 73.83 %
               ErrMsg: 
              Version: BRANCH-3.0-0afb97bbf
               Status: {"lastSuccessReportTabletsTime":"2023-04-13 07:28:26"}
    DataTotalCapacity: 527.437 GB
          DataUsedPct: 0.00 %
             CpuCores: 16
    NumRunningQueries: 0
           MemUsedPct: 0.02 %
           CpuUsedPct: 0.1 %
1 row in set (0.00 sec)
```

If the field `Alive` is `true`, this BE node is properly started and added to the cluster.

## Stop and remove the Docker container

After completing the whole QuickStart tutorial, you can stop and remove the container that hosts your StarRocks cluster with its container ID.

> **NOTE**
>
> You can get the `container_id` of your Docker container by running `sudo docker ps`.

Run the following command to stop the container:

```Bash
# Replace <container_id> with the container ID of your StarRocks cluster.
sudo docker stop <container_id>
```

If you do not need the container any longer, you can remove it by running the following command:

```Bash
# Replace <container_id> with the container ID of your StarRocks cluster.
sudo docker rm <container_id>
```

> **CAUTION**
>
> The removal of the container is irreversible. Make sure you have a backup of the important data in the container before removing it.

## What to do next

Having deployed StarRocks, you can continue the QuickStart tutorials on [creating a table](../quick_start/Create_table.md) and [loading and querying data](../quick_start/Import_and_query.md).

<img referrerpolicy="no-referrer-when-downgrade" src="https://static.scarf.sh/a.png?x-pxid=f5ae0b2c-3578-4a40-9056-178e9837cfe0" />
