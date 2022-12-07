# Deploy a Compute Node [Preview]

A compute node (CN) is a stateless computing service that does not maintain data itself. It only provides extra computing resources for queries. This topic describes how to configure and deploy a CN node. You can add multiple CN nodes by repeating the following steps.

## Principle

The lifecycle of an SQL statement in StarRocks can be divided into three phases: query parsing, planning, and execution. CN takes some or all of the computation tasks in the execution phase. It works as follows: First, the FE splits the parsed SQL statements into logical execution units, splits the logical execution units into physical execution units according to data distribution and operator types, and then assigns them to BEs and CNs.

During query execution, BEs run computation tasks that are prior to data shuffling, and perform data reading and writing. CNs receive the shuffled data from BEs, run part of the computation tasks such as JOIN, and return the results to FEs.

## Download and decompress the installer

[Download](https://www.starrocks.io/download/community) and decompress StarRocks installer.

```bash
tar -xzvf StarRocks-x.x.x.tar.gz
```

> Caution
>
> Replace the file name in the command with the actual name of the file you downloaded.

## Configure a CN node

Navigate to the **StarRocks-x.x.x/be** path.

```bash
cd StarRocks-x.x.x/be
```

> Caution
>
> Replace the file name in the command with the actual name of the file you downloaded.

Modify the CN configuration file **conf/cn.conf**. Because the default configuration can be used directly to start the cluster, the following example does not change any configuration item. If you need to change a configuration item in the production environment, see [BE configuration items](../administration/Configuration.md#be-configuration-items) for instructions, because most of CN's parameters are inherited from BEs.

## Add a CN node

Add a CN node to the StarRocks cluster via your MySQL client.

```sql
mysql> ALTER SYSTEM ADD COMPUTE NODE "host:port";
```

> Caution
>
> `host` must match the pre-specified `priority_networks`, and `port` must match `heartbeat_service_port` specified in **cn.conf** (defaults to `9050`).

If any issue occurs when you add the CN node, you can drop the node by using the following command:

```sql
mysql> ALTER SYSTEM DROP COMPUTE NODE "host:port";
```

> Caution
>
> `host` and `port` must be the same as those of the CN node you added.

## Start the CN node

Run the following command to start the CN node:

```shell
./bin/start_cn.sh --daemon
```

## Check whether the CN node starts

You can check whether the CN node starts properly by via your MySQL client.

```sql
SHOW PROC '/compute_nodes'\G
```

Example:

```Plain Text
MySQL [(none)]> SHOW PROC '/compute_nodes'\G
**************************** 1. row ******************** ******
        ComputeNodeId: 78825
              Cluster: default_cluster
                   IP: 172.26.xxx.xx
        HeartbeatPort: 9050
               BePort: 9060
             HttpPort: 8040
             BrpcPort: 8060
        LastStartTime: NULL
        LastHeartbeat: NULL
                Alive: true
 SystemDecommissioned: false
ClusterDecommissioned: false
               ErrMsg:
              Version: 2.4.0-76-36ea96e
1 row in set (0.01 sec)
```

If `Alive` is `true`, the CN node is properly started and added to the cluster.

If the CN node is not properly added to the cluster, you can check the **log/cn.WARNING** log file for troubleshooting.

After CNs are properly started, you need to set the system variables `prefer_compute_node` and `use_compute_nodes` to scale computing resources during queries. For more information, see [System variables](../reference/System_variable.md).

## Stop the CN node

Run the following command to stop the CN node:

```bash
./bin/stop_cn.sh
```
