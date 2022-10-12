# Deploy a Compute Node

A Compute Node (CN) a stateless computing service that does not maintain data itself. It only provides extra computing resources during queries. This topic describes how to configure and deploy a CN node. You can add multiple CN nodes by repeating the following steps.

## Principle

The life cycle of SQL statements in StarRocks can be divided into three phases: query parsing, planning, and execution. CN takes some or all of the computation tasks in the execution phase. Its working processing is as follows: first, FE divides the parsed SQL statements into logical execution units, splits the logical execution units into physical execution units according to the data distribution and operator types, and then assigns them to BEs and CNs.

During execution, BEs do the computation tasks before data shuffle, and data read and write. CNs receive the shuffled data from BEs, do part of the computation tasks such as JOIN, and return the results to FEs.

## Download and decompress the installer

[Download](https://download.starrocks.com/en-US/download/community) and decompress StarRocks installer.

```bash
tar -xzvf StarRocks-x.x.x.tar.gz
```

> Caution
>
> Replace the file name in the command as the real file name you downloaded.

## Configure CN node

Navigate to the **StarRocks-x.x.x/be** path.

```bash
cd StarRocks-x.x.x/be
```

> Caution
>
> Replace the file name in the command as the real file name you downloaded.

Modify the CN node configuration file **conf/cn.conf**. Because the default configuration can be used directly, no configuration item is changed in the following example. If you need to change any configuration item in the production environment, see [Configuration](../administration/Configuration.md) for more instruction, because most of its parameters are inherited from BE.

## Add a CN node

Add a CN node to StarRocks cluster via your MySQL client.

```sql
mysql> ALTER SYSTEM ADD COMPUTE NODE "host:port";
```

> Caution
>
> Parameter `host` must match the pre-specified `priority_networks`, and parameter `port` must match the `heartbeat_service_port` specified in **cn.conf** (default is`9050`).

If any problem occur while adding the CN node, you can drop it with the following command.

```sql
mysql> ALTER SYSTEM DROP COMPUTE NODE "host:port";
```

> Caution
>
> Parameter `host` and `port` must be identical with those when adding the node.

## Start the CN node

Run the following command to start the CN node.

```shell
sh bin/start_cn.sh --daemon
```

## Verify if CN node starts

You can verify if the CN node is started properly via MySQL client.

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

When the field `Alive` is `true`, the CN node is properly started and added to the cluster.

If the CN node is not properly added to the cluster, you can check the **log/cn.WARNING** log file to troubleshoot the problem.

After the Compute Nodes are started properly, you need to set the system variables `prefer_compute_node`, and `use_compute_nodes` to allow them to scale the computing resources out during queries. See [System Variables](../reference/System_variable.md) for more information.

## Stop CN node

Run the following command to stop the CN node.

```bash
./bin/stop_cn.sh
```
