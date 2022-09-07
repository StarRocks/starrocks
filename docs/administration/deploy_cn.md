# Deploy a Compute Node

A Compute Node (CN) is responsible for SQL execution. It helps improve the computing capacity of a StarRocks cluster. This topic describes how to configure and deploy a CN node. You can add multiple CN nodes by repeating the following steps.

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
bin/start_cn.sh --daemon
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

## Stop CN node

Run the following command to stop the CN node.

```bash
./bin/stop_cn.sh
```
