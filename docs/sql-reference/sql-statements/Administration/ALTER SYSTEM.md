# ALTER SYSTEM

## Description

Manages FE, BE, CN, Broker nodes, and metadata snapshots in a cluster.

> **NOTE**
>
> Only the `cluster_admin` role has the privilege to execute this SQL statement.

## Syntax and parameters

### FE

- Add a Follower FE.

  ```SQL
  ALTER SYSTEM ADD FOLLOWER "<fe_host>:<edit_log_port>"[, ...]
  ```

  You can check the status of the new Follower FE by executing `SHOW PROC '/frontends'\G`.

- Drop a Follower FE.

  ```SQL
  ALTER SYSTEM DROP FOLLOWER "<fe_host>:<edit_log_port>"[, ...]
  ```

- Add an Observer FE.

  ```SQL
  ALTER SYSTEM ADD OBSERVER "<fe_host>:<edit_log_port>"[, ...]
  ```

  You can check the status of the new Observer FE by executing `SHOW PROC '/frontends'\G`.

- Drop an Observer FE.

  ```SQL
  ALTER SYSTEM DROP OBSERVER "<fe_host>:<edit_log_port>"[, ...]
  ```

| **Parameter**      | **Required** | **Description**                                                     |
| ------------------ | ------------ | ------------------------------------------------------------------- |
| fe_host            | Yes          | The host name or IP address of the FE instance. Use the value of configuration item `priority_networks` if your instance has multiple IP addresses. |
| edit_log_port      | Yes          | BDB JE communication port of the FE node. Default: `9010`.          |

### BE

- Add a BE node.

  ```SQL
  ALTER SYSTEM ADD BACKEND "<be_host>:<heartbeat_service_port>"[, ...]
  ```

  You can check the status of the new BE by executing [SHOW BACKENDS](../Administration/SHOW%20BACKENDS.md).

- Drop a BE node.

  > **NOTE**
  >
  > You cannot drop the BE node that stores the tablets of single-replica tables.

  ```SQL
  ALTER SYSTEM DROP BACKEND "<be_host>:<heartbeat_service_port>"[, ...]
  ```

- Decommission a BE node.

  ```SQL
  ALTER SYSTEM DECOMMISSION BACKEND "<be_host>:<heartbeat_service_port>"[, ...]
  ```

  Unlike dropping a BE node, which is removing it forcibly from the cluster, decommissioning a BE means removing it safely. It is an asynchronous operation. When a BE is decommissioned, the data on the BE is first migrated to other BEs, and then the BE is removed from the cluster. Data loading and query will not be affected during the data migration. You can check whether the operation is successful using [SHOW BACKENDS](../Administration/SHOW%20BACKENDS.md). If the operation is successful, the decommissioned BE will not be returned. If the operation fails, the BE will still be online. You can manually cancel the operation using [CANCEL DECOMMISSION](../Administration/CANCEL%20DECOMMISSION.md).

| **Parameter**          | **Required** | **Description**                                                                            |
| ---------------------- | ------------ | ------------------------------------------------------------------------------------------ |
| be_host                | Yes          | The host name or IP address of the BE instance. Use the value of configuration item `priority_networks` if your instance has multiple IP addresses.|
| heartbeat_service_port | Yes          | BE heartbeat service port. BE uses this port to receive heartbeat from FE. Default: `9050`.|

### CN

- Add a CN node.

  ```SQL
  ALTER SYSTEM ADD COMPUTE NODE "<cn_host>:<heartbeat_service_port>"[, ...]
  ```

  You can check the status of the new CN by executing [SHOW COMPUTE NODES](../Administration/SHOW%20COMPUTE%20NODES.md).

- Drop a CN node.

  ```SQL
  ALTER SYSTEM DROP COMPUTE NODE "<cn_host>:<heartbeat_service_port>"[, ...]
  ```

| **Parameter**          | **Required** | **Description**                                                                            |
| ---------------------- | ------------ | ------------------------------------------------------------------------------------------ |
| cn_host                | Yes          | The host name or IP address of the CN instance. Use the value of configuration item `priority_networks` if your instance has multiple IP addresses.|
| heartbeat_service_port | Yes          | CN heartbeat service port. CN uses this port to receive heartbeat from FE. Default: `9050`.|

### Broker

- Add Broker nodes. You can use Broker nodes to load data from HDFS or cloud storage into StarRocks. For more information, see [Load data from HDFS](../../../loading/hdfs_load.md) or [Load data from cloud storage](../../../loading/cloud_storage_load.md).

  ```SQL
  ALTER SYSTEM ADD BROKER <broker_name> "<broker_host>:<broker_ipc_port>"[, ...]
  ```

  You can add multiple Broker nodes with one SQL. Each `<broker_host>:<broker_ipc_port>` pair represents one Broker node. And they share a common `broker_name`. You can check the status of the new Broker node by executing [SHOW BROKER](../Administration/SHOW%20BROKER.md).

- Drop Broker nodes.

> **CAUTION**
>
> Dropping a Broker node terminates the tasks currently running on it.

  - Drop one or multiple Broker nodes with the same `broker_name`.

    ```SQL
    ALTER SYSTEM DROP BROKER <broker_name> "<broker_host>:<broker_ipc_port>"[, ...]
    ```

  - Drop all Broker nodes with the same `broker_name`.

    ```SQL
    ALTER SYSTEM DROP ALL BROKER <broker_name>
    ```

| **Parameter**   | **Required** | **Description**                                                              |
| --------------- | ------------ | ---------------------------------------------------------------------------- |
| broker_name     | Yes          | The name of the Broker node(s). Multiple Broker nodes can use the same name. |
| broker_host     | Yes          | The host name or IP address of the Broker instance. Use the value of configuration item `priority_networks` if your instance has multiple IP addresses.|
| broker_ipc_port | Yes          | The thrift server port on the Broker node. The Broker node uses it to receive requests from FE or BE. Default: `8000`. |

### Create image

Create an image file. An image file is a snapshot of the FE metadata.

```SQL
ALTER SYSTEM CREATE IMAGE
```

Creating an image is an asynchronous operation on the Leader FE. You can check the start time and end time of the operation in the FE log file **fe.log**. A log like `triggering a new checkpoint manually...` indicates that the image creation has started, and a log like `finished save image...` indicates the image has been created.

## Usage notes

- Adding and dropping FE, BE, CN, or Broker nodes are synchronous operations. You cannot cancel the node dropping operations.
- You cannot drop the FE node in a single-FE cluster.
- You cannot directly drop the Leader FE node in a multi-FE cluster. To drop it, you must first restart it. After StarRocks elects a new Leader FE, you can then drop the previous one.
- You cannot drop BE nodes if the number of the remained BE nodes is less than the number of data replicas. For example, if you have three BE nodes in your cluster and you store your data in three replicas, you cannot drop any of the BE nodes. And if you have four BE nodes and three replicas, you can drop one BE node.
- The difference between dropping and decommissioning a BE node is that, when you drop a BE node, StarRocks removes it forcibly from the cluster and make up the dropped tablets after the removal, and when you decommission a BE node, StarRocks first migrates the tablets on the decommissioned BE node to others, and then removes the node.

## Examples

Example 1: Add a Follower FE node.

```SQL
ALTER SYSTEM ADD FOLLOWER "x.x.x.x:9010";
```

Example 2: Drop two Observer FE nodes simultaneously.

```SQL
ALTER SYSTEM DROP OBSERVER "x.x.x.x:9010","x.x.x.x:9010";
```

Example 3: Add a BE node.

```SQL
ALTER SYSTEM ADD BACKEND "x.x.x.x:9050";
```

Example 4: Drop two BE nodes simultaneously.

```SQL
ALTER SYSTEM DROP BACKEND "x.x.x.x:9050", "x.x.x.x:9050";
```

Example 5: Decommission two BE nodes simultaneously.

```SQL
ALTER SYSTEM DECOMMISSION BACKEND "x.x.x.x:9050", "x.x.x.x:9050";
```

Example 6: Add two Broker nodes with the same `broker_name` - `hdfs`.

```SQL
ALTER SYSTEM ADD BROKER hdfs "x.x.x.x:8000", "x.x.x.x:8000";
```

Example 7: Drop two Broker nodes from `amazon_s3`.

```SQL
ALTER SYSTEM DROP BROKER amazon_s3 "x.x.x.x:8000", "x.x.x.x:8000";
```

Example 8: Drop all Broker nodes in `amazon_s3`.

```SQL
ALTER SYSTEM DROP ALL BROKER amazon_s3;
```
