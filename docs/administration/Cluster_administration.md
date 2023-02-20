# Manage a cluster

This topic explains how to manage a cluster.

## Start and stop StarRocks

We recommend that your machines in the cluster are equipped with the following configurations:

- The machines used as frontends (FEs) are equipped with an 8-core CPU and 16 GB of RAM or higher configurations.
- The machines used as backends (BEs) are equipped with a 16-core CPU and 64 GB of RAM or higher configurations.

### Start FE

Before you set up an FE, configure the `meta_dir` parameter and the communication ports:

- `meta_dir`: the directory in which the FE stores metadata. By default, set the `meta_dir` parameter to `${STARROCKS_HOME}/meta`. If you want to specify the directory, define it in the **fe/conf/fe.conf** file. You must create the directory before you configure the `meta_dir` parameter. The metadata stored in the FE is the metadata of the whole system, we recommend that you put the metadata directory in a separate folder.
- Communication ports: there are four communication ports available for the BE.

| **Port name** | **Default port number** | **Description**                                              |
| ------------- | ----------------------- | ------------------------------------------------------------ |
| http_port     | 8030                    | The port that is used by the FE to communicate with an HTTP protocol. |
| rpc_port      | 9020                    | The port that is used by the FE to communicate with a thrift server. |
| query_port    | 9030                    | The port that is used by the FE to communicate with a MySQL protocol. |
| edit_log_port | 9010                    | The port that is used for communication between the FEs in a cluster |

After you complete the previous configurations, perform the following steps to start the FE:

1. Navigate to the deployment directory of the FE.
2. Run `./bin/start_fe.sh --daemon` to start the FE.

You can deploy multiple FEs to ensure high availability. Typically, you can deploy three FEs: one leader FE and two follower FEs.

When you start multiple FEs, start the follower FE one by one. When you upgrade a cluster, upgrade the follower FEs and then upgrade the leader FE. By doing so, you can detect errors that may occur during the upgrades of the follower FEs and make sure that the cluster can continue to properly process queries.

**Note**:

- If you configured multiple follower FEs, the cluster can select one follower FE as the leader FE to process queries only when more than half of configured follower FEs are available.
- We recommend that you verify every FE that you want to start. You can send a query to an FE to verify the FE.

### Start BE

Before you set up the BE, configure the  `storage_root_path` parameter and the communication ports:

- `storage_root_path`: indicates the directory for the BE to store the storage files. The directory needs to be created in advance. We recommend creating one directory per disk.
- Communication ports: there are four communication ports available for the BE.

| **Port** **name**  | **Default** **port** **number** | **Description**                                              |
| ---------------------- | ----------------------------------- | ------------------------------------------------------------ |
| be_port                | 9060                                | The port that is used by the BE to communicate with an FE.   |
| be_http_port           | 8040                                | The port that is used by the BE to communicate with an HTTP protocol. |
| heartbeat_service_port | 9050                                | The port that is used by the BE to receive heartbeats from an FE. |
| brpc                   | 8060                                | The port that is used for communication between the BEs in a cluster. |

### Start a Compute Node (Optional)

#### Check the configuration of the Compute Node

|Configuration|Description|Default|
|---|---|---|
|thrift_port|Thrift Server port of the Compute Node. The port is used to receive requests from FE.|9060|
|be_http_port|HTTP Server port of the Compute Node.|8040|
|heartbeat_service_port|Thrift server port of the Compute Node. The port is used to receive requests from FE.|9050|
|brpc_port|RPC port between BE and the Compute Node.|8060|

#### Start the Compute Node process

Navigate to the deployment directory of the Compute Node and start the Compute Node process.

```shell
cd StarRocks-x.x.x/be
./bin/start_cn.sh --daemon
```

### Check the health status of a cluster

After you set up the FEs, BEs, and CNs of a cluster, you need to check their statuses to ensure they are started normally:

- Run `http://be_host:be_http_port/api/health` to check the statuses of BEs. If `{"status": "OK", "msg": "To Be Added"}` is returned, the BEs are properly started.

- Run `http://fe_host:fe_http_port/api/bootstrap` to check the statuses of FEs. If `{"status": "OK", "msg": "Success"}` is returned, the FEs are properly started.

- Run `http://cn_host:cn_http_port/api/health` to check the statuses of Compute Nodes. If `{"status": "OK", "msg": "To Be Added"}` is returned, the Compute Nodes are properly started.

After the Compute Nodes are started properly, you need to set the system variables `prefer_compute_node`, and `use_compute_nodes` to allow them to scale the computing resources out during queries. See [System Variables](../reference/System_variable.md) for more information.

### Stop a cluster

To stop a cluster, you need to stop all the FEs and BEs in the cluster:

- Go to the deployment directory of each FE and run `./bin/stop_fe.sh`.

- Go to the deployment directory of each BE and run `./bin/stop_be.sh`.

#### Stop a Compute Node

Navigate to the deployment directory of the Compute Node and stop the Compute Node process.

```shell
cd StarRocks-x.x.x/be
./bin/stop_cn.sh
```

## Upgrade StarRocks

StarRocks can perform a rolling upgrade, which allows you to first upgrade the BEs, then the FEs, and finally the Brokers in a cluster. StarRocks ensures that the BEs are backward compatible with the FEs.

> **CAUTION**
>
> - StarRocks ensures that BE is backward compatible with FE. Therefore, you need to **upgrade BE nodes first, and then upgrade FE nodes**. Upgrading them in a wrong order may lead to the incompatibility between FE and BE nodes, which will cause the BE node to stop.
> - When upgrading your StarRocks cluster to a major version from a version earlier than v2.0, you must upgrade it **consecutively from one major version to another**. When upgrading your StarRocks cluster from a version that is later than v2.0, you can upgrade it across major versions. For safety purpose, we recommended upgrading consecutively from one major version to another, for example, 1.19->2.0->2.1->2.2->2.3->2.4. Currently, StarRocks v2.2 and v2.5 are the Long-term Support (LTS) versions. Their support duration lasts more than half a year.
>
> | StarRocks version | Upgrade from | Notice | LTS version |
> | ---- | ------------ | -------- | -------------- |
> | v1.19.x | N/A | | No |
> | v2.0.x | Must be upgraded from v1.19.x | Disable clone before upgrading. | No|
> | v2.1.x | Must be upgraded from v2.0.x | Modify <code>vector_chunk_size</code> and <code>batch_size</code> before grayscale upgrade. | No |
> | v2.2.x | Can be upgraded from v2.1.x and v2.0.x | Set <code>ignore_unknown_log_id</code> to <code>true</code> before rollback. | Yes |
> | v2.3.x | Can be upgraded from v2.2.x, v2.1.x, and v2.0.x | We do not recommend rollback across major versions. Set <code>ignore_unknown_log_id</code> to <code>true</code> before rollback. | No |
> | v2.4.x | Can be upgraded from v2.3.x, v2.2.x, v2.1.x, and v2.0.x | We do not recommend rollback across major versions. Switch to IP address access before rollback if you enabled [FQDN access](../administration/enable_fqdn.md). | No |
> | v2.5.x | Can be upgraded from v2.4.x, v2.3.x, v2.2.x, v2.1.x, and v2.0.x | We do not recommend rollback across major versions. If you have a partitioned table that uses LIST partitioning, you must delete this table before the upgrade. | Yes |

### Before you begin

- Test whether the cluster after the upgrade affects your current data.

    For BE:

    1. Stop a random BE node.
    2. Replace files under **bin** and **lib** of this BE node.
    3. Start this BE node. Check if it is started successfully in the BE log file **be.INFO**.
    4. Check the causes of the failure if the start fails. if the problem is irresolvable, you can drop this BE node, clean the data, restart the BE node with deployment files of the previous version, and add the BE node back to the cluster.

    For FE:

    1. Deploy a new FE node in DEV environment with deployment files of new version.
    2. Modify the FE configuration file **fe.conf**. Assign different ports to this FE node.
    3. Add `cluster_id = 123456` in **fe.conf**.
    4. Add `metadata_failure_recovery = true` in **fe.conf**.
    5. Copy the **meta** directory of the Leader FE node in PRD environment and paste it into that of the DEV environment.
    6. Modify **meta/image/VERSION** in DEV environment. Set `cluster_id` to `123456`.
    7. Start the FE node in DEV environment.
    8. Check if it is started successfully in the FE log file **fe.log**.

- Distribute the BE and FE binary files for new versions of BE and FE to the deployment directory of BE and FE.

  - For a minor version update (for example, from 2.0.x to 2.0.y), you only need to replace **starrocks_be** for the BEs and **starrocks-fe.jar** for the FEs.
  - For a major version upgrade (for example, from 2.0.x to 2.x.x), you need to replace the **bin** and **lib** folders of the BEs and replace the **bin**, **lib**, and **spark-dpp** for FEs.

### Upgrade BE

1. Navigate to the BE working directory and stop the BE node.

    ```shell
    cd StarRocks-x.x.x/be
    sh bin/stop_be.sh
    ```

2. Replace the deployment files of BE.

    ```shell
    mv lib lib.bak 
    mv bin bin.bak
    cp -r /tmp/StarRocks-x.x.x/be/lib  .
    cp -r /tmp/StarRocks-x.x.x/be/bin  .
    ```

3. Start the BE node.

    ```shell
    sh bin/start_be.sh --daemon
    ```

4. Check if the node is started successfully.

    ```shell
    ps aux | grep starrocks_be
    ```

5. Repeat the above procedures to upgrade other BE nodes.

### Upgrade FE

You must upgrade all Follower FE nodes first and then the Leader FE node.

1. Navigate to the FE working directory and stop the FE node.

    ```shell
    cd StarRocks-x.x.x/fe
    sh bin/stop_fe.sh
    ```

2. Replace the deployment files of FE.

    ```shell
    mv lib lib.bak 
    mv bin bin.bak
    cp -r /tmp/StarRocks-x.x.x/fe/lib  .   
    cp -r /tmp/StarRocks-x.x.x/fe/bin  .
    cp -r /tmp/StarRocks-x.x.x/fe/spark-dpp  .
    ```

3. Start the FE node.

    ```shell
    sh bin/start_fe.sh --daemon
    ```

4. Check if the node is started successfully.

    ```shell
    ps aux | grep StarRocksFE
    ```

5. Repeat the above procedures to upgrade other Follower FE nodes, and finally the Leader FE node.

### Upgrade CN

Since the Compute Node node is stateless, you only need to replace the binary file and restart the process. We recommend to stop it gracefully.

```shell
./bin/stop_cn.sh --graceful
```

By using this method, the Compute Node waits until the currently running task finishes before exiting the process.

### Upgrade Broker

1. Navigate to the Broker working directory and stop the Broker node.

    ```shell
    cd StarRocks-x.x.x/apache_hdfs_broker
    sh bin/stop_broker.sh
    ```

2. Replace the deployment files of Broker.

    ```shell
    mv lib lib.bak 
    mv bin bin.bak
    cp -r /tmp/StarRocks-x.x.x/apache_hdfs_broker/lib  .   
    cp -r /tmp/StarRocks-x.x.x/apache_hdfs_broker/bin  .
    ```

3. Start the Broker node.

    ```shell
    sh bin/start_broker.sh --daemon
    ```

4. Check if the node is started successfully.

    ```shell
    ps aux | grep broker
    ```

5. Repeat the above procedures to upgrade other Broker nodes.

## Roll back StarRocks

All StarRocks versions support rollbacks. You need to first roll back the FEs, then the BEs, and finally the Brokers in a cluster. If an exception occurs after you upgrade a cluster, you can perform the following steps to roll back the cluster to the previous version. This way, you can quickly recover the cluster.

### Before you begin

- Distribute the BE and FE binary files for old versions of BE and FE to the deployment directory of BE and FE.

  - For a minor version rollback (for example, from 2.0.y to 2.0.x), you only need to replace **starrocks_be** for the BEs and **starrocks-fe.jar** for the FEs.
  - For a major version rollback (for example, from 2.x.x to 2.0.x), you need to replace the **bin** and **lib** folders of the BEs and replace the **bin**, **lib**, and **spark-dpp** for FEs.

### Roll back FE

You must roll back all Follower FE nodes first and then the Leader FE node.

1. Navigate to the FE working directory and stop the FE node.

    ```shell
    cd StarRocks-x.x.x/fe
    sh bin/stop_fe.sh
    ```

2. Replace the deployment files of FE.

    ```shell
    mv lib lib.bak 
    mv bin bin.bak
    cp -r /tmp/StarRocks-x.x.x/fe/lib  .   
    cp -r /tmp/StarRocks-x.x.x/fe/bin  .
    cp -r /tmp/StarRocks-x.x.x/fe/spark-dpp  .
    ```

3. Start the FE node.

    ```shell
    sh bin/start_fe.sh --daemon
    ```

4. Check if the node is started successfully.

    ```shell
    ps aux | grep StarRocksFE
    ```

5. Repeat the above procedures to rollback other Follower FE nodes, and finally the Leader FE node.

### Roll back BE

1. Navigate to the BE working directory and stop the BE node.

    ```shell
    cd StarRocks-x.x.x/be
    sh bin/stop_be.sh
    ```

2. Replace the deployment files of BE.

    ```shell
    mv lib lib.bak 
    mv bin bin.bak
    cp -r /tmp/StarRocks-x.x.x/be/lib  .
    cp -r /tmp/StarRocks-x.x.x/be/bin  .
    ```

3. Start the BE node.

    ```shell
    sh bin/start_be.sh --daemon
    ```

4. Check if the node is started successfully.

    ```shell
    ps aux | grep starrocks_be
    ```

5. Repeat the above procedures to roll back other BE nodes.

#### Roll back Broker

1. Navigate to the Broker working directory and stop the Broker node.

    ```shell
    cd StarRocks-x.x.x/apache_hdfs_broker
    sh bin/stop_broker.sh
    ```

2. Replace the deployment files of Broker.

    ```shell
    mv lib lib.bak 
    mv bin bin.bak
    cp -r /tmp/StarRocks-x.x.x/apache_hdfs_broker/lib  .   
    cp -r /tmp/StarRocks-x.x.x/apache_hdfs_broker/bin  .
    ```

3. Start the Broker node.

    ```shell
    sh bin/start_broker.sh --daemon
    ```

4. Check if the node is started successfully.

    ```shell
    ps aux | grep broker
    ```

5. Repeat the above procedures to roll back other Broker nodes.

### Usage notes for grayscale upgrade from StarRocks 2.0 to StarRocks 2.1

If you need to perform a grayscale upgrade from StarRocks 2.0 to StarRocks 2.1, set the following configuration items to ensure that the chunk size (which indicates the number of rows of data that can be processed by a BE at a time) is the same for all BEs.

- `vector_chunk_size`: set this configuration item to 4096 for all BEs. Default value: 4096. Unit: rows.

> You must set the `vector_chunk_size` configuration item in the **be.conf** file of each BE. After you set the `vector_chunk_size` configuration item for a BE, you must restart the BE to make the setting take effect.

- `batch_size`: set this configuration item less than or equal to 4096 for all FEs. Default value: 4096. Unit: rows. This option is a global variable in the cluster. If you modified this configuration item in the current session, this configuration item in other sessions also changes.

```plain
-- check batch_size

mysql> show variables like '%batch_size%';

+---------------+-------+

| Variable_name | Value |

+---------------+-------+

| batch_size    | 1024  |

+---------------+-------+

1 row in set (0.00 sec)



-- set batch_size

mysql> set global batch_size = 4096;
```

#### Troubleshooting

Q: I have recently upgraded StarRocks v2.0 to v2.1 or later versions. When I load JSON-format BOOLEAN type data into an integer column using Stream Load, StarRocks returns NULL. How can I solve it?

A: StarRocks v2.0 parses all columns as strings and then performs type conversion for loading. When you load BOOLEAN type data (`true` and `false`) in JSON format into an integer column, StarRocks v2.0 converts the data into `0` and `1` for loading. StarRocks v2.1 refactored its JSON Paerser, which directly extracts the JSON fields according to the target column type, resulting in this problem.

You can solve this problem by adding the expression `tmp, target=if(tmp,1,0)` to the `columns` parameter of the Stream Load command. The complete command is as follows:

```shell
curl --location-trusted -u root:
-H "columns: <col_name_1>, <col_name_2>, <tmp>, <col_name_3>=if(<tmp>,1,0)" 
-T demo.csv -XPUT 
http://<fe_ip>:<fe_http_port>/api/<db_name>/<table_name>/_stream_load
```
