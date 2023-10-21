# Manage a cluster

This topic explains how to manage a cluster.

## Start and stop a cluster

We recommend that your machines in the cluster are equipped with the following configurations:

- The machines used as frontends (FEs) are equipped with an 8-core CPU and 16 GB of RAM or higher configurations.
- The machines used as backends (BEs) are equipped with a 16-core CPU and 64 GB of RAM or higher configurations.

### Start an FE

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
2. Run `sh bin/start_fe.sh --daemon` to start the FE.

You can deploy multiple FEs to ensure high availability. Typically, you can deploy three FEs: one leader FE and two follower FEs.

When you start multiple FEs, start the follower FE one by one. When you upgrade a cluster, upgrade the follower FEs and then upgrade the leader FE. By doing so, you can detect errors that may occur during the upgrades of the follower FEs and make sure that the cluster can continue to properly process queries.

**Note**:

- If you configured multiple follower FEs, the cluster can select one follower FE as the leader FE to process queries only when more than half of configured follower FEs are available.
- We recommend that you verify every FE that you want to start. You can send a query to an FE to verify the FE.

### Start a BE

Before you set up the BE, configure the  `storage_root_path` parameter and the communication ports:

- `storage_root_path`: indicates the directory for the BE to store the storage files. The directory needs to be created in advance. We recommend creating one directory per disk.
- Communication ports: there are four communication ports available for the BE.

| **Port** **name**  | **Default** **port** **number** | **Description**                                              |
| ---------------------- | ----------------------------------- | ------------------------------------------------------------ |
| be_port                | 9060                                | The port that is used by the BE to communicate with an FE.   |
| webserver_port         | 8040                                | The port that is used by the BE to communicate with an HTTP protocol. |
| heartbeat_service_port | 9050                                | The port that is used by the BE to receive heartbeats from an FE. |
| brpc                   | 8060                                | The port that is used for communication between the BEs in a cluster. |

### Check the health status of a cluster

After you set up the BEs and FEs of a cluster, you need to check the statuses of FEs and BEs to ensure the FEs and BEs are started normally:

- Run `http://be_host:be_http_port/api/health` to check the statuses of BEs. If `{"status": "OK", "msg": "To Be Added"}` is returned, the BEs have properly started.

- Run `http://fe_host:fe_http_port/api/bootstrap` to check the statuses of FEs. If `{"status": "OK", "msg": "Success"}` is returned, the FEs have properly started.

### Stop a cluster

To stop a cluster, you need to stop all the FEs and BEs in the cluster:

- Go to the deployment directory of each FE and run `sh bin/stop_fe.sh`.

- Go to the deployment directory of each BE and run `sh bin/stop_be.sh`.

### Upgrade a cluster

StarRocks can perform a rolling upgrade, which allows you to first upgrade the BEs, then the FEs, and finally the Brokers in a cluster. StarRocks ensures that the BEs are backward compatible with the FEs.

#### Before you begin

- (Optional) Test whether the cluster after the upgrade affects your current data.

- Distribute the BE and FE binary files for new versions of BE and FE to the deployment directory of BE and FE.

- For a minor version update (for example, from 2.0.x to 2.0.y), you only need to upgrade `starrocks_be` for the BEs and `starrocks-fe.jar` for the FEs.

- For a major version upgrade (for example, from 2.0.x to 2.x.x), you need to replace the bin and lib folders of the FEs and BEs.

#### Procedure

1. Confirm that the files of the previous version are replaced with the files of the new version.
2. Restart the BEs one by one and then restart the FEs one by one. Start the next BE or FE only after the previous BE or FE has successfully started.

##### Upgrade a  BE

```Plain_Text
cd be_work_dir

sh bin/stop_be.sh

mv lib lib.bak 

mv bin bin.bak

cp -r /tmp/StarRocks-SE-x.x.x/be/lib  .   

cp -r /tmp/StarRocks-SE-x.x.x/be/bin  .  

sh bin/start_be.sh --daemon

ps aux | grep starrocks_be
```

##### Upgrade an FE

```Plain_Text
cd fe_work_dir

sh bin/stop_fe.sh

mv lib lib.bak 

mv bin bin.bak

cp -r /tmp/StarRocks-SE-x.x.x/fe/lib  .   

cp -r /tmp/StarRocks-SE-x.x.x/fe/bin  .

sh bin/start_fe.sh --daemon

ps aux | grep StarRocksFE
```

##### Upgrade a Broker

```Plain_Text
cd broker_work_dir 

mv lib lib.bak 

mv bin bin.bak

cp -r /tmp/StarRocks-SE-x.x.x/apache_hdfs_broker/lib  .   

cp -r /tmp/StarRocks-SE-x.x.x/apache_hdfs_broker/bin  .

sh bin/stop_broker.sh

sh bin/start_broker.sh --daemon

ps aux | grep broker
```

**Note**: You must update the BEs before you upgrade the FEs. If you upgrade the FEs before the BEs, the FEs of the new version may be incompatible with the BEs of the previous version. As a result, the commands that are issued by the FEs may cause the BEs not to work properly.

## Roll back a cluster

All StarRocks versions, which are named StarRocks-xx, support rollbacks. You need to first roll back the FEs, then the BEs, and finally the Brokers in a cluster. If an exception occurs after you upgrade a cluster, you can perform the following steps to roll back the cluster to the previous version. This way, you can quickly recover the cluster.

### Before you begin

- For a minor version rollback (for example, from 2.0.x to 2.0.x), you need to replace `starrocks_be` for the BEs and `starrocks-fe.jar` for the FEs.

- To a major version rollback (for example, from 2.x.x to 2.0.x), you need to replace the bin and lib folders of the FEs and BEs.

### Procedure

1. Confirm that the files of the previous version are replaced with the files of the new version.
2. Restart the FEs one by one and then restart the BEs one by one.

**Note**: Start the next FE or BE only after the previous FE or BE has successfully started.

#### Roll back an FE

```Plain_Text
cd fe_work_dir 

mv lib libtmp.bak 

mv bin bintmp.bak 

mv lib.bak lib   

mv bin.bak bin 

sh bin/stop_fe.sh

sh bin/start_fe.sh --daemon

ps aux | grep StarRocksFe
```

#### Roll back a BE

```Plain_Text
cd be_work_dir 

mv lib libtmp.bak 

mv bin bintmp.bak 

mv lib.bak lib

mv bin.bak bin

sh bin/stop_be.sh

sh bin/start_be.sh --daemon

ps aux | grep starrocks_be
```

#### Roll back a Broker

```Plain_Text
cd broker_work_dir 

mv lib libtmp.bak 

mv bin bintmp.bak

mv lib.bak lib

mv bin.bak bin

sh bin/stop_broker.sh

sh bin/start_broker.sh --daemon

ps aux | grep broker
```

**Note**: You must roll back the FEs before you roll back the BEs. If you rollback the BEs before the FEs, the FEs of the new version may be incompatible with the BEs of the previous version. As a result, the commands that are issued by the FEs may cause the BEs to break down.

### Usage notes for grayscale upgrade from StarRocks 2.0 to StarRocks 2.1

If you need to perform a grayscale upgrade from StarRocks 2.0 to StarRocks 2.1, set the following configuration items to ensure that the chunk size (which indicates the number of rows of data that can be processed by a BE at a time) is the same for all BEs.

- `vector_chunk_size`: set this configuration item to 4096 for all BEs. Default value: 4096. Unit: rows.

> You must set the `vector_chunk_size` configuration item in the **be.conf** file of each BE. After you set the `vector_chunk_size` configuration item for a BE, you must restart the BE to make the setting take effect.

- `batch_size`: set this configuration item less than or equal to 4096 for all FEs. Default value: 4096. Unit: rows. This option is a global variable in the cluster. If you modified this configuration item in the current session, this configuration item in other sessions also changes.

```Lua
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

## Test the upgrade of a cluster (Optional)

Before you upgrade a cluster, you can perform the following steps to test whether the cluster after the upgrade affects your current data.

### Test the upgrade of BEs

1. Select a BE and deploy the **starrocks_be** binary file of the new version on the BE.
2. Restart the BE. Then view the **be.INFO** log file of BE to check whether the restart is successful.
3. If the restart fails, troubleshoot the failure.

### Test the upgrade of FEs

The FE metadata is critical and an abnormal upgrade may cause data loss. We recommend that you test the upgrade of FEs in your test environment (If you do not have a test environment, you can first upgrade a follower FE or an observer FE and then check whether it runs as expected). Proceed with caution when you perform the following steps.

1. Deploy an FE of the new version in your testing environment, such as your local development machine.
2. Modify the **fe.conf** file. Configure the ports of the test FE to be different from the ports of the online FEs.
3. Set `cluster_id` in the **fe.conf** file to `123456`.
4. Set `metadata_failure_recovery` in the **fe.conf** file to `true`.
5. Copy the metadata directory of the leader FE in the production environment to your test environment.
6. Set the `cluster_id` configuration item in the **meta/image/VERSION** file to 123456.
7. In your test environment, run `sh bin/start_fe.sh` to start the test FE.
8. View the **fe.log** file of the test FE to check whether the restart is successful.
9. If the restart is successful, run `sh bin/stop_fe.sh` to stop the test FE.

The step 2 through 6 aim to prevent the test FE from connecting to the production environment after a restart.
