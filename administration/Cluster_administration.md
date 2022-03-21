# Cluster Management

## Cluster start/stop

After the StarRocks cluster is deployed, the next step is to start the cluster for servicing. Before that you need to ensure that the configuration file meets the requirements.

### FE startup

Before you set up the FE, configure the metadata placement directory (meta_dir) and communication port.

***meta_dir***: The directory where the FE stores metadata. The directory needs to be created in advance and written in the configuration file. Since the metadata of the FE is the metadata of the whole system, it is recommended not to mix with other processes for deployment.

There are four communication ports configured by the FE:

|Port Name|Default Port|Role|
|--|--|--|
|http_port|8030|the http server port on the FE|
|rpc_port|9020|the thrift server port on FE|
|query_port|9030| the mysql server port on FE|
|edit_log_port|9010|Port for communication between FE Groups (Master, Follower, Observer)|

To start FE:

1. Go to the deployment directory of the FE process
  
2. * Run `sh bin/start_fe.sh --daemon` to start the service

FE deploys multiple nodes in order to ensure high availability. Typically, users deploy 3 FEs (1 Master + 2 Follower).

When starting multiple nodes, it is recommended to start  the Followers one by one, and then start the Master.

It is recommended to verify the setup of any FE by sending a query.

### BE startup

Before set up the BE, configure the  data directory (storage_root_path) and communication port.

***storage_root_path*** The directory where the BE stores the storage files. The directory needs to be created in advance. It is recommended to create one directory per disk.

There are three communication ports configured by the BE:

|Port Name|Default Port|Role|
|--|--|--|
|be_port|9060|The thrift server port on BE to receive requests from FE|
|webserver_port|8040|The http server port on the BE|
|heartbeat_service_port|9050|The heartbeat service (thrift) port on the BE to receive heartbeats from the FE|

### Confirm cluster health status

After setting up the BE and FE, you need to check the process status to make sure the services are started properly.

* Run `http://be_host:be_http_port3/api/health` to confirm the BE status

  * Returns {"status": "OK", "msg": "To Be Added"} indicating normal startup.

* Run `http://fe_host:fe_http_port/api/bootstrap` to confirm the FE status.

  * Returns {"status": "OK", "msg": "Success"} indicating normal startup.

### Cluster stop

* Go to the FE directory and run `sh bin/stop_fe.sh`

* Enter the BE directory and run `sh bin/stop_be.sh`

### Cluster upgrade

StarRocks can perform a rolling upgrade, that is, to upgrade the BE first and then the FE. StarRocks ensures that the BE is backward compatible with the FE. The upgrade has three steps, including testing the upgrade, rolling the upgrade, and observing the service.

### Upgrade preparation

* After data verification, distribute the new BE and FE binaries to their respective directories.

* For minor upgrades, you only need to  upgrade `starrocks_be` for the BE and `starrocks-fe.jar` for the FE.
* For major upgrades, you may need to upgrade other files (including but not limited to bin/ lib/ etc.); if you are not sure whether you need to replace other files, just replace them all.

### Upgrade

* Confirm that the file replacement for the new version is complete.

* Reboot the BE nodes one by one and then reboot the FE nodes one by one.
  
* Confirm that the previous instance is started successfully, and then restart the next instance.

#### BE Upgrade

```shell
cd be_work_dir 
mv lib lib.bak 
cp -r /tmp/StarRocks-SE-x.x.x/be/lib  .   
sh bin/stop_be.sh
sh bin/start_be.sh --daemon
ps aux | grep starrocks_be
```

#### FE Upgrade

```shell
cd fe_work_dir 
mv lib lib.bak 
cp -r /tmp/StarRocks-SE-x.x.x/fe/lib  .   
sh bin/stop_fe.sh
sh bin/start_fe.sh --daemon
ps aux | grep StarRocksFe
```

Note:

The start order of BE and FE should not be reversed. This is because if the upgrade causes incompatibility between different versions of FE and BE, commands from the new FE may cause the old BE to fail. This problem can be circumvented if the BE is started first. Because a new BE file has been deployed and the BE is already a new BE after it is automatically restarted via the daemon.

##### Usage Notes for performing grayscale upgrade from StarRocks 2.0 to 2.1

If you need to perform grayscale upgrade from StarRocks 2.0 to 2.1, you need to set the following settings in advance to ensure that the chunksize (i.e., the number of rows of data processed by the BE node in each batch) is the same for all BE nodes.

* The configuration item vector_chunk_size for all BE nodes is 4096 (the default value is 4096 in rows).
  > You can set the value of `vector_chunk_size` in the BE node's configuration file be.conf. When you successfully set the value of `vector_chunk_size`, you need to restart the BE node to make the change take effect.
* The global variable batch_size of the FE node is less than or equal to 4096 (the default and recommended value is 4096 in rows).

```plain text
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

### Test the BE upgrade

* Select an arbitrary BE node and deploy the latest `starrocks_be` binary.

* Reboot the BE node and check if the restart is successful by querying the BE log `be.INFO`.
  
* If the restart fails, troubleshoot first. If you can’t reproduce the error for diagnosis, you can directly delete the BE by `DROP BACKEND` to clean up the data. Restart the BE with the previous version of `starrocks_be`, then run `ADD BACKEND` again. (This method will result in the loss of one replication of the data, so be sure to perform this operation with three completel replications)

### Test  the FE upgrade

* Deploy the new version FE to your own local development machine for testing

* Modify the configuration file `fe.conf`. Set **all ports to be different from the online FE**.
  
* Add `cluster_id=123456` to `fe.conf`
  
* Add `metadata_failure_recovery=true` to `fe.conf`
  
* Copy the metadata directory `starrocks-meta` of the online Master FE to the test environment.

* Change `cluster_id` in the `starrocks-meta/image/VERSION` file (this is the file copied to the test environment)

* In the test environment, run `sh bin/start_fe.sh` to start the FE.
  
* Confirm if the restart is successful through the FE log `fe.log`.
  
* If the restart is successful, run `sh bin/stop_fe.sh` to stop the FE process in the test environment.

**The purpose of steps 2-6 above is to prevent the FE in the  test environment from starting and connecting to the online environment by mistake.**

**Because the FE metadata is very critical and an abnormal upgrade may cause data loss, please be cautious when performing the steps abov
