# Deploy and manage Broker node

This topic describes how to deploy Broker nodes. With Broker nodes, StarRocks can read data from sources such as HDFS and S3, and pre-process, load, and backup the data with its own computing resources.

We recommend you deploy ONE Broker node on each instance that hosts a BE node, and add all Broker nodes using the same `broker_name`. Broker nodes automatically balance the data transmission load when processing tasks.

Broker nodes use the network connection to transmit data to BE nodes. When a Broker node and a BE node are deployed on the same machine, the Broker node transmit the data to the local BE node.

## Before you begin

Make sure you have completed the required configurations by following the instructions provided in [Deployment prerequisites](../deployment/deployment_prerequisites.md), [Check environment configurations](../deployment/environment_configurations.md), and [Prepare deployment files](../deployment/prepare_deployment_files.md).

## Start Broker service

The following procedures are performed on the BE instances.

1. Navigate to the directory that stores the [StarRocks Broker deployment files](../deployment/prepare_deployment_files.md) you prepared earlier, and modify the Broker configuration file **apache_hdfs_broker/conf/apache_hdfs_broker.conf**.

   If the HDFS Thrift RPC port (`broker_ipc_port`, Default: `8000`) on the instance is occupied, you must assign a valid alternative in the Broker configuration file.

   ```YAML
   broker_ipc_port = aaaa        # Default: 8000
   ```

   The following table lists the configuration items supported by Broker.

   | Configuration item | Default | Unit | Description |
   | ------------------------- | ------------------ | ------ | ------------------------------------------------------------ |
   | hdfs_read_buffer_size_kb | 8192 | KB | Size of the buffer that is used to read data from HDFS. |
   | hdfs_write_buffer_size_kb | 1024 | KB | Size of the buffer that is used to write data into HDFS. |
   | client_expire_seconds | 300 | Second | Client sessions will be deleted if they do not receive any ping after the specified time. |
   | broker_ipc_port | 8000 | N/A | The HDFS thrift RPC port. |
   | disable_broker_client_expiration_checking | false | N/A | Whether to disable the checking and clearing of the expired OSS file descriptors, which, in some cases, causes the broker to stuck when OSS is close. To avoid this situation, you can set this parameter to `true` to disable the checking. |
   | sys_log_dir | `${BROKER_HOME}/log` | N/A | The directory used to store system logs (including INFO, WARNING, ERROR, and FATAL). |
   | sys_log_level | INFO | N/A | The log level. Valid values include INFO, WARNING, ERROR, and FATAL. |
   | sys_log_roll_mode | SIZE-MB-1024 | N/A | The mode how system logs are segmented into log rolls. Valid values include TIME-DAY, TIME-HOUR, and SIZE-MB-nnn. The default value indicates that logs are segmented into rolls which are 1 GB each. |
   | sys_log_roll_num | 30 | N/A | The number of log rolls to reserve. |
   | audit_log_dir | `${BROKER_HOME}/log` | N/A | The directory that stores audit log files. |
   | audit_log_modules | Empty string | N/A | The modules for which StarRocks generates audit log entries. By default, StarRocks generates audit logs for the slow_query module and the query module. You can specify multiple modules, whose names must be separated by a comma (,) and a space. |
   | audit_log_roll_mode | TIME-DAY | N/A | Valid values include `TIME-DAY`, `TIME-HOUR`, and `SIZE-MB-<size>`. |
   | audit_log_roll_num | 10 | N/A | This configuration does not work if the audit_log_roll_mode is set to `SIZE-MB-<size>`. |
   | sys_log_verbose_modules | com.starrocks | N/A | The modules for which StarRocks generates system logs. Valid values are namespaces in BE, including `starrocks`, `starrocks::debug`, `starrocks::fs`, `starrocks::io`, `starrocks::lake`, `starrocks::pipeline`, `starrocks::query_cache`, `starrocks::stream`, and `starrocks::workgroup`. |

2. Start the Broker node.

   ```bash
   ./apache_hdfs_broker/bin/start_broker.sh --daemon
   ```

3. Check the Broker logs to verify if the Broker node is started successfully.

   ```Bash
   cat apache_hdfs_broker/log/apache_hdfs_broker.log | grep thrift
   ```

4. You can start new Broker nodes by repeating the above procedures on other instances.

## Add Broker node to cluster

The following procedures are performed on a MySQL client. You must have MySQL client 5.5.0 or later installed.

1. Connect to StarRocks via your MySQL client. You need to log in with the initial account `root`, and the password is empty by default.

   ```Bash
   # Replace <fe_address> with the IP address (priority_networks) or FQDN 
   # of the FE node you connect to, and replace <query_port> (Default: 9030) 
   # with the query_port you specified in fe.conf.
   mysql -h <fe_address> -P<query_port> -uroot
   ```

2. Run the following command to add the Broker node to the cluster.

   ```sql
   ALTER SYSTEM ADD BROKER <broker_name> "<broker_address>:<broker_ipc_port>";
   ```

   > **NOTE**
   >
   > - You can use the preceding command to add multiple Broker nodes at a time. Each `<broker_address>:<broker_ipc_port>` pair represents one Broker node.
   > - You can add multiple Brokers nodes with the same `broker_name`.

3. Verify if the Broker node is properly added to the via MySQL client.

```sql
SHOW PROC "/brokers"\G
```

Example:

```plain text
MySQL [(none)]> SHOW PROC "/brokers"\G
*************************** 1. row ***************************
          Name: broker1
            IP: x.x.x.x
          Port: 8000
         Alive: true
 LastStartTime: 2022-05-19 11:21:36
LastUpdateTime: 2022-05-19 11:28:31
        ErrMsg:
1 row in set (0.00 sec)
```

When the field `Alive` is `true`, this Broker is properly started and added to the cluster.

## Stop Broker node

Run the following command to stop the Broker node.

```bash
./bin/stop_broker.sh --daemon
```

## Upgrade Broker node

1. Navigate to the working directory of the Broker node and stop the node.

   ```Bash
   # Replace <broker_dir> with the deployment directory of the Broker node.
   cd <broker_dir>/apache_hdfs_broker
   sh bin/stop_broker.sh
   ```

2. Replace the original deployment files under **bin** and **lib** with the ones of the new version.

   ```Bash
   mv lib lib.bak 
   mv bin bin.bak
   cp -r /tmp/StarRocks-x.x.x/apache_hdfs_broker/lib  .   
   cp -r /tmp/StarRocks-x.x.x/apache_hdfs_broker/bin  .
   ```

3. Start the Broker node.

   ```Bash
   sh bin/start_broker.sh --daemon
   ```

4. Check if the Broker node is started successfully.

   ```Bash
   ps aux | grep broker
   ```

5. Repeat the above procedures to upgrade other Broker nodes.

## Downgrade Broker node

1. Navigate to the working directory of the Broker node and stop the node.

   ```Bash
   # Replace <broker_dir> with the deployment directory of the Broker node.
   cd <broker_dir>/apache_hdfs_broker
   sh bin/stop_broker.sh
   ```

2. Replace the original deployment files under **bin** and **lib** with the ones of the earlier version.

   ```Bash
   mv lib lib.bak 
   mv bin bin.bak
   cp -r /tmp/StarRocks-x.x.x/apache_hdfs_broker/lib  .   
   cp -r /tmp/StarRocks-x.x.x/apache_hdfs_broker/bin  .
   ```

3. Start the Broker node.

   ```Bash
   sh bin/start_broker.sh --daemon
   ```

4. Check if the Broker node is started successfully.

   ```Bash
   ps aux | grep broker
   ```

5. Repeat the above procedures to downgrade other Broker nodes.
