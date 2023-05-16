# Deploy StarRocks manually

This topic describes how to manually deploy StarRocks. For other modes of installation, see [Deployment Overview](../deployment/deployment_overview.md).

## Step 1: Start the FE service

The following procedures are performed on the FE instances.

1. Create a dedicated directory for metadata storage. We recommend storing metadata in a separate directory from the FE deployment files. Make sure that this directory exists and that you have write access to it.

   ```YAML
   # Replace <meta_dir> with the metadata directory you want to create.
   mkdir -p <meta_dir>
   ```

2. Navigate to the directory that stores the [StarRocks FE deployment files](../deployment/prepare_deployment_files.md) you prepared earlier, and modify the FE configuration file **fe/conf/fe.conf**.

   a. Specify the metadata directory in the configuration item `meta_dir`.

      ```YAML
      # Replace <meta_dir> with the metadata directory you have created.
      meta_dir = <meta_dir>
      ```

   b. If any of the FE ports mentioned in the [Environment Configuration Checklist](../deployment/environment_configurations.md#fe-ports) are occupied, you must assign valid alternatives in the FE configuration file.

      ```YAML
      http_port = aaaa        # Default: 8030
      rpc_port = bbbb         # Default: 9020
      query_port = cccc       # Default: 9030
      edit_log_port = dddd    # Default: 9010
      ```

      > **CAUTION**
      >
      > If you want to deploy multiple FE nodes in a cluster, you must assign the same `http_port` to each FE node.

   c. If you want to enable IP address access for your cluster, you must add the configuration item `priority_networks` in the configuration file and assign a dedicated IP address (in the CIDR format) to the FE node. You can ignore this configuration item if you want to enable [FQDN access](../administration/enable_fqdn.md) for your cluster.

      ```YAML
      priority_networks = x.x.x.x/x
      ```

      > **NOTE**
      >
      > You can run `ifconfig` in your terminal to view the IP address(es) owned by the instance.

   d. If you have multiple JDKs installed on the instance, and you want to use a specific JDK that is different from the one specified in the environment variable `JAVA_HOME`, you must specify the path where the chosen JDK is installed by adding the configuration item `JAVA_HOME` in the configuration file.

      ```YAML
      # Replace <path_to_JDK> with the path where the chosen JDK is installed.
      JAVA_HOME = <path_to_JDK>
      ```

   5.  For information about advanced configuration items, see [Parameter Configuration - FE configuration items](../administration/Configuration.md#fe-configuration-items).

3. Start the FE node.

   - To enable IP address access for your cluster, run the following command to start the FE node:

     ```Bash
     ./fe/bin/start_fe.sh --daemon
     ```

   - To enable FQDN access for your cluster, run the following command to start the FE node:

     ```Bash
     ./fe/bin/start_fe.sh --host_type FQDN --daemon
     ```

     Note that you only need to specify the parameter `--host_type` ONCE when you start the node for the first time.

     > **CAUTION**
     >
     > Before starting the FE node with FQDN access enabled, make sure you have assigned hostnames for all instances in **/etc/hosts**. See [Environment Configuration Checklist - Hostnames](../deployment/environment_configurations.md#hostnames) for more information.

4. Check the FE logs to verify if the FE node is started successfully.

   ```Bash
   cat fe/log/fe.log | grep thrift
   ```

   A record of log like "2022-08-10 16:12:29,911 INFO (UNKNOWN x.x.x.x_9010_1660119137253(-1)|1) [FeServer.start():52] thrift server started with port 9020." suggests that the FE node is started properly.

## Step 2: (Optional) Deploy high-availability FE cluster

A high-availability FE cluster requires at least THREE Follower FE nodes in the StarRocks cluster. After the first FE node is started successfully, you can then start two new FE nodes to deploy a high-availability FE cluster.

Basically, you can repeat the procedures in Step 1 **except for the command used to start the FE node**. When adding an extra FE node to a cluster, you must assign a helper node (essentially an existing FE node) to the new FE node to synchronize the metadata of all other FE nodes.

- To start a new FE node with IP address access, run the following command to start the FE node:

  ```Bash
  # Replace <helper_fe_ip> with the IP address (priority_networks) 
  # of the helper node, and replace <helper_edit_log_port> with 
  # the helper node's edit_log_port.
  ./fe/bin/start_fe.sh --helper <helper_fe_ip>:<helper_edit_log_port> --daemon
  ```

  Note that you only need to specify the parameter `--helper` ONCE when you start the node for the first time.

- To start a new FE node with FQDN access, run the following command to start the FE node:

  ```Bash
  # Replace <helper_fqdn> with the FQDN of the helper node, 
  # and replace <helper_edit_log_port> with the helper node's edit_log_port.
  ./fe/bin/start_fe.sh --helper <helper_fqdn>:<helper_edit_log_port> \
      --host_type FQDN --daemon
  ```

  Note that you only need to specify the parameters `--helper` and `--host_type` ONCE when you start the node for the first time.

## Step 3: Start the BE service

The following procedures are performed on the BE instances.

1. Create a dedicated directory for data storage. We recommend storing data in a separate directory from the BE deployment directory. Make sure that this directory exists and you have write access to it.

   ```YAML
   # Replace <storage_root_path> with the data storage directory you want to create.
   mkdir -p <storage_root_path>
   ```

2. Navigate to the directory that stores the [StarRocks BE deployment files](../deployment/prepare_deployment_files.md) you prepared earlier, and modify the BE configuration file **be/conf/be.conf**.

   a. Specify the data directory in the configuration item `storage_root_path`.

      ```YAML
      # Replace <storage_root_path> with the data directory you have created.
      storage_root_path = <storage_root_path>
      ```

   b. If any of the BE ports mentioned in the [Environment Configuration Checklist](../deployment/environment_configurations.md#be-ports) are occupied, you must assign valid alternatives in the BE configuration file.

      ```YAML
      be_port = vvvv                   # Default: 9060
      webserver_port = xxxx            # Default: 8040
      heartbeat_service_port = yyyy    # Default: 9050
      brpc_port = zzzz                 # Default: 8060
      ```

   c. If you want to enable IP address access for your cluster, you must add the configuration item `priority_networks` in the configuration file and assign a dedicated IP address (in the CIDR format) to the BE node. You can ignore this configuration item if you want to enable FQDN access for your cluster.

      ```YAML
      priority_networks = x.x.x.x/x
      ```

      > **NOTE**
      >
      > You can run `ifconfig` in your terminal to view the IP address(es) owned by the instance.

   d. If you have multiple JDKs installed on the instance, and you want to use a specific JDK that is different from the one specified in the environment variable `JAVA_HOME`, you must specify the path where the chosen JDK is installed by adding the configuration item `JAVA_HOME` in the configuration file.

      ```YAML
      # Replace <path_to_JDK> with the path where the chosen JDK is installed.
      JAVA_HOME = <path_to_JDK>
      ```

   For information about advanced configuration items, see [Parameter Configuration - BE configuration items](../administration/Configuration.md#be-configuration-items).

3. Start the BE node.

      ```Bash
      ./be/bin/start_be.sh --daemon
      ```

      > **CAUTION**
      >
      > - Before starting the BE node with FQDN access enabled, make sure you have assigned hostnames for all instances in **/etc/hosts**. See [Environment Configuration Checklist - Hostnames](../deployment/environment_configurations.md#hostnames) for more information.
      > - You do not need to specify the parameter `--host_type` when you start BE nodes.

4. Check the BE logs to verify if the BE node is started successfully.

      ```Bash
      cat be/log/be.INFO | grep heartbeat
      ```

      A record of log like "I0810 16:18:44.487284 3310141 task_worker_pool.cpp:1387] Waiting to receive first heartbeat from frontend" suggests that the BE node is started properly.

5. You can start new BE nodes by repeating the above procedures on other BE instances.

> **NOTE**
>
> A high-availability cluster of BEs is automatically formed when at least three BE nodes are deployed and added to a StarRocks cluster.

## Step 4: (Optional) Start the CN service

A Compute Node (CN) is a stateless computing service that does not maintain data itself. You can optionally add CN nodes to your cluster to provide extra computing resources for queries. You can deploy CN nodes with the BE deployment files. Compute Nodes are supported since v2.4.

1. Navigate to the directory that stores the [StarRocks BE deployment files](../deployment/prepare_deployment_files.md) you prepared earlier, and modify the CN configuration file **be/conf/cn.conf**.

   a. If any of the CN ports mentioned in the [Environment Configuration Checklist](../deployment/environment_configurations.md) are occupied, you must assign valid alternatives in the CN configuration file.

      ```YAML
      thrift_port = vvvv               # Default: 9060
      webserver_port = xxxx            # Default: 8040
      heartbeat_service_port = yyyy    # Default: 9050
      brpc_port = zzzz                 # Default: 8060
      ```

   b. If you want to enable IP address access for your cluster, you must add the configuration item `priority_networks` in the configuration file and assign a dedicated IP address (in the CIDR format) to the CN node. You can ignore this configuration item if you want to enable FQDN access for your cluster.

      ```YAML
      priority_networks = x.x.x.x/x
      ```

      > **NOTE**
      >
      > You can run `ifconfig` in your terminal to view the IP address(es) owned by the instance.

   c. If you have multiple JDKs installed on the instance, and you want to use a specific JDK that is different from the one specified in the environment variable `JAVA_HOME`, you must specify the path where the chosen JDK is installed by adding the configuration item `JAVA_HOME` in the configuration file.

      ```YAML
      # Replace <path_to_JDK> with the path where the chosen JDK is installed.
      JAVA_HOME = <path_to_JDK>
      ```

   For information about advanced configuration items, see [Parameter Configuration - BE configuration items](../administration/Configuration.md#be-configuration-items) because most of CN's parameters are inherited from BE.

2. Start the CN node.

   ```Bash
   ./be/bin/start_cn.sh --daemon
   ```

   > **CAUTION**
   >
   > - Before starting the CN node with FQDN access enabled, make sure you have assigned hostnames for all instances in **/etc/hosts**. See [Environment Configuration Checklist - Hostnames](../deployment/environment_configurations.md#hostnames) for more information.
   > - You do not need to specify the parameter `--host_type` when you start CN nodes.

3. Check the CN logs to verify if the CN node is started successfully.

   ```Bash
   cat be/log/cn.INFO | grep heartbeat
   ```

   A record of log like "I0313 15:03:45.820030 412450 thrift_server.cpp:375] heartbeat has started listening port on 9050" suggests that the CN node is started properly.

4. You can start new CN nodes by repeating the above procedures on other instances.

## Step 5: Set up the cluster

After all FE, BE nodes, and CN nodes are started properly, you can set up the StarRocks cluster.

The following procedures are performed on a MySQL client. You must have MySQL client 5.5.0 or later installed.

1. Connect to StarRocks via your MySQL client. You need to log in with the initial account `root`, and the password is empty by default.

   ```Bash
   # Replace <fe_address> with the IP address (priority_networks) or FQDN 
   # of the FE node you connect to, and replace <query_port> (Default: 9030) 
   # with the query_port you specified in fe.conf.
   mysql -h <fe_address> -P<query_port> -uroot
   ```

2. (Optional) If you have deployed multiple Follower FE nodes, you can add the extra two Follower FE nodes to the cluster by executing the following SQL.

   ```SQL
   -- Replace <fe_address> with the IP address (priority_networks) 
   -- or FQDN of each FE node, and replace <edit_log_port> 
   -- with the edit_log_port (Default: 9010) you specified in fe.conf.
   ALTER SYSTEM ADD FOLLOWER "<fe2_address>:<edit_log_port>", "<fe3_address>:<edit_log_port>";
   ```

   > **NOTE**
   >
   > - You can use the preceding command to add multiple Follower FE nodes at a time. Each `<fe_address>:<edit_log_port>` pair represents one FE node.
   > - If you want to add more Observer FE nodes, execute `ALTER SYSTEM ADD OBSERVER "<fe2_address>:<edit_log_port>"[, ...]`. For detailed instructions, see [ALTER SYSTEM - FE](../sql-reference/sql-statements/Administration/ALTER%20SYSTEM.md).

3. Check the status of the FE nodes by executing the following SQL.

   ```SQL
   SHOW PROC '/frontends'\G
   ```

   Example:

   ```Plain
   MySQL [(none)]> SHOW PROC '/frontends'\G
   *************************** 1. row ***************************
                 Name: x.x.x.x_9010_1660119137253
                   IP: x.x.x.x
          EditLogPort: 9010
             HttpPort: 8030
            QueryPort: 9030
              RpcPort: 9020
                 Role: FOLLOWER
             IsMaster: true
            ClusterId: 58958864
                 Join: true
                Alive: true
    ReplayedJournalId: 30602
        LastHeartbeat: 2022-08-11 20:34:26
             IsHelper: true
               ErrMsg: 
            StartTime: 2022-08-10 16:12:29
              Version: 2.3.0-a9bdb09
   1 row in set (0.01 sec)
   ```

   - If the field `Alive` is `true`, this FE node is properly started and added to the cluster.
   - If the field `Role` is `FOLLOWER`, this FE node is eligible to be elected as the Leader FE node.
   - If the field `Role` is `LEADER`, this FE node is the Leader FE node.

4. Add a BE node to the cluster.

   ```SQL
   -- Replace <be_address> with the IP address (priority_networks) 
   -- or FQDN of the BE node, and replace <heartbeat_service_port> 
   -- with the heartbeat_service_port (Default: 9050) you specified in be.conf.
   ALTER SYSTEM ADD BACKEND "<be_address>:<heartbeat_service_port>";
   ```

   > **NOTE**
   >
   > You can use the preceding command to add multiple BE nodes at a time. Each `<be_address>:<heartbeat_service_port>` pair represents one BE node.

5. Check the status of the BE nodes by executing the following SQL.

   ```SQL
   SHOW PROC '/backends'\G
   ```

   Example:

   ```Plain
   MySQL [(none)]> SHOW PROC '/backends'\G
   *************************** 1. row ***************************
               BackendId: 10036
                 Cluster: default_cluster
                      IP: x.x.x.x
           HeartbeatPort: 9050
                  BePort: 9060
                HttpPort: 8040
                BrpcPort: 8060
           LastStartTime: 2022-08-10 17:39:01
           LastHeartbeat: 2022-08-11 20:34:31
                   Alive: true
    SystemDecommissioned: false
   ClusterDecommissioned: false
               TabletNum: 0
        DataUsedCapacity: .000 
           AvailCapacity: 1.000 B
           TotalCapacity: .000 
                 UsedPct: 0.00 %
          MaxDiskUsedPct: 0.00 %
                  ErrMsg: 
                 Version: 2.3.0-a9bdb09
                  Status: {"lastSuccessReportTabletsTime":"N/A"}
       DataTotalCapacity: .000 
             DataUsedPct: 0.00 %
                CpuCores: 16
   ```

   If the field `Alive` is `true`, this BE node is properly started and added to the cluster.

6. (Optional) Add a CN node to the cluster.

   ```SQL
   -- Replace <cn_address> with the IP address (priority_networks) 
   -- or FQDN of the CN node, and replace <heartbeat_service_port> 
   -- with the heartbeat_service_port (Default: 9050) you specified in cn.conf.
   ALTER SYSTEM ADD COMPUTE NODE "<cn_address>:<heartbeat_service_port>";
   ```

   > **NOTE**
   >
   > You can add multiple CN nodes with one SQL. Each `<cn_address>:<heartbeat_service_port>` pair represents one CN node.

7. (Optional) Check the status of the CN nodes by executing the following SQL.

   ```SQL
   SHOW PROC '/compute_nodes'\G
   ```

   Example:

   ```Plain
   MySQL [(none)]> SHOW PROC '/compute_nodes'\G
   *************************** 1. row ***************************
           ComputeNodeId: 10003
                      IP: x.x.x.x
           HeartbeatPort: 9550
                  BePort: 9060
                HttpPort: 8540
                BrpcPort: 8560
           LastStartTime: 2023-03-13 15:11:13
           LastHeartbeat: 2023-03-13 15:11:13
                   Alive: true
    SystemDecommissioned: false
   ClusterDecommissioned: false
                  ErrMsg: 
                 Version: 2.5.2-c3772fb
   1 row in set (0.00 sec)
   ```

   If the field `Alive` is `true`, this CN node is properly started and added to the cluster.

## Stop the StarRocks cluster

You can stop the StarRocks cluster by running the following commands on the corresponding instances.

- Stop an FE node.

  ```Bash
  ./fe/bin/stop_fe.sh --daemon
  ```

- Stop a BE node.

  ```Bash
  ./be/bin/stop_be.sh --daemon
  ```

- Stop a CN node.

  ```Bash
  ./be/bin/stop_cn.sh --daemon
  ```

## Troubleshooting

Try the following steps to identify the errors that occur when you start the FE, BE, or CN nodes:

- If an FE node is not started properly, you can identify the problem by checking its log in **fe/log/fe.warn.log**.

  ```Bash
  cat fe/log/fe.warn.log
  ```

  Having identified and resolved the problem, you must first terminate the current FE process, delete the existing **meta** directory, create a new metadata storage directory, and then restart the FE node with the correct configuration.

- If a BE node is not started properly, you can identify the problem by checking its log in **be/log/be.WARNING**.

  ```Bash
  cat be/log/be.WARNING
  ```

  Having identified and resolved the problem, you must first terminate the existing BE process, delete the existing **storage** directory, create a new data storage directory, and then restart the BE node with the correct configuration.

- If a CN node is not started properly, you can identify the problem by checking its log in **be/log/cn.WARNING**.

  ```Bash
  cat be/log/cn.WARNING
  ```

  Having identified and resolved the problem, you must first terminate the existing CN process, and then restart the CN node with the correct configuration.

## What to do next

Having deployed your StarRocks cluster, you can move on to [Post-deployment Setup](../deployment/post_deployment_setup.md) for instructions on initial management measures.
