# Deploy StarRocks

This QuickStart tutorial guides you through the procedures to deploy a simple StarRocks cluster. Before getting started, you can read [StarRocks Architecture](../introduction/Architecture.md) for more conceptual details.

Following these steps, you can deploy a StarRocks instance with only one frontend (FE) node and one backend (BE) node on your local machine. This instance can help you complete the upcoming QuickStart tutorials on [creating a table](../quick_start/Create_table.md) and [loading and querying data](../quick_start/Import_and_query.md), and thereby acquaints you with the basic operations of StarRocks.

> **CAUTION**
>
> - To guarantee the high availability and performance **in the production environment**, we recommend that you deploy at least three FE nodes and three BE nodes in your StarRocks cluster.
> - You can deploy an FE node and a BE node on one machine. However, deploying multiple nodes of the same kind on one machine is not allowed, because the same kinds of nodes cannot share a same IP address.

## Prerequisites

Before deploying StarRocks, make sure the following requirements are satisfied.

- **Hardware**
  You can follow these steps on relatively elementary hardware, such as a machine with 4 CPU cores and 16 GB of RAM. The CPU MUST support AVX2 instruction sets.

> **NOTE**
>
> You can run `cat /proc/cpuinfo | grep avx2` in your terminal to check if the CPU supports the AVX2 instruction sets.

- **Software**
  Your machine MUST be running on OS with Linux kernel 3.10 or later. In addition, you must have JDK 1.8 or later and MySQL client 5.5 or later installed on your machine. Please note that JRE is not supported.

- **Environment variable**
  StarRocks relies on system environment variable `JAVA_HOME` to locate the Java dependency on the machine. Set this environment variable as the directory to which Java is installed, for example, `/opt/jdk1.8.0_301**`**.

> **NOTE**
>
> You can run `echo $JAVA_HOME` in your terminal to check if you have specified `JAVA_HOME`. If you have not specified it, see [How to Set and List Environment Variables in Linux](https://linuxize.com/post/how-to-set-and-list-environment-variables-in-linux/) for detailed instructions.

## Step 1: Download and install StarRocks

After all the [prerequisites](#prerequisites) are met, you can download the StarRocks software package to install it on your machine.

1. Launch a terminal, navigate to a local directory to which you have both read and write access, and run the following command to create a dedicated directory for StarRocks deployment.

    ```Plain
    mkdir -p HelloStarRocks
    ```

    > **NOTE**
    >
    > You can remove the instance cleanly at any time by deleting the directory.

2. Download the StarRocks software package to this directory.

    ```Plain
    cd HelloStarRocks
    wget https://releases.starrocks.io/starrocks/StarRocks-2.3.12.tar.gz
    ```

3. Extract the files in the software package to install StarRocks on your machine.

  ```Plain
  tar -xzvf StarRocks-2.3.12.tar.gz --strip-components 1
  ```

  The software package includes the working directories of FE (**fe**), BE (**be**), [Broker](../administration/deploy_broker.md) (**apache_hdfs_broker**), User Defined Function (**udf**), and **LICENSE** and **NOTICE** files.

## Step 2: Start the FE node

Having installed StarRocks, you need to start the FE node. FE is the front layer of StarRocks. It manages the system metadata, client connections, query plan, and query schedule.

1. Create a directory for FE metadata storage under the working directory of FE.

    ```Plain
    mkdir -p fe/meta
    ```

    > **NOTE**
    >
    > It is recommended to create a separate directory for FE metadata storage **in production environment**, because the operation of a StarRocks cluster is largely dependent on its metadata.

2. Check the IP addresses of the machine.

    ```Plain
    ifconfig
    ```

    If your machine has multiple IP addresses, for example, `eth0` and `eth1`, you must specify a dedicated IP address for FE service when configuring the property `priority_networks` of FE node in the following sub-step.

3. Check the connection status of the following ports.

    - FE HTTP server port (`http_port`, Default: `8030`)
    - FE thrift server port (`rpc_port`, Default: `9020`)
    - FE MySQL server port (`query_port`, Default: `9030`)
    - FE internal communication port (`edit_log_port`, Default: `9010`)

    ```Plain
    netstat -tunlp | grep 8030
    netstat -tunlp | grep 9020
    netstat -tunlp | grep 9030
    netstat -tunlp | grep 9010
    ```

    If any of the above ports are occupied, you must find an alternative and specify it when configuring the corresponding ports of the FE node in the following sub-step.

4. Modify the FE configuration file **fe/conf/fe.conf**.

   - If your machine has multiple IP addresses, you must add the configuration item `priority_networks` in the FE configuration file and assign a dedicated IP address to the FE node.

    ```Plain
    priority_networks = x.x.x.x
    ```

   - If any of the above FE ports are occupied, you must assign a valid alternative in the FE configuration file.

    ```Plain
    http_port = aaaa
    rpc_port = bbbb
    query_port = cccc
    edit_log_port = dddd
    ```

   - If you want to set a different `JAVA_HOME` for StarRocks when you have multiple Java dependencies in your machine, you must specify it in the FE configuration file.

    ```Plain
    JAVA_HOME = /path/to/your/java
    ```

5. Start FE node.

    ```Plain
    ./fe/bin/start_fe.sh --daemon
    ```

6. Verify if the FE node is started successfully.

    ```Plain
    cat fe/log/fe.log | grep thrift
    ```

  A record of log like "2022-08-10 16:12:29,911 INFO (UNKNOWN x.x.x.x_9010_1660119137253(-1)|1) [FeServer.start():52] thrift server started with port 9020." suggests that the FE node is started properly.

## Step 3: Start the BE node

After FE node is started, you need to start the BE node. BE is the executing layer of StarRocks. It stores data and executes queries.

1. Create a directory for data storage under the working directory of BE.

    ```Plain
    mkdir -p be/storage
    ```

    > **NOTE**
    >
    > It is recommended to create a separate directory for BE data storage **in production environment** to ensure the security of data.

2. Check the connection status of the following ports.

   - BE thrift server port (`be_port`, Default: `9060`)
   - BE HTTP server port (`webserver_port`, Default: `8040`)
   - heartbeat service port (`heartbeat_service_port`, Default: `9050`)
   - BE BRPC port (`brpc_port`, Default: `8060`)

    ```Plain
    netstat -tunlp | grep 9060
    netstat -tunlp | grep 8040
    netstat -tunlp | grep 9050
    netstat -tunlp | grep 8060
    ```

    If any of the above ports are occupied, you must find an alternative and specify it when configuring BE node in the following sub-step.

3. Modify the BE configuration file **be/conf/be.conf**.

   - If your machine has multiple IP addresses, you must add the configuration item `priority_networks` in the BE configuration file and assign a dedicated IP address to BE node.

    > **NOTE**
    >
    > The BE node can have the same IP addresses as the FE node if they are installed on the same machine.

    ```Plain
    priority_networks = x.x.x.x
    ```

   - If any of the above BE ports are occupied, you must assign a valid alternative in the BE configuration file.

    ```Plain
    be_port = vvvv
    webserver_port = xxxx
    heartbeat_service_port = yyyy
    brpc_port = zzzz
    ```

   - If you want to set a different `JAVA_HOME` for StarRocks when you have multiple Java dependencies in your machine, you must specify it in the BE configuration file.

    ```Plain
    JAVA_HOME = /path/to/your/java
    ```

4. Start BE node.

    ```Plain
    ./be/bin/start_be.sh --daemon
    ```

5. Verify if the BE node is started successfully.

    ```Plain
    cat be/log/be.INFO | grep heartbeat
    ```

  A record of log like "I0810 16:18:44.487284 3310141 task_worker_pool.cpp:1387] Waiting to receive first heartbeat from frontend" suggests that the BE node is started properly.

## Step 4: Set up a StarRocks cluster

After the FE node and BE node are started properly, you can set up the StarRocks cluster.

1. Log in to StarRocks via your MySQL client. You can log in with the default user `root`, and the password is empty by default.

    ```Plain
    mysql -h <fe_ip> -P<fe_query_port> -uroot
    ```

    > **NOTE**
    >
    > - Change the `-P` value accordingly if you have assigned a different FE MySQL server port (`query_port`, Default: `9030`).
    > - Change the `-h` value accordingly if you have specified the configuration item `priority_networks` in the FE configuration file.

2. Check the status of the FE node by running the following SQL in the MySQL client.

    ```Plain
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
    - If the field `Role` is `FOLLOWER`, this FE node is eligible to be elected as the Leader node.
    - If the field `Role` is `LEADER`, this FE node is the Leader node.

3. Add the BE node to the cluster.

    ```Plain
    ALTER SYSTEM ADD BACKEND "<be_ip>:<heartbeat_service_port>";
    ```

    > **NOTE**
    >
    > - If your machine has multiple IP addresses, `be_ip` must match the `priority_networks` you have specified in the BE configuration file.
    > - `heartbeat_service_port` must match the BE heartbeat service port (`heartbeat_service_port`, Default: `9050`) you have specified in the BE configuration file

4. Check the status of the BE node.

  ```Plain
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

## Stop the StarRocks cluster

You can stop the StarRocks cluster by running the following commands.

1. Stop the FE node.

    ```Plain
    ./fe/bin/stop_fe.sh --daemon
    ```

2. Stop the BE node.

    ```Plain
    ./be/bin/stop_be.sh --daemon
    ```

## Troubleshooting

Try the following steps to identify the errors that occurs when you start the FE or BE node:

- If the FE node is not started properly, you can identify the problem by checking the log in **fe/log/fe.warn.log**.

```Plain
cat fe/log/fe.warn.log
```

Having identified and resolved the problem, you must first terminate the existing FE process, delete the FE **meta** directory, create a new metadata storage directory, and then restart the FE node with the correct configuration.

- If the BE node is not started properly, you can identify the problem by checking the log in **be/log/be.WARNING**.

```Plain
cat be/log/be.WARNING
```

Having identified and resolved the problem, you must first terminate the existing BE process, delete the BE **storage** directory, create a new data storage directory, and then restart the BE node with the correct configuration.

## What to do next

Having deployed StarRocks, you can continue the QuickStart tutorials on [creating a table](../quick_start/Create_table.md) and [loading and querying data](../quick_start/Import_and_query.md).

You can also:

- [Create a table](Create_table.md)
- [Upgrade StarRocks](../administration/Cluster_administration.md#upgrade-starrocks)
