# Deploy and Manage StarRocks with StarGo

This topic describes how to deploy and manage StarRocks clusters with StarGo.

StarGo is a command line tool for multiple StarRocks clusters management. You can easily deploy, check, upgrade, downgrade, start and stop multiple clusters through StarGo.

## Install StarGo

Download the following files to your central control node:

- **sr-ctl**: The binary file of StarGo. You do not need to install it after downloading it.
- **sr-c1.yaml**: The template for the deployment configuration file.
- **repo.yaml**: The configuration file for the download path of StarRocks installer.

> Note
> You can access `http://cdn-thirdparty.starrocks.com` to get the corresponding installation index files and installers.

```shell
wget https://github.com/wangtianyi2004/starrocks-controller/raw/main/stargo-pkg.tar.gz
wget https://github.com/wangtianyi2004/starrocks-controller/blob/main/sr-c1.yaml
wget https://github.com/wangtianyi2004/starrocks-controller/blob/main/repo.yaml
```

Grant **sr-ctl** access.

```shell
chmod 751 sr-ctl
```

## Deploy StarRocks cluster

You can deploy a StarRocks cluster with StarGo.

### Prerequisites

- The cluster to be deployed must have at least one central control node and three deployment nodes. All nodes can be deployed on one machine.
- You need to deploy StarGo on the central control node.
- You need to build mutual SSH authentication between the central control node and three deployment nodes.

The following example builds mutual authentication between central control node sr-dev@r0 and three deployment nodes starrocks@r1, starrocks@r2, and starrocks@r3.

```plain text
## Build the mutual authentication between sr-dev@r0 and starrocks@r1, 2, 3.
[sr-dev@r0 ~]$ ssh-keygen
[sr-dev@r0 ~]$ ssh-copy-id starrocks@r1
[sr-dev@r0 ~]$ ssh-copy-id starrocks@r2
[sr-dev@r0 ~]$ ssh-copy-id starrocks@r3

## Verify the mutual authentication between sr-dev@r0 and starrocks@r1, 2, 3.
[sr-dev@r0 ~]$ ssh starrocks@r1 date
[sr-dev@r0 ~]$ ssh starrocks@r2 date
[sr-dev@r0 ~]$ ssh starrocks@r3 date
```

### Create configuration file

Create the StarRocks deployment topology file based on the following YAML template. See [Configuration](../administration/Configuration.md) for detailed information.

```yaml
global:
    user: "starrocks"   ## The current OS user.
    ssh_port: 22

fe_servers:
  - host: 192.168.XX.XX
    ssh_port: 22
    http_port: 8030
    rpc_port: 9020
    query_port: 9030
    edit_log_port: 9010
    deploy_dir: StarRocks/fe
    meta_dir: StarRocks/fe/meta
    log_dir: StarRocks/fe/log
    priority_networks: 192.168.XX.XX/24 # Specify the unique IP for current node when the machine has multiple IP addresses.
    config:
      sys_log_level: "INFO"
  - host: 192.168.XX.XX
    ssh_port: 22
    http_port: 8030
    rpc_port: 9020
    query_port: 9030
    edit_log_port: 9010
    deploy_dir: StarRocks/fe
    meta_dir: StarRocks/fe/meta
    log_dir: StarRocks/fe/log
    priority_networks: 192.168.XX.XX/24 # Specify the unique IP for current node when the machine has multiple IP addresses.
    config:
      sys_log_level: "INFO"
  - host: 192.168.XX.XX
    ssh_port: 22
    http_port: 8030
    rpc_port: 9020
    query_port: 9030
    edit_log_port: 9010
    deploy_dir: StarRocks/fe
    meta_dir: StarRocks/fe/meta
    log_dir: StarRocks/fe/log
    priority_networks: 192.168.XX.XX/24 # Specify the unique IP for current node when the machine has multiple IP addresses.
    config:
      sys_log_level: "INFO"
be_servers:
  - host: 192.168.XX.XX
    ssh_port: 22
    be_port: 9060
    webserver_port: 8040
    heartbeat_service_port: 9050
    brpc_port: 8060
    deploy_dir : StarRocks/be
    storage_dir: StarRocks/be/storage
    log_dir: StarRocks/be/log
    priority_networks: 192.168.XX.XX/24 # Specify the unique IP for current node when the machine has multiple IP addresses.
    config:
      create_tablet_worker_count: 3
  - host: 192.168.XX.XX
    ssh_port: 22
    be_port: 9060
    webserver_port: 8040
    heartbeat_service_port: 9050
    brpc_port: 8060
    deploy_dir : StarRocks/be
    storage_dir: StarRocks/be/storage
    log_dir: StarRocks/be/log
    priority_networks: 192.168.XX.XX/24 # Specify the unique IP for current node when the machine has multiple IP addresses.
    config:
      create_tablet_worker_count: 3
  - host: 192.168.XX.XX
    ssh_port: 22
    be_port: 9060
    webserver_port: 8040
    heartbeat_service_port: 9050
    brpc_port: 8060
    deploy_dir : StarRocks/be
    storage_dir: StarRocks/be/storage
    log_dir: StarRocks/be/log
    priority_networks: 192.168.XX.XX/24 # Specify the unique IP for current node when the machine has multiple IP addresses.
    config:
      create_tablet_worker_count: 3
```

### Create deployment directory (Optional)

If the paths under which StarRocks to be deployed does not exist, and you have the privilege to create such paths, you do not have create these paths, and StarGo will create them for you based on the configration file. If the the paths already exist, make sure you have the write access to them. You can also create necessary deployment directories on each node by running the following commands.

- Create **meta** directory on FE nodes.

```shell
mkdir -p StarRocks/fe/meta
```

- Create **storage** directory on BE nodes.

```shell
mkdir -p StarRocks/be/storage
```

> Caution
> Make sure the above paths are identical with the configuration items `meta_dir` and `storage_dir` in the configuration file.

### Deploy StarRocks

Deploy StarRocks cluster by running the following command.

```shell
./sr-ctl cluster deploy <cluster_name> <version> <topology_file>
```

|Prameter|Description|
|----|----|
|cluster_name|Name of the cluster to deploy.|
|version|StarRocks version.|
|topology_file|Name of the configuration file.|

If the deployment is successful, the cluster will be started automatically. When beStatus and feStatus are true, the cluster is started successfully.

Example:

```plain text
[sr-dev@r0 ~]$ ./sr-ctl cluster deploy sr-c1 v2.0.1 sr-c1.yaml
[20220301-234817  OUTPUT] Deploy cluster [clusterName = sr-c1, clusterVersion = v2.0.1, metaFile = sr-c1.yaml]
[20220301-234836  OUTPUT] PRE CHECK DEPLOY ENV:
PreCheck FE:
IP                    ssh auth         meta dir                   deploy dir                 http port        rpc port         query port       edit log port
--------------------  ---------------  -------------------------  -------------------------  ---------------  ---------------  ---------------  ---------------
192.168.xx.xx         PASS             PASS                       PASS                       PASS             PASS             PASS             PASS
192.168.xx.xx         PASS             PASS                       PASS                       PASS             PASS             PASS             PASS
192.168.xx.xx         PASS             PASS                       PASS                       PASS             PASS             PASS             PASS

PreCheck BE:
IP                    ssh auth         storage dir                deploy dir                 webSer port      heartbeat port   brpc port        be port
--------------------  ---------------  -------------------------  -------------------------  ---------------  ---------------  ---------------  ---------------
192.168.xx.xx         PASS             PASS                       PASS                       PASS             PASS             PASS             PASS
192.168.xx.xx         PASS             PASS                       PASS                       PASS             PASS             PASS             PASS
192.168.xx.xx         PASS             PASS                       PASS                       PASS             PASS             PASS             PASS


[20220301-234836  OUTPUT] PreCheck successfully. RESPECT
[20220301-234836  OUTPUT] Create the deploy folder ...
[20220301-234838  OUTPUT] Download StarRocks package & jdk ...
[20220302-000515    INFO] The file starrocks-2.0.1-quickstart.tar.gz [1227406189] download successfully
[20220302-000515  OUTPUT] Download done.
[20220302-000515  OUTPUT] Decompress StarRocks pakcage & jdk ...
[20220302-000520    INFO] The tar file /home/sr-dev/.starrocks-controller/download/starrocks-2.0.1-quickstart.tar.gz has been decompressed under /home/sr-dev/.starrocks-controller/download
[20220302-000547    INFO] The tar file /home/sr-dev/.starrocks-controller/download/StarRocks-2.0.1.tar.gz has been decompressed under /home/sr-dev/.starrocks-controller/download
[20220302-000556    INFO] The tar file /home/sr-dev/.starrocks-controller/download/jdk-8u301-linux-x64.tar.gz has been decompressed under /home/sr-dev/.starrocks-controller/download
[20220302-000556  OUTPUT] Distribute FE Dir ...
[20220302-000603    INFO] Upload dir feSourceDir = [/home/sr-dev/.starrocks-controller/download/StarRocks-2.0.1/fe] to feTargetDir = [StarRocks/fe] on FeHost = [192.168.xx.xx]
[20220302-000615    INFO] Upload dir JDKSourceDir = [/home/sr-dev/.starrocks-controller/download/jdk1.8.0_301] to JDKTargetDir = [StarRocks/fe/jdk] on FeHost = [192.168.xx.xx]
[20220302-000615    INFO] Modify JAVA_HOME: host = [192.168.xx.xx], filePath = [StarRocks/fe/bin/start_fe.sh]
[20220302-000622    INFO] Upload dir feSourceDir = [/home/sr-dev/.starrocks-controller/download/StarRocks-2.0.1/fe] to feTargetDir = [StarRocks/fe] on FeHost = [192.168.xx.xx]
[20220302-000634    INFO] Upload dir JDKSourceDir = [/home/sr-dev/.starrocks-controller/download/jdk1.8.0_301] to JDKTargetDir = [StarRocks/fe/jdk] on FeHost = [192.168.xx.xx]
[20220302-000634    INFO] Modify JAVA_HOME: host = [192.168.xx.xx], filePath = [StarRocks/fe/bin/start_fe.sh]
[20220302-000640    INFO] Upload dir feSourceDir = [/home/sr-dev/.starrocks-controller/download/StarRocks-2.0.1/fe] to feTargetDir = [StarRocks/fe] on FeHost = [192.168.xx.xx]
[20220302-000652    INFO] Upload dir JDKSourceDir = [/home/sr-dev/.starrocks-controller/download/jdk1.8.0_301] to JDKTargetDir = [StarRocks/fe/jdk] on FeHost = [192.168.xx.xx]
[20220302-000652    INFO] Modify JAVA_HOME: host = [192.168.xx.xx], filePath = [StarRocks/fe/bin/start_fe.sh]
[20220302-000652  OUTPUT] Distribute BE Dir ...
[20220302-000728    INFO] Upload dir BeSourceDir = [/home/sr-dev/.starrocks-controller/download/StarRocks-2.0.1/be] to BeTargetDir = [StarRocks/be] on BeHost = [192.168.xx.xx]
[20220302-000752    INFO] Upload dir BeSourceDir = [/home/sr-dev/.starrocks-controller/download/StarRocks-2.0.1/be] to BeTargetDir = [StarRocks/be] on BeHost = [192.168.xx.xx]
[20220302-000815    INFO] Upload dir BeSourceDir = [/home/sr-dev/.starrocks-controller/download/StarRocks-2.0.1/be] to BeTargetDir = [StarRocks/be] on BeHost = [192.168.xx.xx]
[20220302-000815  OUTPUT] Modify configuration for FE nodes & BE nodes ...
############################################# START FE CLUSTER #############################################
############################################# START FE CLUSTER #############################################
[20220302-000816    INFO] Starting leader FE node [host = 192.168.xx.xx, editLogPort = 9010]
[20220302-000836    INFO] The FE node start succefully [host = 192.168.xx.xx, queryPort = 9030]
[20220302-000836    INFO] Starting follower FE node [host = 192.168.xx.xx, editLogPort = 9010]
[20220302-000857    INFO] The FE node start succefully [host = 192.168.xx.xx, queryPort = 9030]
[20220302-000857    INFO] Starting follower FE node [host = 192.168.xx.xx, editLogPort = 9010]
[20220302-000918    INFO] The FE node start succefully [host = 192.168.xx.xx, queryPort = 9030]
[20220302-000918    INFO] List all FE status:
                                        feHost = 192.168.xx.xx       feQueryPort = 9030     feStatus = true
                                        feHost = 192.168.xx.xx       feQueryPort = 9030     feStatus = true
                                        feHost = 192.168.xx.xx       feQueryPort = 9030     feStatus = true

############################################# START BE CLUSTER #############################################
############################################# START BE CLUSTER #############################################
[20220302-000918    INFO] Starting BE node [BeHost = 192.168.xx.xx HeartbeatServicePort = 9050]
[20220302-000939    INFO] The BE node start succefully [host = 192.168.xx.xx, heartbeatServicePort = 9050]
[20220302-000939    INFO] Starting BE node [BeHost = 192.168.xx.xx HeartbeatServicePort = 9050]
[20220302-001000    INFO] The BE node start succefully [host = 192.168.xx.xx, heartbeatServicePort = 9050]
[20220302-001000    INFO] Starting BE node [BeHost = 192.168.xx.xx HeartbeatServicePort = 9050]
[20220302-001020    INFO] The BE node start succefully [host = 192.168.xx.xx, heartbeatServicePort = 9050]
[20220302-001020  OUTPUT] List all BE status:
                                        beHost = 192.168.xx.xx       beHeartbeatServicePort = 9050      beStatus = true
                                        beHost = 192.168.xx.xx       beHeartbeatServicePort = 9050      beStatus = true
                                        beHost = 192.168.xx.xx       beHeartbeatServicePort = 9050      beStatus = true
```

You can test the cluster by [viewing cluster information](#view-cluster-information).

You can also test it by connecting the cluster with MySQL client.

```shell
mysql -h 127.0.0.1 -P9030 -uroot
```

## View cluster information

You can view the information of the cluster that StarGo manages.

### View the information of all clusters

View the information of all clusters by running the following command.

```shell
./sr-ctl cluster list
```

Example:

```shell
[sr-dev@r0 ~]$ ./sr-ctl cluster list
[20220302-001640  OUTPUT] List all clusters
ClusterName      User        CreateDate                 MetaPath                                                      PrivateKey
---------------  ----------  -------------------------  ------------------------------------------------------------  --------------------------------------------------
sr-c1            starrocks   2022-03-02 00:08:15        /home/sr-dev/.starrocks-controller/cluster/sr-c1              /home/sr-dev/.ssh/id_rsa
```

### View the information of a specific cluster

View the information of a specific cluster by running the following command.

```shell
./sr-ctl cluster display <cluster_name>
```

Example:

```plain text
[sr-dev@r0 ~]$ ./sr-ctl cluster display sr-c1
[20220302-002310  OUTPUT] Display cluster [clusterName = sr-c1]
clusterName = sr-c1
ID                          ROLE    HOST                  PORT             STAT        DATADIR                                             DEPLOYDIR
--------------------------  ------  --------------------  ---------------  ----------  --------------------------------------------------  --------------------------------------------------
192.168.xx.xx:9010          FE      192.168.xx.xx         9010/9030        UP          StarRocks/fe                                   /dataStarRocks/fe/meta
192.168.xx.xx:9010          FE      192.168.xx.xx         9010/9030        UP          StarRocks/fe                                   /dataStarRocks/fe/meta
192.168.xx.xx:9010          FE      192.168.xx.xx         9010/9030        UP          StarRocks/fe                                   /dataStarRocks/fe/meta
192.168.xx.xx:9060          BE      192.168.xx.xx         9060/9050        UP          StarRocks/be                                   /dataStarRocks/be/storage
192.168.xx.xx:9060          BE      192.168.xx.xx         9060/9050        UP          StarRocks/be                                   /dataStarRocks/be/storage
192.168.xx.xx:9060          BE      192.168.xx.xx         9060/9050        UP          StarRocks/be                                   /dataStarRocks/be/storage
```

## Start cluster

You can start StarRocks clusters via StarGo.

### Start all nodes in a cluster

Start all nodes in a cluster by running the following command.

```shell
./sr-ctl cluster start <cluster-name>
```

Example:

```plain text
[root@nd1 sr-controller]# ./sr-ctl cluster start sr-c1
[20220303-190404  OUTPUT] Start cluster [clusterName = sr-c1]
[20220303-190404    INFO] Starting FE node [FeHost = 192.168.xx.xx, EditLogPort = 9010]
[20220303-190435    INFO] Starting FE node [FeHost = 192.168.xx.xx, EditLogPort = 9010]
[20220303-190446    INFO] Starting FE node [FeHost = 192.168.xx.xx, EditLogPort = 9010]
[20220303-190457    INFO] Starting BE node [BeHost = 192.168.xx.xx, HeartbeatServicePort = 9050]
[20220303-190458    INFO] Starting BE node [BeHost = 192.168.xx.xx, HeartbeatServicePort = 9050]
[20220303-190458    INFO] Starting BE node [BeHost = 192.168.xx.xx, HeartbeatServicePort = 9050]
```

### Start nodes of a specific role

- Start all FE nodes in a cluster.

```shell
./sr-ctl cluster start <cluster_name> --role FE
```

- Start all BE nodes in a cluster.

```shell
./sr-ctl cluster start <cluster_name> --role BE
```

Example:

```plain text
[root@nd1 sr-controller]# ./sr-ctl cluster start sr-c1 --role FE
[20220303-191529  OUTPUT] Start cluster [clusterName = sr-c1]
[20220303-191529    INFO] Starting FE cluster ....
[20220303-191529    INFO] Starting FE node [FeHost = 192.168.xx.xx, EditLogPort = 9010]
[20220303-191600    INFO] Starting FE node [FeHost = 192.168.xx.xx, EditLogPort = 9010]
[20220303-191610    INFO] Starting FE node [FeHost = 192.168.xx.xx, EditLogPort = 9010]

[root@nd1 sr-controller]# ./sr-ctl cluster start sr-c1 --role BE
[20220303-194215  OUTPUT] Start cluster [clusterName = sr-c1]
[20220303-194215    INFO] Starting BE node [BeHost = 192.168.xx.xx, HeartbeatServicePort = 9050]
[20220303-194216    INFO] Starting BE node [BeHost = 192.168.xx.xx, HeartbeatServicePort = 9050]
[20220303-194217    INFO] Starting BE node [BeHost = 192.168.xx.xx, HeartbeatServicePort = 9050]
[20220303-194217    INFO] Starting BE cluster ...
```

### Start a specific node

Start a specific node in the cluster. Currently, only BE nodes are supported.

```shell
./sr-ctl cluster start <cluster_name> --node <node_ID>
```

You can check the ID of a specific node by [viewing the information of a specific cluster](#view-the-information-of-a-specific-cluster).

Example:

```plain text
[root@nd1 sr-controller]# ./sr-ctl cluster start sr-c1 --node 192.168.xx.xx:9060
[20220303-194714  OUTPUT] Start cluster [clusterName = sr-c1]
[20220303-194714    INFO] Start BE node. [BeHost = 192.168.xx.xx, HeartbeatServicePort = 9050]
```

## Stop cluster

You can stop StarRocks clusters via StarGo.

### Stop all nodes in a cluster

Stop all nodes in a cluster by running the following command.

```shell
./sr-ctl cluster stop <cluster_name>
```

Example:

```plain text
[sr-dev@nd1 sr-controller]$ ./sr-ctl cluster stop sr-c1
[20220302-180140  OUTPUT] Stop cluster [clusterName = sr-c1]
[20220302-180140  OUTPUT] Stop cluster sr-c1
[20220302-180140    INFO] Waiting for stoping FE node [FeHost = 192.168.xx.xx]
[20220302-180143  OUTPUT] The FE node stop succefully [host = 192.168.xx.xx, queryPort = 9030]
[20220302-180143    INFO] Waiting for stoping FE node [FeHost = 192.168.xx.xx]
[20220302-180145  OUTPUT] The FE node stop succefully [host = 192.168.xx.xx, queryPort = 9030]
[20220302-180145    INFO] Waiting for stoping FE node [FeHost = 192.168.xx.xx]
[20220302-180148  OUTPUT] The FE node stop succefully [host = 192.168.xx.xx, queryPort = 9030]
[20220302-180148  OUTPUT] Stop cluster sr-c1
[20220302-180148    INFO] Waiting for stoping BE node [BeHost = 192.168.xx.xx]
[20220302-180148    INFO] The BE node stop succefully [BeHost = 192.168.xx.xx, HeartbeatServicePort = 9050]
[20220302-180148    INFO] Waiting for stoping BE node [BeHost = 192.168.xx.xx]
[20220302-180149    INFO] The BE node stop succefully [BeHost = 192.168.xx.xx, HeartbeatServicePort = 9050]
[20220302-180149    INFO] Waiting for stoping BE node [BeHost = 192.168.xx.xx]
[20220302-180149    INFO] The BE node stop succefully [BeHost = 192.168.xx.xx, HeartbeatServicePort = 9050]
```

### Stop nodes of a specific role

- Stop all FE nodes in a cluster.

```shell
./sr-ctl cluster stop <cluster_name> --role FE
```

- Stop all BE nodes in a cluster.

```shell
./sr-ctl cluster stop <cluster_name> --role BE
```

Example:

```plain text
[sr-dev@nd1 sr-controller]$ ./sr-ctl cluster stop sr-c1 --role BE
[20220302-180624  OUTPUT] Stop cluster [clusterName = sr-c1]
[20220302-180624  OUTPUT] Stop cluster sr-c1
[20220302-180624    INFO] Waiting for stoping BE node [BeHost = 192.168.xx.xx]
[20220302-180624    INFO] The BE node stop succefully [BeHost = 192.168.xx.xx, HeartbeatServicePort = 9050]
[20220302-180624    INFO] Waiting for stoping BE node [BeHost = 192.168.xx.xx]
[20220302-180625    INFO] The BE node stop succefully [BeHost = 192.168.xx.xx, HeartbeatServicePort = 9050]
[20220302-180625    INFO] Waiting for stoping BE node [BeHost = 192.168.xx.xx]
[20220302-180625    INFO] The BE node stop succefully [BeHost = 192.168.xx.xx, HeartbeatServicePort = 9050]
[20220302-180625    INFO] Stopping BE cluster ...

###########################################################################

[sr-dev@nd1 sr-controller]$ ./sr-ctl cluster stop sr-c1 --role FE
[20220302-180849  OUTPUT] Stop cluster [clusterName = sr-c1]
[20220302-180849    INFO] Stopping FE cluster ....
[20220302-180849  OUTPUT] Stop cluster sr-c1
[20220302-180849    INFO] Waiting for stoping FE node [FeHost = 192.168.xx.xx]
[20220302-180851  OUTPUT] The FE node stop succefully [host = 192.168.xx.xx, queryPort = 9030]
[20220302-180851    INFO] Waiting for stoping FE node [FeHost = 192.168.xx.xx]
[20220302-180854  OUTPUT] The FE node stop succefully [host = 192.168.xx.xx, queryPort = 9030]
[20220302-180854    INFO] Waiting for stoping FE node [FeHost = 192.168.xx.xx]
[20220302-180856  OUTPUT] The FE node stop succefully [host = 192.168.xx.xx, queryPort = 9030]
```

### Stop a specific node

Stop a specific node in the cluster.

```shell
./sr-ctl cluster stop <cluster_name> --node <node_ID>
```

You can check the ID of a specific node by [viewing the information of a specific cluster](#view-the-information-of-a-specific-cluster).

Example:

```plain text
[root@nd1 sr-controller]# ./sr-ctl cluster display sr-c1
[20220303-185400  OUTPUT] Display cluster [clusterName = sr-c1]
clusterName = sr-c1
[20220303-185400    WARN] All FE nodes are down, please start FE node and display the cluster status again.
ID                          ROLE    HOST                  PORT             STAT        DATADIR                                             DEPLOYDIR
--------------------------  ------  --------------------  ---------------  ----------  --------------------------------------------------  --------------------------------------------------
192.168.xx.xx:9010          FE      192.168.xx.xx         9010/9030        DOWN        StarRocks/fe                                   /dataStarRocks/fe/meta
192.168.xx.xx:9010          FE      192.168.xx.xx         9010/9030        DOWN        StarRocks/fe                                   /dataStarRocks/fe/meta
192.168.xx.xx:9010          FE      192.168.xx.xx         9010/9030        DOWN        StarRocks/fe                                   /dataStarRocks/fe/meta
192.168.xx.xx:9060          BE      192.168.xx.xx         9060/9050        DOWN        StarRocks/be                                   /dataStarRocks/be/storage
192.168.xx.xx:9060          BE      192.168.xx.xx         9060/9050        DOWN        StarRocks/be                                   /dataStarRocks/be/storage
192.168.xx.xx:9060          BE      192.168.xx.xx         9060/9050        DOWN        StarRocks/be                                   /dataStarRocks/be/storage

[root@nd1 sr-controller]# ./sr-ctl cluster stop sr-c1 --node 192.168.xx.xx:9060
[20220303-185510  OUTPUT] Stop cluster [clusterName = sr-c1]
[20220303-185510    INFO] Stopping BE node. [BeHost = 192.168.xx.xx]
[20220303-185510    INFO] Waiting for stoping BE node [BeHost = 192.168.xx.xx]
```

## Scale cluster out

You can scale a cluster out via StarGo.

### Create configuration file

Create the scale-out task topology file based on the following template. You can specify the file to add FE and/or BE nodes based on your demand. See [Configuration](../administration/Configuration.md) for detailed information.

```yaml
# Add an FE node.
fe_servers:
  - host: 192.168.xx.xx # The IP address of the new FE node.
    ssh_port: 22
    http_port: 8030
    rpc_port: 9020
    query_port: 9030
    edit_log_port: 9010
    deploy_dir: StarRocks/fe
    meta_dir: StarRocks/fe/meta
    log_dir: StarRocks/fe/log
    priority_networks: 192.168.xx.xx/24 # Specify the unique IP for current node when the machine has multiple IP addresses.
    config:
      sys_log_level: "INFO"
      sys_log_delete_age: "1d"

# Add a BE node.
be_servers:
  - host: 192.168.xx.xx # The IP address of the new BE node.
    ssh_port: 22
    be_port: 9060
    webserver_port: 8040
    heartbeat_service_port: 9050
    brpc_port: 8060
    deploy_dir : StarRocks/be
    storage_dir: StarRocks/be/storage
    log_dir: StarRocks/be/log
    config:
      create_tablet_worker_count: 3
```

### Build SSH mutual authentication

If you are adding a new node to the cluster, you must build mutual authentication between the new node and the central control node. See [Prerequisites](#prerequisites) for detailed instruction.

### Create deployment directory (Optional)

If the path under which the new node to be deployed does not exist, and you have the privilege to create such path, you do not have create these path, and StarGo will create them for you based on the configuration file. If the the paths already exist, make sure you have the write access to them. You can also create necessary deployment directories on each node by running the following commands.

- Create **meta** directory on FE nodes.

```shell
mkdir -p StarRocks/fe/meta
```

- Create **storage** directory on BE nodes.

```shell
mkdir -p StarRocks/be/storage
```

> Caution
> Make sure the above paths are identical with the configuration items `meta_dir` and `storage_dir` in the configuration file.

### Scale the cluster out

Scale the cluster out by running the following command.

```shell
./sr-ctl cluster scale-out <cluster_name> <topology_file>
```

Example:

```plain text
# Status of the cluster before scale-out.
[root@nd1 sr-controller]# ./sr-ctl cluster display sr-test       
[20220503-210047  OUTPUT] Display cluster [clusterName = sr-test]
clusterName = sr-test
clusterVerison = v2.0.1
ID                          ROLE    HOST                  PORT             STAT        DATADIR                                             DEPLOYDIR                                         
--------------------------  ------  --------------------  ---------------  ----------  --------------------------------------------------  --------------------------------------------------
192.168.xx.xx:9010          FE      192.168.xx.xx         9010/9030        UP          /opt/starrocks-test/fe                              /opt/starrocks-test/fe/meta                       
192.168.xx.xx:9060          BE      192.168.xx.xx         9060/9050        UP          /opt/starrocks-test/be                              /opt/starrocks-test/be/storage                    

# Scale the cluster out.
[sr-dev@nd1 sr-controller]$ ./sr-ctl cluster scale-out sr-test sr-out.yaml
[20220503-213725  OUTPUT] Scale out cluster. [ClusterName = sr-test]
[20220503-213731  OUTPUT] PRE CHECK DEPLOY ENV:
PreCheck FE:
IP                    ssh auth         meta dir                        deploy dir                      http port        rpc port         query port       edit log port  
--------------------  ---------------  ------------------------------  ------------------------------  ---------------  ---------------  ---------------  ---------------
192.168.xx.xx         PASS             PASS                            PASS                            PASS             PASS             PASS             PASS           

PreCheck BE:
IP                    ssh auth         storage dir                     deploy dir                      webSer port      heartbeat port   brpc port        be port        
--------------------  ---------------  ------------------------------  ------------------------------  ---------------  ---------------  ---------------  ---------------
192.168.xx.xx         PASS             PASS                            PASS                            PASS             PASS             PASS             PASS           


[20220503-213731  OUTPUT] PreCheck successfully. RESPECT
[20220503-213731  OUTPUT] Create the deploy folder ...
[20220503-213732  OUTPUT] Download StarRocks package & jdk ...
[20220503-213732    INFO] The package has already exist [fileName = starrocks-2.0.1-quickstart.tar.gz, fileSize = 1227406189, fileModTime = 2022-05-03 17:32:03.478661923 +0800 CST]
[20220503-213732  OUTPUT] Download done.
[20220503-213732  OUTPUT] Decompress StarRocks pakage & jdk ...
[20220503-213741    INFO] The tar file /home/sr-dev/.starrocks-controller/download/starrocks-2.0.1-quickstart.tar.gz has been decompressed under /home/sr-dev/.starrocks-controller/download
[20220503-213837    INFO] The tar file /home/sr-dev/.starrocks-controller/download/StarRocks-2.0.1.tar.gz has been decompressed under /home/sr-dev/.starrocks-controller/download
[20220503-213837    INFO] The tar file /home/sr-dev/.starrocks-controller/download/jdk-8u301-linux-x64.tar.gz has been decompressed under /home/sr-dev/.starrocks-controller/download
[20220503-213837  OUTPUT] Distribute FE Dir ...
[20220503-213845    INFO] Upload dir feSourceDir = [/home/sr-dev/.starrocks-controller/download/StarRocks-2.0.1/fe] to feTargetDir = [StarRocks/fe] on FeHost = [192.168.xx.xx]
[20220503-213857    INFO] Upload dir JDKSourceDir = [/home/sr-dev/.starrocks-controller/download/jdk1.8.0_301] to JDKTargetDir = [StarRocks/fe/jdk] on FeHost = [192.168.xx.xx]
[20220503-213857    INFO] Modify JAVA_HOME: host = [192.168.xx.xx], filePath = [StarRocks/fe/bin/start_fe.sh]
[20220503-213857  OUTPUT] Distribute BE Dir ...
[20220503-213924    INFO] Upload dir BeSourceDir = [/home/sr-dev/.starrocks-controller/download/StarRocks-2.0.1/be] to BeTargetDir = [StarRocks/be] on BeHost = [192.168.xx.xx]
[20220503-213924  OUTPUT] Modify configuration for FE nodes & BE nodes ...
############################################# SCALE OUT FE CLUSTER #############################################
############################################# SCALE OUT FE CLUSTER #############################################
[20220503-213925    INFO] Starting follower FE node [host = 192.168.xx.xx, editLogPort = 9010]
[20220503-213945    INFO] The FE node start succefully [host = 192.168.xx.xx, queryPort = 9030]
[20220503-213945    INFO] List all FE status:
                                        feHost = 192.168.xx.xx       feQueryPort = 9030     feStatus = true

############################################# START BE CLUSTER #############################################
############################################# START BE CLUSTER #############################################
[20220503-213945    INFO] Starting BE node [BeHost = 192.168.xx.xx HeartbeatServicePort = 9050]
[20220503-214016    INFO] The BE node start succefully [host = 192.168.xx.xx, heartbeatServicePort = 9050]
[20220503-214016  OUTPUT] List all BE status:
                                        beHost = 192.168.xx.xx       beHeartbeatServicePort = 9050      beStatus = true

# Status of the cluster after scale-out.
[sr-dev@nd1 sr-controller]$ ./sr-ctl cluster display sr-test 
[20220503-214302  OUTPUT] Display cluster [clusterName = sr-test]
clusterName = sr-test
clusterVerison = v2.0.1
ID                          ROLE    HOST                  PORT             STAT        DATADIR                                             DEPLOYDIR                                         
--------------------------  ------  --------------------  ---------------  ----------  --------------------------------------------------  --------------------------------------------------
192.168.xx.xx:9010          FE      192.168.xx.xx         9010/9030        UP          /opt/starrocks-test/fe                              /opt/starrocks-test/fe/meta                       
192.168.xx.xx:9010          FE      192.168.xx.xx         9010/9030        UP          StarRocks/fe                                   StarRocks/fe/meta                            
192.168.xx.xx:9060          BE      192.168.xx.xx         9060/9050        UP          /opt/starrocks-test/be                              /opt/starrocks-test/be/storage                    
192.168.xx.xx:9060          BE      192.168.xx.xx         9060/9050        UP          StarRocks/be                                   StarRocks/be/storage                         
```

## Scale cluster in

Remove a node in the cluster by running the following command.

```shell
./sr-ctl cluster scale-in <cluster_name> --node <node_id>
```

You can check the ID of a specific node by [viewing the information of a specific cluster](#view-the-information-of-a-specific-cluster).

Example:

```plain text
[sr-dev@nd1 sr-controller]$ ./sr-ctl cluster display sr-c1
[20220505-145649  OUTPUT] Display cluster [clusterName = sr-c1]
clusterName = sr-c1
clusterVerison = v2.0.1
ID                          ROLE    HOST                  PORT             STAT        DATADIR                                             DEPLOYDIR                                         
--------------------------  ------  --------------------  ---------------  ----------  --------------------------------------------------  --------------------------------------------------
192.168.xx.xx:9010          FE      192.168.xx.xx         9010/9030        UP          StarRocks/fe                                   /dataStarRocks/fe/meta                           
192.168.xx.xx:9010          FE      192.168.xx.xx         9010/9030        UP          StarRocks/fe                                   /dataStarRocks/fe/meta                           
192.168.xx.xx:9010          FE      192.168.xx.xx         9010/9030        UP          StarRocks/fe                                   /dataStarRocks/fe/meta                           
192.168.xx.xx:9060          BE      192.168.xx.xx         9060/9050        UP          StarRocks/be                                   /dataStarRocks/be/storage                        
192.168.xx.xx:9060          BE      192.168.xx.xx         9060/9050        UP          StarRocks/be                                   /dataStarRocks/be/storage                        
192.168.xx.xx:9060          BE      192.168.xx.xx         9060/9050        UP          StarRocks/be                                   /dataStarRocks/be/storage                        
[sr-dev@nd1 sr-controller]$ ./sr-ctl cluster scale-in sr-c1 --node 192.168.88.83:9010
[20220621-010553  OUTPUT] Scale in cluster [clusterName = sr-c1, nodeId = 192.168.88.83:9010]
[20220621-010553    INFO] Waiting for stoping FE node [FeHost = 192.168.88.83]
[20220621-010606  OUTPUT] Scale in FE node successfully. [clusterName = sr-c1, nodeId = 192.168.88.83:9010]

[sr-dev@nd1 sr-controller]$ ./sr-ctl cluster display sr-c1
[20220621-010623  OUTPUT] Display cluster [clusterName = sr-c1]
clusterName = sr-c1
clusterVerison = 
ID                          ROLE    HOST                  PORT             STAT        DATADIR                                             DEPLOYDIR                                         
--------------------------  ------  --------------------  ---------------  ----------  --------------------------------------------------  --------------------------------------------------
192.168.88.84:9010          FE      192.168.xx.xx         9010/9030        UP          StarRocks/fe                                   /dataStarRocks/fe/meta                           
192.168.88.85:9010          FE      192.168.xx.xx         9010/9030        UP/L        StarRocks/fe                                   /dataStarRocks/fe/meta                           
192.168.88.83:9060          BE      192.168.xx.xx         9060/9050        UP          StarRocks/be                                   /dataStarRocks/be/storage                        
192.168.88.84:9060          BE      192.168.xx.xx         9060/9050        UP          StarRocks/be                                   /dataStarRocks/be/storage                        
192.168.88.85:9060          BE      192.168.xx.xx         9060/9050        UP          StarRocks/be                                   /dataStarRocks/be/storage              
```

## Upgrade or downgrade the cluster

You can upgrade or downgrade a cluster via StarGo.

- Upgrade a cluster.

```shell
./sr-ctl cluster upgrade <cluster_name>  <target_version>
```

- Downgrade a cluster.

```shell
./sr-ctl cluster downgrade <cluster_name>  <target_version>
```

Example:

```plain text
[sr-dev@nd1 sr-controller]$ ./sr-ctl cluster list
[20220515-195827  OUTPUT] List all clusters
ClusterName      Version     User        CreateDate                 MetaPath                                                      PrivateKey                                        
---------------  ----------  ----------  -------------------------  ------------------------------------------------------------  --------------------------------------------------
sr-test2         v2.0.1      test222     2022-05-15 19:35:36        /home/sr-dev/.starrocks-controller/cluster/sr-test2           /home/sr-dev/.ssh/id_rsa                          
[sr-dev@nd1 sr-controller]$ ./sr-ctl cluster upgrade sr-test2 v2.1.3
[20220515-200358  OUTPUT] List all clusters
ClusterName      Version     User        CreateDate                 MetaPath                                                      PrivateKey                                        
---------------  ----------  ----------  -------------------------  ------------------------------------------------------------  --------------------------------------------------
sr-test2         v2.1.3      test222     2022-05-15 20:03:01        /home/sr-dev/.starrocks-controller/cluster/sr-test2           /home/sr-dev/.ssh/id_rsa                          

[sr-dev@nd1 sr-controller]$ ./sr-ctl cluster downgrade sr-test2 v2.0.1 
[sr-dev@nd1 sr-controller]$ ./sr-ctl cluster list
[20220515-200915  OUTPUT] List all clusters
ClusterName      Version     User        CreateDate                 MetaPath                                                      PrivateKey                                        
---------------  ----------  ----------  -------------------------  ------------------------------------------------------------  --------------------------------------------------
sr-test2         v2.0.1      test222     2022-05-15 20:08:40        /home/sr-dev/.starrocks-controller/cluster/sr-test2           /home/sr-dev/.ssh/id_rsa                
```

## Relevant commands

|Command|Description|
|----|----|
|deploy|Deploy a cluster.|
|start|Start a cluster.|
|stop|Stop a cluster.|
|scale-in|Scale in a cluster.|
|scale-out|Scale out a cluster.|
|upgrade|Upgrade a cluster.|
|downgrade|Downgrade a cluster|
|display|View the information of a specific cluater.|
|list|View all clusters.|
