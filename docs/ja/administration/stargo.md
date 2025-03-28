---
displayed_sidebar: docs
---

# StarGo を使用した StarRocks のデプロイと管理

このトピックでは、StarGo を使用して StarRocks クラスターをデプロイおよび管理する方法について説明します。

StarGo は、複数の StarRocks クラスターを管理するためのコマンドラインツールです。StarGo を使用すると、複数のクラスターを簡単にデプロイ、確認、アップグレード、ダウングレード、開始、および停止できます。

## StarGo のインストール

以下のファイルを中央制御ノードにダウンロードします。

- **sr-ctl**: StarGo のバイナリファイル。ダウンロード後にインストールする必要はありません。
- **sr-c1.yaml**: デプロイメント設定ファイルのテンプレート。
- **repo.yaml**: StarRocks インストーラーのダウンロードパスの設定ファイル。

> Note
> `http://cdn-thirdparty.starrocks.com` にアクセスして、対応するインストールインデックスファイルとインストーラーを取得できます。

```shell
wget https://github.com/wangtianyi2004/starrocks-controller/raw/main/stargo-pkg.tar.gz
wget https://github.com/wangtianyi2004/starrocks-controller/blob/main/sr-c1.yaml
wget https://github.com/wangtianyi2004/starrocks-controller/blob/main/repo.yaml
```

**sr-ctl** のアクセス権を付与します。

```shell
chmod 751 sr-ctl
```

## StarRocks クラスターのデプロイ

StarGo を使用して StarRocks クラスターをデプロイできます。

### 前提条件

- デプロイするクラスターには、少なくとも 1 つの中央制御ノードと 3 つのデプロイメントノードが必要です。すべてのノードは 1 台のマシンにデプロイできます。
- 中央制御ノードに StarGo をデプロイする必要があります。
- 中央制御ノードと 3 つのデプロイメントノードの間で相互 SSH 認証を構築する必要があります。

以下の例では、中央制御ノード sr-dev@r0 と 3 つのデプロイメントノード starrocks@r1、starrocks@r2、starrocks@r3 の間で相互認証を構築します。

```plain text
## sr-dev@r0 と starrocks@r1, 2, 3 の間で相互認証を構築します。
[sr-dev@r0 ~]$ ssh-keygen
[sr-dev@r0 ~]$ ssh-copy-id starrocks@r1
[sr-dev@r0 ~]$ ssh-copy-id starrocks@r2
[sr-dev@r0 ~]$ ssh-copy-id starrocks@r3

## sr-dev@r0 と starrocks@r1, 2, 3 の間で相互認証を確認します。
[sr-dev@r0 ~]$ ssh starrocks@r1 date
[sr-dev@r0 ~]$ ssh starrocks@r2 date
[sr-dev@r0 ~]$ ssh starrocks@r3 date
```

### 設定ファイルの作成

以下の YAML テンプレートに基づいて StarRocks デプロイメントトポロジーファイルを作成します。詳細については、[Configuration](../administration/management/FE_configuration.md) を参照してください。

```yaml
global:
    user: "starrocks"   ## 現在の OS ユーザー。
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
    priority_networks: 192.168.XX.XX/24 # マシンに複数の IP アドレスがある場合、現在のノードのユニークな IP を指定します。
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
    priority_networks: 192.168.XX.XX/24 # マシンに複数の IP アドレスがある場合、現在のノードのユニークな IP を指定します。
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
    priority_networks: 192.168.XX.XX/24 # マシンに複数の IP アドレスがある場合、現在のノードのユニークな IP を指定します。
    config:
      sys_log_level: "INFO"
be_servers:
  - host: 192.168.XX.XX
    ssh_port: 22
    be_port: 9060
    be_http_port: 8040
    heartbeat_service_port: 9050
    brpc_port: 8060
    deploy_dir : StarRocks/be
    storage_dir: StarRocks/be/storage
    log_dir: StarRocks/be/log
    priority_networks: 192.168.XX.XX/24 # マシンに複数の IP アドレスがある場合、現在のノードのユニークな IP を指定します。
    config:
      create_tablet_worker_count: 3
  - host: 192.168.XX.XX
    ssh_port: 22
    be_port: 9060
    be_http_port: 8040
    heartbeat_service_port: 9050
    brpc_port: 8060
    deploy_dir : StarRocks/be
    storage_dir: StarRocks/be/storage
    log_dir: StarRocks/be/log
    priority_networks: 192.168.XX.XX/24 # マシンに複数の IP アドレスがある場合、現在のノードのユニークな IP を指定します。
    config:
      create_tablet_worker_count: 3
  - host: 192.168.XX.XX
    ssh_port: 22
    be_port: 9060
    be_http_port: 8040
    heartbeat_service_port: 9050
    brpc_port: 8060
    deploy_dir : StarRocks/be
    storage_dir: StarRocks/be/storage
    log_dir: StarRocks/be/log
    priority_networks: 192.168.XX.XX/24 # マシンに複数の IP アドレスがある場合、現在のノードのユニークな IP を指定します。
    config:
      create_tablet_worker_count: 3
```

### デプロイメントディレクトリの作成（オプション）

StarRocks をデプロイするパスが存在しない場合、またそのパスを作成する権限がある場合は、これらのパスを作成する必要はありません。StarGo は設定ファイルに基づいてそれらを作成します。既にパスが存在する場合は、それらに書き込みアクセス権があることを確認してください。また、次のコマンドを実行して、各ノードに必要なデプロイメントディレクトリを作成することもできます。

- FE ノードに **meta** ディレクトリを作成します。

```shell
mkdir -p StarRocks/fe/meta
```

- BE ノードに **storage** ディレクトリを作成します。

```shell
mkdir -p StarRocks/be/storage
```

> Caution
> 上記のパスが設定ファイルの `meta_dir` および `storage_dir` の設定項目と一致していることを確認してください。

### StarRocks のデプロイ

次のコマンドを実行して StarRocks クラスターをデプロイします。

```shell
./sr-ctl cluster deploy <cluster_name> <version> <topology_file>
```

|Parameter|Description|
|----|----|
|cluster_name|デプロイするクラスターの名前。|
|version|StarRocks のバージョン。|
|topology_file|設定ファイルの名前。|

デプロイが成功すると、クラスターは自動的に開始されます。beStatus と feStatus が true の場合、クラスターは正常に開始されています。

例:

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

クラスターをテストするには、[クラスター情報の表示](#view-cluster-information)を参照してください。

また、MySQL クライアントを使用してクラスターに接続することでもテストできます。

```shell
mysql -h 127.0.0.1 -P9030 -uroot
```

## クラスター情報の表示

StarGo が管理するクラスターの情報を表示できます。

### すべてのクラスターの情報を表示

次のコマンドを実行して、すべてのクラスターの情報を表示します。

```shell
./sr-ctl cluster list
```

例:

```plain text
[sr-dev@r0 ~]$ ./sr-ctl cluster list
[20220302-001640  OUTPUT] List all clusters
ClusterName      User        CreateDate                 MetaPath                                                      PrivateKey
---------------  ----------  -------------------------  ------------------------------------------------------------  --------------------------------------------------
sr-c1            starrocks   2022-03-02 00:08:15        /home/sr-dev/.starrocks-controller/cluster/sr-c1              /home/sr-dev/.ssh/id_rsa
```

### 特定のクラスターの情報を表示

次のコマンドを実行して、特定のクラスターの情報を表示します。

```shell
./sr-ctl cluster display <cluster_name>
```

例:

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

## クラスターの開始

StarGo を使用して StarRocks クラスターを開始できます。

### クラスター内のすべてのノードを開始

次のコマンドを実行して、クラスター内のすべてのノードを開始します。

```shell
./sr-ctl cluster start <cluster-name>
```

例:

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

### 特定の役割のノードを開始

- クラスター内のすべての FE ノードを開始。

```shell
./sr-ctl cluster start <cluster_name> --role FE
```

- クラスター内のすべての BE ノードを開始。

```shell
./sr-ctl cluster start <cluster_name> --role BE
```

例:

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

### 特定のノードを開始

クラスター内の特定のノードを開始します。現在、BE ノードのみがサポートされています。

```shell
./sr-ctl cluster start <cluster_name> --node <node_ID>
```

特定のノードの ID は、[特定のクラスターの情報を表示](#view-the-information-of-a-specific-cluster)することで確認できます。

例:

```plain text
[root@nd1 sr-controller]# ./sr-ctl cluster start sr-c1 --node 192.168.xx.xx:9060
[20220303-194714  OUTPUT] Start cluster [clusterName = sr-c1]
[20220303-194714    INFO] Start BE node. [BeHost = 192.168.xx.xx, HeartbeatServicePort = 9050]
```

## クラスターの停止

StarGo を使用して StarRocks クラスターを停止できます。

### クラスター内のすべてのノードを停止

次のコマンドを実行して、クラスター内のすべてのノードを停止します。

```shell
./sr-ctl cluster stop <cluster_name>
```

例:

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

### 特定の役割のノードを停止

- クラスター内のすべての FE ノードを停止。

```shell
./sr-ctl cluster stop <cluster_name> --role FE
```

- クラスター内のすべての BE ノードを停止。

```shell
./sr-ctl cluster stop <cluster_name> --role BE
```

例:

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

### 特定のノードを停止

クラスター内の特定のノードを停止します。

```shell
./sr-ctl cluster stop <cluster_name> --node <node_ID>
```

特定のノードの ID は、[特定のクラスターの情報を表示](#view-the-information-of-a-specific-cluster)することで確認できます。

例:

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

## クラスターのスケールアウト

StarGo を使用してクラスターをスケールアウトできます。

### 設定ファイルの作成

以下のテンプレートに基づいてスケールアウトタスクのトポロジーファイルを作成します。必要に応じて FE および/または BE ノードを追加するファイルを指定できます。詳細については、[Configuration](../administration/management/FE_configuration.md) を参照してください。

```yaml
# FE ノードを追加します。
fe_servers:
  - host: 192.168.xx.xx # 新しい FE ノードの IP アドレス。
    ssh_port: 22
    http_port: 8030
    rpc_port: 9020
    query_port: 9030
    edit_log_port: 9010
    deploy_dir: StarRocks/fe
    meta_dir: StarRocks/fe/meta
    log_dir: StarRocks/fe/log
    priority_networks: 192.168.xx.xx/24 # マシンに複数の IP アドレスがある場合、現在のノードのユニークな IP を指定します。
    config:
      sys_log_level: "INFO"
      sys_log_delete_age: "1d"

# BE ノードを追加します。
be_servers:
  - host: 192.168.xx.xx # 新しい BE ノードの IP アドレス。
    ssh_port: 22
    be_port: 9060
    be_http_port: 8040
    heartbeat_service_port: 9050
    brpc_port: 8060
    deploy_dir : StarRocks/be
    storage_dir: StarRocks/be/storage
    log_dir: StarRocks/be/log
    config:
      create_tablet_worker_count: 3
```

### SSH 相互認証の構築

クラスターに新しいノードを追加する場合は、新しいノードと中央制御ノードの間で相互認証を構築する必要があります。詳細な手順については、[Prerequisites](#prerequisites) を参照してください。

### デプロイメントディレクトリの作成（オプション）

新しいノードをデプロイするパスが存在しない場合、またそのパスを作成する権限がある場合は、これらのパスを作成する必要はありません。StarGo は設定ファイルに基づいてそれらを作成します。既にパスが存在する場合は、それらに書き込みアクセス権があることを確認してください。また、次のコマンドを実行して、各ノードに必要なデプロイメントディレクトリを作成することもできます。

- FE ノードに **meta** ディレクトリを作成します。

```shell
mkdir -p StarRocks/fe/meta
```

- BE ノードに **storage** ディレクトリを作成します。

```shell
mkdir -p StarRocks/be/storage
```

> Caution
> 上記のパスが設定ファイルの `meta_dir` および `storage_dir` の設定項目と一致していることを確認してください。

### クラスターのスケールアウト

次のコマンドを実行してクラスターをスケールアウトします。

```shell
./sr-ctl cluster scale-out <cluster_name> <topology_file>
```

例:

```plain text
# スケールアウト前のクラスターの状態。
[root@nd1 sr-controller]# ./sr-ctl cluster display sr-test       
[20220503-210047  OUTPUT] Display cluster [clusterName = sr-test]
clusterName = sr-test
clusterVerison = v2.0.1
ID                          ROLE    HOST                  PORT             STAT        DATADIR                                             DEPLOYDIR                                         
--------------------------  ------  --------------------  ---------------  ----------  --------------------------------------------------  --------------------------------------------------
192.168.xx.xx:9010          FE      192.168.xx.xx         9010/9030        UP          /opt/starrocks-test/fe                              /opt/starrocks-test/fe/meta                       
192.168.xx.xx:9060          BE      192.168.xx.xx         9060/9050        UP          /opt/starrocks-test/be                              /opt/starrocks-test/be/storage                    

# クラスターをスケールアウトします。
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

# スケールアウト後のクラスターの状態。
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

## クラスターのスケールイン

次のコマンドを実行して、クラスター内のノードを削除します。

```shell
./sr-ctl cluster scale-in <cluster_name> --node <node_id>
```

特定のノードの ID は、[特定のクラスターの情報を表示](#view-the-information-of-a-specific-cluster)することで確認できます。

例:

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

## クラスターのアップグレードまたはダウングレード

StarGo を使用してクラスターをアップグレードまたはダウングレードできます。

- クラスターをアップグレード。

```shell
./sr-ctl cluster upgrade <cluster_name>  <target_version>
```

- クラスターをダウングレード。

```shell
./sr-ctl cluster downgrade <cluster_name>  <target_version>
```

例:

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

## 関連コマンド

|Command|Description|
|----|----|
|deploy|クラスターをデプロイします。|
|start|クラスターを開始します。|
|stop|クラスターを停止します。|
|scale-in|クラスターをスケールインします。|
|scale-out|クラスターをスケールアウトします。|
|upgrade|クラスターをアップグレードします。|
|downgrade|クラスターをダウングレードします。|
|display|特定のクラスターの情報を表示します。|
|list|すべてのクラスターを表示します。|