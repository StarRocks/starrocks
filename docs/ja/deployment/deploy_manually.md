---
displayed_sidebar: docs
---

# StarRocks を手動でデプロイする

:::tip
手動デプロイの準備は、[デプロイの前提条件](./deployment_prerequisites.md)と[環境設定の確認](./environment_configurations.md)のドキュメントに記載されています。プロダクションデプロイを計画している場合は、まずこちらを参照してください。StarRocks を始める場合やクイックスタートを試したい場合は、[クイックスタート](../quick_start/quick_start.mdx)を参照してください。
:::

このトピックでは、共有なし StarRocks（BE がストレージとコンピューティングの両方を担当する）を手動でデプロイする方法について説明します。他のインストールモードについては、[デプロイメント概要](../deployment/deployment_overview.md)を参照してください。

共有データ StarRocks クラスタ（ストレージとコンピューティングが分離されている）をデプロイするには、[共有データ StarRocks のデプロイと使用](../deployment/shared_data/s3.md)を参照してください。

## ステップ 1: Leader FE ノードを起動する

次の手順は、FE インスタンスで実行されます。

1. メタデータストレージ用の専用ディレクトリを作成します。メタデータは FE デプロイメントファイルとは別のディレクトリに保存することをお勧めします。このディレクトリが存在し、書き込みアクセス権があることを確認してください。

   ```YAML
   # <meta_dir> を作成したいメタデータディレクトリに置き換えます。
   mkdir -p <meta_dir>
   ```

2. 以前に準備した[StarRocks FE デプロイメントファイル](../deployment/prepare_deployment_files.md)を保存しているディレクトリに移動し、FE 設定ファイル **fe/conf/fe.conf** を修正します。

   a. 設定項目 `meta_dir` にメタデータディレクトリを指定します。

      ```YAML
      # <meta_dir> を作成したメタデータディレクトリに置き換えます。
      meta_dir = <meta_dir>
      ```

   b. [環境設定チェックリスト](../deployment/environment_configurations.md#fe-ports)で言及されている FE ポートが占有されている場合は、FE 設定ファイルで有効な代替ポートを割り当てる必要があります。

      ```YAML
      http_port = aaaa        # デフォルト: 8030
      rpc_port = bbbb         # デフォルト: 9020
      query_port = cccc       # デフォルト: 9030
      edit_log_port = dddd    # デフォルト: 9010
      ```

      > **注意**
      >
      > クラスタ内に複数の FE ノードをデプロイする場合は、各 FE ノードに同じ `http_port` を割り当てる必要があります。

   c. クラスタの IP アドレスアクセスを有効にしたい場合は、設定ファイルに `priority_networks` という設定項目を追加し、FE ノードに専用の IP アドレス（CIDR 形式）を割り当てる必要があります。クラスタの[FQDN アクセス](../administration/management/enable_fqdn.md)を有効にしたい場合は、この設定項目を無視できます。

      ```YAML
      priority_networks = x.x.x.x/x
      ```

      > **注意**
      >
      > - インスタンスが所有する IP アドレスを表示するには、ターミナルで `ifconfig` を実行できます。
      > - v3.3.0 以降、StarRocks は IPv6 に基づくデプロイをサポートしています。

   d. インスタンスに複数の JDK がインストールされており、環境変数 `JAVA_HOME` に指定されたものとは異なる特定の JDK を使用したい場合は、設定ファイルに `JAVA_HOME` という設定項目を追加して、選択した JDK がインストールされているパスを指定する必要があります。

      ```YAML
      # <path_to_JDK> を選択した JDK がインストールされているパスに置き換えます。
      JAVA_HOME = <path_to_JDK>
      ```

   f. 高度な設定項目については、[パラメータ設定 - FE 設定項目](../administration/management/FE_configuration.md)を参照してください。

3. FE ノードを起動します。

   - クラスタの IP アドレスアクセスを有効にするには、次のコマンドを実行して FE ノードを起動します。

     ```Bash
     ./fe/bin/start_fe.sh --daemon
     ```

   - クラスタの FQDN アクセスを有効にするには、次のコマンドを実行して FE ノードを起動します。

     ```Bash
     ./fe/bin/start_fe.sh --host_type FQDN --daemon
     ```

     ノードを初めて起動する際には、パラメータ `--host_type` を一度だけ指定する必要があります。

     > **注意**
     >
     > FQDN アクセスを有効にして FE ノードを起動する前に、すべてのインスタンスにホスト名を割り当てたことを確認してください。詳細については、[環境設定チェックリスト - ホスト名](../deployment/environment_configurations.md#hostnames)を参照してください。

4. FE ログを確認して、FE ノードが正常に起動したかどうかを確認します。

   ```Bash
   cat fe/log/fe.log | grep thrift
   ```

   "2022-08-10 16:12:29,911 INFO (UNKNOWN x.x.x.x_9010_1660119137253(-1)|1) [FeServer.start():52] thrift server started with port 9020." のようなログ記録は、FE ノードが正常に起動したことを示しています。

## ステップ 2: (共有なしの場合) BE サービスを起動する

:::note

BE ノードは共有なしクラスタにのみ追加できます。共有データクラスタに BE ノードを追加することは推奨されず、未知の動作を引き起こす可能性があります。

:::

次の手順は、BE インスタンスで実行されます。

1. データストレージ用の専用ディレクトリを作成します。データは BE デプロイメントディレクトリとは別のディレクトリに保存することをお勧めします。このディレクトリが存在し、書き込みアクセス権があることを確認してください。

   ```YAML
   # <storage_root_path> を作成したいデータストレージディレクトリに置き換えます。
   mkdir -p <storage_root_path>
   ```

2. 以前に準備した[StarRocks BE デプロイメントファイル](../deployment/prepare_deployment_files.md)を保存しているディレクトリに移動し、BE 設定ファイル **be/conf/be.conf** を修正します。

   a. 設定項目 `storage_root_path` にデータディレクトリを指定します。

      ```YAML
      # <storage_root_path> を作成したデータディレクトリに置き換えます。
      storage_root_path = <storage_root_path>
      ```

   b. [環境設定チェックリスト](../deployment/environment_configurations.md#be-ports)で言及されている BE ポートが占有されている場合は、BE 設定ファイルで有効な代替ポートを割り当てる必要があります。

      ```YAML
      be_port = vvvv                   # デフォルト: 9060
      be_http_port = xxxx              # デフォルト: 8040
      heartbeat_service_port = yyyy    # デフォルト: 9050
      brpc_port = zzzz                 # デフォルト: 8060
      ```

   c. クラスタの IP アドレスアクセスを有効にしたい場合は、設定ファイルに `priority_networks` という設定項目を追加し、BE ノードに専用の IP アドレス（CIDR 形式）を割り当てる必要があります。クラスタの FQDN アクセスを有効にしたい場合は、この設定項目を無視できます。

      ```YAML
      priority_networks = x.x.x.x/x
      ```

      > **注意**
      >
      > - インスタンスが所有する IP アドレスを表示するには、ターミナルで `ifconfig` を実行できます。
      > - v3.3.0 以降、StarRocks は IPv6 に基づくデプロイをサポートしています。

   d. インスタンスに複数の JDK がインストールされており、環境変数 `JAVA_HOME` に指定されたものとは異なる特定の JDK を使用したい場合は、設定ファイルに `JAVA_HOME` という設定項目を追加して、選択した JDK がインストールされているパスを指定する必要があります。

      ```YAML
      # <path_to_JDK> を選択した JDK がインストールされているパスに置き換えます。
      JAVA_HOME = <path_to_JDK>
      ```

   高度な設定項目については、[パラメータ設定 - BE 設定項目](../administration/management/BE_configuration.md)を参照してください。

3. BE ノードを起動します。

      ```Bash
      ./be/bin/start_be.sh --daemon
      ```

      > **注意**
      >
      > - FQDN アクセスを有効にして BE ノードを起動する前に、すべてのインスタンスにホスト名を割り当てたことを確認してください。詳細については、[環境設定チェックリスト - ホスト名](../deployment/environment_configurations.md#hostnames)を参照してください。
      > - BE ノードを起動する際にパラメータ `--host_type` を指定する必要はありません。

4. BE ログを確認して、BE ノードが正常に起動したかどうかを確認します。

      ```Bash
      cat be/log/be.INFO | grep heartbeat
      ```

      "I0810 16:18:44.487284 3310141 task_worker_pool.cpp:1387] Waiting to receive first heartbeat from frontend" のようなログ記録は、BE ノードが正常に起動したことを示しています。

5. 他の BE インスタンスで上記の手順を繰り返すことで、新しい BE ノードを起動できます。

> **注意**
>
> BE ノードが少なくとも 3 つデプロイされ、StarRocks クラスタに追加されると、BE の高可用性クラスタが自動的に形成されます。
> BE ノードを 1 つだけデプロイしたい場合は、FE 設定ファイル **fe/conf/fe.conf** で `default_replication_num` を `1` に設定する必要があります。

      ```YAML
      default_replication_num = 1
      ```

## ステップ 2: (共有データの場合) CN サービスを起動する

:::note

CN ノードは共有データクラスタにのみ追加できます。共有なしクラスタに CN ノードを追加することは推奨されず、未知の動作を引き起こす可能性があります。

:::

Compute Node (CN) は、データを自ら保持しないステートレスなコンピューティングサービスです。クエリのために追加のコンピューティングリソースを提供するために、クラスタに CN ノードをオプションで追加できます。CN ノードは BE デプロイメントファイルでデプロイできます。Compute Nodes は v2.4 以降でサポートされています。

1. 以前に準備した[StarRocks BE デプロイメントファイル](../deployment/prepare_deployment_files.md)を保存しているディレクトリに移動し、CN 設定ファイル **be/conf/cn.conf** を修正します。

   a. [環境設定チェックリスト](../deployment/environment_configurations.md)で言及されている CN ポートが占有されている場合は、CN 設定ファイルで有効な代替ポートを割り当てる必要があります。

      ```YAML
      be_port = vvvv                   # デフォルト: 9060
      be_http_port = xxxx              # デフォルト: 8040
      heartbeat_service_port = yyyy    # デフォルト: 9050
      brpc_port = zzzz                 # デフォルト: 8060
      ```

   b. クラスタの IP アドレスアクセスを有効にしたい場合は、設定ファイルに `priority_networks` という設定項目を追加し、CN ノードに専用の IP アドレス（CIDR 形式）を割り当てる必要があります。クラスタの FQDN アクセスを有効にしたい場合は、この設定項目を無視できます。

      ```YAML
      priority_networks = x.x.x.x/x
      ```

      > **注意**
      >
      > - インスタンスが所有する IP アドレスを表示するには、ターミナルで `ifconfig` を実行できます。
      > - v3.3.0 以降、StarRocks は IPv6 に基づくデプロイをサポートしています。

   c. インスタンスに複数の JDK がインストールされており、環境変数 `JAVA_HOME` に指定されたものとは異なる特定の JDK を使用したい場合は、設定ファイルに `JAVA_HOME` という設定項目を追加して、選択した JDK がインストールされているパスを指定する必要があります。

      ```YAML
      # <path_to_JDK> を選択した JDK がインストールされているパスに置き換えます。
      JAVA_HOME = <path_to_JDK>
      ```

   高度な設定項目については、[パラメータ設定 - BE 設定項目](../administration/management/BE_configuration.md)を参照してください。CN のパラメータのほとんどは BE から継承されています。

2. CN ノードを起動します。

   ```Bash
   ./be/bin/start_cn.sh --daemon
   ```

   > **注意**
   >
   > - FQDN アクセスを有効にして CN ノードを起動する前に、すべてのインスタンスにホスト名を割り当てたことを確認してください。詳細については、[環境設定チェックリスト - ホスト名](../deployment/environment_configurations.md#hostnames)を参照してください。
   > - CN ノードを起動する際にパラメータ `--host_type` を指定する必要はありません。

3. CN ログを確認して、CN ノードが正常に起動したかどうかを確認します。

   ```Bash
   cat be/log/cn.INFO | grep heartbeat
   ```

   "I0313 15:03:45.820030 412450 thrift_server.cpp:375] heartbeat has started listening port on 9050" のようなログ記録は、CN ノードが正常に起動したことを示しています。

4. 他のインスタンスで上記の手順を繰り返すことで、新しい CN ノードを起動できます。

## ステップ 3: クラスタをセットアップする

すべての FE と BE/CN ノードが正常に起動した後、StarRocks クラスタをセットアップできます。

次の手順は、MySQL クライアントで実行されます。MySQL クライアント 5.5.0 以降がインストールされている必要があります。

1. MySQL クライアントを介して StarRocks に接続します。初期アカウント `root` でログインする必要があり、パスワードはデフォルトで空です。

   ```Bash
   # <fe_address> を Leader FE ノードの IP アドレス (priority_networks) または FQDN に置き換え、
   # <query_port> (デフォルト: 9030) を fe.conf で指定した query_port に置き換えます。
   mysql -h <fe_address> -P<query_port> -uroot
   ```

2. 次の SQL を実行して、Leader FE ノードのステータスを確認します。

   ```SQL
   SHOW PROC '/frontends'\G
   ```

   例:

```Plain
   MySQL [(none)]> SHOW PROC '/frontends'\G
   *************************** 1. row ***************************
                Name: x.x.x.x_9010_1686810741121
                  IP: x.x.x.x
         EditLogPort: 9010
            HttpPort: 8030
           QueryPort: 9030
             RpcPort: 9020
                Role: LEADER
           ClusterId: 919351034
                Join: true
               Alive: true
   ReplayedJournalId: 1220
       LastHeartbeat: 2023-06-15 15:39:04
            IsHelper: true
              ErrMsg: 
           StartTime: 2023-06-15 14:32:28
             Version: 3.0.0-48f4d81
   1 row in set (0.01 sec)
   ```

   - フィールド `Alive` が `true` の場合、この FE ノードは正常に起動し、クラスタに追加されています。
   - フィールド `Role` が `FOLLOWER` の場合、この FE ノードは Leader FE ノードとして選出される資格があります。
   - フィールド `Role` が `LEADER` の場合、この FE ノードは Leader FE ノードです。

3. BE/CN ノードをクラスタに追加します。

   - (共有なしの場合) BE ノードを追加します。

   ```SQL
   -- <be_address> を BE ノードの IP アドレス (priority_networks) 
   -- または FQDN に置き換え、<heartbeat_service_port> を 
   -- be.conf で指定した heartbeat_service_port (デフォルト: 9050) に置き換えます。
   ALTER SYSTEM ADD BACKEND "<be_address>:<heartbeat_service_port>";
   ```

   > **注意**
   >
   > 上記のコマンドを使用して、複数の BE ノードを一度に追加できます。各 `<be_address>:<heartbeat_service_port>` ペアは 1 つの BE ノードを表します。

   - (共有データの場合) CN ノードを追加します。

   ```SQL
   -- <cn_address> を CN ノードの IP アドレス (priority_networks) 
   -- または FQDN に置き換え、<heartbeat_service_port> を 
   -- cn.conf で指定した heartbeat_service_port (デフォルト: 9050) に置き換えます。
   ALTER SYSTEM ADD COMPUTE NODE "<cn_address>:<heartbeat_service_port>";
   ```

   > **注意**
   >
   > 1 つの SQL で複数の CN ノードを追加できます。各 `<cn_address>:<heartbeat_service_port>` ペアは 1 つの CN ノードを表します。

4. 次の SQL を実行して、BE/CN ノードのステータスを確認します。

   - (共有なしの場合) BE ノードのステータスを確認します。

   ```SQL
   SHOW PROC '/backends'\G
   ```

   例:

   ```Plain
   MySQL [(none)]> SHOW PROC '/backends'\G
   *************************** 1. row ***************************
               BackendId: 10007
                      IP: 172.26.195.67
           HeartbeatPort: 9050
                  BePort: 9060
                HttpPort: 8040
                BrpcPort: 8060
           LastStartTime: 2023-06-15 15:23:08
           LastHeartbeat: 2023-06-15 15:57:30
                   Alive: true
    SystemDecommissioned: false
   ClusterDecommissioned: false
               TabletNum: 30
        DataUsedCapacity: 0.000 
           AvailCapacity: 341.965 GB
           TotalCapacity: 1.968 TB
                 UsedPct: 83.04 %
          MaxDiskUsedPct: 83.04 %
                  ErrMsg: 
                 Version: 3.0.0-48f4d81
                  Status: {"lastSuccessReportTabletsTime":"2023-06-15 15:57:08"}
       DataTotalCapacity: 341.965 GB
             DataUsedPct: 0.00 %
                CpuCores: 16
       NumRunningQueries: 0
              MemUsedPct: 0.01 %
              CpuUsedPct: 0.0 %
   ```

   フィールド `Alive` が `true` の場合、この BE ノードは正常に起動し、クラスタに追加されています。

   - (共有データの場合) CN ノードのステータスを確認します。

   ```SQL
   SHOW PROC '/compute_nodes'\G
   ```

   例:

   ```Plain
   MySQL [(none)]> SHOW PROC '/compute_nodes'\G
   *************************** 1. row ***************************
           ComputeNodeId: 10003
                      IP: x.x.x.x
           HeartbeatPort: 9050
                  BePort: 9060
                HttpPort: 8040
                BrpcPort: 8060
           LastStartTime: 2023-03-13 15:11:13
           LastHeartbeat: 2023-03-13 15:11:13
                   Alive: true
    SystemDecommissioned: false
   ClusterDecommissioned: false
                  ErrMsg: 
                 Version: 2.5.2-c3772fb
   1 row in set (0.00 sec)
   ```

   フィールド `Alive` が `true` の場合、この CN ノードは正常に起動し、クラスタに追加されています。

   CN が正常に起動し、クエリ中に CN を使用したい場合は、システム変数 `SET prefer_compute_node = true;` と `SET use_compute_nodes = -1;` を設定します。詳細については、[システム変数](../sql-reference/System_variable.md#descriptions-of-variables)を参照してください。

## ステップ 4: (オプション) 高可用性 FE クラスタをデプロイする

高可用性 FE クラスタには、StarRocks クラスタに少なくとも 3 つの Follower FE ノードが必要です。Leader FE ノードが正常に起動した後、2 つの新しい FE ノードを起動して高可用性 FE クラスタをデプロイできます。

1. MySQL クライアントを介して StarRocks に接続します。初期アカウント `root` でログインする必要があり、パスワードはデフォルトで空です。

   ```Bash
   # <fe_address> を Leader FE ノードの IP アドレス (priority_networks) または FQDN に置き換え、
   # <query_port> (デフォルト: 9030) を fe.conf で指定した query_port に置き換えます。
   mysql -h <fe_address> -P<query_port> -uroot
   ```

2. 次の SQL を実行して、新しい Follower FE ノードをクラスタに追加します。

   ```SQL
   -- <fe_address> を新しい Follower FE ノードの IP アドレス (priority_networks) 
   -- または FQDN に置き換え、<edit_log_port> を fe.conf で指定した 
   -- edit_log_port (デフォルト: 9010) に置き換えます。
   ALTER SYSTEM ADD FOLLOWER "<fe2_address>:<edit_log_port>";
   ```

   > **注意**
   >
   > - 上記のコマンドを使用して、1 回に 1 つの Follower FE ノードを追加できます。
   > - Observer FE ノードを追加したい場合は、`ALTER SYSTEM ADD OBSERVER "<fe_address>:<edit_log_port>"=` を実行します。詳細な手順については、[ALTER SYSTEM - FE](../sql-reference/sql-statements/cluster-management/nodes_processes/ALTER_SYSTEM.md)を参照してください。

3. 新しい FE インスタンスでターミナルを起動し、メタデータストレージ用の専用ディレクトリを作成し、StarRocks FE デプロイメントファイルを保存しているディレクトリに移動し、FE 設定ファイル **fe/conf/fe.conf** を修正します。詳細な手順については、[ステップ 1: Leader FE ノードを起動する](#step-1-start-the-leader-fe-node)を参照してください。基本的には、FE ノードを起動するためのコマンドを除いて、ステップ 1 の手順を繰り返すことができます。

   Follower FE ノードを設定した後、次の SQL を実行して Follower FE ノードにヘルパーノードを割り当て、Follower FE ノードを起動します。

   > **注意**
   >
   > クラスタに新しい Follower FE ノードを追加する際には、Follower FE ノードにメタデータを同期するためのヘルパーノード（基本的には既存の Follower FE ノード）を割り当てる必要があります。

   - IP アドレスアクセスで新しい FE ノードを起動するには、次のコマンドを実行して FE ノードを起動します。

     ```Bash
     # <helper_fe_ip> を Leader FE ノードの IP アドレス (priority_networks) に置き換え、
     # <helper_edit_log_port> (デフォルト: 9010) を Leader FE ノードの edit_log_port に置き換えます。
     ./fe/bin/start_fe.sh --helper <helper_fe_ip>:<helper_edit_log_port> --daemon
     ```

     ノードを初めて起動する際には、パラメータ `--helper` を一度だけ指定する必要があります。

   - FQDN アクセスで新しい FE ノードを起動するには、次のコマンドを実行して FE ノードを起動します。

     ```Bash
     # <helper_fqdn> を Leader FE ノードの FQDN に置き換え、
     # <helper_edit_log_port> (デフォルト: 9010) を Leader FE ノードの edit_log_port に置き換えます。
     ./fe/bin/start_fe.sh --helper <helper_fqdn>:<helper_edit_log_port> \
           --host_type FQDN --daemon
     ```

     ノードを初めて起動する際には、パラメータ `--helper` と `--host_type` を一度だけ指定する必要があります。

4. FE ログを確認して、FE ノードが正常に起動したかどうかを確認します。

   ```Bash
   cat fe/log/fe.log | grep thrift
   ```

   "2022-08-10 16:12:29,911 INFO (UNKNOWN x.x.x.x_9010_1660119137253(-1)|1) [FeServer.start():52] thrift server started with port 9020." のようなログ記録は、FE ノードが正常に起動したことを示しています。

5. 上記の手順 2、3、4 を繰り返して、すべての新しい Follower FE ノードを正常に起動し、次に MySQL クライアントから次の SQL を実行して FE ノードのステータスを確認します。

   ```SQL
   SHOW PROC '/frontends'\G
   ```

   例:

   ```Plain
   MySQL [(none)]> SHOW PROC '/frontends'\G
   *************************** 1. row ***************************
                Name: x.x.x.x_9010_1686810741121
                  IP: x.x.x.x
         EditLogPort: 9010
            HttpPort: 8030
           QueryPort: 9030
             RpcPort: 9020
                Role: LEADER
           ClusterId: 919351034
                Join: true
               Alive: true
   ReplayedJournalId: 1220
       LastHeartbeat: 2023-06-15 15:39:04
            IsHelper: true
              ErrMsg: 
           StartTime: 2023-06-15 14:32:28
             Version: 3.0.0-48f4d81
   *************************** 2. row ***************************
                Name: x.x.x.x_9010_1686814080597
                  IP: x.x.x.x
         EditLogPort: 9010
            HttpPort: 8030
           QueryPort: 9030
             RpcPort: 9020
                Role: FOLLOWER
           ClusterId: 919351034
                Join: true
               Alive: true
   ReplayedJournalId: 1219
       LastHeartbeat: 2023-06-15 15:39:04
            IsHelper: true
              ErrMsg: 
           StartTime: 2023-06-15 15:38:53
             Version: 3.0.0-48f4d81
   *************************** 3. row ***************************
                Name: x.x.x.x_9010_1686814090833
                  IP: x.x.x.x
         EditLogPort: 9010
            HttpPort: 8030
           QueryPort: 9030
             RpcPort: 9020
                Role: FOLLOWER
           ClusterId: 919351034
                Join: true
               Alive: true
   ReplayedJournalId: 1219
       LastHeartbeat: 2023-06-15 15:39:04
            IsHelper: true
              ErrMsg: 
           StartTime: 2023-06-15 15:37:52
             Version: 3.0.0-48f4d81
   3 rows in set (0.02 sec)
   ```

   - フィールド `Alive` が `true` の場合、この FE ノードは正常に起動し、クラスタに追加されています。
   - フィールド `Role` が `FOLLOWER` の場合、この FE ノードは Leader FE ノードとして選出される資格があります。
   - フィールド `Role` が `LEADER` の場合、この FE ノードは Leader FE ノードです。

## StarRocks クラスタを停止する

次のコマンドを対応するインスタンスで実行することで、StarRocks クラスタを停止できます。

- FE ノードを停止します。

  ```Bash
  ./fe/bin/stop_fe.sh --daemon
  ```

- BE ノードを停止します。

  ```Bash
  ./be/bin/stop_be.sh --daemon
  ```

- CN ノードを停止します。

  ```Bash
  ./be/bin/stop_cn.sh --daemon
  ```

## トラブルシューティング

FE または BE ノードを起動する際に発生するエラーを特定するために、次の手順を試してください。

- FE ノードが正常に起動しない場合は、**fe/log/fe.warn.log** にあるログを確認して問題を特定できます。

  ```Bash
  cat fe/log/fe.warn.log
  ```

  問題を特定して解決した後、現在の FE プロセスを終了し、既存の **meta** ディレクトリを削除し、新しいメタデータストレージディレクトリを作成して、正しい設定で FE ノードを再起動する必要があります。

- BE ノードが正常に起動しない場合は、**be/log/be.WARNING** にあるログを確認して問題を特定できます。

  ```Bash
  cat be/log/be.WARNING
  ```

  問題を特定して解決した後、既存の BE プロセスを終了し、既存の **storage** ディレクトリを削除し、新しいデータストレージディレクトリを作成して、正しい設定で BE ノードを再起動する必要があります。

- CN ノードが正常に起動しない場合は、**be/log/cn.WARNING** にあるログを確認して問題を特定できます。

  ```Bash
  cat be/log/cn.WARNING
  ```

  問題を特定して解決した後、既存の CN プロセスを終了し、正しい設定で CN ノードを再起動する必要があります。

## 次に行うこと

StarRocks クラスタをデプロイした後、初期管理手順については[デプロイ後のセットアップ](../deployment/post_deployment_setup.md)を参照してください。