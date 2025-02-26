---
displayed_sidebar: docs
---

# 環境設定の確認

このトピックでは、StarRocks をデプロイする前に確認し設定する必要があるすべての環境およびシステム設定項目を一覧にしています。これらの設定項目を適切に設定することで、StarRocks クラスターが高可用性とパフォーマンスで動作します。

## ポート

StarRocks は異なるサービスに特定のポートを使用します。これらのポートが他のサービスで使用されていないか、各インスタンスで確認してください。

### FE ポート

FE のデプロイメントに使用されるインスタンスでは、以下のポートを確認する必要があります。

- `8030`: FE HTTP サーバーポート (`http_port`)
- `9020`: FE Thrift サーバーポート (`rpc_port`)
- `9030`: FE MySQL サーバーポート (`query_port`)
- `9010`: FE 内部通信ポート (`edit_log_port`)
- `6090`: FE クラウドネイティブメタデータサーバー RPC リッスンポート (`cloud_native_meta_port`)

FE インスタンスで以下のコマンドを実行し、これらのポートが使用されていないか確認してください。

```Bash
netstat -tunlp | grep 8030
netstat -tunlp | grep 9020
netstat -tunlp | grep 9030
netstat -tunlp | grep 9010
netstat -tunlp | grep 6090
```

上記のポートが使用されている場合は、代替ポートを見つけ、FE ノードをデプロイする際に指定する必要があります。詳細な手順については、[Deploy StarRocks - Start the Leader FE node](../deployment/deploy_manually.md#step-1-start-the-leader-fe-node) を参照してください。

### BE ポート

BE のデプロイメントに使用されるインスタンスでは、以下のポートを確認する必要があります。

- `9060`: BE Thrift サーバーポート (`be_port`)
- `8040`: BE HTTP サーバーポート (`be_http_port`)
- `9050`: BE ハートビートサービスポート (`heartbeat_service_port`)
- `8060`: BE bRPC ポート (`brpc_port`)
- `9070`: BE と CN のための追加エージェントサービスポート (`starlet_port`)

BE インスタンスで以下のコマンドを実行し、これらのポートが使用されていないか確認してください。

```Bash
netstat -tunlp | grep 9060
netstat -tunlp | grep 8040
netstat -tunlp | grep 9050
netstat -tunlp | grep 8060
netstat -tunlp | grep 9070
```

上記のポートが使用されている場合は、代替ポートを見つけ、BE ノードをデプロイする際に指定する必要があります。詳細な手順については、[Deploy StarRocks - Start the BE service](../deployment/deploy_manually.md#step-2-start-the-be-service) を参照してください。

### CN ポート

CN のデプロイメントに使用されるインスタンスでは、以下のポートを確認する必要があります。

- `9060`: CN Thrift サーバーポート (`be_port`)
- `8040`: CN HTTP サーバーポート (`be_http_port`)
- `9050`: CN ハートビートサービスポート (`heartbeat_service_port`)
- `8060`: CN bRPC ポート (`brpc_port`)
- `9070`: BE と CN のための追加エージェントサービスポート (`starlet_port`)

CN インスタンスで以下のコマンドを実行し、これらのポートが使用されていないか確認してください。

```Bash
netstat -tunlp | grep 9060
netstat -tunlp | grep 8040
netstat -tunlp | grep 9050
netstat -tunlp | grep 8060
netstat -tunlp | grep 9070
```

上記のポートが使用されている場合は、代替ポートを見つけ、CN ノードをデプロイする際に指定する必要があります。詳細な手順については、[Deploy StarRocks - Start the CN service](../deployment/deploy_manually.md#step-3-optional-start-the-cn-service) を参照してください。

## ホスト名

StarRocks クラスターで [FQDN アクセスを有効にする](../administration/management/enable_fqdn.md) には、各インスタンスにホスト名を割り当てる必要があります。

各インスタンスの **/etc/hosts** ファイルに、クラスター内の他のすべてのインスタンスの IP アドレスと対応するホスト名を指定する必要があります。

> **注意**
>
> **/etc/hosts** ファイル内のすべての IP アドレスは一意でなければなりません。

## JDK 設定

StarRocks は、インスタンス上の Java 依存関係を見つけるために環境変数 `JAVA_HOME` に依存しています。

環境変数 `JAVA_HOME` を確認するには、以下のコマンドを実行してください。

```Bash
echo $JAVA_HOME
```

`JAVA_HOME` を設定する手順は次のとおりです。

1. **/etc/profile** ファイルで `JAVA_HOME` を設定します。

   ```Bash
   sudo vi /etc/profile
   # <path_to_JDK> を JDK がインストールされているパスに置き換えます。
   export JAVA_HOME=<path_to_JDK>
   export PATH=$PATH:$JAVA_HOME/bin
   ```

2. 変更を反映させます。

   ```Bash
   source /etc/profile
   ```

変更を確認するには、以下のコマンドを実行してください。

```Bash
java -version
```

## CPU スケーリングガバナー

この設定項目は**オプション**です。CPU がスケーリングガバナーをサポートしていない場合はスキップできます。

CPU スケーリングガバナーは CPU の電力モードを制御します。CPU がサポートしている場合は、より良い CPU パフォーマンスのために `performance` に設定することをお勧めします。

```Bash
echo 'performance' | sudo tee /sys/devices/system/cpu/cpu*/cpufreq/scaling_governor
```

## メモリ設定

### メモリオーバーコミット

メモリオーバーコミットは、オペレーティングシステムがプロセスにメモリリソースをオーバーコミットすることを許可します。メモリオーバーコミットを有効にすることをお勧めします。

```Bash
# 設定ファイルを変更します。
cat >> /etc/sysctl.conf << EOF
vm.overcommit_memory=1
EOF
# 変更を反映させます。
sysctl -p
```

### トランスペアレントヒュージページ

トランスペアレントヒュージページはデフォルトで有効になっています。この機能はメモリアロケータに干渉し、パフォーマンスの低下を招く可能性があるため、無効にすることをお勧めします。

```Bash
# 設定を一時的に変更します。
echo madvise | sudo tee /sys/kernel/mm/transparent_hugepage/enabled
echo madvise | sudo tee /sys/kernel/mm/transparent_hugepage/defrag
# 設定を永続的に変更します。
cat >> /etc/rc.d/rc.local << EOF
if test -f /sys/kernel/mm/transparent_hugepage/enabled; then
   echo madvise > /sys/kernel/mm/transparent_hugepage/enabled
fi
if test -f /sys/kernel/mm/transparent_hugepage/defrag; then
   echo madvise > /sys/kernel/mm/transparent_hugepage/defrag
fi
EOF
chmod +x /etc/rc.d/rc.local
```

### スワップスペース

スワップスペースを無効にすることをお勧めします。

スワップスペースを確認し無効にする手順は次のとおりです。

1. スワップスペースを無効にします。

   ```SQL
   swapoff /<path_to_swap_space>
   swapoff -a
   ```

2. 設定ファイル **/etc/fstab** からスワップスペース情報を削除します。

   ```Bash
   /<path_to_swap_space> swap swap defaults 0 0
   ```

3. スワップスペースが無効になっていることを確認します。

   ```Bash
   free -m
   ```

### スワップネス

スワップネスを無効にして、パフォーマンスへの影響を排除することをお勧めします。

```Bash
# 設定ファイルを変更します。
cat >> /etc/sysctl.conf << EOF
vm.swappiness=0
EOF
# 変更を反映させます。
sysctl -p
```

## ストレージ設定

使用する記憶媒体に応じて、適切なスケジューラーアルゴリズムを選択することをお勧めします。

使用しているスケジューラーアルゴリズムを確認するには、以下のコマンドを実行してください。

```Bash
cat /sys/block/${disk}/queue/scheduler
# 例として、cat /sys/block/vdb/queue/scheduler を実行します。
```

SATA ディスクには mq-deadline スケジューラーを、SSD および NVMe ディスクには kyber スケジューラーアルゴリズムを使用することをお勧めします。

### SATA

mq-deadline スケジューラーアルゴリズムは SATA ディスクに適しています。

```Bash
# 設定を一時的に変更します。
echo mq-deadline | sudo tee /sys/block/${disk}/queue/scheduler
# 設定を永続的に変更します。
cat >> /etc/rc.d/rc.local << EOF
echo mq-deadline | sudo tee /sys/block/${disk}/queue/scheduler
EOF
chmod +x /etc/rc.d/rc.local
```

### SSD および NVMe

- NVMe または SSD ディスクが kyber スケジューラーアルゴリズムをサポートしている場合:

   ```Bash
   # 設定を一時的に変更します。
   echo kyber | sudo tee /sys/block/${disk}/queue/scheduler
   # 設定を永続的に変更します。
   cat >> /etc/rc.d/rc.local << EOF
   echo kyber | sudo tee /sys/block/${disk}/queue/scheduler
   EOF
   chmod +x /etc/rc.d/rc.local
   ```

- NVMe または SSD ディスクが none (または noop) スケジューラーをサポートしている場合。

   ```Bash
   # 設定を一時的に変更します。
   echo none | sudo tee /sys/block/vdb/queue/scheduler
   # 設定を永続的に変更します。
   cat >> /etc/rc.d/rc.local << EOF
   echo none | sudo tee /sys/block/${disk}/queue/scheduler
   EOF
   chmod +x /etc/rc.d/rc.local
   ```

## SELinux

SELinux を無効にすることをお勧めします。

```Bash
# 設定を一時的に変更します。
setenforce 0
# 設定を永続的に変更します。
sed -i 's/SELINUX=.*/SELINUX=disabled/' /etc/selinux/config
sed -i 's/SELINUXTYPE/#SELINUXTYPE/' /etc/selinux/config
```

## ファイアウォール

ファイアウォールが有効になっている場合は、FE ノード、BE ノード、および Broker の内部ポートを開放してください。

```Bash
systemctl stop firewalld.service
systemctl disable firewalld.service
```

## LANG 変数

以下のコマンドを実行して、LANG 変数を手動で確認および設定してください。

```Bash
# 設定ファイルを変更します。
echo "export LANG=en_US.UTF8" >> /etc/profile
# 変更を反映させます。
source /etc/profile
```

## タイムゾーン

実際のタイムゾーンに応じてこの項目を設定してください。

以下の例では、タイムゾーンを `/Asia/Shanghai` に設定しています。

```Bash
cp -f /usr/share/zoneinfo/Asia/Shanghai /etc/localtime
hwclock
```

## ulimit 設定

**最大ファイル記述子**と**最大ユーザープロセス**の値が異常に小さい場合、StarRocks に問題が発生する可能性があります。これらの値を大きくすることをお勧めします。

```Bash
cat >> /etc/security/limits.conf << EOF
* soft nproc 65535
* hard nproc 65535
* soft nofile 655350
* hard nofile 655350
* soft stack unlimited
* hard stack unlimited
* hard memlock unlimited
* soft memlock unlimited
EOF

cat >> /etc/security/limits.d/20-nproc.conf << EOF 
*          soft    nproc     65535
root       soft    nproc     65535
EOF
```

## ファイルシステム設定

ext4 または xfs ジャーナリングファイルシステムを使用することをお勧めします。マウントタイプを確認するには、以下のコマンドを実行してください。

```Bash
df -Th
```

## ネットワーク設定

### tcp_abort_on_overflow

システムが新しい接続試行でオーバーフローしている場合に、新しい接続をリセットするようにシステムを許可します。

```Bash
# 設定ファイルを変更します。
cat >> /etc/sysctl.conf << EOF
net.ipv4.tcp_abort_on_overflow=1
EOF
# 変更を反映させます。
sysctl -p
```

### somaxconn

任意のリスニングソケットに対してキューに入れられる接続要求の最大数を `1024` に指定します。

```Bash
# 設定ファイルを変更します。
cat >> /etc/sysctl.conf << EOF
net.core.somaxconn=1024
EOF
# 変更を反映させます。
sysctl -p
```

## NTP 設定

トランザクションの線形一貫性を確保するために、StarRocks クラスター内のノード間で時間同期を構成する必要があります。pool.ntp.org が提供するインターネット時間サービスを使用するか、オフライン環境で組み込まれた NTP サービスを使用できます。たとえば、クラウドサービスプロバイダーが提供する NTP サービスを使用できます。

1. NTP タイムサーバーまたは Chrony サービスが存在するか確認します。

   ```Bash
   rpm -qa | grep ntp
   systemctl status chronyd
   ```

2. NTP サービスが存在しない場合はインストールします。

   ```Bash
   sudo yum install ntp ntpdate && \
   sudo systemctl start ntpd.service && \
   sudo systemctl enable ntpd.service
   ```

3. NTP サービスを確認します。

   ```Bash
   systemctl list-unit-files | grep ntp
   ```

4. NTP サービスの接続性と監視状態を確認します。

   ```Bash
   netstat -tunlp | grep ntp
   ```

5. アプリケーションが NTP サーバーと同期しているか確認します。

   ```Bash
   ntpstat
   ```

6. ネットワーク内で設定されたすべての NTP サーバーの状態を確認します。

   ```Bash
   ntpq -p
   ```

## 高負荷同時実行設定

StarRocks クラスターが高負荷の同時実行を持つ場合、以下の設定を行うことをお勧めします。

### max_map_count

プロセスが持つことができるメモリマップ領域の最大数を `262144` に指定します。

```bash
# 設定ファイルを変更します。
cat >> /etc/sysctl.conf << EOF
vm.max_map_count = 262144
EOF
# 変更を反映させます。
sysctl -p
```

### その他

```Bash
echo 120000 > /proc/sys/kernel/threads-max
echo 200000 > /proc/sys/kernel/pid_max
```