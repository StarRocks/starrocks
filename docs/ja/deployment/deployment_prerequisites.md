---
displayed_sidebar: docs
---

# Deployment prerequisites

このトピックでは、StarRocks をデプロイする前にサーバーが満たすべきハードウェアおよびソフトウェアの要件について説明します。StarRocks クラスターの推奨ハードウェア仕様については、[Plan your StarRocks cluster](../deployment/plan_cluster.md) を参照してください。

## Hardware

### CPU

StarRocks は、そのベクトル化機能を完全に発揮するために AVX2 命令セットに依存しています。したがって、実稼働環境では、x86 アーキテクチャの CPU を搭載したマシンに StarRocks をデプロイすることを強くお勧めします。

マシンの CPU が AVX2 命令セットをサポートしているかどうかを確認するには、ターミナルで次のコマンドを実行します。

```Bash
cat /proc/cpuinfo | grep avx2
```

### Memory

StarRocks に使用されるメモリキットに特定の要件はありません。推奨されるメモリサイズについては、[Plan StarRocks cluster - CPU and Memory](../deployment/plan_cluster.md#cpu-and-memory) を参照してください。

### Storage

StarRocks は、HDD と SSD の両方を記憶媒体としてサポートしています。

アプリケーションがリアルタイムのデータ分析、集中的なデータスキャン、またはランダムディスクアクセスを必要とする場合、SSD ストレージを使用することを強くお勧めします。

アプリケーションが永続性インデックスを持つ [Primary Key tables](../table_design/table_types/primary_key_table.md) を含む場合、SSD ストレージを使用する必要があります。

### Network

StarRocks クラスター内のノード間で安定したデータ伝送を確保するために、10 ギガビットイーサネットネットワーキングを使用することをお勧めします。

## Operating system

StarRocks は、Red Hat Enterprise Linux 7.9、CentOS Linux 7.9、または Ubuntu Linux 22.04 でのデプロイメントをサポートしています。

## Software

StarRocks を実行するには、サーバーに JDK 8 をインストールする必要があります。バージョン 2.5 以降では、JDK 11 が推奨されます。

> **注意**
>
> - StarRocks は JRE をサポートしていません。
> - Ubuntu 22.04 に StarRocks をインストールする場合は、JDK 11 をインストールする必要があります。

JDK 8 をインストールする手順は次のとおりです。

1. JDK インストール用のパスに移動します。
2. 次のコマンドを実行して JDK をダウンロードします。

   ```Bash
   wget --no-check-certificate --no-cookies \
       --header "Cookie: oraclelicense=accept-securebackup-cookie"  \
       http://download.oracle.com/otn-pub/java/jdk/8u131-b11/d54c1d3a095b4ff2b6607d096fa80163/jdk-8u131-linux-x64.tar.gz
   ```