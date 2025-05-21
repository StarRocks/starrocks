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

StarRocks バージョン 3.5 以降を実行するには、サーバーに JDK 17 をインストールする必要があります。

> **注意**
>
> StarRocks は JRE をサポートしていません。
