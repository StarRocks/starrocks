---
displayed_sidebar: docs
---

# デプロイの前提条件

このトピックでは、StarRocks をデプロイする前にサーバーが満たすべきハードウェアおよびソフトウェアの要件について説明します。StarRocks クラスターの推奨ハードウェア仕様については、[StarRocks クラスターの計画](../deployment/plan_cluster.md) を参照してください。

## ハードウェア

### CPU

StarRocks は、そのベクトル化機能を完全に発揮するために AVX2 命令セットに依存しています。したがって、実稼働環境では、x86 アーキテクチャの CPU を搭載したマシンに StarRocks をデプロイすることを強くお勧めします。

マシンの CPU が AVX2 命令セットをサポートしているかどうかを確認するには、ターミナルで次のコマンドを実行します。

```Bash
cat /proc/cpuinfo | grep avx2
```

### メモリ

StarRocks に使用されるメモリキットに特定の要件はありません。推奨されるメモリサイズについては、[StarRocks クラスターの計画- CPU とメモリ](../deployment/plan_cluster.md#cpu-とメモリ) を参照してください。

### ストレージ

StarRocks は、HDD と SSD の両方を記憶媒体としてサポートしています。

アプリケーションがリアルタイムのデータ分析、集中的なデータスキャン、またはランダムディスクアクセスを必要とする場合、SSD ストレージを使用することを強くお勧めします。

アプリケーションが永続性インデックスを持つ [主キーテーブル](../table_design/table_types/primary_key_table.md) を含む場合、SSD ストレージを使用する必要があります。

### ネットワーク

StarRocks クラスター内のノード間で安定したデータ伝送を確保するために、10 ギガビットイーサネットネットワーキングを使用することをお勧めします。

## オペレーティングシステム

StarRocks は、Red Hat Enterprise Linux 7.9、CentOS Linux 7.9、または Ubuntu Linux 22.04 でのデプロイメントをサポートしています。

## ソフトウェア

StarRocks を実行するには、サーバーに適切な JDK バージョンをインストールする必要があります。

- StarRocks v3.3、v3.4 では JDK 11 以降を使用してください。
- StarRocks v3.5 以降では JDK 17 以降を使用してください。

:::important
StarRocks は JRE をサポートしていません。
:::
