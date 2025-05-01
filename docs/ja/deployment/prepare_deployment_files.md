---
displayed_sidebar: docs
---

# Prepare deployment files

このトピックでは、StarRocks のデプロイメントファイルを準備する方法について説明します。

現在、[StarRocks 公式ウェブサイト](https://www.starrocks.io/download/community) で提供されているバイナリ配布パッケージは、x86 ベースの CPU 上でのデプロイメントのみをサポートしています。ARM ベースの CPU で StarRocks をデプロイしたい場合は、StarRocks Docker イメージを使用してデプロイメントファイルを準備する必要があります。

## x86 ベースの CPU の場合

v3.1.14、v3.2.10、および v3.3.3 以降、StarRocks のバイナリ配布パッケージは `StarRocks-{Version}-{OS}-{ARCH}.tar.gz` 形式で命名されています。ここで、`Version` はバイナリ配布パッケージのバージョン情報を示す番号（例: `3.3.3`）、`OS` はオペレーティングシステム（`centos` や `ubuntu` を含む）、`ARCH` は CPU アーキテクチャ（現在は `amd64` のみ利用可能で、x86_64 と同等）を示します。正しいバージョンのパッケージを選択したことを確認してください。

:::note

v3.1.14、v3.2.10、および v3.3.3 より前のバージョンでは、StarRocks は `StarRocks-{Version}.tar.gz` 形式でバイナリ配布パッケージを提供しています。

:::

x86 ベースのプラットフォーム用にデプロイメントファイルを準備するには、次の手順に従います。

1. [Download StarRocks](https://www.starrocks.io/download/community) ページから直接、またはターミナルで次のコマンドを実行して StarRocks バイナリ配布パッケージを取得します。

   ```Bash
   # <version> をダウンロードしたい StarRocks のバージョン（例: 3.3.3）に置き換え、
   # <OS> を centos または ubuntu に置き換えます。
   wget https://releases.starrocks.io/starrocks/StarRocks-<version>-<OS>-amd64.tar.gz
   ```

2. パッケージ内のファイルを抽出します。

   ```Bash
   # <version> をダウンロードしたい StarRocks のバージョン（例: 3.3.3）に置き換え、
   # <OS> を centos または ubuntu に置き換えます。
   tar -xzvf StarRocks-<version>-<OS>-amd64.tar.gz
   ```

   パッケージには次のディレクトリとファイルが含まれています。

   | **Directory/File**     | **Description**                                              |
   | ---------------------- | ------------------------------------------------------------ |
   | **apache_hdfs_broker** | Broker ノードのデプロイメントディレクトリ。StarRocks v2.5 以降、一般的なシナリオでは Broker ノードをデプロイする必要はありません。StarRocks クラスターに Broker ノードをデプロイする必要がある場合は、[Deploy Broker node](../deployment/deploy_broker.md) を参照して詳細な手順を確認してください。 |
   | **fe**                 | FE のデプロイメントディレクトリ。                                 |
   | **be**                 | BE のデプロイメントディレクトリ。                                 |
   | **LICENSE.txt**        | StarRocks のライセンスファイル。                                  |
   | **NOTICE.txt**         | StarRocks の通知ファイル。                                   |

3. ディレクトリ **fe** をすべての FE インスタンスに、ディレクトリ **be** をすべての BE または CN インスタンスに [manual deployment](../deployment/deploy_manually.md) のために配布します。

## ARM ベースの CPU の場合

### 前提条件

マシンに [Docker Engine](https://docs.docker.com/engine/install/) (17.06.0 以降) がインストールされている必要があります。

### 手順

v3.1.14、v3.2.10、および v3.3.3 以降、StarRocks は `starrocks/{Component}-{OS}:{Version}` 形式で Docker イメージを提供しています。ここで、`Component` はイメージのコンポーネント（`fe`、`be`、`cn` を含む）、`OS` はオペレーティングシステム（`centos` や `ubuntu` を含む）、`Version` はバージョン番号（例: `3.3.3`）を示します。Docker は自動的に CPU アーキテクチャを識別し、対応するイメージをプルします。正しいバージョンのイメージを選択したことを確認してください。

:::note

v3.1.14、v3.2.10、および v3.3.3 より前のバージョンでは、StarRocks は `starrocks/artifacts-ubuntu` および `starrocks/artifacts-centos7` リポジトリで Docker イメージを提供しています。

:::

1. [StarRocks Docker Hub](https://hub.docker.com/r/starrocks/artifacts-ubuntu/tags) から StarRocks Docker イメージをダウンロードします。イメージのタグに基づいて特定のバージョンを選択できます。

   ```Bash
   # <component> をダウンロードしたいコンポーネント（例: fe）に置き換え、
   # <version> をダウンロードしたい StarRocks のバージョン（例: 3.3.3）に置き換え、
   # <OS> を centos または ubuntu に置き換えます。
   docker pull starrocks/<Component>-<OS>:<version>
   ```

2. 次のコマンドを実行して、Docker イメージからホストマシンに StarRocks デプロイメントファイルをコピーします。

   ```Bash
   # <component> をダウンロードしたいコンポーネント（例: fe）に置き換え、
   # <version> をダウンロードしたい StarRocks のバージョン（例: 3.3.3）に置き換え、
   # <OS> を centos または ubuntu に置き換えます。
   docker run --rm starrocks/<Component>-<OS>:<version> \
       tar -cf - -C /release . | tar -xvf -
   ```

3. デプロイメントファイルを対応するインスタンスに [manual deployment](../deployment/deploy_manually.md) のために配布します。