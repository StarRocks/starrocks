---
displayed_sidebar: docs
---

# Prepare deployment files

このトピックでは、StarRocks のデプロイメントファイルを準備する方法について説明します。

現在、[StarRocks 公式ウェブサイト](https://www.starrocks.io/download/community) で提供されているバイナリ配布パッケージは、x86 ベースの CentOS 7.9 でのみデプロイメントをサポートしています。ARM アーキテクチャの CPU または Ubuntu 22.04 で StarRocks をデプロイしたい場合は、StarRocks Docker イメージを使用してデプロイメントファイルを準備する必要があります。

## x86 ベースの CentOS 7.9 の場合

StarRocks のバイナリ配布パッケージは **StarRocks-version.tar.gz** 形式で命名されており、**version** はバイナリ配布パッケージのバージョン情報を示す番号（例: **2.5.2**）です。正しいバージョンのパッケージを選択したことを確認してください。

x86 ベースの CentOS 7.9 プラットフォーム用にデプロイメントファイルを準備するには、次の手順に従います。

1. [Download StarRocks](https://www.starrocks.io/download/community) ページから直接、またはターミナルで次のコマンドを実行して StarRocks バイナリ配布パッケージを取得します。

   ```Bash
   # <version> をダウンロードしたい StarRocks のバージョンに置き換えてください。例: 2.5.2。
   wget https://releases.starrocks.io/starrocks/StarRocks-<version>.tar.gz
   ```

2. パッケージ内のファイルを展開します。

   ```Bash
   # <version> をダウンロードした StarRocks のバージョンに置き換えてください。
   tar -xzvf StarRocks-<version>.tar.gz
   ```

   パッケージには次のディレクトリとファイルが含まれています。

   | **Directory/File**     | **Description**                                              |
   | ---------------------- | ------------------------------------------------------------ |
   | **apache_hdfs_broker** | Broker ノードのデプロイメントディレクトリ。StarRocks v2.5 以降、一般的なシナリオでは Broker ノードをデプロイする必要はありません。StarRocks クラスターに Broker ノードをデプロイする必要がある場合は、[Deploy Broker node](../deployment/deploy_broker.md) を参照してください。 |
   | **fe**                 | FE デプロイメントディレクトリ。                                 |
   | **be**                 | BE デプロイメントディレクトリ。                                 |
   | **LICENSE.txt**        | StarRocks ライセンスファイル。                                  |
   | **NOTICE.txt**         | StarRocks 通知ファイル。                                       |

3. ディレクトリ **fe** をすべての FE インスタンスに、ディレクトリ **be** をすべての BE または CN インスタンスに [manual deployment](../deployment/deploy_manually.md) のために配布します。

## ARM ベースの CPU または Ubuntu 22.04 の場合

### 前提条件

マシンに [Docker Engine](https://docs.docker.com/engine/install/) (17.06.0 以降) がインストールされている必要があります。

### 手順

1. [StarRocks Docker Hub](https://hub.docker.com/r/starrocks/artifacts-ubuntu/tags) から StarRocks Docker イメージをダウンロードします。イメージのタグに基づいて特定のバージョンを選択できます。

   - Ubuntu 22.04 を使用する場合:

     ```Bash
     # <image_tag> をダウンロードしたいイメージのタグに置き換えてください。例: 2.5.4。
     docker pull starrocks/artifacts-ubuntu:<image_tag>
     ```

   - ARM ベースの CentOS 7.9 を使用する場合:

     ```Bash
     # <image_tag> をダウンロードしたいイメージのタグに置き換えてください。例: 2.5.4。
     docker pull starrocks/artifacts-centos7:<image_tag>
     ```

2. Docker イメージからホストマシンに StarRocks デプロイメントファイルをコピーするには、次のコマンドを実行します。

   - Ubuntu 22.04 を使用する場合:

     ```Bash
     # <image_tag> をダウンロードしたイメージのタグに置き換えてください。例: 2.5.4。
     docker run --rm starrocks/artifacts-ubuntu:<image_tag> \
         tar -cf - -C /release . | tar -xvf -
     ```

   - ARM ベースの CentOS 7.9 を使用する場合:

     ```Bash
     # <image_tag> をダウンロードしたイメージのタグに置き換えてください。例: 2.5.4。
     docker run --rm starrocks/artifacts-centos7:<image_tag> \
         tar -cf - -C /release . | tar -xvf -
     ```

   デプロイメントファイルには次のディレクトリが含まれています。

   | **Directory**        | **Description**                                              |
   | -------------------- | ------------------------------------------------------------ |
   | **be_artifacts**     | このディレクトリには BE または CN デプロイメントディレクトリ **be**、StarRocks ライセンスファイル **LICENSE.txt**、および StarRocks 通知ファイル **NOTICE.txt** が含まれています。 |
   | **broker_artifacts** | このディレクトリには Broker デプロイメントディレクトリ **apache_hdfs_broker** が含まれています。StarRocks 2.5 以降、一般的なシナリオでは Broker ノードをデプロイする必要はありません。StarRocks クラスターに Broker ノードをデプロイする必要がある場合は、[Deploy Broker](../deployment/deploy_broker.md) を参照してください。 |
   | **fe_artifacts**     | このディレクトリには FE デプロイメントディレクトリ **fe**、StarRocks ライセンスファイル **LICENSE.txt**、および StarRocks 通知ファイル **NOTICE.txt** が含まれています。 |

3. ディレクトリ **fe** をすべての FE インスタンスに、ディレクトリ **be** をすべての BE または CN インスタンスに [manual deployment](../deployment/deploy_manually.md) のために配布します。