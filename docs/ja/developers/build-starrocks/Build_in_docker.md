---
displayed_sidebar: docs
---

# Docker を使用して StarRocks をコンパイルする

このトピックでは、Docker を使用して StarRocks をコンパイルする方法について説明します。

## 概要

StarRocks は、Ubuntu 22.04 と CentOS 7.9 の両方に対応した開発環境イメージを提供しています。このイメージを使用すると、Docker コンテナを起動して、その中で StarRocks をコンパイルできます。

### StarRocks のバージョンと DEV ENV イメージ

StarRocks の異なるブランチは、[StarRocks Docker Hub](https://hub.docker.com/u/starrocks) で提供されている異なる開発環境イメージに対応しています。

- Ubuntu 22.04 の場合:

  | **ブランチ名** | **イメージ名**                        |
  | -------------- | ------------------------------------- |
  | main           | starrocks/dev-env-ubuntu:latest       |
  | branch-3.2     | starrocks/dev-env-ubuntu:3.2-latest   |
  | branch-3.1     | starrocks/dev-env-ubuntu:3.1-latest   |
  | branch-3.0     | starrocks/dev-env-ubuntu:3.0-latest   |
  | branch-2.5     | starrocks/dev-env-ubuntu:2.5-latest   |

- CentOS 7.9 の場合:

  | **ブランチ名** | **イメージ名**                         |
  | -------------- | -------------------------------------- |
  | main           | starrocks/dev-env-centos7:latest       |
  | branch-3.2     | starrocks/dev-env-centos7:3.2-latest   |
  | branch-3.1     | starrocks/dev-env-centos7:3.1-latest   |
  | branch-3.0     | starrocks/dev-env-centos7:3.0-latest   |
  | branch-2.5     | starrocks/dev-env-centos7:2.5-latest   |

## 前提条件

StarRocks をコンパイルする前に、以下の要件を満たしていることを確認してください。

- **ハードウェア**

  マシンには少なくとも 8 GB の RAM が必要です。

- **ソフトウェア**

  - マシンは Ubuntu 22.04 または CentOS 7.9 で動作している必要があります。
  - Docker がマシンにインストールされており、バージョンは少なくとも v20.10.10 である必要があります。

## ステップ 1: イメージをダウンロードする

次のコマンドを実行して開発環境イメージをダウンロードします。

```Bash
# <image_name> をダウンロードしたいイメージの名前に置き換えてください。
# 例えば、`starrocks/dev-env-ubuntu:latest` です。
# OS に適したイメージを選択してください。
docker pull <image_name>
```

Docker は自動的にマシンの CPU アーキテクチャを識別し、それに適したイメージをプルします。`linux/amd64` イメージは x86 ベースの CPU 用で、`linux/arm64` イメージは ARM ベースの CPU 用です。

## ステップ 2: Docker コンテナ内で StarRocks をコンパイルする

ローカルホストパスをマウントするかしないかで、開発環境 Docker コンテナを起動できます。次回のコンパイル時に Java の依存関係を再ダウンロードするのを避けるため、ローカルホストパスをマウントしてコンテナを起動することをお勧めします。また、コンテナからローカルホストにバイナリファイルを手動でコピーする必要もありません。

- **ローカルホストパスをマウントしてコンテナを起動する**:

  1. StarRocks のソースコードをローカルホストにクローンします。

     ```Bash
     git clone https://github.com/StarRocks/starrocks.git
     ```

  2. コンテナを起動します。

     ```Bash
     # <code_dir> を StarRocks ソースコードディレクトリの親ディレクトリに置き換えてください。
     # <branch_name> をイメージ名に対応するブランチ名に置き換えてください。
     # <image_name> をダウンロードしたイメージの名前に置き換えてください。
     docker run -it -v <code_dir>/.m2:/root/.m2 \
         -v <code_dir>/starrocks:/root/starrocks \
         --name <branch_name> -d <image_name>
     ```

  3. 起動したコンテナ内で bash シェルを起動します。

     ```Bash
     # <branch_name> をイメージ名に対応するブランチ名に置き換えてください。
     docker exec -it <branch_name> /bin/bash
     ```

  4. コンテナ内で StarRocks をコンパイルします。

     ```Bash
     cd /root/starrocks && ./build.sh
     ```

- **ローカルホストパスをマウントせずにコンテナを起動する**:

  1. コンテナを起動します。

     ```Bash
     # <branch_name> をイメージ名に対応するブランチ名に置き換えてください。
     # <image_name> をダウンロードしたイメージの名前に置き換えてください。
     docker run -it --name <branch_name> -d <image_name>
     ```

  2. コンテナ内で bash シェルを起動します。

     ```Bash
     # <branch_name> をイメージ名に対応するブランチ名に置き換えてください。
     docker exec -it <branch_name> /bin/bash
     ```

  3. コンテナに StarRocks のソースコードをクローンします。

     ```Bash
     git clone https://github.com/StarRocks/starrocks.git
     ```

  4. コンテナ内で StarRocks をコンパイルします。

     ```Bash
     cd starrocks && ./build.sh
     ```

## トラブルシューティング

Q: StarRocks BE のビルドが失敗し、次のエラーメッセージが返されました:

```Bash
g++: fatal error: Killed signal terminated program cc1plus
compilation terminated.
```

どうすればよいですか？

A: このエラーメッセージは、Docker コンテナ内のメモリ不足を示しています。コンテナに少なくとも 8 GB のメモリリソースを割り当てる必要があります。