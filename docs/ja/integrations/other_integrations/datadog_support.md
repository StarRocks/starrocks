---
description: Datadog でモニタリングする
displayed_sidebar: docs
---

# Monitor with Datadog

このトピックでは、StarRocks クラスターを監視およびセキュリティプラットフォームである [Datadog](https://www.datadoghq.com/) と統合する方法について説明します。

## 前提条件

始める前に、インスタンスに以下のソフトウェアがインストールされている必要があります。

- [Datadog Agent](https://docs.datadoghq.com/getting_started/agent/)
- Python

> **NOTE**
>
> Datadog Agent を初めてインストールする際、Python も依存関係としてインストールされます。以下の手順では、この Python を使用することをお勧めします。

## StarRocks ソースコードの準備

Datadog はまだ StarRocks 用の統合キットを提供していないため、ソースコードを使用して統合する必要があります。

1. ターミナルを起動し、読み書きアクセス権のあるローカルディレクトリに移動し、StarRocks ソースコード用の専用ディレクトリを作成するために次のコマンドを実行します。

    ```shell
    mkdir -p starrocks
    ```

2. 次のコマンドまたは [GitHub](https://github.com/StarRocks/starrocks/tags) から、作成したディレクトリに StarRocks ソースコードパッケージをダウンロードします。

    ```shell
    cd starrocks
    # <starrocks_ver> を StarRocks の実際のバージョンに置き換えてください。例: "2.5.2"。
    wget https://github.com/StarRocks/starrocks/archive/refs/tags/<starrocks_ver>.tar.gz
    ```

3. パッケージ内のファイルを抽出します。

    ```shell
    # <starrocks_ver> を StarRocks の実際のバージョンに置き換えてください。例: "2.5.2"。
    tar -xzvf <starrocks_ver>.tar.gz --strip-components 1
    ```

## FE 統合キットのインストールと設定

1. ソースコードを使用して FE 用の Datadog 統合キットをインストールします。

    ```shell
    /opt/datadog-agent/embedded/bin/pip install contrib/datadog-connector/starrocks_fe
    ```

2. FE 統合設定ファイル **/etc/datadog-agent/conf.d/starrocks_fe.d/conf.yaml** を作成します。

    ```shell
    sudo mkdir -p /etc/datadog-agent/conf.d/starrocks_fe.d
    sudo cp contrib/datadog-connector/starrocks_fe/datadog_checks/starrocks_fe/data/conf.yaml.example /etc/datadog-agent/conf.d/starrocks_fe.d/conf.yaml
    ```

3. FE 統合設定ファイル **/etc/datadog-agent/conf.d/starrocks_fe.d/conf.yaml** を修正します。

    重要な設定項目の例:

    | **Config** | **Example** | **Description** |
    | -------------------------------------- | ------------ | ------------------------------------------------------------ |
    | fe_metric_url | `http://localhost:8030/metrics` | StarRocks FE メトリクスにアクセスするための URL。 |
    | metrics | `- starrocks_fe_*` | FE で監視するメトリクス。ワイルドカード `*` を使用して設定項目をマッチさせることができます。 |

## BE 統合キットのインストールと設定

1. ソースコードを使用して BE 用の Datadog 統合キットをインストールします。

    ```shell
    /opt/datadog-agent/embedded/bin/pip install contrib/datadog-connector/starrocks_be
    ```

2. BE 統合設定ファイル **/etc/datadog-agent/conf.d/starrocks_be.d/conf.yaml** を作成します。

    ```shell
    sudo mkdir -p /etc/datadog-agent/conf.d/starrocks_be.d
    sudo cp contrib/datadog-connector/starrocks_be/datadog_checks/starrocks_be/data/conf.yaml.example /etc/datadog-agent/conf.d/starrocks_be.d/conf.yaml
    ```

3. BE 統合設定ファイル **/etc/datadog-agent/conf.d/starrocks_be.d/conf.yaml** を修正します。

    重要な設定項目の例:

    | **Config** | **Example** | **Description** |
    | -------------------------------------- | ------------ | ------------------------------------------------------------ |
    | be_metric_url | `http://localhost:8040/metrics` | StarRocks BE メトリクスにアクセスするための URL。 |
    | metrics | `- starrocks_be_*` | BE で監視するメトリクス。ワイルドカード `*` を使用して設定項目をマッチさせることができます。 |

## Datadog Agent の再起動

設定を有効にするために Datadog Agent を再起動します。

```shell
sudo systemctl stop datadog-agent
sudo systemctl start datadog-agent
```

## 統合の確認

統合を確認する手順については、[Datadog Application](https://docs.datadoghq.com/getting_started/application/) を参照してください。

## 統合キットのアンインストール

統合キットが不要になった場合、アンインストールすることができます。

- FE 統合キットをアンインストールするには、次のコマンドを実行します。

  ```shell
  /opt/datadog-agent/embedded/bin/pip uninstall datadog-starrocks-fe
  ```

- BE 統合キットをアンインストールするには、次のコマンドを実行します。

  ```shell
  /opt/datadog-agent/embedded/bin/pip uninstall datadog-starrocks-be
  ```