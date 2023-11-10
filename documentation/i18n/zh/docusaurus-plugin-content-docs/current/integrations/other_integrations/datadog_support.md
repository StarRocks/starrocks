---
displayed_sidebar: "Chinese"
---

# StarRocks 集成 Datadog

本文介绍如何将 StarRocks 集群与监控和安全平台 [Datadog](https://www.datadoghq.com/) 集成。

## 前提条件

开始之前，请确保已安装以下环境：

- [Datadog Agent](https://docs.datadoghq.com/getting_started/agent/)
- Python

> **说明**
>
> 首次安装 Datadog Agent 时，Python 也会作为依赖项一同安装。我们建议您在以下步骤中使用此 Python。

## 准备 StarRocks 源码

由于 Datadog 平台目前尚未提供 StarRocks 的集成套件，您需要使用源码进行安装。

1. 启动终端，进入到您具有读写权限的本地目录，然后运行以下命令为 StarRocks 源代码创建专用目录。

    ```sh
    mkdir -p starrocks
    ```

2. 使用以下命令或在 [GitHub](https://github.com/StarRocks/starrocks/tags) 上下载 StarRocks 源码包至您先前创建的目录。

    ```sh
    cd starrocks
    # Replace <starrocks_ver> with the actual version of StarRocks, for example, "2.5.2".
    wget https://github.com/StarRocks/starrocks/archive/refs/tags/<starrocks_ver>.tar.gz
    ```

3. 解压包中的文件。

    ```sh
    # Replace <starrocks_ver> with the actual version of StarRocks, for example, "2.5.2".
    tar -xzvf <starrocks_ver>.tar.gz --strip-components 1
    ```

## 安装配置 FE 集成套件

1. 使用源代码为 FE 安装 Datadog 集成套件。

    ```sh
    /opt/datadog-agent/embedded/bin/pip install contrib/datadog-connector/starrocks_fe
    ```

2. 创建 FE 集成配置文件 **/etc/datadog-agent/conf.d/starrocks_fe.d/conf.yaml**。

    ```sh
    sudo mkdir -p /etc/datadog-agent/conf.d/starrocks_fe.d
    sudo cp contrib/datadog-connector/starrocks_fe/datadog_checks/starrocks_fe/data/conf.yaml.example /etc/datadog-agent/conf.d/starrocks_fe.d/conf.yaml
    ```

3. 修改 FE 集成配置文件 **/etc/datadog-agent/conf.d/starrocks_fe.d/conf.yaml**。

    重要的配置项示例：

    | **配置项** | **示例** | **描述** |
    | -------------------------------------- | ------------ | ------------------------------------------------------------ |
    | fe_metric_url | `http://localhost:8030/metrics` | 用于访问 StarRocks FE 指标的 URL。 |
    | metrics | `- starrocks_fe_*` | 需要监控的 FE 指标。 您可以使用通配符`*`来匹配配置项。 |

## 安装配置 BE 集成套件

1. 使用源代码为 BE 安装 Datadog 集成套件。

    ```sh
    /opt/datadog-agent/embedded/bin/pip install contrib/datadog-connector/starrocks_be
    ```

2. 创建 BE 集成配置文件 **/etc/datadog-agent/conf.d/starrocks_be.d/conf.yaml**。

    ```sh
    sudo mkdir -p /etc/datadog-agent/conf.d/starrocks_be.d
    sudo cp contrib/datadog-connector/starrocks_be/datadog_checks/starrocks_be/data/conf.yaml.example /etc/datadog-agent/conf.d/starrocks_be.d/conf.yaml
    ```

3. 修改 BE 集成配置文件 **/etc/datadog-agent/conf.d/starrocks_be.d/conf.yaml**。

    重要的配置项示例：

    | **配置项** | **示例** | **描述** |
    | -------------------------------------- | ------------ | ------------------------------------------------------------ |
    | be_metric_url | `http://localhost:8040/metrics` | 用于访问 StarRocks BE 指标的 URL。 |
    | metrics | `- starrocks_be_*` | 需要监控的 BE 指标。 您可以使用通配符`*`来匹配配置项。 |

## 重启 Datadog Agent

重启 Datadog Agent 以使配置生效。

```sh
sudo systemctl stop datadog-agent
sudo systemctl start datadog-agent
```

## 验证集成

有关验证集成的说明，请参阅 [Datadog Application](https://docs.datadoghq.com/getting_started/application/)。

## 卸载集成套件

您可以在不再需要时卸载集成套件。

- 运行以下命令卸载 FE 集成工具包：

  ```sh
  /opt/datadog-agent/embedded/bin/pip uninstall datadog-starrocks-fe
  ```

- 运行以下命令卸载 BE 集成工具包：

  ```sh
  /opt/datadog-agent/embedded/bin/pip uninstall datadog-starrocks-be
  ```
