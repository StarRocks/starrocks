---
displayed_sidebar: "English"
description: Monitor with Datadog
sidebar_label: Datadog
---

# Monitor with Datadog

This topic describes how to integrate your StarRocks cluster with [Datadog](https://www.datadoghq.com/), a monitoring and security platform.

## Prerequisites

Before getting started, you must have the following software installed on your instances:

- [Datadog Agent](https://docs.datadoghq.com/getting_started/agent/)
- Python

> **NOTE**
>
> When you install Datadog Agent for the first time, Python is also installed as a dependency. We recommend you use this Python in the following steps.

## Prepare StarRocks source code

Since Datadog does not provide the integration kit for StarRocks yet, you need to integrate them using the source code.

1. Launch a terminal, navigate to a local directory to which you have both read and write access, and run the following command to create a dedicated directory for StarRocks source code.

    ```shell
    mkdir -p starrocks
    ```

2. Download the StarRocks source code package using the following command or on [GitHub](https://github.com/StarRocks/starrocks/tags) to the directory you created.

    ```shell
    cd starrocks
    # Replace <starrocks_ver> with the actual version of StarRocks, for example, "2.5.2".
    wget https://github.com/StarRocks/starrocks/archive/refs/tags/<starrocks_ver>.tar.gz
    ```

3. Extract the files in the package.

    ```shell
    # Replace <starrocks_ver> with the actual version of StarRocks, for example, "2.5.2".
    tar -xzvf <starrocks_ver>.tar.gz --strip-components 1
    ```

## Install and configure FE integration kit

1. Install Datadog integration kit for FE using source code.

    ```shell
    /opt/datadog-agent/embedded/bin/pip install contrib/datadog-connector/starrocks_fe
    ```

2. Create the FE integration configuration file **/etc/datadog-agent/conf.d/starrocks_fe.d/conf.yaml**.

    ```shell
    sudo mkdir -p /etc/datadog-agent/conf.d/starrocks_fe.d
    sudo cp contrib/datadog-connector/starrocks_fe/datadog_checks/starrocks_fe/data/conf.yaml.example /etc/datadog-agent/conf.d/starrocks_fe.d/conf.yaml
    ```

3. Modify the FE integration configuration file **/etc/datadog-agent/conf.d/starrocks_fe.d/conf.yaml**.

    Examples of some important configuration items:

    | **Config** | **Example** | **Description** |
    | -------------------------------------- | ------------ | ------------------------------------------------------------ |
    | fe_metric_url | `http://localhost:8030/metrics` | The URL used to access the StarRocks FE metrics. |
    | metrics | `- starrocks_fe_*` | Metrics to be monitored on FE. You can use wildcards `*` to match the configuration items. |

## Install and configure BE integration kit

1. Install Datadog integration kit for BE using source code.

    ```shell
    /opt/datadog-agent/embedded/bin/pip install contrib/datadog-connector/starrocks_be
    ```

2. Create the BE integration configuration file **/etc/datadog-agent/conf.d/starrocks_be.d/conf.yaml**.

    ```shell
    sudo mkdir -p /etc/datadog-agent/conf.d/starrocks_be.d
    sudo cp contrib/datadog-connector/starrocks_be/datadog_checks/starrocks_be/data/conf.yaml.example /etc/datadog-agent/conf.d/starrocks_be.d/conf.yaml
    ```

3. Modify the BE integration configuration file **/etc/datadog-agent/conf.d/starrocks_be.d/conf.yaml**.

    Examples of some important configuration items:

    | **Config** | **Example** | **Description** |
    | -------------------------------------- | ------------ | ------------------------------------------------------------ |
    | be_metric_url | `http://localhost:8040/metrics` | The URL used to access the StarRocks BE metrics. |
    | metrics | `- starrocks_be_*` | Metrics to be monitored on BE. You can use wildcards `*` to match the configuration items. |

## Restart Datadog Agent

Restart Datadog Agent to allow the configuration to take effect.

```shell
sudo systemctl stop datadog-agent
sudo systemctl start datadog-agent
```

## Verify integration

For instructions to verify the integration, see [Datadog Application](https://docs.datadoghq.com/getting_started/application/).

## Uninstall integration kits

You can uninstall the integration kits when you no longer need them.

- To uninstall FE integration kit, run the following command:

  ```shell
  /opt/datadog-agent/embedded/bin/pip uninstall datadog-starrocks-fe
  ```

- To uninstall BE integration kit, run the following command:

  ```shell
  /opt/datadog-agent/embedded/bin/pip uninstall datadog-starrocks-be
  ```
