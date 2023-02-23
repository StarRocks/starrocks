# 支持 Datadog

[Datadog](https://www.datadoghq.com/) 是一个现代监控&安全管理平台。

我们为 StarRocks 开发对应的 [Integrations](https://docs.datadoghq.com/integrations/):

- [StarRocks BE Integrations](https://github.com/StarRocks/starrocks/tree/main/contrib/datadog-connector/starrocks_be)
- [StarRocks FE Integrations](https://github.com/StarRocks/starrocks/tree/main/contrib/datadog-connector/starrocks_fe)

## 环境准备

- [Datadog Agent](https://docs.datadoghq.com/getting_started/agent/)
- Python

注意: 使用 `Datadog Agent`的 python 环境，比如：

```shell
/opt/datadog-agent/embedded/bin/python | pip)
```

## 安装

由于该 `Integrations` 还未贡献给 `Datadog`，需要使用源码进行安装。

### 安装 StarRocks-FE

通过源码安装 [StarRocks FE Integrations](https://github.com/StarRocks/starrocks/tree/main/contrib/datadog-connector/starrocks_fe)

```shell
/opt/datadog-agent/embedded/bin/pip install .
```

卸载

```shell
/opt/datadog-agent/embedded/bin/pip uninstall datadog-starrocks-fe
```

### 安装 StarRocks-BE

通过源码安装 [StarRocks BE Integrations](https://github.com/StarRocks/starrocks/tree/main/contrib/datadog-connector/starrocks_be)

```shell
/opt/datadog-agent/embedded/bin/pip install .
```

卸载

```shell
/opt/datadog-agent/embedded/bin/pip uninstall datadog-starrocks-be
```

## 使用

### 配置

#### 配置 FE

对于 FE，拷贝 [conf.yaml.example](https://github.com/StarRocks/starrocks/blob/main/contrib/datadog-connector/starrocks_fe/datadog_checks/starrocks_fe/data/conf.yaml.example) 到 `/etc/datadog-agent/conf.d/starrocks_fe.d/conf.yaml` 来进行配置。

一些重要参数的解释:

| **配置** | **例子** | **描述** |
| -------------------------------------- | ------------ | ---------------------------------------------- |
| fe_metric_url | `http://localhost:8030/metrics` | 用于获取 StarRocks FE 配置的 URL。|
| metrics | - starrocks_fe_* | 需要监控的指标，支持通配符`*`。|

#### 配置 BE

对于BE，拷贝 [conf.yaml.example](https://github.com/StarRocks/starrocks/blob/main/contrib/datadog-connector/starrocks_be/datadog_checks/starrocks_be/data/conf.yaml.example) 到 `/etc/datadog-agent/conf.d/starrocks_be.d/conf.yaml` 来进行配置。

一些重要参数的解释:

| **配置** | **例子** | **描述** |
  | -------------------------------------- | ------------ | ------------------------------------------------------------ |
   | be_metric_url | `http://localhost:8040/metrics` | 用于获取 StarRocks BE 配置的 URL。 |
   | metrics | - starrocks_be_* | 需要监控的指标，支持通配符`*`。 |

### 重启 Datadog Agent

参考 [Restart the Agent](https://docs.datadoghq.com/agent/guide/agent-commands/#start-stop-and-restart-the-agent)。

### 验证

查看 [Datadog Application](https://docs.datadoghq.com/getting_started/application/)。
