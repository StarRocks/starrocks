# Datadog Support

[Datadog](https://www.datadoghq.com/) is a modern monitoring & security platform.

We developed [Integrations](https://docs.datadoghq.com/integrations/) for StarRocks: 
- [StarRocks BE Integrations](https://github.com/StarRocks/starrocks/tree/main/contrib/datadog-connector/starrocks_be)
- [StarRocks FE Integrations](https://github.com/StarRocks/starrocks/tree/main/contrib/datadog-connector/starrocks_fe)

## Environment

- [Datadog Agent](https://docs.datadoghq.com/getting_started/agent/)
- Python
Notice: Use `Datadog Agent`'s python, eg: 
```
/opt/datadog-agent/embedded/bin/python | pip)
```

## Installation

Since `Integrations` does not contribute to `Datadog`, it needs to be installed from the source code.

### Install StarRocks-FE
Install from Source Code([StarRocks FE Integrations](https://github.com/StarRocks/starrocks/tree/main/contrib/datadog-connector/starrocks_fe))
```
/opt/datadog-agent/embedded/bin/pip install .
```
Uninstall
```
/opt/datadog-agent/embedded/bin/pip uninstall datadog-starrocks-fe
```

### Install StarRocks-BE
Install from Source Code([StarRocks BE Integrations](https://github.com/StarRocks/starrocks/tree/main/contrib/datadog-connector/starrocks_be))
```
/opt/datadog-agent/embedded/bin/pip install .
```
Uninstall
```
/opt/datadog-agent/embedded/bin/pip uninstall datadog-starrocks-be
```

## Usage

### Configuration

#### Configuration FE
For FE, copy [conf.yaml.example](https://github.com/StarRocks/starrocks/blob/main/contrib/datadog-connector/starrocks_fe/datadog_checks/starrocks_fe/data/conf.yaml.example) to `/etc/datadog-agent/conf.d/starrocks_fe.d/conf.yaml`.

Explanation of some important config:

| **Config** | **Example** | **Description** |
  | -------------------------------------- | ------------ | ------------------------------------------------------------ |
   | fe_metric_url | http://localhost:8030/metrics | The URL where StarRocks FE metrics. |
   | metrics | - starrocks_fe_* | Indicators to be monitored, supporting wildcards `*` |

#### Configuration BE
For BE, copy [conf.yaml.example](https://github.com/StarRocks/starrocks/blob/main/contrib/datadog-connector/starrocks_be/datadog_checks/starrocks_be/data/conf.yaml.example) to `/etc/datadog-agent/conf.d/starrocks_be.d/conf.yaml`.

Explanation of some important config:

| **Config** | **Example** | **Description** |
  | -------------------------------------- | ------------ | ------------------------------------------------------------ |
   | be_metric_url | http://localhost:8040/metrics | The URL where StarRocks BE metrics. |
   | metrics | - starrocks_be_* | Indicators to be monitored, supporting wildcards `*` |

### Restart Datadog Agent
Reference [Restart the Agent](https://docs.datadoghq.com/agent/guide/agent-commands/#start-stop-and-restart-the-agent)

### Verification
Look [Datadog Application](https://docs.datadoghq.com/getting_started/application/)
