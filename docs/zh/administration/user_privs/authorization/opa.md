---
displayed_sidebar: docs
sidebar_position: 45
---

# 使用 Open Policy Agent 管理权限

[Open Policy Agent](https://www.openpolicyagent.org/) (OPA) 是一个策略引擎，可用于集中管理多个系统的授权决策。StarRocks 可以将权限检查委托给 OPA，同时继续使用现有的认证、Group Provider、语义分析、行访问策略和列掩码能力。

StarRocks 会向 OPA 发送 StarRocks 原生 JSON 请求。OPA 的返回结果决定操作是否被允许，也可以返回用于行过滤和列掩码的 SQL 表达式。

## 功能

- 对 System、Catalog、Database、Table、Column、View、Materialized View、Function、Resource、Resource Group、Storage Volume、Pipe 和 Warehouse 等 StarRocks 对象进行授权。
- 通过返回 SQL 过滤表达式实现行访问策略。
- 通过返回 SQL 掩码表达式实现列掩码策略。
- 通过批量列掩码减少引用多列查询中的 OPA 请求次数。

OPA 授权默认失败关闭。如果 OPA 服务返回 `false`、省略 `result`、返回非法 JSON、返回非 2xx HTTP 状态码或请求超时，StarRocks 都会拒绝该操作。

`GRANT`、`REVOKE`、用户管理和角色管理等权限管理操作仍使用 StarRocks 原生权限检查。OPA 控制对象授权、行过滤和列掩码，但不会替换原生权限管理模型。

## 配置 OPA 权限控制

在每个 FE 节点的 FE 配置文件 **fe.conf** 中添加以下配置项：

```properties
access_control = opa
opa_policy_url = http://opa.example.com:8181/v1/data/starrocks/allow

# 可选策略端点。
opa_row_filters_url = http://opa.example.com:8181/v1/data/starrocks/row_filters
opa_column_masking_url = http://opa.example.com:8181/v1/data/starrocks/column_mask
opa_batch_column_masking_url = http://opa.example.com:8181/v1/data/starrocks/batch_column_masks
```

修改 `access_control` 或任何 `opa_*` 配置项后，需要重启所有 FE 节点。

您也可以通过 Catalog 属性 `"catalog.access.control" = "opa"` 为 External Catalog 启用 OPA。如果未设置该属性，Catalog 会使用全局 `access_control` 的值。

```sql
CREATE EXTERNAL CATALOG hive_catalog
PROPERTIES (
    "type" = "hive",
    "hive.metastore.type" = "hive",
    "hive.metastore.uris" = "thrift://127.0.0.1:9083",
    "catalog.access.control" = "opa"
);
```

## 授权请求

StarRocks 按 OPA Data API 格式向 `opa_policy_url` 发送 POST 请求：

```json
{
  "input": {
    "context": {
      "user": "alice",
      "groups": ["finance"],
      "host": "%",
      "queryId": "5e4b8e2d-2c80-4c49-94c2-1d8d7c11f8cc",
      "catalog": "default_catalog",
      "database": "sales"
    },
    "action": {
      "operation": "check",
      "privilege": "SELECT",
      "objectType": "COLUMN",
      "resource": {
        "catalog": "default_catalog",
        "database": "sales",
        "table": "orders",
        "column": "amount"
      }
    }
  }
}
```

返回 `{"result": true}` 表示允许请求。其他任何结果都会拒绝请求。

对于 `SELECT` 查询，StarRocks 会在列裁剪后逐个扫描列进行授权。如需在 OPA 中实现表级 `SELECT` 语义，可以编写 `COLUMN` 策略并忽略 `input.action.resource.column`。

Rego 策略示例：

```rego
package starrocks

default allow := false

allow if {
    input.action.privilege == "SELECT"
    input.action.objectType == "COLUMN"
    input.action.resource.database == "sales"
    input.action.resource.table == "orders"
    input.context.user == "alice"
}
```

## 行过滤

设置 `opa_row_filters_url` 后，StarRocks 会在查询改写前向 OPA 请求行访问表达式：

```json
{
  "result": [
    {"expression": "region = 'EMEA'"},
    {"expression": "tenant_id = 100"}
  ]
}
```

StarRocks 会使用 `AND` 连接多个返回表达式。表达式必须是合法的 StarRocks SQL 表达式。

## 列掩码

设置 `opa_column_masking_url` 后，StarRocks 会逐列向 OPA 请求列掩码：

```json
{
  "result": {
    "expression": "NULL"
  }
}
```

设置 `opa_batch_column_masking_url` 后，StarRocks 会优先使用批量列掩码请求，而不是逐列请求：

```json
{
  "result": [
    {"column": "phone", "expression": "NULL"},
    {"column": "email", "expression": "concat('***@', split_part(email, '@', 2))"}
  ]
}
```

表达式必须是合法的 StarRocks SQL 表达式。非法的行过滤或列掩码表达式会导致查询失败。

## FE 配置项

所有 OPA 相关 FE 配置项，参见 [FE 配置 - 用户、角色和权限](../../management/FE_parameters/user_query_loading.md#用户角色和权限)。
