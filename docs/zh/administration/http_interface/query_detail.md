---
displayed_sidebar: docs
---

# Query detail API

**query detail** API 用于查询 FE 内存中缓存的查询执行详情。仅当 FE 配置项 `enable_collect_query_detail_info`
开启时，系统才会收集并返回 query detail 记录。

## 接口

- `GET /api/query_detail` (v1)
- `GET /api/v2/query_detail` (v2，支持 `is_request_all_frontend`，返回封装结构)

## 请求参数

| 参数 | 必填 | 默认值 | 说明 |
| --- | --- | --- | --- |
| `event_time` | 是 | - | 过滤下限，仅返回 `eventTime` 大于该值的记录。可传 `0` 获取当前缓存的全部记录。 |
| `user` | 否 | - | 按 `user` 字段过滤，大小写不敏感。 |
| `is_request_all_frontend` | 否（仅 v2） | `false` | 为 `true` 时会向其他存活 FE 拉取并合并结果。 |

## 响应

- v1 返回 QueryDetail 数组。
- v2 返回 `{ "code": "0", "message": "OK", "result": [ ... ] }`，其中 `result` 为 QueryDetail 列表。

## 认证与权限

该接口需要 HTTP Basic 认证。除登录认证外没有额外权限校验。任何已认证用户都可以访问该接口，并可查看所有缓存的
query detail（除非通过 `user` 参数进行过滤）。

### QueryDetail 字段说明

| 字段 | 类型 | 说明 |
| --- | --- | --- |
| `queryId` | string | 查询 ID。 |
| `eventTime` | long | 内部过滤时间戳。由墙钟时间派生的单调纳秒时间戳。 |
| `isQuery` | boolean | 是否为查询语句。 |
| `remoteIP` | string | 客户端 IP，或 `System`。 |
| `connId` | int | 连接 ID。 |
| `startTime` | long | 查询开始时间（毫秒时间戳）。 |
| `endTime` | long | 查询结束时间（毫秒时间戳），未结束为 `-1`。 |
| `latency` | long | 查询耗时（毫秒），未结束为 `-1`。 |
| `pendingTime` | long | 排队等待时间（毫秒）。 |
| `netTime` | long | 净执行时间（毫秒）。 |
| `netComputeTime` | long | 净计算时间（毫秒）。 |
| `state` | string | `RUNNING`/`FINISHED`/`FAILED`/`CANCELLED`。 |
| `database` | string | 当前数据库。 |
| `sql` | string | SQL 文本（可能被脱敏）。 |
| `user` | string | 登录用户（qualified user）。 |
| `impersonatedUser` | string | `EXECUTE AS` 的目标用户，未执行时为 `null`。 |
| `errorMessage` | string | 失败时的错误信息。 |
| `explain` | string | 执行计划（由 `query_detail_explain_level` 控制级别）。 |
| `profile` | string | Profile 内容（如有）。 |
| `resourceGroupName` | string | 资源组名称。 |
| `scanRows` | long | 扫描行数。 |
| `scanBytes` | long | 扫描字节数。 |
| `returnRows` | long | 返回行数。 |
| `cpuCostNs` | long | CPU 开销（纳秒）。 |
| `memCostBytes` | long | 内存开销（字节）。 |
| `spillBytes` | long | 溢写字节数。 |
| `cacheMissRatio` | float | 缓存未命中率（百分比 0-100）。 |
| `warehouse` | string | Warehouse 名称。 |
| `digest` | string | SQL 摘要。 |
| `catalog` | string | Catalog 名称。 |
| `command` | string | MySQL 命令名。 |
| `preparedStmtId` | string | 预编译语句 ID。 |
| `queryFeMemory` | long | FE 端为该查询分配的内存（字节）。 |
| `querySource` | string | 查询来源：`EXTERNAL`/`INTERNAL`/`MV`/`TASK`。 |

## 示例

### v1

```bash
curl -u root: "http://fe_host:fe_http_port/api/query_detail?event_time=0"
```

### v2

```bash
curl -u root: "http://fe_host:fe_http_port/api/v2/query_detail?event_time=0&is_request_all_frontend=true"
```
