---
displayed_sidebar: docs
---

# Query detail API

**Query detail** API 用于查询 FE 内存中缓存的查询执行详情。

:::note
仅当 FE 配置项 `enable_collect_query_detail_info` 为 `true` 时，系统才会收集并返回 Query detail 记录。
:::

## 接口

- `GET /api/query_detail` (v1)
- `GET /api/v2/query_detail` (v2，支持 `is_request_all_frontend`，返回封装结构)

## 请求参数

| 参数名                     | 是否必选    | 默认值  | 说明                                                         |
| ------------------------- | ----------- | ------- | ------------------------------------------------------------ |
| `event_time`              | 是          | -       | 下界过滤条件，仅返回 `eventTime` 大于该值的记录。传入 `0` 表示返回所有缓存记录。 |
| `user`                    | 否          | -       | 按 `user` 字段过滤，大小写不敏感。                           |
| `is_request_all_frontend` | 否（仅 v2） | `false` | 为 `true` 时，当前 FE 会向其他存活的 FE 查询并合并结果。     |

## 响应

- v1 返回 `QueryDetail` 数组。
- v2 返回 `{ "code": "0", "message": "OK", "result": [ ... ] }`，其中 `result` 为 `QueryDetail` 列表。

## 认证与权限

该 API 需要使用 **HTTP Basic 认证**。

接口不会进行额外的权限校验，只要登录成功即可访问。任何已认证用户都可以查看所有缓存的查询详情；如果指定了 `user` 参数，则只返回对应用户的记录。

## QueryDetail 字段说明

| 字段名              | 类型    | 说明                                                         |
| ------------------- | ------- | ------------------------------------------------------------ |
| `queryId`           | string  | 查询 ID。                                                    |
| `eventTime`         | long    | 用于过滤的内部时间戳，由系统时间生成的单调递增纳秒时间戳。   |
| `isQuery`           | boolean | 是否为查询语句。                                             |
| `remoteIP`          | string  | 客户端 IP 地址，或 `System`。                                |
| `connId`            | int     | 连接 ID。                                                    |
| `startTime`         | long    | 查询开始时间（毫秒，Unix 时间戳）。                          |
| `endTime`           | long    | 查询结束时间（毫秒，Unix 时间戳）；未完成时为 `-1`。         |
| `latency`           | long    | 查询延迟（毫秒）；未完成时为 `-1`。                          |
| `pendingTime`       | long    | Pending 阶段耗时（毫秒）。                                   |
| `netTime`           | long    | 净执行时间（毫秒）。                                         |
| `netComputeTime`    | long    | 净计算时间（毫秒）。                                         |
| `state`             | string  | 查询状态：`RUNNING`、`FINISHED`、`FAILED`、`CANCELLED`。     |
| `database`          | string  | 当前数据库。                                                 |
| `sql`               | string  | SQL 语句文本（可能根据配置进行脱敏）。                       |
| `user`              | string  | 登录用户（全限定用户名）。                                   |
| `impersonatedUser`  | string  | `EXECUTE AS` 的目标用户；未使用时为 `null`。                 |
| `errorMessage`      | string  | 查询失败时的错误信息。                                       |
| `explain`           | string  | Explain 执行计划（级别由 `query_detail_explain_level` 控制）。 |
| `profile`           | string  | 若收集则返回 Profile 信息。                                  |
| `resourceGroupName` | string  | 资源组名称。                                                 |
| `scanRows`          | long    | 扫描的行数。                                                 |
| `scanBytes`         | long    | 扫描的数据量（字节）。                                       |
| `returnRows`        | long    | 返回的行数。                                                 |
| `cpuCostNs`         | long    | CPU 消耗时间（纳秒）。                                       |
| `memCostBytes`      | long    | 内存消耗（字节）。                                           |
| `spillBytes`        | long    | 落盘数据量（字节）。                                         |
| `cacheMissRatio`    | float   | 缓存未命中率（百分比，0–100）。                              |
| `warehouse`         | string  | Warehouse 名称。                                             |
| `digest`            | string  | SQL Digest。                                                 |
| `catalog`           | string  | Catalog 名称。                                               |
| `command`           | string  | MySQL 命令名称。                                             |
| `preparedStmtId`    | string  | 预编译语句 ID。                                              |
| `queryFeMemory`     | long    | 查询在 FE 上分配的内存（字节）。                             |
| `querySource`       | string  | 查询来源：`EXTERNAL`、`INTERNAL`、`MV` 或 `TASK`。           |

## 示例

### v1

```bash
curl -u root: "http://<fe_host>:<fe_http_port>/api/query_detail?event_time=0"
```

### v2

```bash
curl -u root: "http://<fe_host>:<fe_http_port>/api/v2/query_detail?event_time=0&is_request_all_frontend=true"
```
