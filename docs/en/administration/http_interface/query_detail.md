---
displayed_sidebar: docs
---

# Query detail API

The **query detail** API returns recent query execution details that are cached in FE memory.

:::note
Query detail records are collected only when the FE configuration `enable_collect_query_detail_info` is set to `true`.
:::

## Endpoints

- `GET /api/query_detail` (v1)
- `GET /api/v2/query_detail` (v2, supports `is_request_all_frontend` and returns a wrapped result)

## Parameters

| Name                      | Required     | Default | Description                                                                   |
| ------------------------- | ------------ | ------- | ----------------------------------------------------------------------------- |
| `event_time`              | Yes          | -       | Lower bound filter. Returns items whose `eventTime` is greater than this value. You can pass `0` to get all cached items. |
| `user`                    | No           | -       | Filter by the `user` field. Case-insensitive match.                           |
| `is_request_all_frontend` | No (v2 only) | `false` | When `true`, the current FE queries other alive FEs and merges their results. |

## Response

- v1 returns a JSON array of `QueryDetail` objects.
- v2 returns `{ "code": "0", "message": "OK", "result": [ ... ] }` where `result` is a `QueryDetail` list.

## Authentication and authorization

This API requires HTTP Basic authentication. There is no additional privilege check beyond a successful login. Any authenticated user can access the endpoint and can view all cached query details unless a `user` filter is applied.

## QueryDetail fields

| Field               | Type    | Description                                                                                         |
| ------------------- | ------- | --------------------------------------------------------------------------------------------------- |
| `queryId`           | string  | Query ID.                                                                                           |
| `eventTime`         | long    | Internal timestamp used for filtering. Monotonic nanosecond timestamp derived from wall-clock time. |
| `isQuery`           | boolean | Whether the statement is a query.                                                                   |
| `remoteIP`          | string  | Client IP address or `System`.                                                                      |
| `connId`            | int     | Connection ID.                                                                                      |
| `startTime`         | long    | Query start time in milliseconds since epoch.                                                       |
| `endTime`           | long    | Query end time in milliseconds since epoch. `-1` if not finished.                                   |
| `latency`           | long    | Query latency in milliseconds. `-1` if not finished.                                                |
| `pendingTime`       | long    | Pending time in milliseconds.                                                                       |
| `netTime`           | long    | Net execution time in milliseconds.                                                                 |
| `netComputeTime`    | long    | Net compute time in milliseconds.                                                                   |
| `state`             | string  | One of `RUNNING`, `FINISHED`, `FAILED`, `CANCELLED`.                                                |
| `database`          | string  | Current database.                                                                                   |
| `sql`               | string  | SQL text (may be desensitized if configured).                                                       |
| `user`              | string  | Login user (qualified user).                                                                        |
| `impersonatedUser`  | string  | Target user of `EXECUTE AS`. `null` if not executing as another user.                               |
| `errorMessage`      | string  | Error message when failed.                                                                          |
| `explain`           | string  | Explain plan (level controlled by `query_detail_explain_level`).                                    |
| `profile`           | string  | Profile text if collected.                                                                          |
| `resourceGroupName` | string  | Resource group name.                                                                                |
| `scanRows`          | long    | Scanned rows.                                                                                       |
| `scanBytes`         | long    | Scanned bytes.                                                                                      |
| `returnRows`        | long    | Returned rows.                                                                                      |
| `cpuCostNs`         | long    | CPU cost in nanoseconds.                                                                            |
| `memCostBytes`      | long    | Memory cost in bytes.                                                                               |
| `spillBytes`        | long    | Spill bytes.                                                                                        |
| `cacheMissRatio`    | float   | Cache miss ratio in percent (0-100).                                                                |
| `warehouse`         | string  | Warehouse name.                                                                                     |
| `digest`            | string  | SQL digest.                                                                                         |
| `catalog`           | string  | Catalog name.                                                                                       |
| `command`           | string  | MySQL command name.                                                                                 |
| `preparedStmtId`    | string  | Prepared statement ID.                                                                              |
| `queryFeMemory`     | long    | FE memory allocated by the query, in bytes.                                                         |
| `querySource`       | string  | Query source: `EXTERNAL`, `INTERNAL`, `MV`, or `TASK`.                                              |

## Examples

### v1

```bash
curl -u root: "http://<fe_host>:<fe_http_port>/api/query_detail?event_time=0"
```

### v2

```bash
curl -u root: "http://<fe_host>:<fe_http_port>/api/v2/query_detail?event_time=0&is_request_all_frontend=true"
```
