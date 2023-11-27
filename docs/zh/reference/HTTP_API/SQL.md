---
displayed_sidebar: "Chinese"
---

# HTTP SQL API

## 功能

StarRocks 3.2.0 版本提供了 HTTP SQL API，方便用户通过 HTTP 协议使用 StarRocks 的查询功能，当前支持 SELECT、SHOW、EXPLAIN、KILL 语句。

使用 curl 命令的一个语法示例：

```shell
curl -X POST 'http://<fe_ip>:<fe_http_port>/api/v1/catalogs/<catalog_name>/databases/<database_name>/sql' \
   -u '<username>:<password>'  -d '{"query": "<sql_query>;", "sessionVariables":{"<var_name>":<var_value>}}' \
   --header "Content-Type: application/json"
```

## 请求报文

### Request line

```shell
POST 'http://<fe_ip>:<fe_http_port>/api/v1/catalogs/<catalog_name>/databases/<database_name>/sql'
```

| Field                    | Description                                                  |
| ------------------------ | :----------------------------------------------------------- |
|  fe_ip                   | FE 节点 IP。                                                  |
|  fe_http_port            | FE 节点 HTTP Port。                                           |
|  catalog_name            | 数据目录名称，当前仅支持 StarRocks 内表查询，即 `<catalog_name>` 仅支持为 `default_catalog`。|
|  database_name           | 数据库名称。如果未指定数据库名称，那么在 SQL query 语句中出现的表名前面需要加上 database 名，比如 `database_name.table_name`。 |

- 指定 catalog, 跨 database 查询。SQL 语句中出现的表前面需要加上 database 名。

   ```shell
   POST /api/v1/catalogs/<catalog_name>/sql
   ```

- 指定 catalog 和 database 查询。

   ```shell
   POST /api/v1/catalogs/<catalog_name>/databases/<database_name>/sql
   ```

### 认证方式

```SQL
Authorization: Basic <credentials>
```

使用 Basic authentication 进行认证，即 `credentials` 里填写用户名和密码 (`-u '<username>:<password>'`)。注意如果账号没有设置密码，只需传入 `<username>:`，密码留空。举例，如果 root 账号没有设置密码，则写作 `-u 'root:'`。

### Request body

```shell
-d '{"query": "<sql_query>;", "sessionVariables":{"<var_name>":<var_value>}}'
```

| Field                    | Description                                                  |
| ------------------------ | :----------------------------------------------------------- |
| query                    | SQL 语句，STRING 格式。当前支持 SELECT、SHOW、EXPLAIN、KILL 语句。一次 HTTP 请求只允许执行一条 SQL。 |
| sessionVariables         | 指定的 session 变量，JSON 格式。可选，默认为空。设置的 session 变量在同一连接中始终有效，连接断开后 session 变量失效。 |

### Request header

```shell
--header "Content-Type: application/json"
```

表示发送的请求体是 JSON 字符串。

## 响应报文

### Status code

- 200：HTTP 请求成功，且在发送数据给客户端之前服务器未出现异常。
- 4xx：HTTP 请求错误，表示客户端出错。
- `500 Internal Server Error`：HTTP 请求成功，但是在发送数据给客户端之前出现异常。
- 503：HTTP 请求成功，但是 FE 当前无法提供服务。

### Response header

content-type 表示 response body 的格式。这里使用 Newline delimited JSON 格式，即 response body 由若干 JSON object 组成，JSON object 之间以 `\n` 隔开。

|                      | Description                                                  |
| -------------------- | :----------------------------------------------------------- |
| content-type         | 格式为 Newline delimited JSON。默认为 "application/x-ndjson charset=UTF-8"。 |
| X-StarRocks-Query-Id | Query ID。                                                          |

### Response body

#### 在结果发出之前执行失败

客户端的请求出错或者服务端在将数据发送给客户端之前出现异常，此时 response body 格式如下。其中 `msg` 为对应的错误异常信息。

```json
{
   "status":"FAILED",
   "msg":"xxx"
}
```

#### 在结果发出部分后执行失败

此时结果已经部分发出，HTTP 状态码为 200，因此选择不再继续发送数据，并关闭连接，记录错误日志。

#### 执行成功

返回结果中每行是一个 JSON object，JSON object 之间以换行符 `\n` 隔开，从而便于客户端解析。

- 对于 SELECT 语句，会返回以下 JSON object。

| Object       | Description                                                  |
| ------------ | :----------------------------------------------------------- |
| `connectionId` | 该连接对应的 ID，可通过 KILL `<connectionId>` 来取消某个执行时间过长的 query。 |
| `meta`        | 用来描述每个列。Key 为 meta, Value 为一个 JSON array。array 中的每个 object 表示一个 column。 |
| `data`         | 代表一行数据。Key 为 data, Value 为一个 JSON  array，array 中包含一条数据。 |
| `statistics`   | 本次执行结果的统计信息。                                       |

- 对于 SHOW，会返回 `meta`，`data`，`statistics`。
- 对于 EXPLAIN，会返回一个 `explain` 对象，展示这条查询详细的执行计划。

样例：这里换行以 `\n` 表示。发送时 StarRocks 使用 HTTP chunked 方式传输数据，FE 每获取一批数据，就将该批数据流式转发给客户端。客户端可以按行解析 StarRocks 发送的数据，而不需要缓存现有结果直到所有数据发送完毕再进行解析。这可以降低客户端的内存消耗。

```json
{"connectionId": 7}\n
{"meta": [
    {
      "name": "stock_symbol",
      "type": "varchar"
    },
    {
      "name": "closing_price",
      "type": "decimal64(8, 2)"
    },
    {
      "name": "closing_date",
      "type": "datetime"
    }
  ]}\n
{"data": ["JDR", 12.86, "2014-10-02 00:00:00"]}\n
{"data": ["JDR",14.8, "2014-10-10 00:00:00"]}\n
...
{"statistics": {"scanRows": 0,"scanBytes": 0,"returnRows": 9}}
```

## 接口调用示例

### 发起查询

```shell
curl -X POST 'http://127.0.0.1:8030/api/v1/catalogs/default_catalog/databases/test/sql' -u 'root:' -d '{"query": "select * from agg;"}' --header "Content-Type: application/json"
```

返回结果：

```json
{"connectionId":49}
{"meta":[{"name":"no","type":"int(11)"},{"name":"k","type":"decimal64(10, 2)"},{"name":"v","type":"decimal64(10, 2)"}]}
{"data":[1,"10.00",null]}
{"data":[2,"10.00","11.00"]}
{"data":[2,"20.00","22.00"]}
{"data":[2,"25.00",null]}
{"data":[2,"30.00","35.00"]}
{"statistics":{"scanRows":0,"scanBytes":0,"returnRows":5}}
```

### 取消查询

想要取消一个耗时过长的查询时，可以直接断开连接。当检测到连接已断开，StarRocks 会自动取消该查询。

此外，也可以通过发送 KILL `connectionId` 的方式取消该查询。举例：

```shell
curl -X POST 'http://127.0.0.1:8030/api/v1/catalogs/default_catalog/databases/test/sql' -u 'root:' -d '{"query": "kill 17;"}' --header "Content-Type: application/json"
```

`connectionId` 除了在 Response body 中会返回，也可以通过 SHOW PROCESSLIST 命令获得。举例：

```shell
curl -X POST 'http://127.0.0.1:8030/api/v1/catalogs/default_catalog/databases/test/sql' -u 'root:' -d '{"query": "show processlist;"}' --header "Content-Type: application/json"
```

### 发起查询的同时设置 session 变量

```shell
curl -X POST 'http://127.0.0.1:8030/api/v1/catalogs/default_catalog/databases/test/sql' -u 'root:'  -d '{"query": "SHOW VARIABLES;", "sessionVariables":{"broadcast_row_limit":14000000}}'  --header "Content-Type: application/json"
```
