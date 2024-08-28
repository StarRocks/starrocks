---
displayed_sidebar: docs
---

# HTTP SQL API

## Description

StarRocks v3.2.0 introduces the HTTP SQL API for users to perform various types of queries using HTTP. Currently, this API supports SELECT, SHOW, EXPLAIN, and KILL statements.

Syntax using the curl command:

```shell
curl -X POST 'http://<fe_ip>:<fe_http_port>/api/v1/catalogs/<catalog_name>/databases/<database_name>/sql' \
   -u '<username>:<password>'  -d '{"query": "<sql_query>;", "sessionVariables":{"<var_name>":<var_value>}}' \
   --header "Content-Type: application/json"
```

## Request message

### Request line

```shell
POST 'http://<fe_ip>:<fe_http_port>/api/v1/catalogs/<catalog_name>/databases/<database_name>/sql'
```

| Field                    | Description                                                  |
| ------------------------ | :----------------------------------------------------------- |
|  fe_ip                   | FE node IP address.                                                  |
|  fe_http_port            | FE HTTP port.                                           |
|  catalog_name            | The catalog name. In v3.2.0, you can use this API to query only StarRocks internal tables, which means `<catalog_name>` can only be set to `default_catalog`. Since v3.2.1, you can use this API to query tables in [external catalogs](../data_source/catalog/catalog_overview.md). |
|  database_name           | The database name. If no database name is specified in the request line and a table name is used in the SQL query, you must prefix the table name with its database name, for example, `database_name.table_name`. |

- Query data across databases in a specified catalog. If a table is used in the SQL query, you must prefix the table name with its database name.

   ```shell
   POST /api/v1/catalogs/<catalog_name>/sql
   ```

- Query data from a specified catalog and database.

   ```shell
   POST /api/v1/catalogs/<catalog_name>/databases/<database_name>/sql
   ```

### Authentication method

```shell
Authorization: Basic <credentials>
```

Basic authentication is used, that is, enter the username and password for `credentials` (`-u '<username>:<password>'`). If no password is set for the username, you can pass in only `<username>:` and leave the password empty. For example, if the root account is used, you can enter `-u 'root:'`.

### Request body

```shell
-d '{"query": "<sql_query>;", "sessionVariables":{"<var_name>":<var_value>}}'
```

| Field                    | Description                                                  |
| ------------------------ | :----------------------------------------------------------- |
| query                    | The SQL query, in STRING format. Only SELECT, SHOW, EXPLAIN, and KILL statements are supported. You can run only one SQL query for an HTTP request. |
| sessionVariables         | The [session variable](./System_variable.md) you want to set for the query, in JSON format. This field is optional. Default is empty. The session variable you set takes effect for the same connection and becomes ineffective when the connection is closed. |

### Request header

```shell
--header "Content-Type: application/json"
```

This header indicates that the request body is a JSON string.

## Response message

### Status code

- 200: HTTP request succeeded and the server is normal before data is sent to the client.
- 4xx: HTTP request error, which indicates a client error.
- `500 Internal Server Error`: HTTP request succeeded but the server encounters an error before data is sent to the client.
- 503: HTTP request succeeded but the FE cannot provide service.

### Response header

`content-type` indicates the format of the response body. Newline delimited JSON is used, which means the response body consists of multiple JSON objects that are separated by `\n`.

|                      | Description                                                  |
| -------------------- | :----------------------------------------------------------- |
| content-type         | The format is Newline delimited JSON, defaults to "application/x-ndjson charset=UTF-8". |
| X-StarRocks-Query-Id | Query ID.                                                          |

### Response body

#### Failed before the request is sent

Request fails on the client side or the server encounters an error before it returns data to the client. The response body is in the following format, where `msg` is the error information.

```json
{
   "status":"FAILED",
   "msg":"xxx"
}
```

#### Failed after the request is sent

Part of the result is returned and the HTTP status code is 200. Data sending is suspended, the connection is closed, and the error is logged.

#### Succeeded

Each row in the response message is a JSON object. JSON objects separated by `\n`.

- For a SELECT statement, the following JSON objects are returned.

| Object       | Description                                                  |
| ------------ | :----------------------------------------------------------- |
| `connectionId` | Connection ID. You can cancel a query that is pending for a long time by calling KILL `<connectionId>`. |
| `meta`        | Represents a column. The key is `meta` and the value is a JSON array, where each object in the array represents a column. |
| `data`         | The data row, where the key is `data` and the value is a JSON  array which contains a row of data. |
| `statistics`   | Statistical information of the query.                                       |

- For a SHOW statement, `meta`, `data`, and `statistics` are returned.
- For the EXPLAIN statement, an `explain` object is returned to show detailed execution plan of the query.

The following example uses `\n` as the separator. StarRocks transmits data using HTTP chunked mode. Each time the FE obtains a data chunk, it streams the data chunk to the client. The client can parse data by row, which eliminates the need for data caching and the need to wait for the entire data, reducing memory consumption for the client.

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

## Examples

### Run a SELECT query

- Query data from a StarRocks internal table (`catalog_name` is `default_catalog`).

  ```shell
  curl -X POST 'http://127.0.0.1:8030/api/v1/catalogs/default_catalog/databases/test/sql' -u 'root:' -d '{"query": "select * from agg;"}' --header "Content-Type: application/json"
  ```

  Result:

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

- Query data from an Iceberg table.

  ```shell
  curl -X POST 'http://172.26.93.145:8030/api/v1/catalogs/iceberg_catalog/databases/ywb/sql' -u 'root:' -d '{"query": "select * from iceberg_analyze;"}' --header "Content-Type: application/json"
  ```

  Result:

  ```json
  {"connectionId":13}
  {"meta":[{"name":"k1","type":"int(11)"},{"name":"k2","type":"int(11)"}]}
  {"data":[1,2]}
  {"data":[1,1]}
  {"statistics":{"scanRows":0,"scanBytes":0,"returnRows":2}}
  ```

### Cancel a query

To cancel a query that run an unexpected long time, you can close the connection. StarRocks will cancel this query when it detects the connection is closed.

You can also call KILL `connectionId` to cancel this query. For example:

```shell
curl -X POST 'http://127.0.0.1:8030/api/v1/catalogs/default_catalog/databases/test/sql' -u 'root:' -d '{"query": "kill 17;"}' --header "Content-Type: application/json"
```

You can obtain the `connectionId` from the response body or by calling SHOW PROCESSLIST. For example:

```shell
curl -X POST 'http://127.0.0.1:8030/api/v1/catalogs/default_catalog/databases/test/sql' -u 'root:' -d '{"query": "show processlist;"}' --header "Content-Type: application/json"
```

### Run a query with a session variable set for this query

```shell
curl -X POST 'http://127.0.0.1:8030/api/v1/catalogs/default_catalog/databases/test/sql' -u 'root:'  -d '{"query": "SHOW VARIABLES;", "sessionVariables":{"broadcast_row_limit":14000000}}'  --header "Content-Type: application/json"
```
