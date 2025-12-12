---
displayed_sidebar: docs
---

# url

Executes an HTTP request and returns the response as a JSON string.

## Syntax

```Haskell
VARCHAR url(VARCHAR url)
VARCHAR url(VARCHAR url, VARCHAR config)
```

## Parameters

`url`: the target URL for the HTTP request. It must be a valid HTTP or HTTPS URL.

`config`: a JSON string specifying request options. Valid fields:

- `method`: HTTP method (`GET`, `POST`, `PUT`, `DELETE`). Default: `GET`.
- `headers`: custom HTTP headers as object. Default: `{}`.
- `body`: request body. String or object (auto-stringified to JSON). Default: `null`.
- `timeout_ms`: timeout in milliseconds. (Default: `30000`, min : `1`, max : `300,000`)
- `ssl_verify`: SSL certificate verification. Default: `true`.
- `username`: HTTP Basic Auth username. Default: `null`.
- `password`: HTTP Basic Auth password. Default: `null`.

## Return value

Returns a VARCHAR containing a JSON object:

- `{"status": <code>, "body": ...}` for HTTP responses (body is JSON or escaped string)
- `{"status": -1, "body": null, "error": "..."}` for errors

Error cases that return JSON error response:
- Invalid or empty URL
- Invalid JSON config format
- Network/connection errors
- Response size exceeds 1MB limit
- Request timeout

## Examples

Example 1: Simple GET request.

```Plain Text
mysql> SELECT url('https://httpbin.org/get');
+--------------------------------------------------+
| url('https://httpbin.org/get')                   |
+--------------------------------------------------+
| {"status": 200, "body": {"args": {}, ...}}       |
+--------------------------------------------------+
```

Example 2: POST with JSON body. The `body` object is automatically stringified.

```Plain Text
mysql> SELECT url('https://httpbin.org/post',
    '{"method": "POST", "headers": {"Content-Type": "application/json"}, "body": {"key": "value"}}');
+--------------------------------------------------+
| url(...)                                         |
+--------------------------------------------------+
| {"status": 200, "body": {"data": ...}}           |
+--------------------------------------------------+
```

Example 3: Extract status code using json_query.

```Plain Text
mysql> SELECT json_query(url('https://httpbin.org/get'), '$.status');
+--------------------------------------------------------+
| json_query(url('https://httpbin.org/get'), '$.status') |
+--------------------------------------------------------+
| 200                                                    |
+--------------------------------------------------------+
```

Example 4: GET with custom headers.

```Plain Text
mysql> SELECT url('https://api.example.com/data',
    '{"headers": {"Authorization": "Bearer token123"}}');
```

Example 5: POST with timeout configuration.

```Plain Text
mysql> SELECT url('https://api.example.com/data',
    '{"method": "POST", "timeout_ms": 5000}');
```

Example 6: HTTP Basic Authentication.

```Plain Text
mysql> SELECT url('https://api.example.com/secure',
    '{"username": "admin", "password": "secret"}');
```

Example 7: Disable SSL verification for self-signed certificates.

```Plain Text
mysql> SELECT url('https://self-signed.local/api', '{"ssl_verify": false}');
```

> Note: Only disable SSL verification in development environments with self-signed certificates.

## keyword

URL, HTTP, REST, API
