---
displayed_sidebar: docs
---

# http_request

Executes an HTTP request and returns the response as a JSON string. Supports both named and positional parameters.

## Limits

| Limit | Value |
|-------|-------|
| Maximum response size | 1 MB |
| Maximum redirects | 20 |
| Minimum timeout | 1 ms |
| Maximum timeout | 300,000 ms (5 minutes) |
| Supported protocols | HTTP, HTTPS only |

## Syntax

```sql
-- Named parameters (recommended)
http_request(
    url => <url>,
    [method => <method>,]
    [body => <body>,]
    [headers => <headers>,]
    [timeout_ms => <timeout_ms>,]
    [ssl_verify => <ssl_verify>,]
    [username => <username>,]
    [password => <password>]
)

-- Positional parameters
http_request(<url>[, <method>[, <body>[, <headers>[, <timeout_ms>[, <ssl_verify>[, <username>[, <password>]]]]]]])
```

## Parameters

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `url` | VARCHAR | Yes | - | Target URL. Must be HTTP or HTTPS. |
| `method` | VARCHAR | No | `'GET'` | HTTP method: `GET`, `POST`, `PUT`, `DELETE`, `HEAD`, `OPTIONS`. |
| `body` | VARCHAR | No | `''` | Request body content. |
| `headers` | VARCHAR | No | `'{}'` | Custom HTTP headers as a JSON object string. |
| `timeout_ms` | INT | No | `30000` | Request timeout in milliseconds (1 ~ 300,000). |
| `ssl_verify` | BOOLEAN | No | `true` | Enable SSL certificate verification. |
| `username` | VARCHAR | No | `''` | HTTP Basic Authentication username. |
| `password` | VARCHAR | No | `''` | HTTP Basic Authentication password. |

## Return value

Returns a VARCHAR containing a JSON object.

**Success:**
```json
{"status": <http_code>, "body": <response_content>}
```

**Error:**
```json
{"status": -1, "body": null, "error": "<error_message>"}
```

## Examples

### Simple GET request

```sql
SELECT http_request(url => 'https://httpbin.org/get');
```

### Extract status code using json_query

```sql
SELECT json_query(
    http_request(url => 'https://httpbin.org/get'),
    '$.status'
) AS status_code;
```

### POST request with JSON body

```sql
SELECT http_request(
    url => 'https://httpbin.org/post',
    method => 'POST',
    headers => '{"Content-Type": "application/json"}',
    body => '{"name": "StarRocks", "type": "database"}'
);
```

### Custom headers

```sql
SELECT http_request(
    url => 'https://api.example.com/data',
    headers => '{"Authorization": "Bearer token123", "Accept": "application/json"}'
);
```

### HTTP Basic Authentication

```sql
SELECT http_request(
    url => 'https://httpbin.org/basic-auth/user/passwd',
    username => 'user',
    password => 'passwd'
);
```

### Custom timeout

```sql
SELECT http_request(
    url => 'https://slow-api.example.com/data',
    timeout_ms => 60000
);
```

### Named parameters in any order

```sql
SELECT http_request(
    method => 'POST',
    timeout_ms => 5000,
    url => 'https://httpbin.org/post',
    body => '{"key": "value"}'
);
```

### Positional parameters

```sql
SELECT http_request('https://httpbin.org/post', 'POST', '{"key": "value"}');
```

### Sending Slack webhook notification

```sql
SELECT http_request(
    url => 'https://hooks.slack.com/services/YOUR/WEBHOOK/URL',
    method => 'POST',
    headers => '{"Content-Type": "application/json"}',
    body => '{"text": "Alert: Daily report generated from StarRocks!"}'
);
```

### Parse nested JSON response

```sql
SELECT json_query(
    json_query(
        http_request(url => 'https://jsonplaceholder.typicode.com/posts/1'),
        '$.body'
    ),
    '$.title'
) AS post_title;
```

### Disable SSL verification

```sql
SELECT http_request(
    url => 'https://self-signed.example.com/api',
    ssl_verify => false
);
```

> **Note:** If `http_request_ssl_verification_required` is set to `true` by the administrator, this option will be ignored.

## Security configuration

This function includes built-in SSRF (Server-Side Request Forgery) protection with DNS rebinding prevention and redirect limits (max 20 hops).

### Security levels

| Level | Name | Description |
|-------|------|-------------|
| 1 | TRUSTED | Allow all requests including private IPs. |
| 2 | PUBLIC | Block private IPs, allow all public hosts. |
| 3 | RESTRICTED | Require allowlist for all hosts. **(Default)** |
| 4 | PARANOID | Block all requests. |

### Configuration options

| Config | Type | Default | Description |
|--------|------|---------|-------------|
| `http_request_security_level` | INT | `3` | Security level (1-4). |
| `http_request_host_allowlist_regexp` | VARCHAR | `''` | Regex pattern for allowed hostnames. |
| `http_request_ip_allowlist` | VARCHAR | `''` | Comma-separated allowed IPv4 addresses. |
| `http_request_allow_private_in_allowlist` | BOOLEAN | `false` | Allow private IPs if in allowlist. |
| `http_request_ssl_verification_required` | BOOLEAN | `true` | Enforce SSL verification (cannot be disabled by users). |

```sql
ADMIN SET FRONTEND CONFIG ("http_request_security_level" = "3");
ADMIN SET FRONTEND CONFIG ("http_request_host_allowlist_regexp" = "api\\.example\\.com|.*\\.trusted\\.org");
ADMIN SET FRONTEND CONFIG ("http_request_ip_allowlist" = "203.0.113.1,198.51.100.0");
```

### Allowlist behavior

| Target | Level 2 (PUBLIC) | Level 3 (RESTRICTED) |
|--------|------------------|---------------------|
| Public IP (not in allowlist) | Allowed | Blocked |
| Public IP (in allowlist) | Allowed | Allowed |
| Private IP (not in allowlist) | Blocked | Blocked |
| Private IP (in allowlist + `allow_private=true`) | Allowed | Allowed |

### Blocked IP ranges (levels 2-4)

`127.0.0.0/8`, `10.0.0.0/8`, `172.16.0.0/12`, `192.168.0.0/16`, `169.254.0.0/16`, `0.0.0.0/8`, IPv6 loopback (`::1`), link-local (`fe80::/10`), unique local (`fc00::/7`).

## keyword

HTTP, REQUEST, API, CALL
