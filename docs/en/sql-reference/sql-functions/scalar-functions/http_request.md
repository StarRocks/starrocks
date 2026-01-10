---
displayed_sidebar: docs
---

# http_request

Executes an HTTP request and returns the response as a JSON string. This function supports Named Parameters for improved readability and flexibility.

## Syntax

```sql
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
```

## Parameters

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `url` | VARCHAR | Yes | - | Target URL for the HTTP request. Must be a valid HTTP or HTTPS URL. |
| `method` | VARCHAR | No | `'GET'` | HTTP method. Supported: `GET`, `POST`, `PUT`, `DELETE`, `HEAD`, `OPTIONS`. |
| `body` | VARCHAR | No | `''` | Request body content. |
| `headers` | VARCHAR | No | `'{}'` | Custom HTTP headers as a JSON object string. |
| `timeout_ms` | INT | No | `30000` | Request timeout in milliseconds. Range: 1 ~ 300,000 (5 minutes). |
| `ssl_verify` | BOOLEAN | No | `true` | Enable SSL certificate verification. |
| `username` | VARCHAR | No | `''` | HTTP Basic Authentication username. |
| `password` | VARCHAR | No | `''` | HTTP Basic Authentication password. |

## Return value

Returns a VARCHAR containing a JSON object:

**Success response:**
```json
{"status": <http_code>, "body": <response_content>}
```

**Error response:**
```json
{"status": -1, "body": null, "error": "<error_message>"}
```

### Error cases

The function returns a JSON error response (status: -1) for:

- Invalid or empty URL
- Unsupported protocol (only HTTP/HTTPS allowed)
- Invalid HTTP method
- Malformed headers JSON
- Network/connection errors
- DNS resolution failure
- Response size exceeds 1MB limit
- Request timeout
- SSRF protection violation (blocked by security policy)

## Security Configuration

This function includes built-in SSRF (Server-Side Request Forgery) protection. Administrators can configure security settings using Frontend (FE) configuration options.

### Security Levels

| Level | Name | Description |
|-------|------|-------------|
| 1 | TRUSTED | Allow all requests including private IPs. Use only in trusted environments. |
| 2 | PUBLIC | Block private IPs, allow all public hosts. |
| 3 | RESTRICTED | Block private IPs, require allowlist for hosts. **(Default)** |
| 4 | PARANOID | Block all requests. Useful for disabling the function entirely. |

### Configuration Options

```sql
-- Set security level (1-4)
ADMIN SET FRONTEND CONFIG ("http_request_security_level" = "3");

-- Configure allowed hosts using regex pattern
ADMIN SET FRONTEND CONFIG ("http_request_host_allowlist_regexp" = "api\\.example\\.com|.*\\.trusted\\.org");

-- Configure allowed IP addresses
ADMIN SET FRONTEND CONFIG ("http_request_ip_allowlist" = "203.0.113.1,198.51.100.0");

-- Allow private IPs if explicitly in allowlist (default: false)
ADMIN SET FRONTEND CONFIG ("http_request_allow_private_in_allowlist" = "true");

-- Enforce SSL verification (cannot be disabled by users)
ADMIN SET FRONTEND CONFIG ("http_request_ssl_verification_required" = "true");
```

### Blocked IP Ranges

The following IP ranges are blocked by default (security levels 2-4):

- `127.0.0.0/8` - Loopback
- `10.0.0.0/8` - Private Class A
- `172.16.0.0/12` - Private Class B
- `192.168.0.0/16` - Private Class C
- `169.254.0.0/16` - Link-local (cloud metadata endpoints)
- `0.0.0.0/8` - Current network
- IPv6 loopback (`::1`)
- IPv6 link-local (`fe80::/10`)
- IPv6 unique local (`fc00::/7`)

### Allowlist Behavior

> **Important Notes:**
>
> **For Public IPs/Hosts:**
> - At security level 2 (PUBLIC): Allowlist is NOT required. All public hosts are allowed.
> - At security level 3 (RESTRICTED): Only hosts/IPs in the allowlist are allowed.
>
> **For Private IPs:**
> - Private IPs are **always blocked by default**, even if they are in the allowlist.
> - To allow a private IP, you must **BOTH**:
>   1. Add the IP to `http_request_ip_allowlist`
>   2. Set `http_request_allow_private_in_allowlist` to `true`
>
> **Security level 4 (PARANOID):** All requests are blocked regardless of allowlist settings.

**Example: Allow internal API server at `10.0.0.100`:**

```sql
-- Step 1: Set security level
ADMIN SET FRONTEND CONFIG ("http_request_security_level" = "3");

-- Step 2: Add private IP to allowlist
ADMIN SET FRONTEND CONFIG ("http_request_ip_allowlist" = "10.0.0.100");

-- Step 3: Enable private IP allowlist (REQUIRED for private IPs)
ADMIN SET FRONTEND CONFIG ("http_request_allow_private_in_allowlist" = "true");
```

**Allowlist Summary Table:**

| Target | Level 2 (PUBLIC) | Level 3 (RESTRICTED) |
|--------|------------------|---------------------|
| Public IP (not in allowlist) | Allowed | Blocked |
| Public IP (in allowlist) | Allowed | Allowed |
| Private IP (not in allowlist) | Blocked | Blocked |
| Private IP (in allowlist only) | Blocked | Blocked |
| Private IP (in allowlist + `allow_private_in_allowlist=true`) | Allowed | Allowed |

## Limits

| Limit | Value |
|-------|-------|
| Maximum response size | 1 MB |
| Minimum timeout | 1 ms |
| Maximum timeout | 300,000 ms (5 minutes) |
| Supported protocols | HTTP, HTTPS only |

## Examples

### Example 1: Simple GET request

```sql
SELECT http_request(url => 'https://httpbin.org/get');
```

Result:
```json
{"status": 200, "body": {"args": {}, "headers": {...}, "origin": "...", "url": "..."}}
```

### Example 2: Extract status code using json_query

```sql
SELECT json_query(
    http_request(url => 'https://httpbin.org/get'),
    '$.status'
) AS status_code;
```

Result:
```
+-------------+
| status_code |
+-------------+
| 200         |
+-------------+
```

### Example 3: POST request with JSON body

```sql
SELECT http_request(
    url => 'https://httpbin.org/post',
    method => 'POST',
    headers => '{"Content-Type": "application/json"}',
    body => '{"name": "StarRocks", "type": "database"}'
);
```

### Example 4: GET with custom headers

```sql
SELECT http_request(
    url => 'https://api.example.com/data',
    headers => '{"Authorization": "Bearer token123", "Accept": "application/json"}'
);
```

### Example 5: Request with custom timeout

```sql
SELECT http_request(
    url => 'https://slow-api.example.com/data',
    timeout_ms => 60000
);
```

### Example 6: HTTP Basic Authentication

```sql
SELECT http_request(
    url => 'https://httpbin.org/basic-auth/user/passwd',
    username => 'user',
    password => 'passwd'
);
```

### Example 7: Disable SSL verification (for self-signed certificates)

```sql
SELECT http_request(
    url => 'https://self-signed.example.com/api',
    ssl_verify => false
);
```

> **Warning:** Only disable SSL verification in development environments with self-signed certificates. If `http_request_ssl_verification_required` is set to `true` by the administrator, this option will be ignored and SSL verification will always be enabled.

### Example 8: DELETE request

```sql
SELECT http_request(
    url => 'https://api.example.com/resources/123',
    method => 'DELETE'
);
```

### Example 9: HEAD request (returns headers only)

```sql
SELECT http_request(
    url => 'https://httpbin.org/get',
    method => 'HEAD'
);
```

### Example 10: Named parameters in any order

Named parameters can be specified in any order:

```sql
SELECT http_request(
    method => 'POST',
    timeout_ms => 5000,
    url => 'https://httpbin.org/post',
    body => '{"key": "value"}'
);
```

### Example 11: Sending Slack webhook notification

```sql
SELECT http_request(
    url => 'https://hooks.slack.com/services/YOUR/WEBHOOK/URL',
    method => 'POST',
    headers => '{"Content-Type": "application/json"}',
    body => '{"text": "Alert: Daily report generated from StarRocks!"}'
);
```

### Example 12: Parse JSON response body

```sql
SELECT json_query(
    json_query(
        http_request(url => 'https://jsonplaceholder.typicode.com/posts/1'),
        '$.body'
    ),
    '$.title'
) AS post_title;
```

## Use Cases

- **API Integration:** Call external REST APIs directly from SQL queries
- **Webhook Notifications:** Send alerts to Slack, Teams, or other services
- **Data Enrichment:** Fetch additional data from external services during query execution
- **Health Checks:** Monitor external service availability

## Keywords

HTTP, HTTPS, REST, API, REQUEST, WEBHOOK, CURL
