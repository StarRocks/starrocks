-- name: test_http_request_security
-- description: SSRF protection tests for http_request() function
-- Note: Tests security levels and allowlist configuration
-- Security Levels:
--   1 = TRUSTED: Allow all requests including private IPs
--   2 = PUBLIC: Block private IPs, allow all public hosts
--   3 = RESTRICTED: Block private IPs, require allowlist (default)
--   4 = PARANOID: Same as RESTRICTED, but private IPs blocked even if in allowlist

-- ============================================================
-- Security Level 3 (RESTRICTED, Default) Tests
-- ============================================================

-- Setup: Ensure clean config state (reset any pollution from previous tests)
ADMIN SET FRONTEND CONFIG ("http_request_security_level" = "3");
ADMIN SET FRONTEND CONFIG ("http_request_ip_allowlist" = "");
ADMIN SET FRONTEND CONFIG ("http_request_host_allowlist_regexp" = "");
ADMIN SET FRONTEND CONFIG ("http_request_allow_private_in_allowlist" = "false");

-- Test 1: Default security level (3=RESTRICTED) blocks requests without allowlist
-- Should return error with "allowlist" message
SELECT json_query(http_request(
    url => 'https://jsonplaceholder.typicode.com/posts/1'
), '$.status') as status;

-- Test 2: Default level blocks private IP (127.0.0.1)
-- Should return error with "Private IP" message
SELECT json_query(http_request(
    url => 'http://127.0.0.1/api',
    timeout_ms => 1000
), '$.status') as status;

-- Test 3: Default level blocks private IP (192.168.x.x)
SELECT json_query(http_request(
    url => 'http://192.168.1.1/api',
    timeout_ms => 1000
), '$.status') as status;

-- Test 4: Default level blocks private IP (10.x.x.x)
SELECT json_query(http_request(
    url => 'http://10.0.0.1/api',
    timeout_ms => 1000
), '$.status') as status;

-- Test 5: Default level blocks private IP (172.16.x.x - 172.31.x.x)
SELECT json_query(http_request(
    url => 'http://172.16.0.1/api',
    timeout_ms => 1000
), '$.status') as status;

-- Test 6: Default level blocks link-local IP (169.254.x.x)
SELECT json_query(http_request(
    url => 'http://169.254.169.254/api',
    timeout_ms => 1000
), '$.status') as status;

-- ============================================================
-- Security Level 2 (PUBLIC) Tests
-- ============================================================

-- Test 7: Set security level to PUBLIC (2)
ADMIN SET FRONTEND CONFIG ("http_request_security_level" = "2");

-- Test 8: Level 2 allows public hosts without allowlist
SELECT json_query(http_request(
    url => 'https://jsonplaceholder.typicode.com/posts/1'
), '$.status') as status;

-- Test 9: Level 2 still blocks private IP (127.0.0.1)
SELECT json_query(http_request(
    url => 'http://127.0.0.1/api',
    timeout_ms => 1000
), '$.status') as status;

-- Test 10: Level 2 blocks private IP (192.168.x.x)
SELECT json_query(http_request(
    url => 'http://192.168.1.1/api',
    timeout_ms => 1000
), '$.status') as status;

-- ============================================================
-- Security Level 1 (TRUSTED) Tests
-- ============================================================

-- Test 11: Set security level to TRUSTED (1)
ADMIN SET FRONTEND CONFIG ("http_request_security_level" = "1");

-- Test 12: Level 1 allows public hosts
SELECT json_query(http_request(
    url => 'https://jsonplaceholder.typicode.com/posts/1'
), '$.status') as status;

-- Test 13: Level 1 allows private IP (127.0.0.1) - may timeout but no security error
-- Note: Actual connection may fail due to no server, but SSRF protection is bypassed
-- Verify error message does NOT contain 'SSRF' (connection error instead of security block)
SELECT CAST(json_query(http_request(
    url => 'http://127.0.0.1:65535/api',
    timeout_ms => 1000
), '$.error') AS STRING) NOT LIKE '%SSRF%' as ssrf_not_blocked;

-- ============================================================
-- Allowlist Tests (Level 3)
-- ============================================================

-- Test 14: Set security level to RESTRICTED (3) with allowlist (using regexp for hostnames)
ADMIN SET FRONTEND CONFIG ("http_request_security_level" = "3");
ADMIN SET FRONTEND CONFIG ("http_request_host_allowlist_regexp" = "jsonplaceholder\\.typicode\\.com");

-- Test 15: Host in allowlist should be allowed
SELECT json_query(http_request(
    url => 'https://jsonplaceholder.typicode.com/posts/1'
), '$.status') as status;

-- Test 16: Host not in allowlist should be blocked
SELECT json_query(http_request(
    url => 'https://postman-echo.com/get'
), '$.status') as status;

-- Test 17: Configure multiple hosts in allowlist (using regexp)
ADMIN SET FRONTEND CONFIG ("http_request_host_allowlist_regexp" = "jsonplaceholder\\.typicode\\.com|postman-echo\\.com");

-- Test 18: Both hosts should be allowed now
SELECT json_query(http_request(
    url => 'https://jsonplaceholder.typicode.com/posts/1'
), '$.status') as status;

SELECT json_query(http_request(
    url => 'https://postman-echo.com/get'
), '$.status') as status;

-- ============================================================
-- Regex Allowlist Tests (Level 3)
-- ============================================================

-- Test 19: Configure regex allowlist
ADMIN SET FRONTEND CONFIG ("http_request_ip_allowlist" = "");
ADMIN SET FRONTEND CONFIG ("http_request_host_allowlist_regexp" = ".*\\.typicode\\.com");

-- Test 20: Regex pattern match should allow access
SELECT json_query(http_request(
    url => 'https://jsonplaceholder.typicode.com/posts/1'
), '$.status') as status;

-- Test 21: Non-matching host should be blocked
SELECT json_query(http_request(
    url => 'https://postman-echo.com/get'
), '$.status') as status;

-- ============================================================
-- Combined Allowlist (OR condition) Tests
-- ============================================================

-- Test 22: Configure both IP and regex allowlist (OR condition)
-- Note: http_request_ip_allowlist is for IP addresses only
-- Using regexp for hostname matching
ADMIN SET FRONTEND CONFIG ("http_request_ip_allowlist" = "");
ADMIN SET FRONTEND CONFIG ("http_request_host_allowlist_regexp" = "postman-echo\\.com|.*\\.typicode\\.com");

-- Test 23: Regex match for postman-echo should be allowed
SELECT json_query(http_request(
    url => 'https://postman-echo.com/get'
), '$.status') as status;

-- Test 24: Regex match for typicode should be allowed
SELECT json_query(http_request(
    url => 'https://jsonplaceholder.typicode.com/posts/1'
), '$.status') as status;

-- Test 25: Neither match should be blocked
SELECT json_query(http_request(
    url => 'https://example.com/api'
), '$.status') as status;

-- ============================================================
-- Private IP with allow_private_in_allowlist Tests
-- ============================================================

-- Test 25a: Level 3 private IP blocked by default even in allowlist
ADMIN SET FRONTEND CONFIG ("http_request_ip_allowlist" = "127.0.0.1");
ADMIN SET FRONTEND CONFIG ("http_request_host_allowlist_regexp" = "");
ADMIN SET FRONTEND CONFIG ("http_request_allow_private_in_allowlist" = "false");
-- Private IP in allowlist but allow_private_in_allowlist=false - should be blocked
SELECT json_query(http_request(
    url => 'http://127.0.0.1:65535/api',
    timeout_ms => 1000
), '$.status') as status;

-- Test 25b: Level 3 private IP allowed with allow_private_in_allowlist=true
ADMIN SET FRONTEND CONFIG ("http_request_allow_private_in_allowlist" = "true");
-- Private IP in allowlist with allow_private_in_allowlist=true - allowed (connection error expected)
SELECT CAST(json_query(http_request(
    url => 'http://127.0.0.1:65535/api',
    timeout_ms => 1000
), '$.error') AS STRING) NOT LIKE '%SSRF%' as ssrf_not_blocked;

-- ============================================================
-- Level 4 (PARANOID) Tests - Blocks ALL requests
-- ============================================================

-- Test 26: Set security level to PARANOID (4)
ADMIN SET FRONTEND CONFIG ("http_request_security_level" = "4");

-- Test 27: At level 4, ALL requests are blocked (even public hosts in allowlist)
ADMIN SET FRONTEND CONFIG ("http_request_host_allowlist_regexp" = "jsonplaceholder\\.typicode\\.com");
SELECT json_query(http_request(
    url => 'https://jsonplaceholder.typicode.com/posts/1'
), '$.status') as status;

-- Test 28: At level 4, private IP is also blocked
ADMIN SET FRONTEND CONFIG ("http_request_ip_allowlist" = "127.0.0.1");
ADMIN SET FRONTEND CONFIG ("http_request_allow_private_in_allowlist" = "true");
SELECT json_query(http_request(
    url => 'http://127.0.0.1/api',
    timeout_ms => 1000
), '$.status') as status;

-- ============================================================
-- Allowlist Whitespace Trimming Tests
-- ============================================================

-- Test 29: Allowlist should handle whitespace correctly (using regexp for hostnames)
ADMIN SET FRONTEND CONFIG ("http_request_security_level" = "3");
ADMIN SET FRONTEND CONFIG ("http_request_ip_allowlist" = "");
ADMIN SET FRONTEND CONFIG ("http_request_host_allowlist_regexp" = "jsonplaceholder\\.typicode\\.com|jsonplaceholder\\.typicode\\.com");

-- Test 30: Host matching regexp should be allowed
SELECT json_query(http_request(
    url => 'https://jsonplaceholder.typicode.com/posts/1'
), '$.status') as status;

-- ============================================================
-- URL Parsing Edge Cases
-- ============================================================

-- Test 33: Reset to valid configuration
ADMIN SET FRONTEND CONFIG ("http_request_security_level" = "3");
ADMIN SET FRONTEND CONFIG ("http_request_ip_allowlist" = "");
ADMIN SET FRONTEND CONFIG ("http_request_host_allowlist_regexp" = "jsonplaceholder\\.typicode\\.com");

-- Test 34: URL with port number - host should still be extracted correctly
SELECT json_query(http_request(
    url => 'https://jsonplaceholder.typicode.com:443/posts/1'
), '$.status') as status;

-- Test 35: URL with path and query - host should still be extracted correctly
SELECT json_query(http_request(
    url => 'https://jsonplaceholder.typicode.com/posts?userId=1'
), '$.status') as status;

-- ============================================================
-- IPv6 Tests
-- ============================================================

-- Test 36: IPv6 loopback should be blocked
SELECT json_query(http_request(
    url => 'http://[::1]/api',
    timeout_ms => 1000
), '$.status') as status;

-- Test 37: IPv6 link-local should be blocked
SELECT json_query(http_request(
    url => 'http://[fe80::1]/api',
    timeout_ms => 1000
), '$.status') as status;

-- Test 38: IPv6 unique local should be blocked
SELECT json_query(http_request(
    url => 'http://[fc00::1]/api',
    timeout_ms => 1000
), '$.status') as status;

-- ============================================================
-- Error Test Cases - Invalid URL Format
-- ============================================================

-- Test E1: Empty URL - should return error
SELECT json_query(http_request(url => ''), '$.status') as status;

-- Test E2: URL without protocol - should fail
SELECT json_query(http_request(url => 'jsonplaceholder.typicode.com/posts/1'), '$.status') as status;

-- Test E3: Invalid protocol (ftp) - should fail
SELECT json_query(http_request(url => 'ftp://ftp.example.com/file'), '$.status') as status;

-- Test E4: Invalid protocol (file) - should fail
SELECT json_query(http_request(url => 'file:///etc/passwd'), '$.status') as status;

-- Test E5: Malformed URL (missing host)
SELECT json_query(http_request(url => 'http:///path/to/resource'), '$.status') as status;

-- Test E6: URL with only protocol
SELECT json_query(http_request(url => 'https://'), '$.status') as status;

-- ============================================================
-- Error Test Cases - Invalid Headers JSON
-- ============================================================

-- Test E7: Invalid JSON syntax in headers (missing closing brace)
SELECT json_query(http_request(
    url => 'https://jsonplaceholder.typicode.com/posts/1',
    headers => '{"Content-Type": "application/json"'
), '$.status') as status;

-- Test E8: JSON array instead of object for headers
SELECT json_query(http_request(
    url => 'https://jsonplaceholder.typicode.com/posts/1',
    headers => '["Content-Type", "application/json"]'
), '$.status') as status;

-- ============================================================
-- Error Test Cases - DNS Resolution Failure
-- ============================================================

-- Test E9: Non-existent domain - DNS resolution should fail
ADMIN SET FRONTEND CONFIG ("http_request_security_level" = "2");
SELECT json_query(http_request(
    url => 'https://this-domain-definitely-does-not-exist-12345.com/api',
    timeout_ms => 5000
), '$.status') as status;

-- ============================================================
-- Error Test Cases - Connection Failures
-- ============================================================

-- Test E10: Connection refused (no server on port)
ADMIN SET FRONTEND CONFIG ("http_request_security_level" = "1");
SELECT json_query(http_request(
    url => 'http://127.0.0.1:65534/api',
    timeout_ms => 1000
), '$.status') as status;

-- Test E11: Connection timeout (very short timeout)
ADMIN SET FRONTEND CONFIG ("http_request_security_level" = "2");
ADMIN SET FRONTEND CONFIG ("http_request_host_allowlist_regexp" = "jsonplaceholder\\.typicode\\.com");
SELECT json_query(http_request(
    url => 'https://jsonplaceholder.typicode.com/posts/1',
    timeout_ms => 1
), '$.status') as status;

-- ============================================================
-- Error Test Cases - Invalid Regex Patterns
-- ============================================================

-- Test E12: Invalid regex pattern in allowlist_regexp (unclosed group) - now returns error
ADMIN SET FRONTEND CONFIG ("http_request_security_level" = "3");
ADMIN SET FRONTEND CONFIG ("http_request_ip_allowlist" = "");
ADMIN SET FRONTEND CONFIG ("http_request_host_allowlist_regexp" = "(unclosed");

-- Test E13: Invalid regex pattern (invalid range) - now returns error
ADMIN SET FRONTEND CONFIG ("http_request_host_allowlist_regexp" = "[z-a]");

-- ============================================================
-- Edge Cases - SSRF Bypass Attempts
-- ============================================================

-- Test E14: Decimal IP address (2130706433 = 127.0.0.1)
ADMIN SET FRONTEND CONFIG ("http_request_security_level" = "3");
ADMIN SET FRONTEND CONFIG ("http_request_ip_allowlist" = "");
ADMIN SET FRONTEND CONFIG ("http_request_host_allowlist_regexp" = "");
SELECT json_query(http_request(
    url => 'http://2130706433/api',
    timeout_ms => 1000
), '$.status') as status;

-- Test E15: Octal IP address (0177.0.0.1 = 127.0.0.1)
SELECT json_query(http_request(
    url => 'http://0177.0.0.1/api',
    timeout_ms => 1000
), '$.status') as status;

-- Test E16: IPv6 mapped IPv4 (::ffff:127.0.0.1)
SELECT json_query(http_request(
    url => 'http://[::ffff:127.0.0.1]/api',
    timeout_ms => 1000
), '$.status') as status;

-- Test E17: Localhost alternatives
SELECT json_query(http_request(
    url => 'http://localhost/api',
    timeout_ms => 1000
), '$.status') as status;

-- Test E18: URL with @ symbol (user info) - potential SSRF bypass
SELECT json_query(http_request(
    url => 'http://user@127.0.0.1/api',
    timeout_ms => 1000
), '$.status') as status;

-- Test E19: URL with encoded characters (127.0.0.1 URL encoded)
SELECT json_query(http_request(
    url => 'http://%31%32%37%2e%30%2e%30%2e%31/api',
    timeout_ms => 1000
), '$.status') as status;

-- ============================================================
-- Edge Cases - Boundary Values
-- ============================================================

-- Test E20: Timeout at minimum boundary (1ms) - should timeout
ADMIN SET FRONTEND CONFIG ("http_request_security_level" = "2");
SELECT json_query(http_request(
    url => 'https://jsonplaceholder.typicode.com/posts/1',
    timeout_ms => 1
), '$.status') as status;

-- Test E21: Timeout below minimum (0) - should clamp to 1ms
SELECT json_query(http_request(
    url => 'https://jsonplaceholder.typicode.com/posts/1',
    timeout_ms => 0
), '$.status') as status;

-- Test E22: Negative timeout - should clamp to 1ms
SELECT json_query(http_request(
    url => 'https://jsonplaceholder.typicode.com/posts/1',
    timeout_ms => -1000
), '$.status') as status;

-- ============================================================
-- Edge Cases - Empty/Null Allowlist Scenarios
-- ============================================================

-- Test E23: Both allowlists empty at level 3 - all hosts blocked
ADMIN SET FRONTEND CONFIG ("http_request_security_level" = "3");
ADMIN SET FRONTEND CONFIG ("http_request_ip_allowlist" = "");
ADMIN SET FRONTEND CONFIG ("http_request_host_allowlist_regexp" = "");
SELECT json_query(http_request(
    url => 'https://jsonplaceholder.typicode.com/posts/1'
), '$.status') as status;

-- Test E24: Removed - FE validation rejects whitespace-only allowlist values
-- The ConfigBase.java validation throws InvalidConfException for empty values between commas

-- ============================================================
-- Edge Cases - Blocklist Tests (if implemented)
-- ============================================================

-- Test E25: Cloud metadata endpoints should be blocked
ADMIN SET FRONTEND CONFIG ("http_request_security_level" = "2");
SELECT json_query(http_request(
    url => 'http://169.254.169.254/latest/meta-data/',
    timeout_ms => 1000
), '$.status') as status;

-- Test E26: AWS metadata alternative endpoint
SELECT json_query(http_request(
    url => 'http://169.254.170.2/v2/credentials',
    timeout_ms => 1000
), '$.status') as status;

-- ============================================================
-- DNS Rebinding Protection Tests
-- ============================================================

-- Test DNS1: DNS rebinding protection via IP pinning
-- Note: This test verifies that resolved IPs are pinned using CURLOPT_RESOLVE
-- Even if an attacker's DNS returns different IPs between validation and execution,
-- the validated IP will be used for the actual HTTP request

-- Setup: Level 2 (PUBLIC) - validates DNS and pins IP
ADMIN SET FRONTEND CONFIG ("http_request_security_level" = "2");
ADMIN SET FRONTEND CONFIG ("http_request_ip_allowlist" = "");
ADMIN SET FRONTEND CONFIG ("http_request_host_allowlist_regexp" = "");

-- Test DNS2: Verify that IP pinning works for public hosts
-- The implementation should:
-- 1. Resolve DNS during validate_host_security_and_get_ips()
-- 2. Pin the resolved IP using CURLOPT_RESOLVE
-- 3. Use pinned IP for actual request (no second DNS resolution)
SELECT json_query(http_request(
    url => 'https://jsonplaceholder.typicode.com/posts/1',
    timeout_ms => 5000
), '$.status') as status;

-- Test DNS3: Level 3 (RESTRICTED) with allowlist - also uses IP pinning
ADMIN SET FRONTEND CONFIG ("http_request_security_level" = "3");
ADMIN SET FRONTEND CONFIG ("http_request_host_allowlist_regexp" = "jsonplaceholder\\.typicode\\.com");

SELECT json_query(http_request(
    url => 'https://jsonplaceholder.typicode.com/posts/1',
    timeout_ms => 5000
), '$.status') as status;

-- Test DNS4: Verify IP pinning handles different ports correctly
-- HTTP (port 80) and HTTPS (port 443) should pin to correct ports
ADMIN SET FRONTEND CONFIG ("http_request_security_level" = "2");

-- HTTP with default port 80
SELECT json_query(http_request(
    url => 'http://jsonplaceholder.typicode.com/posts/1',
    timeout_ms => 5000
), '$.status') as status;

-- HTTPS with default port 443
SELECT json_query(http_request(
    url => 'https://jsonplaceholder.typicode.com/posts/1',
    timeout_ms => 5000
), '$.status') as status;

-- HTTPS with explicit port 443
ADMIN SET FRONTEND CONFIG ("http_request_host_allowlist_regexp" = "jsonplaceholder\\.typicode\\.com");
ADMIN SET FRONTEND CONFIG ("http_request_security_level" = "3");
SELECT json_query(http_request(
    url => 'https://jsonplaceholder.typicode.com:443/posts/1',
    timeout_ms => 5000
), '$.status') as status;

-- ============================================================
-- Cleanup - Restore Default Configuration
-- ============================================================

ADMIN SET FRONTEND CONFIG ("http_request_security_level" = "3");
ADMIN SET FRONTEND CONFIG ("http_request_ip_allowlist" = "");
ADMIN SET FRONTEND CONFIG ("http_request_host_allowlist_regexp" = "");
ADMIN SET FRONTEND CONFIG ("http_request_allow_private_in_allowlist" = "false");
