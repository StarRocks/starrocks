// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "exprs/http_request_functions.h"

#include <fmt/format.h>
#include <simdjson.h>

#include <algorithm>
#include <cctype>
#include <map>
#include <optional>
#include <sstream>

#include "column/column_helper.h"
#include "http/http_client.h"
#include "http/http_method.h"
#include "runtime/runtime_state.h"
#include "util/network_util.h"

namespace starrocks {

// HTTP Request Function Implementation
//
// Usage with Named Parameters:
//   SELECT http_request(url => 'https://api.example.com/data');
//   SELECT http_request(url => 'https://api.example.com', method => 'POST', body => '{}');
//   SELECT http_request(url => 'https://api.example.com', headers => '{"Authorization": "Bearer token"}');
//
// Parameters:
//   url (VARCHAR, required) - The URL to request
//   method (VARCHAR, default: 'GET') - HTTP method (GET, POST, PUT, DELETE, HEAD, OPTIONS)
//   body (VARCHAR, default: '') - Request body
//   headers (VARCHAR, default: '{}') - JSON object of headers
//   timeout_ms (INT, default: 30000) - Request timeout in milliseconds
//   ssl_verify (BOOLEAN, default: true) - Whether to verify SSL certificates
//   username (VARCHAR, default: '') - Basic auth username
//   password (VARCHAR, default: '') - Basic auth password

// HTTP request configuration parsed from JSON config string
struct HttpRequestConfig {
    std::string method = "GET";
    std::map<std::string, std::string> headers;
    std::string body;
    int32_t timeout_ms = 30000;
    bool ssl_verify = true;
    std::string username;
    std::string password;
};

// Default values for HTTP request function configuration
const int64_t DEFAULT_MAX_RESPONSE_SIZE = 1048576;  // 1MB

// Helper function: Parse HTTP method from string
// Returns error for invalid methods instead of silently defaulting to GET
static StatusOr<HttpMethod> parse_http_method(const Slice& method_str) {
    std::string method_upper = method_str.to_string();
    std::transform(method_upper.begin(), method_upper.end(), method_upper.begin(),
                   [](unsigned char c) { return std::toupper(c); });

    if (method_upper == "GET") {
        return HttpMethod::GET;
    } else if (method_upper == "POST") {
        return HttpMethod::POST;
    } else if (method_upper == "PUT") {
        return HttpMethod::PUT;
    } else if (method_upper == "DELETE") {
        return HttpMethod::DELETE;
    } else if (method_upper == "HEAD") {
        return HttpMethod::HEAD;
    } else if (method_upper == "OPTIONS") {
        return HttpMethod::OPTIONS;
    }

    return Status::InvalidArgument(fmt::format(
            "Invalid HTTP method '{}'. Allowed: GET, POST, PUT, DELETE, HEAD, OPTIONS",
            method_str.to_string()));
}

// Public helper: Validate HTTP method string (exposed for testing)
StatusOr<bool> validate_http_method(const std::string& method) {
    auto result = parse_http_method(Slice(method));
    if (!result.ok()) {
        return result.status();
    }
    return true;
}

// Helper function: Validate UTF-8 string
// Returns true if the string is valid UTF-8, false otherwise
static bool is_valid_utf8(const std::string& s) {
    size_t i = 0;
    while (i < s.size()) {
        unsigned char c = static_cast<unsigned char>(s[i]);

        int char_len;
        if ((c & 0x80) == 0) {
            char_len = 1;  // ASCII
        } else if ((c & 0xE0) == 0xC0) {
            char_len = 2;
        } else if ((c & 0xF0) == 0xE0) {
            char_len = 3;
        } else if ((c & 0xF8) == 0xF0) {
            char_len = 4;
        } else {
            return false;  // Invalid start byte
        }

        if (i + char_len > s.size()) {
            return false;  // Truncated sequence
        }

        // Check continuation bytes
        for (int j = 1; j < char_len; ++j) {
            if ((static_cast<unsigned char>(s[i + j]) & 0xC0) != 0x80) {
                return false;  // Invalid continuation byte
            }
        }

        i += char_len;
    }
    return true;
}

// Helper function: Escape string for JSON
static std::string escape_json_string(const std::string& s) {
    std::string result;
    result.reserve(s.size() + 16);  // Reserve some extra space for escapes
    for (char c : s) {
        switch (c) {
            case '"': result += "\\\""; break;
            case '\\': result += "\\\\"; break;
            case '\n': result += "\\n"; break;
            case '\r': result += "\\r"; break;
            case '\t': result += "\\t"; break;
            case '\b': result += "\\b"; break;
            case '\f': result += "\\f"; break;
            default:
                if (static_cast<unsigned char>(c) < 0x20) {
                    // Control characters - encode as \uXXXX
                    result += fmt::format("\\u{:04x}", static_cast<unsigned char>(c));
                } else {
                    result += c;
                }
        }
    }
    return result;
}

// Helper function: Parse headers JSON string into map
static StatusOr<std::map<std::string, std::string>> parse_headers_json(const std::string& headers_json) {
    std::map<std::string, std::string> headers;

    if (headers_json.empty() || headers_json == "{}") {
        return headers;
    }

    simdjson::ondemand::parser parser;
    simdjson::padded_string padded(headers_json);

    auto doc_result = parser.iterate(padded);
    if (doc_result.error()) {
        return Status::InvalidArgument(
                fmt::format("Invalid headers JSON: {}", simdjson::error_message(doc_result.error())));
    }

    simdjson::ondemand::document doc = std::move(doc_result.value());
    simdjson::ondemand::object obj;
    if (doc.get_object().get(obj)) {
        return Status::InvalidArgument("Headers must be a JSON object");
    }

    for (auto field : obj) {
        auto key_result = field.escaped_key();
        if (key_result.error() != simdjson::SUCCESS) continue;
        std::string_view key = key_result.value();
        std::string_view value;
        if (field.value().get_string().get(value) == simdjson::SUCCESS) {
            headers[std::string(key)] = std::string(value);
        }
    }

    return headers;
}

// Helper function: Check if string is valid JSON using simdjson
static bool is_valid_json(const std::string& s) {
    if (s.empty()) return false;

    // Skip leading whitespace (RFC 8259 allows insignificant whitespace)
    size_t i = 0;
    while (i < s.size() && std::isspace(static_cast<unsigned char>(s[i]))) {
        ++i;
    }
    if (i >= s.size()) return false;

    // Quick check: must start with { or [ (after whitespace)
    char first = s[i];
    if (first != '{' && first != '[') return false;

    // Use simdjson for proper validation
    simdjson::ondemand::parser parser;
    simdjson::padded_string padded(s);
    auto result = parser.iterate(padded);
    return result.error() == simdjson::SUCCESS;
}

// Helper function: Build JSON response string
// Returns: {"status": <code>, "body": <json_or_string>} or {"status": -1, "body": null, "error": "<message>"}
// If body is valid JSON, it's embedded directly; otherwise it's escaped as a string
// Returns error if body contains invalid UTF-8
static std::string build_json_response(long http_status, const std::string& body) {
    // Validate UTF-8 encoding
    if (!is_valid_utf8(body)) {
        return fmt::format(R"({{"status": {}, "body": null, "error": "Response contains invalid UTF-8 encoding"}})", http_status);
    }

    if (is_valid_json(body)) {
        // Body is JSON - embed directly without escaping
        return fmt::format(R"({{"status": {}, "body": {}}})", http_status, body);
    } else {
        // Body is plain text - escape as string
        return fmt::format(R"({{"status": {}, "body": "{}"}})", http_status, escape_json_string(body));
    }
}

static std::string build_json_error_response(const std::string& error_message) {
    return fmt::format(R"({{"status": -1, "body": null, "error": "{}"}})", escape_json_string(error_message));
}

// Helper function: Trim whitespace from string
static std::string trim_string(const std::string& s) {
    size_t start = s.find_first_not_of(" \t\n\r");
    if (start == std::string::npos) return "";
    size_t end = s.find_last_not_of(" \t\n\r");
    return s.substr(start, end - start + 1);
}

// Helper function: Split string by delimiter (internal)
static std::vector<std::string> split_string(const std::string& s, char delimiter) {
    std::vector<std::string> tokens;
    std::string token;
    std::istringstream tokenStream(s);
    while (std::getline(tokenStream, token, delimiter)) {
        std::string trimmed = trim_string(token);
        if (!trimmed.empty()) {
            tokens.push_back(trimmed);
        }
    }
    return tokens;
}

// Public helper: Parse comma-separated list with whitespace trimming
std::vector<std::string> parse_comma_separated_list(const std::string& s) {
    return split_string(s, ',');
}

// Public helper: Compile regex patterns from comma-separated string
std::vector<std::regex> compile_regex_patterns(const std::string& pattern_list) {
    std::vector<std::regex> patterns;
    auto pattern_strings = split_string(pattern_list, ',');
    for (const auto& pattern : pattern_strings) {
        try {
            patterns.emplace_back(pattern, std::regex::optimize);
        } catch (const std::regex_error& e) {
            LOG(WARNING) << "Invalid regex pattern: " << pattern << " - " << e.what();
        }
    }
    return patterns;
}

// Helper function: Initialize security state from RuntimeState
static void init_security_state(HttpRequestFunctionState* state, RuntimeState* runtime_state) {
    if (runtime_state == nullptr) {
        state->security_level = 3; // Default: RESTRICTED
        state->allow_private_in_allowlist = false;
        return;
    }

    state->security_level = runtime_state->http_request_security_level();
    state->allow_private_in_allowlist = runtime_state->http_request_allow_private_in_allowlist();

    // Parse IP allowlist
    const std::string& ip_allowlist = runtime_state->http_request_ip_allowlist();
    state->ip_allowlist = split_string(ip_allowlist, ',');

    // Parse regex patterns
    state->host_allowlist_patterns.clear();
    const std::string& regexp_list = runtime_state->http_request_host_allowlist_regexp();
    auto patterns = split_string(regexp_list, ',');
    for (const auto& pattern : patterns) {
        try {
            state->host_allowlist_patterns.emplace_back(pattern, std::regex::optimize);
        } catch (const std::regex_error& e) {
            LOG(WARNING) << "Invalid regex pattern in http_request_host_allowlist_regexp: " << pattern << " - "
                         << e.what();
        }
    }
}

// Public helper: Check if IP is in allowlist (exact match)
bool check_ip_allowlist(const std::string& ip, const HttpRequestFunctionState& state) {
    for (const auto& allowed : state.ip_allowlist) {
        if (ip == allowed) {
            return true;
        }
    }
    return false;
}

// Public helper: Check if host matches regex patterns
bool check_host_regex(const std::string& host, const HttpRequestFunctionState& state) {
    for (const auto& pattern : state.host_allowlist_patterns) {
        if (std::regex_match(host, pattern)) {
            return true;
        }
    }
    return false;
}

// Check if host/IP is in any allowlist (IP exact match or host regex)
static bool check_allowlist(const std::string& host, const std::vector<std::string>& resolved_ips,
                            const HttpRequestFunctionState& state) {
    // Check if any resolved IP is in IP allowlist
    for (const auto& ip : resolved_ips) {
        if (check_ip_allowlist(ip, state)) {
            return true;
        }
    }
    // Check if host matches regex patterns
    return check_host_regex(host, state);
}

// Public helper: Validate host security based on security level
// Security Levels:
//   1 = TRUSTED: Allow all requests including private IPs
//   2 = PUBLIC: Block private IPs (unless allow_private_in_allowlist + in allowlist), allow public hosts
//   3 = RESTRICTED: Require allowlist for all hosts (default)
//   4 = PARANOID: Block all requests
// Returns Status::OK() if allowed, error Status if blocked
Status validate_host_security(const std::string& url, const HttpRequestFunctionState& state,
                              std::vector<std::string>* out_resolved_ips) {
    // Protocol validation (before DNS resolution)
    // Only http:// and https:// are supported (case-insensitive)
    auto starts_with_icase = [](const std::string& str, const std::string& prefix) {
        if (str.size() < prefix.size()) return false;
        for (size_t i = 0; i < prefix.size(); ++i) {
            if (std::tolower(static_cast<unsigned char>(str[i])) !=
                std::tolower(static_cast<unsigned char>(prefix[i]))) {
                return false;
            }
        }
        return true;
    };

    bool has_http = starts_with_icase(url, "http://");
    bool has_https = starts_with_icase(url, "https://");
    if (!has_http && !has_https) {
        return Status::InvalidArgument(
                "Invalid protocol. Only http:// and https:// are supported");
    }

    int level = state.security_level;

    // Level 1 (TRUSTED): Allow everything including private IPs
    // Still resolve DNS for pinning to prevent DNS rebinding attacks
    if (level == static_cast<int>(HttpSecurityLevel::TRUSTED)) {
        if (out_resolved_ips != nullptr) {
            std::string host = extract_host_from_url(url);
            if (!host.empty()) {
                auto resolved = resolve_hostname_all_ips(host);
                if (resolved.ok()) {
                    *out_resolved_ips = resolved.value();
                }
                // If DNS resolution fails in TRUSTED mode, continue without pinning
            }
        }
        return Status::OK();
    }

    // Level 2 (PUBLIC) and Level 3 (RESTRICTED) require host extraction and private IP check
    // Level 4 (PARANOID): Block all requests
    if (level == static_cast<int>(HttpSecurityLevel::PARANOID)) {
        return Status::InvalidArgument("SSRF Protection: All requests blocked at security level 4 (PARANOID).");
    }

    // Unknown security level - block by default
    if (level != static_cast<int>(HttpSecurityLevel::PUBLIC) &&
        level != static_cast<int>(HttpSecurityLevel::RESTRICTED)) {
        return Status::InvalidArgument(fmt::format(
                "SSRF Protection: Unknown security level {}. Request blocked.", level));
    }

    // Level 2 and 3: Extract host from URL
    std::string host = extract_host_from_url(url);
    if (host.empty()) {
        return Status::InvalidArgument(fmt::format("Invalid URL: cannot extract host from '{}'", url));
    }

    // Level 2 (PUBLIC) and Level 3 (RESTRICTED): Resolve DNS first
    auto resolved_result = resolve_hostname_all_ips(host);
    if (!resolved_result.ok()) {
        // Pass through the error message from resolve_hostname_all_ips directly
        // (it already contains "DNS resolution failed for host: error")
        return Status::InvalidArgument(resolved_result.status().message());
    }
    const auto& resolved_ips = resolved_result.value();

    // Level 2 (PUBLIC): Allow all public hosts, block private IPs only
    // (No allowlist check required, skip to private IP check below)

    // Level 3 (RESTRICTED): Require allowlist for all hosts
    if (level == static_cast<int>(HttpSecurityLevel::RESTRICTED)) {
        if (state.ip_allowlist.empty() && state.host_allowlist_patterns.empty()) {
            return Status::InvalidArgument(fmt::format(
                    "SSRF Protection: '{}' blocked. Configure: "
                    "(\"http_request_ip_allowlist\" = \"<ip>\") or "
                    "(\"http_request_host_allowlist_regexp\" = \"{}\")",
                    host, host));
        }
        if (!check_allowlist(host, resolved_ips, state)) {
            return Status::InvalidArgument(
                    fmt::format("SSRF Protection: '{}' not in allowlist.", host));
        }
    }

    // Level 2 (PUBLIC) and Level 3 (RESTRICTED): Check private IP
    bool is_private = false;
    bool is_link_local = false;
    std::string private_ip;
    for (const auto& ip : resolved_ips) {
        if (is_private_ip(ip)) {
            is_private = true;
            private_ip = ip;
            // Check if it's specifically a link-local IP (cloud metadata service)
            if (is_link_local_ip(ip)) {
                is_link_local = true;
            }
            break;
        }
    }

    // Block private IPs unless allow_private_in_allowlist is enabled and in allowlist
    if (is_private) {
        if (state.allow_private_in_allowlist && check_allowlist(host, resolved_ips, state)) {
            // Private IP allowed because it's in allowlist and setting is enabled
            // Fall through to store resolved IPs for DNS pinning
        } else {
            // Different error messages based on IP type
            if (is_link_local) {
                // CRITICAL: Link-local IPs (169.254.x.x) are commonly used for cloud metadata services
                return Status::InvalidArgument(fmt::format(
                        "SSRF Protection: Link-local IP '{}' blocked (cloud metadata service). "
                        "WARNING: Allowing this IP can expose cloud credentials and sensitive metadata.",
                        private_ip));
            }
            // Standard private IP warning
            return Status::InvalidArgument(fmt::format(
                    "SSRF Protection: Private IP '{}' blocked. "
                    "To allow: Set (\"http_request_allow_private_in_allowlist\" = \"true\") AND add to allowlist. "
                    "WARNING: Allowing private IPs can expose internal services.",
                    private_ip));
        }
    }

    // Store resolved IPs for DNS pinning if requested (prevents DNS rebinding)
    // This applies to both public IPs and allowed private IPs
    if (out_resolved_ips != nullptr) {
        *out_resolved_ips = resolved_ips;
    }

    return Status::OK();
}

// Helper function: Execute HTTP request with HttpRequestConfig
static StatusOr<std::string> execute_http_request_with_config(HttpClient& client, const Slice& url_slice,
                                                               const HttpRequestConfig& config,
                                                               const HttpRequestFunctionState* state) {
    std::string url_str = url_slice.to_string();

    // SSRF protection: Validate host and get resolved IPs for DNS pinning
    // The resolved IPs are returned via out parameter to prevent DNS rebinding attacks
    // (same IPs used for validation and pinning - no TOCTOU vulnerability)
    std::vector<std::string> validated_ips;
    Status validation_status = validate_host_security(url_str, *state, &validated_ips);
    if (!validation_status.ok()) {
        return build_json_error_response(std::string(validation_status.message()));
    }

    // Pin DNS to validated IP to prevent DNS rebinding attacks
    // Extract host and port before init
    std::string host;
    int port = 0;
    if (!validated_ips.empty()) {
        host = extract_host_from_url(url_str);
        port = extract_port_from_url(url_str);

        // If port extraction failed (returns 0), determine default port from URL scheme
        // Use case-insensitive comparison for scheme detection
        if (port <= 0) {
            // extract_port_from_url returns 0 on failure, fallback to scheme-based default
            if (url_str.size() >= 8) {
                std::string scheme_prefix = url_str.substr(0, 8);
                std::transform(scheme_prefix.begin(), scheme_prefix.end(),
                               scheme_prefix.begin(), ::tolower);
                if (scheme_prefix == "https://") {
                    port = 443;
                } else {
                    port = 80;
                }
            } else {
                port = 80;  // Default to HTTP
            }
        }
    }

    // Initialize with URL
    Status init_status = client.init(url_str);
    if (!init_status.ok()) {
        return build_json_error_response(std::string(init_status.message()));
    }

    // Apply DNS pinning immediately after init
    // CRITICAL: Must be applied before execute() to prevent DNS rebinding
    if (!validated_ips.empty() && !host.empty() && port > 0) {
        // Format: "hostname:port:ip_address"
        std::string resolve_entry = fmt::format("{}:{}:{}", host, port, validated_ips[0]);
        client.set_resolve_host(resolve_entry);
    }

    // Disable CURLOPT_FAILONERROR to get HTTP error responses
    client.set_fail_on_error(false);

    // SECURITY: Disable automatic redirects to prevent SSRF bypass
    // Redirects could send requests to internal/private IPs without re-validation
    client.set_follow_redirects(false);

    // Set HTTP method
    auto method_result = parse_http_method(Slice(config.method));
    if (!method_result.ok()) {
        return build_json_error_response(std::string(method_result.status().message()));
    }
    HttpMethod method = method_result.value();
    client.set_method(method);

    // Apply headers from config
    for (const auto& [key, value] : config.headers) {
        client.set_header(key, value);
    }

    // Apply body
    if (!config.body.empty() && (method == HttpMethod::POST || method == HttpMethod::PUT || method == HttpMethod::DELETE)) {
        client.set_payload(config.body);
    }

    // Apply timeout
    client.set_timeout_ms(config.timeout_ms);

    // Apply SSL settings
    // If user requests ssl_verify=false but admin enforces SSL verification, return error
    if (!config.ssl_verify) {
        if (state->ssl_verify_required) {
            return build_json_error_response(
                    "SSL verification is enforced by administrator. "
                    "Cannot disable SSL verification (ssl_verify: false is not allowed)");
        }
        client.trust_all_ssl();
    }

    // Apply Basic Auth
    if (!config.username.empty()) {
        client.set_basic_auth(config.username, config.password);
    }

    // Execute request with streaming size check to prevent memory exhaustion
    // The callback aborts download immediately when size limit is exceeded
    std::string response;
    size_t total_size = 0;
    bool size_exceeded = false;

    auto size_check_callback = [&](const void* data, size_t length) -> bool {
        total_size += length;
        if (total_size > static_cast<size_t>(DEFAULT_MAX_RESPONSE_SIZE)) {
            size_exceeded = true;
            return false;  // Abort download immediately
        }
        response.append(static_cast<const char*>(data), length);
        return true;
    };

    Status exec_status = client.execute(size_check_callback);

    // Get HTTP status code
    long http_status = client.get_http_status();

    // Check if size limit was exceeded during streaming
    if (size_exceeded) {
        return build_json_error_response(fmt::format("Response size exceeds limit ({} bytes)", DEFAULT_MAX_RESPONSE_SIZE));
    }

    // Check for network/curl errors
    if (!exec_status.ok()) {
        return build_json_error_response(std::string(exec_status.message()));
    }

    // Return JSON response with HTTP status code and body
    return build_json_response(http_status, response);
}

// Prepare function: Initialize state
Status HttpRequestFunctions::http_request_prepare(FunctionContext* context, FunctionContext::FunctionStateScope scope) {
    if (scope != FunctionContext::FRAGMENT_LOCAL) {
        return Status::OK();
    }

    auto* state = new HttpRequestFunctionState();

    // Get admin-enforced settings from RuntimeState (passed from FE Config via Thrift)
    RuntimeState* runtime_state = context->state();
    if (runtime_state != nullptr) {
        // SSL verification setting
        state->ssl_verify_required = runtime_state->http_request_ssl_verification_required();

        // SSRF protection settings
        init_security_state(state, runtime_state);
    } else {
        state->ssl_verify_required = false;
        state->security_level = 3; // Default: RESTRICTED
    }

    context->set_function_state(scope, state);
    return Status::OK();
}

// Close function: Cleanup resources
Status HttpRequestFunctions::http_request_close(FunctionContext* context, FunctionContext::FunctionStateScope scope) {
    if (scope != FunctionContext::FRAGMENT_LOCAL) {
        return Status::OK();
    }

    auto* state = reinterpret_cast<HttpRequestFunctionState*>(context->get_function_state(scope));
    if (state != nullptr) {
        delete state;
    }

    return Status::OK();
}

// Main HTTP request function implementation with Named Parameters
// http_request(url, method, body, headers, timeout_ms, ssl_verify, username, password)
// FE always passes 8 user parameters (fills defaults for omitted named parameters),
// plus up to 2 hidden columns (nondeterministic, _is_returning_random_value), so will be 10.
StatusOr<ColumnPtr> HttpRequestFunctions::http_request(FunctionContext* context, const Columns& columns) {
    RETURN_IF_COLUMNS_ONLY_NULL(columns);

    size_t num_rows = columns[0]->size();

    // Get function state
    auto* state = reinterpret_cast<HttpRequestFunctionState*>(
            context->get_function_state(FunctionContext::FRAGMENT_LOCAL));
    if (state == nullptr) {
        return Status::InternalError("HTTP request function state not initialized");
    }

    // Create ColumnViewers for all parameters
    auto url_viewer = ColumnViewer<TYPE_VARCHAR>(columns[0]);
    auto method_viewer = ColumnViewer<TYPE_VARCHAR>(columns[1]);
    auto body_viewer = ColumnViewer<TYPE_VARCHAR>(columns[2]);
    auto headers_viewer = ColumnViewer<TYPE_VARCHAR>(columns[3]);
    auto timeout_viewer = ColumnViewer<TYPE_INT>(columns[4]);
    auto ssl_verify_viewer = ColumnViewer<TYPE_BOOLEAN>(columns[5]);
    auto username_viewer = ColumnViewer<TYPE_VARCHAR>(columns[6]);
    auto password_viewer = ColumnViewer<TYPE_VARCHAR>(columns[7]);

    // Build result column
    ColumnBuilder<TYPE_VARCHAR> result(num_rows);

    // Reuse HttpClient across rows for better performance
    HttpClient client;

    // Timeout bounds
    constexpr int32_t MIN_TIMEOUT_MS = 1;
    constexpr int32_t MAX_TIMEOUT_MS = 300000;

    // Process each row
    for (size_t i = 0; i < num_rows; i++) {
        if (url_viewer.is_null(i)) {
            result.append_null();
            continue;
        }

        // Build config from individual columns
        HttpRequestConfig config;

        // url (required)
        Slice url_slice = url_viewer.value(i);

        // method (default: 'GET')
        if (!method_viewer.is_null(i)) {
            config.method = method_viewer.value(i).to_string();
        }

        // body (default: '')
        if (!body_viewer.is_null(i)) {
            config.body = body_viewer.value(i).to_string();
        }

        // headers (default: '{}')
        if (!headers_viewer.is_null(i)) {
            std::string headers_json = headers_viewer.value(i).to_string();
            auto headers_result = parse_headers_json(headers_json);
            if (!headers_result.ok()) {
                result.append(Slice(build_json_error_response("Invalid headers JSON format")));
                continue;
            }
            config.headers = headers_result.value();
        }

        // timeout_ms (default: 30000, clamped to [1, 300000])
        if (!timeout_viewer.is_null(i)) {
            int32_t timeout = timeout_viewer.value(i);
            if (timeout < MIN_TIMEOUT_MS) {
                timeout = MIN_TIMEOUT_MS;
            } else if (timeout > MAX_TIMEOUT_MS) {
                timeout = MAX_TIMEOUT_MS;
            }
            config.timeout_ms = timeout;
        }

        // ssl_verify (default: true)
        if (!ssl_verify_viewer.is_null(i)) {
            config.ssl_verify = ssl_verify_viewer.value(i);
        }

        // username (default: '')
        if (!username_viewer.is_null(i)) {
            config.username = username_viewer.value(i).to_string();
        }

        // password (default: '')
        if (!password_viewer.is_null(i)) {
            config.password = password_viewer.value(i).to_string();
        }

        // Execute HTTP request with config
        auto response = execute_http_request_with_config(client, url_slice, config, state);

        if (!response.ok()) {
            result.append_null();
            context->add_warning(std::string(response.status().message()).c_str());
            continue;
        }

        result.append(Slice(response.value()));
    }

    return result.build(ColumnHelper::is_all_const(columns));
}

} // namespace starrocks

#include "gen_cpp/opcode/HttpRequestFunctions.inc"
