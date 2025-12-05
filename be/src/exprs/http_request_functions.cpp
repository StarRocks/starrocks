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
static HttpMethod parse_http_method(const Slice& method_str) {
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

    return HttpMethod::GET; // Default to GET
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
    // Quick check: must start with { or [
    char first = s[0];
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

// Helper function: Split string by delimiter
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

// Helper function: Initialize security state from RuntimeState
static void init_security_state(HttpRequestFunctionState* state, RuntimeState* runtime_state) {
    if (runtime_state == nullptr) {
        state->security_level = 3; // Default: RESTRICTED
        return;
    }

    state->security_level = runtime_state->http_request_security_level();

    // Parse host allowlist
    const std::string& allowlist = runtime_state->http_request_host_allowlist();
    state->host_allowlist = split_string(allowlist, ',');

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

// Helper function: Check if host is in allowlist
static bool check_host_allowlist(const std::string& host, const HttpRequestFunctionState& state) {
    // Check exact match allowlist
    for (const auto& allowed : state.host_allowlist) {
        if (host == allowed) {
            return true;
        }
    }

    // Check regex patterns
    for (const auto& pattern : state.host_allowlist_patterns) {
        if (std::regex_match(host, pattern)) {
            return true;
        }
    }

    return false;
}

// Helper function: Validate host security based on security level
// Returns Status::OK() if allowed, error Status if blocked
static Status validate_host_security(const std::string& url, const HttpRequestFunctionState& state) {
    int level = state.security_level;

    // Security level names for error messages
    static const char* level_names[] = {"", "TRUSTED", "PUBLIC", "RESTRICTED", "PARANOID"};
    const char* level_name = (level >= 1 && level <= 4) ? level_names[level] : "UNKNOWN";

    // Level 1 (TRUSTED): Allow everything
    if (level == 1) {
        return Status::OK();
    }

    // Extract host from URL
    std::string host = extract_host_from_url(url);
    if (host.empty()) {
        return Status::InvalidArgument(fmt::format("Invalid URL format: could not extract host from '{}'", url));
    }

    // Level 2+: Resolve DNS and check for private IPs
    if (level >= 2) {
        auto resolved_result = resolve_hostname_all_ips(host);
        if (!resolved_result.ok()) {
            return Status::InvalidArgument(fmt::format("DNS resolution failed for host '{}': {}", host,
                                                       resolved_result.status().message()));
        }

        const auto& resolved_ips = resolved_result.value();
        for (const auto& ip : resolved_ips) {
            if (is_private_ip(ip)) {
                if (level == 4) {
                    // Level 4 (PARANOID): Always block private IPs even if in allowlist
                    return Status::InvalidArgument(fmt::format(
                            "SSRF Protection: Private IP '{}' is blocked in {} mode (level {}). "
                            "Host '{}' resolved to a private network address. "
                            "At security level 4 (PARANOID), private IPs are always blocked even if in allowlist. "
                            "To allow this request, lower the security level using "
                            "ADMIN SET FRONTEND CONFIG (\"http_request_security_level\" = \"1\").",
                            ip, level_name, level, host));
                } else {
                    return Status::InvalidArgument(fmt::format(
                            "SSRF Protection: Private IP '{}' is blocked in {} mode (level {}). "
                            "Host '{}' resolved to a private network address. "
                            "Private network ranges (127.0.0.0/8, 10.0.0.0/8, 172.16.0.0/12, 192.168.0.0/16, etc.) "
                            "are not allowed at this security level. "
                            "Use security level 1 (TRUSTED) to allow private IPs: "
                            "ADMIN SET FRONTEND CONFIG (\"http_request_security_level\" = \"1\").",
                            ip, level_name, level, host));
                }
            }
        }
    }

    // Level 3+: Check allowlist
    if (level >= 3) {
        // If both allowlists are empty, block all requests
        if (state.host_allowlist.empty() && state.host_allowlist_patterns.empty()) {
            return Status::InvalidArgument(fmt::format(
                    "SSRF Protection: Host '{}' is blocked in {} mode (level {}). "
                    "Allowlist is empty - no hosts are permitted. "
                    "Configure allowlist using ADMIN SET FRONTEND CONFIG: "
                    "(\"http_request_host_allowlist\" = \"api.example.com,other.com\") or "
                    "(\"http_request_host_allowlist_regexp\" = \".*\\\\.example\\\\.com\"), "
                    "or use security level 2 (PUBLIC) to allow all public hosts.",
                    host, level_name, level));
        }

        if (!check_host_allowlist(host, state)) {
            return Status::InvalidArgument(fmt::format(
                    "SSRF Protection: Host '{}' is blocked in {} mode (level {}). "
                    "Host is not in the configured allowlist. "
                    "Add '{}' to allowlist using ADMIN SET FRONTEND CONFIG: "
                    "(\"http_request_host_allowlist\" = \"{}\") or update http_request_host_allowlist_regexp.",
                    host, level_name, level, host, host));
        }
    }

    return Status::OK();
}

// Helper function: Execute HTTP request with HttpRequestConfig
static StatusOr<std::string> execute_http_request_with_config(HttpClient& client, const Slice& url_slice,
                                                               const HttpRequestConfig& config,
                                                               const HttpRequestFunctionState* state) {
    std::string url_str = url_slice.to_string();

    // SSRF protection: Validate host before making the request
    Status security_status = validate_host_security(url_str, *state);
    if (!security_status.ok()) {
        return build_json_error_response(std::string(security_status.message()));
    }

    // Initialize with URL
    Status init_status = client.init(url_str);
    if (!init_status.ok()) {
        return build_json_error_response(std::string(init_status.message()));
    }

    // Disable CURLOPT_FAILONERROR to get HTTP error responses
    client.set_fail_on_error(false);

    // Set HTTP method
    HttpMethod method = parse_http_method(Slice(config.method));
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
