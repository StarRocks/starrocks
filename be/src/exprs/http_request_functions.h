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

#pragma once

#include <regex>
#include <string>
#include <vector>

#include "column/column.h"
#include "column/column_builder.h"
#include "column/column_viewer.h"
#include "exprs/function_context.h"
#include "exprs/function_helper.h"

namespace starrocks {

// Security levels for http_request() SSRF protection
enum class HttpSecurityLevel : int {
    TRUSTED = 1,    // Allow all requests including private IPs
    PUBLIC = 2,     // Block private IPs, allow all public hosts
    RESTRICTED = 3, // Require allowlist (default)
    PARANOID = 4    // Block all requests
};

// State structure for HTTP request function
// Stores admin-enforced settings from FE Config
struct HttpRequestFunctionState {
    // SSL verification (global setting from Config)
    bool ssl_verify_required = false;

    // SSRF protection settings (from FE Config)
    int security_level = 3; // Default: RESTRICTED
    std::vector<std::string> ip_allowlist;
    std::vector<std::regex> host_allowlist_patterns;
    bool allow_private_in_allowlist = false; // Allow private IPs if in allowlist
};

// Helper functions for SSRF protection (exposed for testing)
// Split string by delimiter and trim whitespace from each element
std::vector<std::string> parse_comma_separated_list(const std::string& s);

// Compile regex patterns from comma-separated string, skipping invalid patterns
std::vector<std::regex> compile_regex_patterns(const std::string& pattern_list);

// Check if IP is in allowlist (exact match)
bool check_ip_allowlist(const std::string& ip, const HttpRequestFunctionState& state);

// Check if host matches regex patterns
bool check_host_regex(const std::string& host, const HttpRequestFunctionState& state);

// Validate URL against security settings (level, private IP check, allowlist)
// If out_resolved_ips is provided, the resolved IPs will be stored there for DNS pinning
// This prevents DNS rebinding attacks by using the same IPs for validation and pinning
Status validate_host_security(const std::string& url, const HttpRequestFunctionState& state,
                              std::vector<std::string>* out_resolved_ips = nullptr);

// Validate HTTP method string (exposed for testing)
// Returns StatusOr<bool> - OK with true if valid, error status if invalid
StatusOr<bool> validate_http_method(const std::string& method);

class HttpRequestFunctions {
public:
    /**
     * HTTP request function with Named Parameters
     *
     * Signature:
     *   http_request(
     *     url VARCHAR,           -- Required: The URL to request
     *     method VARCHAR,        -- Default: 'GET'
     *     body VARCHAR,          -- Default: ''
     *     headers VARCHAR,       -- Default: '{}' (JSON object)
     *     timeout_ms INT,        -- Default: 30000
     *     ssl_verify BOOLEAN,    -- Default: true
     *     username VARCHAR,      -- Default: ''
     *     password VARCHAR       -- Default: ''
     *   ) -> VARCHAR
     *
     * Usage:
     *   SELECT http_request(url => 'https://api.example.com');
     *   SELECT http_request(url => 'https://api.example.com', method => 'POST', body => '{}');
     */
    DEFINE_VECTORIZED_FN(http_request);

    /**
     * Prepare function - Called once per fragment
     * Reads global Config and initializes HttpRequestFunctionState
     */
    static Status http_request_prepare(FunctionContext* context, FunctionContext::FunctionStateScope scope);

    /**
     * Close function - Called once per fragment
     * Cleanup resources allocated in prepare
     */
    static Status http_request_close(FunctionContext* context, FunctionContext::FunctionStateScope scope);
};

} // namespace starrocks
