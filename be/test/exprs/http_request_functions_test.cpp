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

#include <glog/logging.h>
#include <gtest/gtest.h>

#include <memory>

#include "column/column_helper.h"
#include "column/map_column.h"
#include "exprs/function_helper.h"
#include "exprs/mock_vectorized_expr.h"
#include "exprs/http_request_functions.h"
#include "runtime/runtime_state.h"
#include "testutil/assert.h"
#include "testutil/parallel_test.h"

namespace starrocks {

class HttpRequestFunctionsTest : public ::testing::Test {
protected:
    void SetUp() override {
        _runtime_state = std::make_unique<RuntimeState>(TQueryGlobals());
        _ctx.reset(FunctionContext::create_test_context());
    }

    void TearDown() override {
        _ctx.reset();
        _runtime_state.reset();
    }

    // Helper function to create 8-column input for http_request function
    // Columns: url, method, body, headers, timeout_ms, ssl_verify, username, password
    Columns create_http_request_columns(const ColumnPtr& url_column, size_t num_rows) {
        Columns columns;
        columns.emplace_back(url_column);

        // method (default: 'GET')
        auto method_column = BinaryColumn::create();
        for (size_t i = 0; i < num_rows; i++) {
            method_column->append("GET");
        }
        columns.emplace_back(method_column);

        // body (default: '')
        auto body_column = BinaryColumn::create();
        for (size_t i = 0; i < num_rows; i++) {
            body_column->append("");
        }
        columns.emplace_back(body_column);

        // headers (default: '{}')
        auto headers_column = BinaryColumn::create();
        for (size_t i = 0; i < num_rows; i++) {
            headers_column->append("{}");
        }
        columns.emplace_back(headers_column);

        // timeout_ms (default: 30000)
        auto timeout_column = Int32Column::create();
        for (size_t i = 0; i < num_rows; i++) {
            timeout_column->append(30000);
        }
        columns.emplace_back(timeout_column);

        // ssl_verify (default: true)
        auto ssl_verify_column = BooleanColumn::create();
        for (size_t i = 0; i < num_rows; i++) {
            ssl_verify_column->append(true);
        }
        columns.emplace_back(ssl_verify_column);

        // username (default: '')
        auto username_column = BinaryColumn::create();
        for (size_t i = 0; i < num_rows; i++) {
            username_column->append("");
        }
        columns.emplace_back(username_column);

        // password (default: '')
        auto password_column = BinaryColumn::create();
        for (size_t i = 0; i < num_rows; i++) {
            password_column->append("");
        }
        columns.emplace_back(password_column);

        return columns;
    }

    // Helper to create 8 null columns for null input test
    Columns create_null_columns(size_t num_rows) {
        Columns columns;
        for (int i = 0; i < 8; i++) {
            columns.emplace_back(ColumnHelper::create_const_null_column(num_rows));
        }
        return columns;
    }

    std::unique_ptr<RuntimeState> _runtime_state;
    std::unique_ptr<FunctionContext> _ctx;
};

// Basic prepare and close test
TEST_F(HttpRequestFunctionsTest, prepareCloseTest) {
    // Test http_request_prepare
    FunctionContext::FunctionStateScope scope = FunctionContext::FRAGMENT_LOCAL;
    ASSERT_OK(HttpRequestFunctions::http_request_prepare(_ctx.get(), scope));

    // Verify state is created
    auto* state = reinterpret_cast<HttpRequestFunctionState*>(_ctx->get_function_state(scope));
    ASSERT_NE(nullptr, state);

    // Verify default values
    // ssl_verify_required is true by default (secure-by-default)
    ASSERT_TRUE(state->ssl_verify_required);

    // Test http_request_close
    ASSERT_OK(HttpRequestFunctions::http_request_close(_ctx.get(), scope));
}

// Test NULL input handling - when URL is NULL, result should be NULL
TEST_F(HttpRequestFunctionsTest, nullInputTest) {
    FunctionContext::FunctionStateScope scope = FunctionContext::FRAGMENT_LOCAL;
    ASSERT_OK(HttpRequestFunctions::http_request_prepare(_ctx.get(), scope));

    // Create 8 null columns (url, method, body, headers, timeout_ms, ssl_verify, username, password)
    Columns columns = create_null_columns(10);

    auto result = HttpRequestFunctions::http_request(_ctx.get(), columns);
    ASSERT_TRUE(result.ok());
    ASSERT_TRUE(result.value()->only_null());
    ASSERT_EQ(10, result.value()->size());

    ASSERT_OK(HttpRequestFunctions::http_request_close(_ctx.get(), scope));
}

// Test empty URL - returns JSON error response with status -1
TEST_F(HttpRequestFunctionsTest, emptyUrlTest) {
    FunctionContext::FunctionStateScope scope = FunctionContext::FRAGMENT_LOCAL;
    ASSERT_OK(HttpRequestFunctions::http_request_prepare(_ctx.get(), scope));

    // Create URL column with empty string
    auto url_column = BinaryColumn::create();
    url_column->append("");
    Columns columns = create_http_request_columns(url_column, 1);

    auto result = HttpRequestFunctions::http_request(_ctx.get(), columns);
    ASSERT_TRUE(result.ok());
    ASSERT_EQ(1, result.value()->size());
    // Empty URL returns JSON error response, not NULL
    ASSERT_FALSE(result.value()->is_null(0));
    // Response should contain status -1 (error)
    auto* binary_col = ColumnHelper::get_binary_column(result.value().get());
    std::string response = binary_col->get_slice(0).to_string();
    ASSERT_TRUE(response.find("\"status\": -1") != std::string::npos ||
                response.find("\"status\":-1") != std::string::npos);

    ASSERT_OK(HttpRequestFunctions::http_request_close(_ctx.get(), scope));
}

// Test invalid URL format - returns JSON error response with status -1
TEST_F(HttpRequestFunctionsTest, invalidUrlTest) {
    FunctionContext::FunctionStateScope scope = FunctionContext::FRAGMENT_LOCAL;
    ASSERT_OK(HttpRequestFunctions::http_request_prepare(_ctx.get(), scope));

    // Create URL column with invalid URL
    auto url_column = BinaryColumn::create();
    url_column->append("not a valid url");
    Columns columns = create_http_request_columns(url_column, 1);

    auto result = HttpRequestFunctions::http_request(_ctx.get(), columns);
    ASSERT_TRUE(result.ok());
    ASSERT_EQ(1, result.value()->size());
    // Invalid URL returns JSON error response, not NULL
    ASSERT_FALSE(result.value()->is_null(0));
    // Response should contain status -1 (error)
    auto* binary_col = ColumnHelper::get_binary_column(result.value().get());
    std::string response = binary_col->get_slice(0).to_string();
    ASSERT_TRUE(response.find("\"status\": -1") != std::string::npos ||
                response.find("\"status\":-1") != std::string::npos);

    ASSERT_OK(HttpRequestFunctions::http_request_close(_ctx.get(), scope));
}

// Note: Full HTTP functionality testing requires a running HTTP server.
// These tests will be covered in SQL integration tests using:
// 1. Local HTTP server with libevent
// 2. Real-world integration tests
// See: test/sql/test_http_request_function/

//=============================================================================
// Security Configuration Tests
//=============================================================================

// Test default security level is set correctly
TEST_F(HttpRequestFunctionsTest, securityLevelDefaultTest) {
    FunctionContext::FunctionStateScope scope = FunctionContext::FRAGMENT_LOCAL;
    ASSERT_OK(HttpRequestFunctions::http_request_prepare(_ctx.get(), scope));

    auto* state = reinterpret_cast<HttpRequestFunctionState*>(_ctx->get_function_state(scope));
    ASSERT_NE(nullptr, state);
    // Default security level should be 3 (RESTRICTED)
    ASSERT_EQ(3, state->security_level);

    ASSERT_OK(HttpRequestFunctions::http_request_close(_ctx.get(), scope));
}

// Test security state initialization with empty allowlists
TEST_F(HttpRequestFunctionsTest, securityStateEmptyAllowlistTest) {
    FunctionContext::FunctionStateScope scope = FunctionContext::FRAGMENT_LOCAL;
    ASSERT_OK(HttpRequestFunctions::http_request_prepare(_ctx.get(), scope));

    auto* state = reinterpret_cast<HttpRequestFunctionState*>(_ctx->get_function_state(scope));
    ASSERT_NE(nullptr, state);
    // Empty allowlists by default
    ASSERT_TRUE(state->ip_allowlist.empty());
    ASSERT_TRUE(state->host_allowlist_patterns.empty());

    ASSERT_OK(HttpRequestFunctions::http_request_close(_ctx.get(), scope));
}

// Test IP allowlist parsing with whitespace trimming
TEST_F(HttpRequestFunctionsTest, ipAllowlistParseTest) {
    // Test that parse_comma_separated_list correctly handles whitespace for IP addresses
    auto result = parse_comma_separated_list(" 192.168.1.1 , 10.0.0.1 ");
    ASSERT_EQ(2, result.size());
    ASSERT_EQ("192.168.1.1", result[0]);
    ASSERT_EQ("10.0.0.1", result[1]);
}

// Test allowlist regex pattern compilation
TEST_F(HttpRequestFunctionsTest, allowlistRegexPatternTest) {
    // Test valid regex pattern
    auto patterns = compile_regex_patterns(".*\\.example\\.com,api\\.github\\.com");
    ASSERT_EQ(2, patterns.size());

    // Verify regex matches
    ASSERT_TRUE(std::regex_match("api.example.com", patterns[0]));
    ASSERT_TRUE(std::regex_match("www.example.com", patterns[0]));
    ASSERT_TRUE(std::regex_match("api.github.com", patterns[1]));
    ASSERT_FALSE(std::regex_match("evil.com", patterns[0]));
}

// Test check_ip_allowlist with exact IP match
TEST_F(HttpRequestFunctionsTest, checkIpAllowlistTest) {
    HttpRequestFunctionState state;
    state.ip_allowlist = {"192.168.1.1", "10.0.0.1"};

    // Exact IP match should pass
    ASSERT_TRUE(check_ip_allowlist("192.168.1.1", state));
    ASSERT_TRUE(check_ip_allowlist("10.0.0.1", state));

    // Non-matching IP should fail
    ASSERT_FALSE(check_ip_allowlist("172.16.0.1", state));
    ASSERT_FALSE(check_ip_allowlist("127.0.0.1", state));
}

// Test check_ip_allowlist with IPv6 addresses
TEST_F(HttpRequestFunctionsTest, checkIpAllowlistIPv6Test) {
    HttpRequestFunctionState state;
    state.ip_allowlist = {"::1", "2001:db8::1", "fe80::1"};

    // Exact IPv6 match should pass
    ASSERT_TRUE(check_ip_allowlist("::1", state));
    ASSERT_TRUE(check_ip_allowlist("2001:db8::1", state));
    ASSERT_TRUE(check_ip_allowlist("fe80::1", state));

    // Non-matching IPv6 should fail
    ASSERT_FALSE(check_ip_allowlist("::2", state));
    ASSERT_FALSE(check_ip_allowlist("2001:db8::2", state));
    ASSERT_FALSE(check_ip_allowlist("fc00::1", state));
}

// Test check_ip_allowlist with mixed IPv4 and IPv6 addresses
TEST_F(HttpRequestFunctionsTest, checkIpAllowlistMixedTest) {
    HttpRequestFunctionState state;
    state.ip_allowlist = {"192.168.1.1", "::1", "2001:db8::1"};

    // Both IPv4 and IPv6 matches should pass
    ASSERT_TRUE(check_ip_allowlist("192.168.1.1", state));
    ASSERT_TRUE(check_ip_allowlist("::1", state));
    ASSERT_TRUE(check_ip_allowlist("2001:db8::1", state));

    // Non-matching should fail
    ASSERT_FALSE(check_ip_allowlist("192.168.1.2", state));
    ASSERT_FALSE(check_ip_allowlist("::2", state));
}

// Test check_host_regex with regex patterns for hostname matching
TEST_F(HttpRequestFunctionsTest, checkHostRegexTest) {
    HttpRequestFunctionState state;
    state.host_allowlist_patterns = compile_regex_patterns(".*\\.example\\.com");

    // Regex pattern match should pass
    ASSERT_TRUE(check_host_regex("api.example.com", state));
    ASSERT_TRUE(check_host_regex("www.example.com", state));
    ASSERT_TRUE(check_host_regex("test.example.com", state));

    // Non-matching host should fail
    ASSERT_FALSE(check_host_regex("evil.com", state));
    ASSERT_FALSE(check_host_regex("example.com.evil.com", state));
}

// Test IP allowlist and host regex (OR condition for security check)
TEST_F(HttpRequestFunctionsTest, ipAndHostRegexOrConditionTest) {
    HttpRequestFunctionState state;
    state.ip_allowlist = {"192.168.1.1"};
    state.host_allowlist_patterns = compile_regex_patterns(".*\\.api\\.com");

    // IP exact match should pass
    ASSERT_TRUE(check_ip_allowlist("192.168.1.1", state));

    // Host regex match should pass
    ASSERT_TRUE(check_host_regex("test.api.com", state));
    ASSERT_TRUE(check_host_regex("v1.api.com", state));

    // Neither match should fail
    ASSERT_FALSE(check_ip_allowlist("10.0.0.1", state));
    ASSERT_FALSE(check_host_regex("evil.com", state));
}

// Test validate_host_security at Level 1 (TRUSTED) - allows everything
TEST_F(HttpRequestFunctionsTest, securityLevel1AllowsEverythingTest) {
    HttpRequestFunctionState state;
    state.security_level = 1;  // TRUSTED

    // Private IPs should be allowed at level 1
    ASSERT_TRUE(validate_host_security("http://127.0.0.1/api", state).ok());
    ASSERT_TRUE(validate_host_security("http://192.168.1.1/api", state).ok());
    ASSERT_TRUE(validate_host_security("http://10.0.0.1/api", state).ok());

    // Public hosts should be allowed
    ASSERT_TRUE(validate_host_security("http://example.com/api", state).ok());
}

// Test validate_host_security returns resolved IPs for DNS pinning at Level 1 (TRUSTED)
TEST_F(HttpRequestFunctionsTest, securityLevel1ReturnsDnsPinningIpsTest) {
    HttpRequestFunctionState state;
    state.security_level = 1;  // TRUSTED

    std::vector<std::string> resolved_ips;
    // Use localhost which should always resolve
    auto status = validate_host_security("http://127.0.0.1/api", state, &resolved_ips);
    ASSERT_TRUE(status.ok());
    // For IP addresses, resolved_ips should contain the IP itself
    ASSERT_FALSE(resolved_ips.empty());
    ASSERT_EQ("127.0.0.1", resolved_ips[0]);
}

// Test validate_host_security returns resolved IPs for DNS pinning at Level 2 (PUBLIC)
TEST_F(HttpRequestFunctionsTest, securityLevel2ReturnsDnsPinningIpsTest) {
    HttpRequestFunctionState state;
    state.security_level = 2;  // PUBLIC
    state.ip_allowlist = {"127.0.0.1"};
    state.allow_private_in_allowlist = true;

    std::vector<std::string> resolved_ips;
    auto status = validate_host_security("http://127.0.0.1/api", state, &resolved_ips);
    ASSERT_TRUE(status.ok());
    // Should have resolved IPs for DNS pinning
    ASSERT_FALSE(resolved_ips.empty());
    ASSERT_EQ("127.0.0.1", resolved_ips[0]);
}

// Test validate_host_security returns resolved IPs for DNS pinning at Level 3 (RESTRICTED)
TEST_F(HttpRequestFunctionsTest, securityLevel3ReturnsDnsPinningIpsTest) {
    HttpRequestFunctionState state;
    state.security_level = 3;  // RESTRICTED
    state.ip_allowlist = {"127.0.0.1"};
    state.allow_private_in_allowlist = true;

    std::vector<std::string> resolved_ips;
    auto status = validate_host_security("http://127.0.0.1/api", state, &resolved_ips);
    ASSERT_TRUE(status.ok());
    // Should have resolved IPs for DNS pinning
    ASSERT_FALSE(resolved_ips.empty());
    ASSERT_EQ("127.0.0.1", resolved_ips[0]);
}

// Test validate_host_security does NOT return IPs at Level 4 (PARANOID) - request blocked
TEST_F(HttpRequestFunctionsTest, securityLevel4NoDnsPinningTest) {
    HttpRequestFunctionState state;
    state.security_level = 4;  // PARANOID

    std::vector<std::string> resolved_ips;
    auto status = validate_host_security("http://127.0.0.1/api", state, &resolved_ips);
    // Request should be blocked
    ASSERT_FALSE(status.ok());
    // No DNS resolution should happen
    ASSERT_TRUE(resolved_ips.empty());
}

// Test validate_host_security at Level 2 (PUBLIC) - public hosts allowed, private IPs need allowlist
TEST_F(HttpRequestFunctionsTest, securityLevel2PublicOpenPrivateNeedsAllowlistTest) {
    HttpRequestFunctionState state;
    state.security_level = 2;  // PUBLIC

    // Public hosts should be allowed without allowlist at level 2
    ASSERT_TRUE(validate_host_security("http://example.com/api", state).ok());

    // Private IPs should be blocked without allowlist at level 2
    auto status1 = validate_host_security("http://127.0.0.1/api", state);
    ASSERT_FALSE(status1.ok());
    ASSERT_TRUE(status1.message().find("Private IP") != std::string::npos ||
                status1.message().find("allowlist") != std::string::npos);

    auto status2 = validate_host_security("http://192.168.1.1/api", state);
    ASSERT_FALSE(status2.ok());

    auto status3 = validate_host_security("http://10.0.0.1/api", state);
    ASSERT_FALSE(status3.ok());

    auto status4 = validate_host_security("http://172.16.0.1/api", state);
    ASSERT_FALSE(status4.ok());
}

// Test validate_host_security at Level 2 (PUBLIC) - private IPs allowed with allowlist + allow_private_in_allowlist
TEST_F(HttpRequestFunctionsTest, securityLevel2PrivateIPAllowedWithAllowlistTest) {
    HttpRequestFunctionState state;
    state.security_level = 2;  // PUBLIC
    state.ip_allowlist = {"127.0.0.1", "192.168.1.1"};
    state.allow_private_in_allowlist = true;  // Must enable this flag

    // Private IPs in allowlist should be allowed at level 2 with allow_private_in_allowlist=true
    ASSERT_TRUE(validate_host_security("http://127.0.0.1/api", state).ok());
    ASSERT_TRUE(validate_host_security("http://192.168.1.1/api", state).ok());

    // Private IPs NOT in allowlist should still be blocked
    auto status = validate_host_security("http://10.0.0.1/api", state);
    ASSERT_FALSE(status.ok());
}

// Test validate_host_security at Level 2 - private IPs blocked even in allowlist without allow_private_in_allowlist
TEST_F(HttpRequestFunctionsTest, securityLevel2PrivateIPBlockedWithoutFlagTest) {
    HttpRequestFunctionState state;
    state.security_level = 2;  // PUBLIC
    state.ip_allowlist = {"127.0.0.1", "192.168.1.1"};
    state.allow_private_in_allowlist = false;  // Default: false

    // Private IPs in allowlist should be BLOCKED without allow_private_in_allowlist flag
    auto status1 = validate_host_security("http://127.0.0.1/api", state);
    ASSERT_FALSE(status1.ok());

    auto status2 = validate_host_security("http://192.168.1.1/api", state);
    ASSERT_FALSE(status2.ok());
}

// Test validate_host_security at Level 3 (RESTRICTED) - requires allowlist
TEST_F(HttpRequestFunctionsTest, securityLevel3RequiresAllowlistTest) {
    HttpRequestFunctionState state;
    state.security_level = 3;  // RESTRICTED
    // Empty allowlists

    // With empty allowlist, all requests should be blocked
    auto status = validate_host_security("http://example.com/api", state);
    ASSERT_FALSE(status.ok());
    ASSERT_TRUE(status.message().find("allowlist") != std::string::npos ||
                status.message().find("Allowlist") != std::string::npos);
}

// Test validate_host_security at Level 3 with IP allowlist configured
// Note: This test uses IP addresses to avoid DNS resolution dependencies
TEST_F(HttpRequestFunctionsTest, securityLevel3AllowlistWorksTest) {
    HttpRequestFunctionState state;
    state.security_level = 3;  // RESTRICTED
    // Use IP allowlist for direct IP access
    state.ip_allowlist = {"93.184.216.34", "1.2.3.4"};  // example.com's IP and test IP

    // IPs in allowlist should be allowed (using public IPs)
    ASSERT_TRUE(validate_host_security("http://93.184.216.34/v1", state).ok());
    ASSERT_TRUE(validate_host_security("https://1.2.3.4/query", state).ok());

    // IPs not in allowlist should be blocked
    auto status = validate_host_security("http://5.6.7.8/api", state);
    ASSERT_FALSE(status.ok());
}

// Test validate_host_security at Level 3 - private IP allowed with allowlist + allow_private_in_allowlist
TEST_F(HttpRequestFunctionsTest, securityLevel3PrivateIPAllowedWithAllowlistTest) {
    HttpRequestFunctionState state;
    state.security_level = 3;  // RESTRICTED
    state.ip_allowlist = {"127.0.0.1", "192.168.1.1"};
    state.allow_private_in_allowlist = true;  // Must enable this flag

    // Private IPs in allowlist should be allowed at level 3 with allow_private_in_allowlist=true
    ASSERT_TRUE(validate_host_security("http://127.0.0.1/api", state).ok());
    ASSERT_TRUE(validate_host_security("http://192.168.1.1/api", state).ok());

    // Private IPs NOT in allowlist should be blocked
    auto status = validate_host_security("http://10.0.0.1/api", state);
    ASSERT_FALSE(status.ok());
}

// Test validate_host_security at Level 3 - private IP blocked even in allowlist without flag
TEST_F(HttpRequestFunctionsTest, securityLevel3PrivateIPBlockedWithoutFlagTest) {
    HttpRequestFunctionState state;
    state.security_level = 3;  // RESTRICTED
    state.ip_allowlist = {"127.0.0.1", "192.168.1.1"};
    state.allow_private_in_allowlist = false;  // Default: false

    // Private IPs in allowlist should be BLOCKED without allow_private_in_allowlist flag
    auto status1 = validate_host_security("http://127.0.0.1/api", state);
    ASSERT_FALSE(status1.ok());

    auto status2 = validate_host_security("http://192.168.1.1/api", state);
    ASSERT_FALSE(status2.ok());
}

// Test validate_host_security with IPv6 addresses in allowlist
TEST_F(HttpRequestFunctionsTest, securityLevel3IPv6AllowlistTest) {
    HttpRequestFunctionState state;
    state.security_level = 3;  // RESTRICTED
    state.ip_allowlist = {"::1", "fe80::1", "2001:db8::1"};
    state.allow_private_in_allowlist = true;  // Enable for private IPv6

    // Private IPv6 loopback in allowlist should be allowed
    ASSERT_TRUE(validate_host_security("http://[::1]/api", state).ok());

    // Private IPv6 link-local in allowlist should be allowed
    ASSERT_TRUE(validate_host_security("http://[fe80::1]/api", state).ok());

    // Public IPv6 in allowlist should be allowed
    ASSERT_TRUE(validate_host_security("http://[2001:db8::1]/api", state).ok());

    // IPv6 NOT in allowlist should be blocked
    auto status = validate_host_security("http://[2001:db8::2]/api", state);
    ASSERT_FALSE(status.ok());
}

// Test validate_host_security with mixed IPv4 and IPv6 in allowlist
TEST_F(HttpRequestFunctionsTest, securityLevel3MixedIPAllowlistTest) {
    HttpRequestFunctionState state;
    state.security_level = 3;  // RESTRICTED
    state.ip_allowlist = {"192.168.1.1", "::1", "2001:db8::1"};
    state.allow_private_in_allowlist = true;

    // IPv4 in allowlist should be allowed
    ASSERT_TRUE(validate_host_security("http://192.168.1.1/api", state).ok());

    // IPv6 in allowlist should be allowed
    ASSERT_TRUE(validate_host_security("http://[::1]/api", state).ok());
    ASSERT_TRUE(validate_host_security("http://[2001:db8::1]/api", state).ok());

    // IPs NOT in allowlist should be blocked
    ASSERT_FALSE(validate_host_security("http://192.168.1.2/api", state).ok());
    ASSERT_FALSE(validate_host_security("http://[::2]/api", state).ok());
}

// Test validate_host_security at Level 4 (PARANOID) - blocks ALL requests
TEST_F(HttpRequestFunctionsTest, securityLevel4BlocksAllRequestsTest) {
    HttpRequestFunctionState state;
    state.security_level = 4;  // PARANOID
    state.ip_allowlist = {"127.0.0.1"};
    state.host_allowlist_patterns = compile_regex_patterns("example\\.com");
    state.allow_private_in_allowlist = true;

    // Level 4: ALL requests are blocked regardless of allowlist or settings
    auto status1 = validate_host_security("http://example.com/api", state);
    ASSERT_FALSE(status1.ok());
    ASSERT_TRUE(status1.message().find("PARANOID") != std::string::npos ||
                status1.message().find("level 4") != std::string::npos);

    auto status2 = validate_host_security("http://127.0.0.1/api", state);
    ASSERT_FALSE(status2.ok());

    auto status3 = validate_host_security("https://google.com/api", state);
    ASSERT_FALSE(status3.ok());
}

// Test validate_host_security with invalid URL format
TEST_F(HttpRequestFunctionsTest, securityInvalidUrlTest) {
    HttpRequestFunctionState state;
    state.security_level = 3;  // RESTRICTED
    state.host_allowlist_patterns = compile_regex_patterns("example\\.com");

    // Invalid URL format should be blocked
    auto status1 = validate_host_security("", state);
    ASSERT_FALSE(status1.ok());

    auto status2 = validate_host_security("not-a-url", state);
    ASSERT_FALSE(status2.ok());

    auto status3 = validate_host_security("://missing-scheme.com", state);
    ASSERT_FALSE(status3.ok());
}

// Test protocol validation - only http:// and https:// allowed
TEST_F(HttpRequestFunctionsTest, protocolValidationTest) {
    HttpRequestFunctionState state;
    state.security_level = 1;  // TRUSTED (allow everything except protocol check)

    // Valid protocols should pass protocol check
    ASSERT_TRUE(validate_host_security("http://example.com/api", state).ok());
    ASSERT_TRUE(validate_host_security("https://example.com/api", state).ok());
    ASSERT_TRUE(validate_host_security("HTTP://example.com/api", state).ok());  // case insensitive
    ASSERT_TRUE(validate_host_security("HTTPS://example.com/api", state).ok());

    // Invalid protocols should be blocked BEFORE DNS resolution
    auto status1 = validate_host_security("ftp://example.com/file", state);
    ASSERT_FALSE(status1.ok());
    ASSERT_TRUE(status1.message().find("Invalid protocol") != std::string::npos);
    ASSERT_TRUE(status1.message().find("http://") != std::string::npos);

    auto status2 = validate_host_security("file:///etc/passwd", state);
    ASSERT_FALSE(status2.ok());
    ASSERT_TRUE(status2.message().find("Invalid protocol") != std::string::npos);

    auto status3 = validate_host_security("gopher://example.com", state);
    ASSERT_FALSE(status3.ok());
    ASSERT_TRUE(status3.message().find("Invalid protocol") != std::string::npos);

    auto status4 = validate_host_security("data:text/html,<script>alert(1)</script>", state);
    ASSERT_FALSE(status4.ok());
    ASSERT_TRUE(status4.message().find("Invalid protocol") != std::string::npos);

    // Edge cases
    auto status5 = validate_host_security("javascript:alert(1)", state);
    ASSERT_FALSE(status5.ok());

    auto status6 = validate_host_security("custom://host/path", state);
    ASSERT_FALSE(status6.ok());
}

// Test invalid regex pattern handling (should be skipped without crashing)
TEST_F(HttpRequestFunctionsTest, invalidRegexPatternTest) {
    // Invalid regex patterns should be handled gracefully
    auto patterns = compile_regex_patterns("[invalid(regex,valid\\.pattern\\.com");
    // Should have at least the valid pattern, invalid ones are skipped
    ASSERT_LE(patterns.size(), 2);
    // The valid pattern should work
    for (const auto& pattern : patterns) {
        ASSERT_TRUE(std::regex_match("valid.pattern.com", pattern));
    }
}

// Test validate_http_method with valid methods
TEST_F(HttpRequestFunctionsTest, validateHttpMethodValidTest) {
    // All valid methods should return OK
    ASSERT_TRUE(validate_http_method("GET").ok());
    ASSERT_TRUE(validate_http_method("POST").ok());
    ASSERT_TRUE(validate_http_method("PUT").ok());
    ASSERT_TRUE(validate_http_method("DELETE").ok());
    ASSERT_TRUE(validate_http_method("HEAD").ok());
    ASSERT_TRUE(validate_http_method("OPTIONS").ok());

    // Case insensitive
    ASSERT_TRUE(validate_http_method("get").ok());
    ASSERT_TRUE(validate_http_method("Get").ok());
    ASSERT_TRUE(validate_http_method("GeT").ok());
    ASSERT_TRUE(validate_http_method("post").ok());
    ASSERT_TRUE(validate_http_method("Post").ok());
}

// Test validate_http_method with invalid methods
TEST_F(HttpRequestFunctionsTest, validateHttpMethodInvalidTest) {
    // Invalid method should return error
    auto status1 = validate_http_method("INVALID");
    ASSERT_FALSE(status1.ok());
    ASSERT_TRUE(status1.status().message().find("Invalid HTTP method") != std::string::npos);
    ASSERT_TRUE(status1.status().message().find("INVALID") != std::string::npos);

    // PATCH is not in allowed list
    auto status2 = validate_http_method("PATCH");
    ASSERT_FALSE(status2.ok());
    ASSERT_TRUE(status2.status().message().find("PATCH") != std::string::npos);

    // Empty string
    auto status3 = validate_http_method("");
    ASSERT_FALSE(status3.ok());

    // Random string
    auto status4 = validate_http_method("HELLO");
    ASSERT_FALSE(status4.ok());
    ASSERT_TRUE(status4.status().message().find("HELLO") != std::string::npos);

    // Partial match should fail
    auto status5 = validate_http_method("GE");
    ASSERT_FALSE(status5.ok());

    auto status6 = validate_http_method("GETS");
    ASSERT_FALSE(status6.ok());
}

//=============================================================================
// RuntimeState HTTP Option Getter Tests
//=============================================================================

// Test RuntimeState::http_request_ip_allowlist() getter
PARALLEL_TEST(RuntimeStateHttpOptionsTest, ipAllowlistGetterWithValueSet) {
    TQueryOptions query_options;
    query_options.__set_http_request_ip_allowlist("192.168.1.1,10.0.0.1");

    TQueryGlobals query_globals;
    RuntimeState state(TUniqueId(), query_options, query_globals, nullptr);

    EXPECT_EQ("192.168.1.1,10.0.0.1", state.http_request_ip_allowlist());
}

PARALLEL_TEST(RuntimeStateHttpOptionsTest, ipAllowlistGetterWithoutValueSet) {
    TQueryOptions query_options;
    // Don't set http_request_ip_allowlist

    TQueryGlobals query_globals;
    RuntimeState state(TUniqueId(), query_options, query_globals, nullptr);

    // Should return empty string when not set
    EXPECT_EQ("", state.http_request_ip_allowlist());
}

// Test RuntimeState::http_request_host_allowlist_regexp() getter
PARALLEL_TEST(RuntimeStateHttpOptionsTest, hostAllowlistRegexpGetterWithValueSet) {
    TQueryOptions query_options;
    query_options.__set_http_request_host_allowlist_regexp(".*\\.example\\.com");

    TQueryGlobals query_globals;
    RuntimeState state(TUniqueId(), query_options, query_globals, nullptr);

    EXPECT_EQ(".*\\.example\\.com", state.http_request_host_allowlist_regexp());
}

PARALLEL_TEST(RuntimeStateHttpOptionsTest, hostAllowlistRegexpGetterWithoutValueSet) {
    TQueryOptions query_options;
    // Don't set http_request_host_allowlist_regexp

    TQueryGlobals query_globals;
    RuntimeState state(TUniqueId(), query_options, query_globals, nullptr);

    // Should return empty string when not set
    EXPECT_EQ("", state.http_request_host_allowlist_regexp());
}

// Test RuntimeState::http_request_security_level() getter
PARALLEL_TEST(RuntimeStateHttpOptionsTest, securityLevelGetterWithValueSet) {
    TQueryOptions query_options;
    query_options.__set_http_request_security_level(2);

    TQueryGlobals query_globals;
    RuntimeState state(TUniqueId(), query_options, query_globals, nullptr);

    EXPECT_EQ(2, state.http_request_security_level());
}

PARALLEL_TEST(RuntimeStateHttpOptionsTest, securityLevelGetterDefault) {
    TQueryOptions query_options;
    // Don't set http_request_security_level

    TQueryGlobals query_globals;
    RuntimeState state(TUniqueId(), query_options, query_globals, nullptr);

    // Default should be 3 (RESTRICTED)
    EXPECT_EQ(3, state.http_request_security_level());
}

// Test RuntimeState::http_request_allow_private_in_allowlist() getter
PARALLEL_TEST(RuntimeStateHttpOptionsTest, allowPrivateInAllowlistGetterTrue) {
    TQueryOptions query_options;
    query_options.__set_http_request_allow_private_in_allowlist(true);

    TQueryGlobals query_globals;
    RuntimeState state(TUniqueId(), query_options, query_globals, nullptr);

    EXPECT_TRUE(state.http_request_allow_private_in_allowlist());
}

PARALLEL_TEST(RuntimeStateHttpOptionsTest, allowPrivateInAllowlistGetterFalse) {
    TQueryOptions query_options;
    query_options.__set_http_request_allow_private_in_allowlist(false);

    TQueryGlobals query_globals;
    RuntimeState state(TUniqueId(), query_options, query_globals, nullptr);

    EXPECT_FALSE(state.http_request_allow_private_in_allowlist());
}

PARALLEL_TEST(RuntimeStateHttpOptionsTest, allowPrivateInAllowlistGetterDefault) {
    TQueryOptions query_options;
    // Don't set http_request_allow_private_in_allowlist

    TQueryGlobals query_globals;
    RuntimeState state(TUniqueId(), query_options, query_globals, nullptr);

    // Default should be false
    EXPECT_FALSE(state.http_request_allow_private_in_allowlist());
}

// Test RuntimeState::http_request_ssl_verification_required() getter
PARALLEL_TEST(RuntimeStateHttpOptionsTest, sslVerificationRequiredGetterTrue) {
    TQueryOptions query_options;
    query_options.__set_http_request_ssl_verification_required(true);

    TQueryGlobals query_globals;
    RuntimeState state(TUniqueId(), query_options, query_globals, nullptr);

    EXPECT_TRUE(state.http_request_ssl_verification_required());
}

PARALLEL_TEST(RuntimeStateHttpOptionsTest, sslVerificationRequiredGetterDefault) {
    TQueryOptions query_options;
    // Don't set http_request_ssl_verification_required

    TQueryGlobals query_globals;
    RuntimeState state(TUniqueId(), query_options, query_globals, nullptr);

    // Default should be false
    EXPECT_FALSE(state.http_request_ssl_verification_required());
}

} // namespace starrocks
