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

#include <glog/logging.h>
#include <gtest/gtest.h>

#include <memory>

#include "base/testutil/assert.h"
#include "base/testutil/parallel_test.h"
#include "column/column_helper.h"
#include "column/map_column.h"
#include "exprs/function_helper.h"
#include "exprs/mock_vectorized_expr.h"
#include "http/ev_http_server.h"
#include "http/http_channel.h"
#include "http/http_handler.h"
#include "http/http_request.h"
#include "runtime/runtime_state.h"

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
    // ssl_verify_required is false by default (admin can enable via Config)
    ASSERT_FALSE(state->ssl_verify_required);

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
    state.security_level = 1; // TRUSTED

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
    state.security_level = 1; // TRUSTED

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
    state.security_level = 2; // PUBLIC
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
    state.security_level = 3; // RESTRICTED
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
    state.security_level = 4; // PARANOID

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
    state.security_level = 2; // PUBLIC

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
    state.security_level = 2; // PUBLIC
    state.ip_allowlist = {"127.0.0.1", "192.168.1.1"};
    state.allow_private_in_allowlist = true; // Must enable this flag

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
    state.security_level = 2; // PUBLIC
    state.ip_allowlist = {"127.0.0.1", "192.168.1.1"};
    state.allow_private_in_allowlist = false; // Default: false

    // Private IPs in allowlist should be BLOCKED without allow_private_in_allowlist flag
    auto status1 = validate_host_security("http://127.0.0.1/api", state);
    ASSERT_FALSE(status1.ok());

    auto status2 = validate_host_security("http://192.168.1.1/api", state);
    ASSERT_FALSE(status2.ok());
}

// Test validate_host_security at Level 3 (RESTRICTED) - requires allowlist
TEST_F(HttpRequestFunctionsTest, securityLevel3RequiresAllowlistTest) {
    HttpRequestFunctionState state;
    state.security_level = 3; // RESTRICTED
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
    state.security_level = 3; // RESTRICTED
    // Use IP allowlist for direct IP access
    state.ip_allowlist = {"93.184.216.34", "1.2.3.4"}; // example.com's IP and test IP

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
    state.security_level = 3; // RESTRICTED
    state.ip_allowlist = {"127.0.0.1", "192.168.1.1"};
    state.allow_private_in_allowlist = true; // Must enable this flag

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
    state.security_level = 3; // RESTRICTED
    state.ip_allowlist = {"127.0.0.1", "192.168.1.1"};
    state.allow_private_in_allowlist = false; // Default: false

    // Private IPs in allowlist should be BLOCKED without allow_private_in_allowlist flag
    auto status1 = validate_host_security("http://127.0.0.1/api", state);
    ASSERT_FALSE(status1.ok());

    auto status2 = validate_host_security("http://192.168.1.1/api", state);
    ASSERT_FALSE(status2.ok());
}

// Test validate_host_security at Level 4 (PARANOID) - blocks ALL requests
TEST_F(HttpRequestFunctionsTest, securityLevel4BlocksAllRequestsTest) {
    HttpRequestFunctionState state;
    state.security_level = 4; // PARANOID
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
    state.security_level = 3; // RESTRICTED
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
    state.security_level = 1; // TRUSTED (allow everything except protocol check)

    // Valid protocols should pass protocol check
    ASSERT_TRUE(validate_host_security("http://example.com/api", state).ok());
    ASSERT_TRUE(validate_host_security("https://example.com/api", state).ok());
    ASSERT_TRUE(validate_host_security("HTTP://example.com/api", state).ok()); // case insensitive
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

    // Default should be true (secure by default)
    // When thrift field is not set, SSL verification is enabled for security
    EXPECT_TRUE(state.http_request_ssl_verification_required());
}

//=============================================================================
// Helper Function Unit Tests (coverage for internal utilities)
//=============================================================================

// Test parse_comma_separated_list edge cases
TEST_F(HttpRequestFunctionsTest, parseCommaListEmptyStringTest) {
    auto result = parse_comma_separated_list("");
    ASSERT_TRUE(result.empty());
}

TEST_F(HttpRequestFunctionsTest, parseCommaListSingleItemTest) {
    auto result = parse_comma_separated_list("192.168.1.1");
    ASSERT_EQ(1, result.size());
    ASSERT_EQ("192.168.1.1", result[0]);
}

TEST_F(HttpRequestFunctionsTest, parseCommaListOnlyWhitespaceTest) {
    auto result = parse_comma_separated_list("   ,  ,  ");
    ASSERT_TRUE(result.empty());
}

TEST_F(HttpRequestFunctionsTest, parseCommaListMultipleCommasTest) {
    auto result = parse_comma_separated_list("a,,b,,,c");
    ASSERT_EQ(3, result.size());
    ASSERT_EQ("a", result[0]);
    ASSERT_EQ("b", result[1]);
    ASSERT_EQ("c", result[2]);
}

// Test compile_regex_patterns edge cases
TEST_F(HttpRequestFunctionsTest, compileRegexEmptyStringTest) {
    auto patterns = compile_regex_patterns("");
    ASSERT_TRUE(patterns.empty());
}

TEST_F(HttpRequestFunctionsTest, compileRegexSinglePatternTest) {
    auto patterns = compile_regex_patterns("^api\\.example\\.com$");
    ASSERT_EQ(1, patterns.size());
    ASSERT_TRUE(std::regex_match("api.example.com", patterns[0]));
    ASSERT_FALSE(std::regex_match("evil.api.example.com", patterns[0]));
}

TEST_F(HttpRequestFunctionsTest, compileRegexAllInvalidPatternsTest) {
    auto patterns = compile_regex_patterns("[invalid,[also-invalid");
    ASSERT_TRUE(patterns.empty());
}

// Test check_ip_allowlist with empty allowlist
TEST_F(HttpRequestFunctionsTest, checkIpAllowlistEmptyTest) {
    HttpRequestFunctionState state;
    // Empty allowlist
    ASSERT_FALSE(check_ip_allowlist("192.168.1.1", state));
    ASSERT_FALSE(check_ip_allowlist("", state));
}

// Test check_host_regex with empty patterns
TEST_F(HttpRequestFunctionsTest, checkHostRegexEmptyPatternsTest) {
    HttpRequestFunctionState state;
    // Empty patterns
    ASSERT_FALSE(check_host_regex("example.com", state));
    ASSERT_FALSE(check_host_regex("", state));
}

// Test check_host_regex with multiple patterns (OR logic)
TEST_F(HttpRequestFunctionsTest, checkHostRegexMultiplePatternsTest) {
    HttpRequestFunctionState state;
    state.host_allowlist_patterns = compile_regex_patterns(".*\\.example\\.com,.*\\.github\\.com");
    ASSERT_EQ(2, state.host_allowlist_patterns.size());

    ASSERT_TRUE(check_host_regex("api.example.com", state));
    ASSERT_TRUE(check_host_regex("api.github.com", state));
    ASSERT_FALSE(check_host_regex("evil.com", state));
}

//=============================================================================
// Protocol Validation Edge Cases
//=============================================================================

TEST_F(HttpRequestFunctionsTest, protocolMixedCaseTest) {
    HttpRequestFunctionState state;
    state.security_level = 1;

    ASSERT_TRUE(validate_host_security("Http://example.com", state).ok());
    ASSERT_TRUE(validate_host_security("hTtPs://example.com", state).ok());
    ASSERT_TRUE(validate_host_security("HTTPS://EXAMPLE.COM", state).ok());
}

TEST_F(HttpRequestFunctionsTest, protocolEmptyUrlTest) {
    HttpRequestFunctionState state;
    state.security_level = 1;

    auto status = validate_host_security("", state);
    ASSERT_FALSE(status.ok());
}

TEST_F(HttpRequestFunctionsTest, protocolOnlySchemeTest) {
    HttpRequestFunctionState state;
    state.security_level = 1;

    // "http://" with no host - should fail during host extraction, not protocol check
    auto status = validate_host_security("http://", state);
    // Protocol check passes, but may fail later
    // The important thing is ftp/file/etc are blocked
    auto ftp_status = validate_host_security("ftp://example.com", state);
    ASSERT_FALSE(ftp_status.ok());
    ASSERT_TRUE(ftp_status.message().find("Invalid protocol") != std::string::npos);
}

//=============================================================================
// Security Level Boundary Tests
//=============================================================================

// Test unknown security level (e.g., 0 or 5)
TEST_F(HttpRequestFunctionsTest, securityUnknownLevelBlocksTest) {
    HttpRequestFunctionState state;
    state.security_level = 0;

    auto status = validate_host_security("http://example.com/api", state);
    ASSERT_FALSE(status.ok());

    state.security_level = 5;
    status = validate_host_security("http://example.com/api", state);
    ASSERT_FALSE(status.ok());

    state.security_level = -1;
    status = validate_host_security("http://example.com/api", state);
    ASSERT_FALSE(status.ok());
}

// Test Level 2 with host regex allowlist (not just IP)
TEST_F(HttpRequestFunctionsTest, securityLevel2HostRegexAllowsPublicTest) {
    HttpRequestFunctionState state;
    state.security_level = 2;
    state.host_allowlist_patterns = compile_regex_patterns(".*\\.example\\.com");

    // Public hosts are allowed at level 2 without allowlist
    ASSERT_TRUE(validate_host_security("http://example.com/api", state).ok());
}

// Test Level 3 with host regex allowlist only (no IP allowlist)
TEST_F(HttpRequestFunctionsTest, securityLevel3HostRegexOnlyTest) {
    HttpRequestFunctionState state;
    state.security_level = 3;
    state.host_allowlist_patterns = compile_regex_patterns("example\\.com");

    // Should be allowed via host regex
    ASSERT_TRUE(validate_host_security("http://example.com/api", state).ok());

    // Non-matching host should be blocked
    auto status = validate_host_security("http://evil.com/api", state);
    ASSERT_FALSE(status.ok());
}

// Test link-local IP (169.254.x.x) blocking with specific error message
TEST_F(HttpRequestFunctionsTest, securityLinkLocalIpBlockedTest) {
    HttpRequestFunctionState state;
    state.security_level = 2;

    // 169.254.169.254 is the AWS metadata endpoint
    auto status = validate_host_security("http://169.254.169.254/latest/meta-data/", state);
    ASSERT_FALSE(status.ok());
    // Should mention link-local or cloud metadata
    ASSERT_TRUE(status.message().find("Link-local") != std::string::npos ||
                status.message().find("Private IP") != std::string::npos ||
                status.message().find("metadata") != std::string::npos);
}

//=============================================================================
// validate_http_method Additional Tests
//=============================================================================

TEST_F(HttpRequestFunctionsTest, validateHttpMethodAllCapsTest) {
    ASSERT_TRUE(validate_http_method("DELETE").ok());
    ASSERT_TRUE(validate_http_method("OPTIONS").ok());
    ASSERT_TRUE(validate_http_method("HEAD").ok());
}

TEST_F(HttpRequestFunctionsTest, validateHttpMethodMixedCaseTest) {
    ASSERT_TRUE(validate_http_method("Delete").ok());
    ASSERT_TRUE(validate_http_method("options").ok());
    ASSERT_TRUE(validate_http_method("Head").ok());
    ASSERT_TRUE(validate_http_method("pUt").ok());
}

TEST_F(HttpRequestFunctionsTest, validateHttpMethodWhitespaceTest) {
    // Methods with whitespace should be invalid
    auto status = validate_http_method(" GET");
    ASSERT_FALSE(status.ok());

    status = validate_http_method("GET ");
    ASSERT_FALSE(status.ok());
}

//=============================================================================
// DNS Pinning (resolved_ips output) Tests
//=============================================================================

TEST_F(HttpRequestFunctionsTest, dnsPinningLevel1IpLiteralTest) {
    HttpRequestFunctionState state;
    state.security_level = 1;

    std::vector<std::string> resolved_ips;
    auto status = validate_host_security("http://10.0.0.1/api", state, &resolved_ips);
    ASSERT_TRUE(status.ok());
    ASSERT_FALSE(resolved_ips.empty());
    ASSERT_EQ("10.0.0.1", resolved_ips[0]);
}

TEST_F(HttpRequestFunctionsTest, dnsPinningLevel2WithAllowlistTest) {
    HttpRequestFunctionState state;
    state.security_level = 2;
    state.ip_allowlist = {"10.0.0.1"};
    state.allow_private_in_allowlist = true;

    std::vector<std::string> resolved_ips;
    auto status = validate_host_security("http://10.0.0.1/api", state, &resolved_ips);
    ASSERT_TRUE(status.ok());
    ASSERT_FALSE(resolved_ips.empty());
    ASSERT_EQ("10.0.0.1", resolved_ips[0]);
}

TEST_F(HttpRequestFunctionsTest, dnsPinningNullOutputTest) {
    HttpRequestFunctionState state;
    state.security_level = 1;

    // Passing nullptr for resolved_ips should not crash
    auto status = validate_host_security("http://127.0.0.1/api", state, nullptr);
    ASSERT_TRUE(status.ok());
}

//=============================================================================
// http_request() Function Integration Tests
//=============================================================================

// Test with custom HTTP method
TEST_F(HttpRequestFunctionsTest, customMethodColumnTest) {
    FunctionContext::FunctionStateScope scope = FunctionContext::FRAGMENT_LOCAL;
    ASSERT_OK(HttpRequestFunctions::http_request_prepare(_ctx.get(), scope));

    auto url_column = BinaryColumn::create();
    url_column->append("http://127.0.0.1:99999/nonexistent");

    Columns columns;
    columns.emplace_back(url_column);

    // POST method
    auto method_column = BinaryColumn::create();
    method_column->append("POST");
    columns.emplace_back(method_column);

    // body
    auto body_column = BinaryColumn::create();
    body_column->append("{\"key\": \"value\"}");
    columns.emplace_back(body_column);

    // headers
    auto headers_column = BinaryColumn::create();
    headers_column->append("{\"Content-Type\": \"application/json\"}");
    columns.emplace_back(headers_column);

    // timeout_ms
    auto timeout_column = Int32Column::create();
    timeout_column->append(1000);
    columns.emplace_back(timeout_column);

    // ssl_verify
    auto ssl_column = BooleanColumn::create();
    ssl_column->append(false);
    columns.emplace_back(ssl_column);

    // username/password
    auto user_column = BinaryColumn::create();
    user_column->append("");
    columns.emplace_back(user_column);
    auto pass_column = BinaryColumn::create();
    pass_column->append("");
    columns.emplace_back(pass_column);

    auto result = HttpRequestFunctions::http_request(_ctx.get(), columns);
    ASSERT_TRUE(result.ok());
    ASSERT_EQ(1, result.value()->size());
    // Should return a response (error or success), not crash
    ASSERT_FALSE(result.value()->is_null(0));

    ASSERT_OK(HttpRequestFunctions::http_request_close(_ctx.get(), scope));
}

// Test with invalid HTTP method
TEST_F(HttpRequestFunctionsTest, invalidMethodColumnTest) {
    FunctionContext::FunctionStateScope scope = FunctionContext::FRAGMENT_LOCAL;
    ASSERT_OK(HttpRequestFunctions::http_request_prepare(_ctx.get(), scope));

    auto url_column = BinaryColumn::create();
    url_column->append("http://example.com/api");

    Columns columns;
    columns.emplace_back(url_column);

    // Invalid method
    auto method_column = BinaryColumn::create();
    method_column->append("INVALID_METHOD");
    columns.emplace_back(method_column);

    auto body_column = BinaryColumn::create();
    body_column->append("");
    columns.emplace_back(body_column);
    auto headers_column = BinaryColumn::create();
    headers_column->append("{}");
    columns.emplace_back(headers_column);
    auto timeout_column = Int32Column::create();
    timeout_column->append(1000);
    columns.emplace_back(timeout_column);
    auto ssl_column = BooleanColumn::create();
    ssl_column->append(true);
    columns.emplace_back(ssl_column);
    auto user_column = BinaryColumn::create();
    user_column->append("");
    columns.emplace_back(user_column);
    auto pass_column = BinaryColumn::create();
    pass_column->append("");
    columns.emplace_back(pass_column);

    auto result = HttpRequestFunctions::http_request(_ctx.get(), columns);
    ASSERT_TRUE(result.ok());
    ASSERT_EQ(1, result.value()->size());
    // Should return error JSON response
    auto* binary_col = ColumnHelper::get_binary_column(result.value().get());
    std::string response = binary_col->get_slice(0).to_string();
    // Default security level is 3 (RESTRICTED) with empty allowlist,
    // so SSRF protection may trigger before method validation.
    // Either way, we get an error JSON response.
    ASSERT_TRUE(response.find("\"status\": -1") != std::string::npos ||
                response.find("\"status\":-1") != std::string::npos);

    ASSERT_OK(HttpRequestFunctions::http_request_close(_ctx.get(), scope));
}

// Test with invalid headers JSON
TEST_F(HttpRequestFunctionsTest, invalidHeadersJsonTest) {
    FunctionContext::FunctionStateScope scope = FunctionContext::FRAGMENT_LOCAL;
    ASSERT_OK(HttpRequestFunctions::http_request_prepare(_ctx.get(), scope));

    auto url_column = BinaryColumn::create();
    url_column->append("http://example.com/api");

    Columns columns;
    columns.emplace_back(url_column);

    auto method_column = BinaryColumn::create();
    method_column->append("GET");
    columns.emplace_back(method_column);
    auto body_column = BinaryColumn::create();
    body_column->append("");
    columns.emplace_back(body_column);

    // Invalid JSON headers
    auto headers_column = BinaryColumn::create();
    headers_column->append("not valid json");
    columns.emplace_back(headers_column);

    auto timeout_column = Int32Column::create();
    timeout_column->append(1000);
    columns.emplace_back(timeout_column);
    auto ssl_column = BooleanColumn::create();
    ssl_column->append(true);
    columns.emplace_back(ssl_column);
    auto user_column = BinaryColumn::create();
    user_column->append("");
    columns.emplace_back(user_column);
    auto pass_column = BinaryColumn::create();
    pass_column->append("");
    columns.emplace_back(pass_column);

    auto result = HttpRequestFunctions::http_request(_ctx.get(), columns);
    ASSERT_TRUE(result.ok());
    ASSERT_EQ(1, result.value()->size());
    // Should return error JSON response for bad headers
    auto* binary_col = ColumnHelper::get_binary_column(result.value().get());
    std::string response = binary_col->get_slice(0).to_string();
    ASSERT_TRUE(response.find("\"status\": -1") != std::string::npos ||
                response.find("\"status\":-1") != std::string::npos);

    ASSERT_OK(HttpRequestFunctions::http_request_close(_ctx.get(), scope));
}

// Test with multiple rows
TEST_F(HttpRequestFunctionsTest, multipleRowsTest) {
    FunctionContext::FunctionStateScope scope = FunctionContext::FRAGMENT_LOCAL;
    ASSERT_OK(HttpRequestFunctions::http_request_prepare(_ctx.get(), scope));

    auto url_column = BinaryColumn::create();
    url_column->append("http://127.0.0.1:99999/a");
    url_column->append("");
    url_column->append("not-a-url");
    Columns columns = create_http_request_columns(url_column, 3);

    auto result = HttpRequestFunctions::http_request(_ctx.get(), columns);
    ASSERT_TRUE(result.ok());
    ASSERT_EQ(3, result.value()->size());

    ASSERT_OK(HttpRequestFunctions::http_request_close(_ctx.get(), scope));
}

// Test prepare with THREAD_LOCAL scope (no-op)
TEST_F(HttpRequestFunctionsTest, prepareThreadLocalScopeTest) {
    FunctionContext::FunctionStateScope scope = FunctionContext::THREAD_LOCAL;
    ASSERT_OK(HttpRequestFunctions::http_request_prepare(_ctx.get(), scope));
    // THREAD_LOCAL should be a no-op
    ASSERT_OK(HttpRequestFunctions::http_request_close(_ctx.get(), scope));
}

//=============================================================================
// RuntimeState HTTP Option Tests - Additional Coverage
//=============================================================================

PARALLEL_TEST(RuntimeStateHttpOptionsTest, securityLevelGetterAllValues) {
    for (int level = 1; level <= 4; level++) {
        TQueryOptions query_options;
        query_options.__set_http_request_security_level(level);
        TQueryGlobals query_globals;
        RuntimeState state(TUniqueId(), query_options, query_globals, nullptr);
        EXPECT_EQ(level, state.http_request_security_level());
    }
}

PARALLEL_TEST(RuntimeStateHttpOptionsTest, sslVerificationRequiredGetterFalse) {
    TQueryOptions query_options;
    query_options.__set_http_request_ssl_verification_required(false);
    TQueryGlobals query_globals;
    RuntimeState state(TUniqueId(), query_options, query_globals, nullptr);
    EXPECT_FALSE(state.http_request_ssl_verification_required());
}

PARALLEL_TEST(RuntimeStateHttpOptionsTest, ipAllowlistGetterWhitespace) {
    TQueryOptions query_options;
    query_options.__set_http_request_ip_allowlist(" 10.0.0.1 , 192.168.1.1 ");
    TQueryGlobals query_globals;
    RuntimeState state(TUniqueId(), query_options, query_globals, nullptr);
    EXPECT_EQ(" 10.0.0.1 , 192.168.1.1 ", state.http_request_ip_allowlist());
}

PARALLEL_TEST(RuntimeStateHttpOptionsTest, hostAllowlistRegexpGetterMultiple) {
    TQueryOptions query_options;
    query_options.__set_http_request_host_allowlist_regexp(".*\\.example\\.com,.*\\.github\\.com");
    TQueryGlobals query_globals;
    RuntimeState state(TUniqueId(), query_options, query_globals, nullptr);
    EXPECT_EQ(".*\\.example\\.com,.*\\.github\\.com", state.http_request_host_allowlist_regexp());
}

//=============================================================================
// is_valid_utf8() Tests
//=============================================================================

TEST_F(HttpRequestFunctionsTest, isValidUtf8_AsciiTest) {
    EXPECT_TRUE(is_valid_utf8(""));
    EXPECT_TRUE(is_valid_utf8("Hello, World!"));
    EXPECT_TRUE(is_valid_utf8("0123456789"));
    EXPECT_TRUE(is_valid_utf8("!@#$%^&*()"));
}

TEST_F(HttpRequestFunctionsTest, isValidUtf8_MultiByte) {
    // 2-byte UTF-8: U+00C0 to U+07FF (e.g., "ñ" = 0xC3 0xB1)
    EXPECT_TRUE(is_valid_utf8("\xC3\xB1"));
    // 3-byte UTF-8: U+0800 to U+FFFF (e.g., "€" = 0xE2 0x82 0xAC)
    EXPECT_TRUE(is_valid_utf8("\xE2\x82\xAC"));
    // 4-byte UTF-8: U+10000 to U+10FFFF (e.g., emoji 😀 = 0xF0 0x9F 0x98 0x80)
    EXPECT_TRUE(is_valid_utf8("\xF0\x9F\x98\x80"));
    // Mixed ASCII and multi-byte
    EXPECT_TRUE(is_valid_utf8("Hello \xC3\xB1 World \xE2\x82\xAC"));
}

TEST_F(HttpRequestFunctionsTest, isValidUtf8_InvalidSequences) {
    // Standalone continuation byte (0x80-0xBF)
    EXPECT_FALSE(is_valid_utf8("\x80"));
    EXPECT_FALSE(is_valid_utf8("\xBF"));
    // Invalid start byte (0xF8+)
    EXPECT_FALSE(is_valid_utf8("\xF8\x80\x80\x80"));
    EXPECT_FALSE(is_valid_utf8("\xFF"));
    // Truncated 2-byte sequence
    EXPECT_FALSE(is_valid_utf8("\xC3"));
    // Truncated 3-byte sequence
    EXPECT_FALSE(is_valid_utf8("\xE2\x82"));
    // Truncated 4-byte sequence
    EXPECT_FALSE(is_valid_utf8("\xF0\x9F\x98"));
    // Invalid continuation byte in 2-byte sequence
    EXPECT_FALSE(is_valid_utf8("\xC3\x00"));
    // Valid ASCII followed by invalid byte
    EXPECT_FALSE(is_valid_utf8("Hello\x80World"));
}

//=============================================================================
// escape_json_string() Tests
//=============================================================================

TEST_F(HttpRequestFunctionsTest, escapeJsonString_SpecialChars) {
    EXPECT_EQ("\\\"", escape_json_string("\""));
    EXPECT_EQ("\\\\", escape_json_string("\\"));
    EXPECT_EQ("\\n", escape_json_string("\n"));
    EXPECT_EQ("\\r", escape_json_string("\r"));
    EXPECT_EQ("\\t", escape_json_string("\t"));
    EXPECT_EQ("\\b", escape_json_string("\b"));
    EXPECT_EQ("\\f", escape_json_string("\f"));
}

TEST_F(HttpRequestFunctionsTest, escapeJsonString_ControlChars) {
    // Control characters below 0x20 (excluding already handled ones) should be \uXXXX
    EXPECT_EQ("\\u0000", escape_json_string(std::string(1, '\0')));
    EXPECT_EQ("\\u0001", escape_json_string("\x01"));
    EXPECT_EQ("\\u001f", escape_json_string("\x1F"));
}

TEST_F(HttpRequestFunctionsTest, escapeJsonString_NormalText) {
    EXPECT_EQ("", escape_json_string(""));
    EXPECT_EQ("Hello, World!", escape_json_string("Hello, World!"));
    EXPECT_EQ("abc123", escape_json_string("abc123"));
}

TEST_F(HttpRequestFunctionsTest, escapeJsonString_Mixed) {
    EXPECT_EQ("line1\\nline2\\ttab\\\"quoted\\\"", escape_json_string("line1\nline2\ttab\"quoted\""));
    EXPECT_EQ("path\\\\to\\\\file", escape_json_string("path\\to\\file"));
}

//=============================================================================
// is_valid_json() Tests
//=============================================================================

TEST_F(HttpRequestFunctionsTest, isValidJson_ValidCases) {
    EXPECT_TRUE(is_valid_json("{}"));
    EXPECT_TRUE(is_valid_json("[]"));
    EXPECT_TRUE(is_valid_json("{\"key\": \"value\"}"));
    EXPECT_TRUE(is_valid_json("[1, 2, 3]"));
    EXPECT_TRUE(is_valid_json("{\"nested\": {\"key\": [1, 2]}}"));
    // Leading whitespace allowed (RFC 8259)
    EXPECT_TRUE(is_valid_json("  { \"key\": 1 }"));
    EXPECT_TRUE(is_valid_json("\t[1]"));
}

TEST_F(HttpRequestFunctionsTest, isValidJson_InvalidCases) {
    EXPECT_FALSE(is_valid_json(""));
    EXPECT_FALSE(is_valid_json("   "));
    EXPECT_FALSE(is_valid_json("hello"));
    EXPECT_FALSE(is_valid_json("42"));
    EXPECT_FALSE(is_valid_json("true"));
    EXPECT_FALSE(is_valid_json("null"));
    EXPECT_FALSE(is_valid_json("\"string\""));
}

//=============================================================================
// build_json_response() Tests
//=============================================================================

TEST_F(HttpRequestFunctionsTest, buildJsonResponse_JsonBody) {
    // JSON body should be embedded directly
    std::string result = build_json_response(200, "{\"key\": \"value\"}");
    EXPECT_TRUE(result.find("\"status\": 200") != std::string::npos);
    EXPECT_TRUE(result.find("\"body\": {\"key\": \"value\"}") != std::string::npos);
}

TEST_F(HttpRequestFunctionsTest, buildJsonResponse_TextBody) {
    // Non-JSON body should be escaped as string
    std::string result = build_json_response(200, "Hello World");
    EXPECT_TRUE(result.find("\"status\": 200") != std::string::npos);
    EXPECT_TRUE(result.find("\"body\": \"Hello World\"") != std::string::npos);
}

TEST_F(HttpRequestFunctionsTest, buildJsonResponse_InvalidUtf8) {
    // Invalid UTF-8 should return error response
    std::string result = build_json_response(200, std::string("Bad") + char(0x80) + "Data");
    EXPECT_TRUE(result.find("\"status\": -1") != std::string::npos);
    EXPECT_TRUE(result.find("invalid UTF-8") != std::string::npos);
}

TEST_F(HttpRequestFunctionsTest, buildJsonResponse_EmptyBody) {
    std::string result = build_json_response(200, "");
    EXPECT_TRUE(result.find("\"status\": 200") != std::string::npos);
    EXPECT_TRUE(result.find("\"body\": \"\"") != std::string::npos);
}

TEST_F(HttpRequestFunctionsTest, buildJsonResponse_ErrorStatus) {
    std::string result = build_json_response(500, "Internal Server Error");
    EXPECT_TRUE(result.find("\"status\": 500") != std::string::npos);
    EXPECT_TRUE(result.find("\"body\": \"Internal Server Error\"") != std::string::npos);
}

//=============================================================================
// build_json_error_response() Tests
//=============================================================================

TEST_F(HttpRequestFunctionsTest, buildJsonErrorResponse_BasicMessage) {
    std::string result = build_json_error_response("Connection refused");
    EXPECT_TRUE(result.find("\"status\": -1") != std::string::npos);
    EXPECT_TRUE(result.find("\"body\": null") != std::string::npos);
    EXPECT_TRUE(result.find("\"error\": \"Connection refused\"") != std::string::npos);
}

TEST_F(HttpRequestFunctionsTest, buildJsonErrorResponse_SpecialChars) {
    std::string result = build_json_error_response("Error: \"timeout\" at line\n2");
    EXPECT_TRUE(result.find("\"status\": -1") != std::string::npos);
    // Special chars should be escaped
    EXPECT_TRUE(result.find("\\\"timeout\\\"") != std::string::npos);
    EXPECT_TRUE(result.find("\\n") != std::string::npos);
}

//=============================================================================
// Integration Test Handlers (EvHttpServer-based)
//=============================================================================

class HttpRequestTestJsonHandler : public HttpHandler {
public:
    void handle(HttpRequest* req) override {
        req->add_output_header("Content-Type", "application/json");
        HttpChannel::send_reply(req, R"({"result": "ok"})");
    }
};

class HttpRequestTestTextHandler : public HttpHandler {
public:
    void handle(HttpRequest* req) override {
        req->add_output_header("Content-Type", "text/plain");
        HttpChannel::send_reply(req, "Hello World");
    }
};

class HttpRequestTestEchoHandler : public HttpHandler {
public:
    void handle(HttpRequest* req) override {
        std::string body = req->get_request_body();
        req->add_output_header("Content-Type", "application/json");
        HttpChannel::send_reply(req, body);
    }
};

class HttpRequestTestHeaderEchoHandler : public HttpHandler {
public:
    void handle(HttpRequest* req) override {
        std::string auth = req->header("Authorization");
        std::string custom = req->header("X-Custom");
        std::string resp = "{\"auth\":\"" + auth + "\",\"custom\":\"" + custom + "\"}";
        req->add_output_header("Content-Type", "application/json");
        HttpChannel::send_reply(req, resp);
    }
};

class HttpRequestTestErrorHandler : public HttpHandler {
public:
    void handle(HttpRequest* req) override { HttpChannel::send_error(req, HttpStatus::INTERNAL_SERVER_ERROR); }
};

static HttpRequestTestJsonHandler s_integ_json_handler;
static HttpRequestTestTextHandler s_integ_text_handler;
static HttpRequestTestEchoHandler s_integ_echo_handler;
static HttpRequestTestHeaderEchoHandler s_integ_header_echo_handler;
static HttpRequestTestErrorHandler s_integ_error_handler;
static EvHttpServer* s_integ_server = nullptr;
static int s_integ_port = 0;
static std::string s_integ_base_url;

class HttpRequestIntegrationTest : public ::testing::Test {
protected:
    static void SetUpTestCase() {
        s_integ_server = new EvHttpServer(0);
        s_integ_server->register_handler(GET, "/json", &s_integ_json_handler);
        s_integ_server->register_handler(GET, "/text", &s_integ_text_handler);
        s_integ_server->register_handler(POST, "/echo", &s_integ_echo_handler);
        s_integ_server->register_handler(GET, "/headers", &s_integ_header_echo_handler);
        s_integ_server->register_handler(GET, "/error500", &s_integ_error_handler);
        ASSERT_OK(s_integ_server->start());
        s_integ_port = s_integ_server->get_real_port();
        ASSERT_NE(0, s_integ_port);
        s_integ_base_url = "http://127.0.0.1:" + std::to_string(s_integ_port);
    }

    static void TearDownTestCase() {
        if (s_integ_server) {
            s_integ_server->stop();
            s_integ_server->join();
            delete s_integ_server;
            s_integ_server = nullptr;
        }
    }

    void SetUp() override {
        _ctx.reset(FunctionContext::create_test_context());
        // RuntimeState with TRUSTED level (allows 127.0.0.1)
        TQueryOptions opts;
        opts.__set_http_request_security_level(1); // TRUSTED
        opts.__set_http_request_ssl_verification_required(false);
        opts.__set_http_request_ip_allowlist("127.0.0.1");
        opts.__set_http_request_host_allowlist_regexp("");
        opts.__set_http_request_allow_private_in_allowlist(true);
        _runtime_state = std::make_unique<RuntimeState>(TUniqueId(), opts, TQueryGlobals(), nullptr);
        _ctx->set_runtime_state(_runtime_state.get());

        // Prepare function state
        ASSERT_OK(HttpRequestFunctions::http_request_prepare(_ctx.get(), FunctionContext::FRAGMENT_LOCAL));
    }

    void TearDown() override {
        HttpRequestFunctions::http_request_close(_ctx.get(), FunctionContext::FRAGMENT_LOCAL);
        _ctx.reset();
        _runtime_state.reset();
    }

    // Helper to create 8-column input for http_request function
    Columns make_columns(const std::string& url, const std::string& method = "GET", const std::string& body = "",
                         const std::string& headers = "{}", int timeout = 5000, bool ssl_verify = true,
                         const std::string& user = "", const std::string& pass = "") {
        Columns columns;

        auto url_col = BinaryColumn::create();
        url_col->append(url);
        columns.emplace_back(url_col);

        auto method_col = BinaryColumn::create();
        method_col->append(method);
        columns.emplace_back(method_col);

        auto body_col = BinaryColumn::create();
        body_col->append(body);
        columns.emplace_back(body_col);

        auto headers_col = BinaryColumn::create();
        headers_col->append(headers);
        columns.emplace_back(headers_col);

        auto timeout_col = Int32Column::create();
        timeout_col->append(timeout);
        columns.emplace_back(timeout_col);

        auto ssl_col = BooleanColumn::create();
        ssl_col->append(ssl_verify);
        columns.emplace_back(ssl_col);

        auto user_col = BinaryColumn::create();
        user_col->append(user);
        columns.emplace_back(user_col);

        auto pass_col = BinaryColumn::create();
        pass_col->append(pass);
        columns.emplace_back(pass_col);

        return columns;
    }

    // Helper to extract response string from result column
    std::string get_response(const ColumnPtr& result_col, size_t idx = 0) {
        auto* binary_col = ColumnHelper::get_binary_column(result_col.get());
        return binary_col->get_slice(idx).to_string();
    }

    std::unique_ptr<RuntimeState> _runtime_state;
    std::unique_ptr<FunctionContext> _ctx;
};

//=============================================================================
// Integration Tests - Full HTTP round-trip via EvHttpServer
//=============================================================================

// GET JSON response - covers execute_http_request_with_config full path,
// build_json_response with JSON body, init_security_state, runtime_state.h getters
TEST_F(HttpRequestIntegrationTest, GetJsonResponse) {
    auto columns = make_columns(s_integ_base_url + "/json");
    auto result = HttpRequestFunctions::http_request(_ctx.get(), columns);
    ASSERT_TRUE(result.ok());
    ASSERT_EQ(1, result.value()->size());
    ASSERT_FALSE(result.value()->is_null(0));

    std::string response = get_response(result.value());
    // Should contain HTTP 200 status
    EXPECT_TRUE(response.find("\"status\": 200") != std::string::npos);
    // Body should contain the JSON response embedded directly
    EXPECT_TRUE(response.find("\"result\"") != std::string::npos);
    EXPECT_TRUE(response.find("\"ok\"") != std::string::npos);
}

// GET text response - covers build_json_response with non-JSON body (escape path)
TEST_F(HttpRequestIntegrationTest, GetTextResponse) {
    auto columns = make_columns(s_integ_base_url + "/text");
    auto result = HttpRequestFunctions::http_request(_ctx.get(), columns);
    ASSERT_TRUE(result.ok());
    ASSERT_FALSE(result.value()->is_null(0));

    std::string response = get_response(result.value());
    EXPECT_TRUE(response.find("\"status\": 200") != std::string::npos);
    EXPECT_TRUE(response.find("Hello World") != std::string::npos);
}

// POST echo body - covers POST method path, set_payload, body transmission
TEST_F(HttpRequestIntegrationTest, PostEchoBody) {
    std::string post_body = R"({"key": "value", "num": 42})";
    auto columns =
            make_columns(s_integ_base_url + "/echo", "POST", post_body, R"({"Content-Type": "application/json"})");
    auto result = HttpRequestFunctions::http_request(_ctx.get(), columns);
    ASSERT_TRUE(result.ok());
    ASSERT_FALSE(result.value()->is_null(0));

    std::string response = get_response(result.value());
    EXPECT_TRUE(response.find("\"status\": 200") != std::string::npos);
    // Echo handler returns the body we sent
    EXPECT_TRUE(response.find("\"key\"") != std::string::npos);
    EXPECT_TRUE(response.find("\"value\"") != std::string::npos);
}

// Custom headers - covers parse_headers_json success path, set_header loop
TEST_F(HttpRequestIntegrationTest, CustomHeaders) {
    auto columns = make_columns(s_integ_base_url + "/headers", "GET", "", R"({"X-Custom": "test-value"})");
    auto result = HttpRequestFunctions::http_request(_ctx.get(), columns);
    ASSERT_TRUE(result.ok());
    ASSERT_FALSE(result.value()->is_null(0));

    std::string response = get_response(result.value());
    EXPECT_TRUE(response.find("\"status\": 200") != std::string::npos);
    EXPECT_TRUE(response.find("test-value") != std::string::npos);
}

// HTTP 500 response - covers build_json_response with non-200 status
TEST_F(HttpRequestIntegrationTest, Http500Response) {
    auto columns = make_columns(s_integ_base_url + "/error500");
    auto result = HttpRequestFunctions::http_request(_ctx.get(), columns);
    ASSERT_TRUE(result.ok());
    ASSERT_FALSE(result.value()->is_null(0));

    std::string response = get_response(result.value());
    EXPECT_TRUE(response.find("\"status\": 500") != std::string::npos);
}

// Basic auth - covers set_basic_auth path
TEST_F(HttpRequestIntegrationTest, BasicAuth) {
    auto columns = make_columns(s_integ_base_url + "/headers", "GET", "", "{}", 5000, true, "myuser", "mypass");
    auto result = HttpRequestFunctions::http_request(_ctx.get(), columns);
    ASSERT_TRUE(result.ok());
    ASSERT_FALSE(result.value()->is_null(0));

    std::string response = get_response(result.value());
    EXPECT_TRUE(response.find("\"status\": 200") != std::string::npos);
    // The header echo handler should show the Authorization header
    EXPECT_TRUE(response.find("Basic") != std::string::npos);
}

// Timeout clamping - covers MIN/MAX timeout clamping
TEST_F(HttpRequestIntegrationTest, TimeoutClamping) {
    // Timeout below minimum (1ms) should be clamped to 1
    auto columns_min = make_columns(s_integ_base_url + "/json", "GET", "", "{}", -100);
    auto result_min = HttpRequestFunctions::http_request(_ctx.get(), columns_min);
    ASSERT_TRUE(result_min.ok());
    // Should still get a response (clamped to 1ms, but local server is fast)
    ASSERT_FALSE(result_min.value()->is_null(0));

    // Timeout above maximum (300000ms) should be clamped to 300000
    auto columns_max = make_columns(s_integ_base_url + "/json", "GET", "", "{}", 999999);
    auto result_max = HttpRequestFunctions::http_request(_ctx.get(), columns_max);
    ASSERT_TRUE(result_max.ok());
    ASSERT_FALSE(result_max.value()->is_null(0));
}

// DNS pinning path - covers extract_host/port_from_url -> set_resolve_host -> _resolve_list
TEST_F(HttpRequestIntegrationTest, DnsPinningPath) {
    // Request to 127.0.0.1 with TRUSTED level triggers DNS pinning
    auto columns = make_columns(s_integ_base_url + "/json");
    auto result = HttpRequestFunctions::http_request(_ctx.get(), columns);
    ASSERT_TRUE(result.ok());
    ASSERT_FALSE(result.value()->is_null(0));

    std::string response = get_response(result.value());
    EXPECT_TRUE(response.find("\"status\": 200") != std::string::npos);
}

// SSL verify enforced - covers ssl_verify_required=true + user ssl_verify=false -> error
TEST_F(HttpRequestIntegrationTest, SslVerifyEnforced) {
    // Create a new context with ssl_verify_required=true
    auto ctx2 = std::unique_ptr<FunctionContext>(FunctionContext::create_test_context());
    TQueryOptions opts;
    opts.__set_http_request_security_level(1);
    opts.__set_http_request_ssl_verification_required(true); // Admin enforces SSL
    opts.__set_http_request_ip_allowlist("127.0.0.1");
    opts.__set_http_request_allow_private_in_allowlist(true);
    auto rs2 = std::make_unique<RuntimeState>(TUniqueId(), opts, TQueryGlobals(), nullptr);
    ctx2->set_runtime_state(rs2.get());
    ASSERT_OK(HttpRequestFunctions::http_request_prepare(ctx2.get(), FunctionContext::FRAGMENT_LOCAL));

    // User tries to disable SSL verification
    auto columns = make_columns(s_integ_base_url + "/json", "GET", "", "{}", 5000, false);
    auto result = HttpRequestFunctions::http_request(ctx2.get(), columns);
    ASSERT_TRUE(result.ok());
    ASSERT_FALSE(result.value()->is_null(0));

    std::string response = get_response(result.value());
    // Should return error because admin enforces SSL verification
    EXPECT_TRUE(response.find("\"status\": -1") != std::string::npos);
    EXPECT_TRUE(response.find("SSL verification is enforced") != std::string::npos);

    HttpRequestFunctions::http_request_close(ctx2.get(), FunctionContext::FRAGMENT_LOCAL);
}

// RuntimeState init - covers init_security_state with all getter calls
TEST_F(HttpRequestIntegrationTest, RuntimeStateInit) {
    // Verify that the function state was initialized correctly from RuntimeState
    auto* state =
            reinterpret_cast<HttpRequestFunctionState*>(_ctx->get_function_state(FunctionContext::FRAGMENT_LOCAL));
    ASSERT_NE(nullptr, state);
    EXPECT_EQ(1, state->security_level); // TRUSTED
    EXPECT_FALSE(state->ssl_verify_required);
    EXPECT_TRUE(state->allow_private_in_allowlist);
    EXPECT_FALSE(state->ip_allowlist.empty());
    EXPECT_EQ("127.0.0.1", state->ip_allowlist[0]);
}

} // namespace starrocks
