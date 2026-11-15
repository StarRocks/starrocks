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

#include "http/action/greplog_action.h"

#include <event2/http.h>
#include <gtest/gtest.h>

#include "platform/http/http_channel.h"
#include "platform/http/http_request.h"
#include "platform/http/http_status.h"

namespace starrocks {

extern void (*s_injected_send_reply)(HttpRequest*, HttpStatus, std::string_view);

namespace {
HttpStatus k_response_status = HttpStatus::OK;
std::string k_response_str;
void inject_send_reply(HttpRequest* request, HttpStatus status, std::string_view content) {
    k_response_status = status;
    k_response_str = content;
}
} // namespace

class GrepLogActionTest : public testing::Test {
public:
    static void SetUpTestSuite() { s_injected_send_reply = inject_send_reply; }
    static void TearDownTestSuite() { s_injected_send_reply = nullptr; }

    void SetUp() override {
        k_response_status = HttpStatus::OK;
        k_response_str = "";
        _evhttp_req = evhttp_request_new(nullptr, nullptr);
    }

    void TearDown() override {
        if (_evhttp_req != nullptr) {
            evhttp_request_free(_evhttp_req);
        }
    }

protected:
    void handle(const std::map<std::string, std::string>& params, HttpMethod method = HttpMethod::GET) {
        GrepLogAction action;
        HttpRequest request(_evhttp_req);
        request.set_method(method);
        request.set_handler(&action);
        for (const auto& [name, value] : params) {
            request.add_param(name, value);
        }
        action.handle(&request);
    }

    evhttp_request* _evhttp_req = nullptr;
};

TEST_F(GrepLogActionTest, non_get_method_rejected) {
    handle({}, HttpMethod::POST);
    EXPECT_EQ(HttpStatus::METHOD_NOT_ALLOWED, k_response_status);
}

TEST_F(GrepLogActionTest, absent_params_accepted) {
    handle({});
    EXPECT_EQ(HttpStatus::OK, k_response_status);
}

TEST_F(GrepLogActionTest, non_numeric_start_ts_rejected) {
    handle({{"start_ts", "abc"}});
    EXPECT_EQ(HttpStatus::BAD_REQUEST, k_response_status);
    EXPECT_EQ("Invalid param start_ts", k_response_str);
}

TEST_F(GrepLogActionTest, start_ts_that_does_not_fit_in_int64_rejected) {
    handle({{"start_ts", "99999999999999999999"}});
    EXPECT_EQ(HttpStatus::BAD_REQUEST, k_response_status);
    EXPECT_EQ("Invalid param start_ts", k_response_str);
}

// std::stoll used to stop at the first non-digit and silently accept this as 12.
TEST_F(GrepLogActionTest, start_ts_with_trailing_garbage_rejected) {
    handle({{"start_ts", "12abc"}});
    EXPECT_EQ(HttpStatus::BAD_REQUEST, k_response_status);
    EXPECT_EQ("Invalid param start_ts", k_response_str);
}

TEST_F(GrepLogActionTest, non_numeric_end_ts_rejected) {
    handle({{"end_ts", "abc"}});
    EXPECT_EQ(HttpStatus::BAD_REQUEST, k_response_status);
    EXPECT_EQ("Invalid param end_ts", k_response_str);
}

TEST_F(GrepLogActionTest, non_numeric_limit_rejected) {
    handle({{"limit", "abc"}});
    EXPECT_EQ(HttpStatus::BAD_REQUEST, k_response_status);
    EXPECT_EQ("Invalid param limit", k_response_str);
}

TEST_F(GrepLogActionTest, zero_limit_rejected) {
    handle({{"limit", "0"}});
    EXPECT_EQ(HttpStatus::BAD_REQUEST, k_response_status);
    EXPECT_EQ("Invalid param limit", k_response_str);
}

TEST_F(GrepLogActionTest, limit_above_the_cap_rejected) {
    handle({{"limit", "1000001"}});
    EXPECT_EQ(HttpStatus::BAD_REQUEST, k_response_status);
    EXPECT_EQ("Invalid param limit", k_response_str);
}

TEST_F(GrepLogActionTest, limit_at_the_cap_accepted) {
    handle({{"limit", "1000000"}});
    EXPECT_EQ(HttpStatus::OK, k_response_status);
}

} // namespace starrocks
