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

#include "http/action/stop_be_action.h"

#include <event2/http.h>
#include <gtest/gtest.h>
#include <rapidjson/document.h>

#include "common/config_http_fwd.h"
#include "common/process_exit.h"
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

class StopBeActionTest : public testing::Test {
public:
    static void SetUpTestSuite() { s_injected_send_reply = inject_send_reply; }
    static void TearDownTestSuite() { s_injected_send_reply = nullptr; }

    void SetUp() override {
        _old_enable_stop_be_action = config::enable_stop_be_action;
        k_response_status = HttpStatus::OK;
        k_response_str = "";
        _evhttp_req = evhttp_request_new(nullptr, nullptr);
    }

    void TearDown() override {
        config::enable_stop_be_action = _old_enable_stop_be_action;
        if (_evhttp_req != nullptr) {
            evhttp_request_free(_evhttp_req);
        }
    }

protected:
    bool _old_enable_stop_be_action = true;
    evhttp_request* _evhttp_req = nullptr;
};

// When the config is disabled, the request must be rejected with HTTP 403 and
// the BE must not be put into the quick-exit path.
TEST_F(StopBeActionTest, disabled_rejects_with_forbidden) {
    config::enable_stop_be_action = false;

    StopBeAction action(nullptr);
    HttpRequest request(_evhttp_req);
    request.set_method(HttpMethod::POST);
    request.set_handler(&action);

    // The disabled path must not change the process quick-exit state, regardless
    // of any state left by other tests in this binary.
    const bool quick_exit_before = process_quick_exit_in_progress();

    action.handle(&request);

    EXPECT_EQ(HttpStatus::FORBIDDEN, k_response_status);
    rapidjson::Document doc;
    doc.Parse(k_response_str.c_str());
    ASSERT_TRUE(doc.HasMember("status"));
    EXPECT_STREQ("stop_be action is disabled by config", doc["status"].GetString());

    EXPECT_EQ(quick_exit_before, process_quick_exit_in_progress());
}

} // namespace starrocks
