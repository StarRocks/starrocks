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

#include "http/action/health_action.h"

#include <event2/http.h>
#include <gtest/gtest.h>

#include <string>

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

class HealthActionTest : public testing::Test {
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
    evhttp_request* _evhttp_req = nullptr;
};

// The healthy reply must stay byte-identical to the pre-change hardcoded reply so
// consumers that parse the body observe no difference while the process is healthy.
TEST_F(HealthActionTest, healthy_response_stays_byte_identical_200) {
    auto [status, body] = HealthAction::health_response(false);
    EXPECT_EQ(HttpStatus::OK, status);
    EXPECT_EQ(R"({"status": "OK","msg": "To Be Added"})", body);
}

// Once the fatal-signal handler has marked the process as crashing, the endpoint must
// turn non-2xx so orchestrator health checks (e.g. Kubernetes liveness probes) fail
// and the node can be restarted even if the crash handler itself hangs.
TEST_F(HealthActionTest, crashing_response_is_503) {
    auto [status, body] = HealthAction::health_response(true);
    EXPECT_EQ(HttpStatus::SERVICE_UNAVAILABLE, status);
    EXPECT_EQ(R"({"status": "CRASHING","msg": "fatal signal handler is running; process is terminating"})", body);
}

// handle() must reply with exactly the mapping health_response() defines for the live
// process state. The crash flag is process-lifetime one-way, so instead of assuming it
// is still clear here, assert against is_process_crashing() to stay independent of
// whatever ran earlier in this binary.
TEST_F(HealthActionTest, handle_maps_process_state_through_health_response) {
    HealthAction action(nullptr);
    HttpRequest request(_evhttp_req);
    request.set_method(HttpMethod::GET);
    request.set_handler(&action);

    action.handle(&request);

    auto [expected_status, expected_body] = HealthAction::health_response(is_process_crashing());
    EXPECT_EQ(expected_status, k_response_status);
    EXPECT_EQ(expected_body, k_response_str);
}

} // namespace starrocks
