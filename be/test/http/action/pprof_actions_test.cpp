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

#include "http/action/pprof_actions.h"

#include <event2/http.h>
#include <gtest/gtest.h>

#include "platform/http/http_channel.h"
#include "platform/http/http_request.h"

namespace starrocks {

extern void (*s_injected_send_reply)(HttpRequest*, HttpStatus, std::string_view);

namespace {
std::string k_response;

void inject_send_reply(HttpRequest* request, HttpStatus status, std::string_view content) {
    (void)request;
    (void)status;
    k_response = content;
}
} // namespace

class PprofActionsTest : public testing::Test {
public:
    static void SetUpTestSuite() { s_injected_send_reply = inject_send_reply; }
    static void TearDownTestSuite() { s_injected_send_reply = nullptr; }

    void SetUp() override {
        k_response.clear();
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

TEST_F(PprofActionsTest, cmdline_returns_process_name) {
    CmdlineAction action;
    HttpRequest request(_evhttp_req);

    action.handle(&request);

    EXPECT_FALSE(k_response.empty());
    EXPECT_NE(std::string("read cmdline failed"), k_response);
#ifndef __APPLE__
    EXPECT_NE(std::string("Unable to open file: /proc/self/cmdline"), k_response);
#endif
}

} // namespace starrocks
