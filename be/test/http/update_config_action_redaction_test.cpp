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

#include <event2/http.h>
#include <event2/http_struct.h>
#include <glog/logging.h>
#include <gtest/gtest.h>

#include <cstring>
#include <mutex>
#include <string>

#include "common/config_update_registry.h"
#include "http/action/update_config_action.h"
#include "platform/http/http_channel.h"
#include "platform/http/http_request.h"
#include "platform/http/http_status.h"

namespace starrocks {

extern void (*s_injected_send_reply)(HttpRequest*, HttpStatus, std::string_view);

namespace {

std::string k_response_str;
void inject_send_reply(HttpRequest*, HttpStatus, std::string_view content) {
    k_response_str = content;
}

class CapturingLogSink final : public google::LogSink {
public:
    CapturingLogSink() { google::AddLogSink(this); }
    ~CapturingLogSink() override { google::RemoveLogSink(this); }

    void send(google::LogSeverity, const char*, const char*, int, const google::LogMessageTime&, const char* message,
              size_t message_len) override {
        std::lock_guard lock(_mutex);
        _messages.append(message, message_len);
        _messages.push_back('\n');
    }

    std::string messages() {
        std::lock_guard lock(_mutex);
        return _messages;
    }

private:
    std::mutex _mutex;
    std::string _messages;
};

} // namespace

class UpdateConfigActionRedactionTest : public testing::Test {
public:
    static void SetUpTestSuite() { s_injected_send_reply = inject_send_reply; }
    static void TearDownTestSuite() { s_injected_send_reply = nullptr; }

    // A registry that is not ready accepts every update without trying it, so make it ready to reach the
    // failure path.
    void SetUp() override {
        k_response_str.clear();
        ConfigUpdateRegistry::instance()->TEST_reset();
        ConfigUpdateRegistry::instance()->set_ready();
    }
    void TearDown() override { ConfigUpdateRegistry::instance()->TEST_reset(); }

protected:
    // Runs POST <uri> through the action and returns what it logged; the reply ends up in k_response_str.
    static std::string handle(const char* uri) {
        evhttp_request* ev_req = evhttp_request_new(nullptr, nullptr);
        ev_req->type = EVHTTP_REQ_POST;
        ev_req->uri = strdup(uri);
        ev_req->uri_elems = evhttp_uri_parse(ev_req->uri);
        std::string logged;
        {
            HttpRequest req(ev_req);
            EXPECT_EQ(0, req.init_from_evhttp());
            CapturingLogSink sink;
            UpdateConfigAction action;
            action.handle(&req);
            logged = sink.messages();
        }
        evhttp_request_free(ev_req);
        return logged;
    }
};

// object_storage_secret_access_key is immutable, so setting it over HTTP always fails, and that failure used to
// echo the new value in the warning log and in the reply, on top of the request log.
TEST_F(UpdateConfigActionRedactionTest, credential_value_is_masked_in_log_and_reply) {
    const std::string logged = handle("/api/update_config?object_storage_secret_access_key=FakeSecret123");

    EXPECT_EQ(std::string::npos, logged.find("FakeSecret123")) << logged;
    EXPECT_EQ(std::string::npos, k_response_str.find("FakeSecret123")) << k_response_str;
    EXPECT_NE(std::string::npos, logged.find("set_config object_storage_secret_access_key=******")) << logged;
    EXPECT_NE(std::string::npos, k_response_str.find("set object_storage_secret_access_key=****** failed"))
            << k_response_str;
}

TEST_F(UpdateConfigActionRedactionTest, other_value_is_shown) {
    const std::string logged = handle("/api/update_config?no_such_config_for_test=visible_value");

    EXPECT_NE(std::string::npos, logged.find("visible_value")) << logged;
    EXPECT_NE(std::string::npos, k_response_str.find("set no_such_config_for_test=visible_value failed"))
            << k_response_str;
}

} // namespace starrocks
