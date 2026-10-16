// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include <event2/http.h>
#include <event2/http_struct.h>
#include <gtest/gtest.h>

#include <cstring>
#include <string>

#include "common/logging.h"
#include "http/http_headers.h"
#include "http/http_request.h"
#include "http/utils.h"
#include "util/credential_mask.h"
#include "util/url_coding.h"

namespace starrocks {

class HttpUtilsTest : public testing::Test {
public:
    HttpUtilsTest() = default;
    ~HttpUtilsTest() override = default;
    void SetUp() override { _evhttp_req = evhttp_request_new(nullptr, nullptr); }
    void TearDown() override {
        if (_evhttp_req != nullptr) {
            evhttp_request_free(_evhttp_req);
        }
    }

private:
    evhttp_request* _evhttp_req = nullptr;
};

TEST_F(HttpUtilsTest, parse_basic_auth) {
    {
        HttpRequest req(_evhttp_req);
        auto auth = encode_basic_auth("starrocks", "passwd");
        req._headers.emplace(HttpHeaders::AUTHORIZATION, auth);
        std::string user;
        std::string passwd;
        auto res = parse_basic_auth(req, &user, &passwd);
        ASSERT_TRUE(res);
        ASSERT_STREQ("starrocks", user.data());
        ASSERT_STREQ("passwd", passwd.data());
    }
    {
        HttpRequest req(_evhttp_req);
        std::string auth = "Basic ";
        std::string encoded_str = "starrocks:passwd";
        auth += encoded_str;
        req._headers.emplace(HttpHeaders::AUTHORIZATION, auth);
        std::string user;
        std::string passwd;
        auto res = parse_basic_auth(req, &user, &passwd);
        ASSERT_FALSE(res);
    }
    {
        HttpRequest req(_evhttp_req);
        std::string auth = "Basic ";
        std::string encoded_str;
        base64_encode("starrockspasswd", &encoded_str);
        auth += encoded_str;
        req._headers.emplace(HttpHeaders::AUTHORIZATION, auth);
        std::string user;
        std::string passwd;
        auto res = parse_basic_auth(req, &user, &passwd);
        ASSERT_FALSE(res);
    }
    {
        HttpRequest req(_evhttp_req);
        std::string auth = "Basic";
        std::string encoded_str;
        base64_encode("starrocks:passwd", &encoded_str);
        auth += encoded_str;
        req._headers.emplace(HttpHeaders::AUTHORIZATION, auth);
        std::string user;
        std::string passwd;
        auto res = parse_basic_auth(req, &user, &passwd);
        ASSERT_FALSE(res);
    }
}

TEST_F(HttpUtilsTest, DebugStringMasksCredentialHeaders) {
    HttpRequest req(nullptr);
    std::string encoded;
    base64_encode("root:FakePwd123", &encoded);
    req.set_header(HttpHeaders::AUTHORIZATION, "Basic " + encoded);
    // Header names are case-insensitive, so a lower-case name must be masked too.
    req.set_header("proxy-authorization", "Basic cHJveHk6cHJveHlQd2Q=");
    req.set_header(HttpHeaders::COOKIE, "session_id=FakeSessionCookie");
    req.set_header(HttpHeaders::CONTENT_TYPE, "application/json");
    req.add_param("label", "load_1");

    const std::string s = req.debug_string();
    EXPECT_EQ(std::string::npos, s.find(encoded));
    EXPECT_EQ(std::string::npos, s.find("cHJveHk6cHJveHlQd2Q="));
    EXPECT_EQ(std::string::npos, s.find("FakeSessionCookie"));
    const std::string mask(kCredentialMask);
    EXPECT_NE(std::string::npos, s.find("key=Authorization, value=" + mask + "\n"));
    EXPECT_NE(std::string::npos, s.find("key=proxy-authorization, value=" + mask + "\n"));
    EXPECT_NE(std::string::npos, s.find("key=Cookie, value=" + mask + "\n"));
    // Everything else is printed as is.
    EXPECT_NE(std::string::npos, s.find("key=Content-Type, value=application/json\n"));
    EXPECT_NE(std::string::npos, s.find("key=label, value=load_1\n"));
}

TEST_F(HttpUtilsTest, DebugStringMasksSensitiveParams) {
    // Feed the request through init_from_evhttp() so the uri and the query params are filled in as in production.
    evhttp_request* ev_req = evhttp_request_new(nullptr, nullptr);
    ev_req->type = EVHTTP_REQ_POST;
    ev_req->uri = strdup("/api/update_config?secret_key=FakeSecret123&label=load_1");
    ev_req->uri_elems = evhttp_uri_parse(ev_req->uri);
    HttpRequest req(ev_req);
    ASSERT_EQ(0, req.init_from_evhttp());

    const std::string mask(kCredentialMask);
    const std::string masked = req.debug_string([](std::string_view key) { return key == "secret_key"; });
    EXPECT_EQ(std::string::npos, masked.find("FakeSecret123"));
    EXPECT_NE(std::string::npos, masked.find("uri:/api/update_config?label=load_1&secret_key=" + mask + "\n"));
    EXPECT_NE(std::string::npos, masked.find("key=secret_key, value=" + mask + "\n"));
    EXPECT_NE(std::string::npos, masked.find("key=label, value=load_1\n"));

    // Without a predicate, the uri and params are printed as is.
    const std::string plain = req.debug_string();
    EXPECT_NE(std::string::npos, plain.find("uri:/api/update_config?secret_key=FakeSecret123&label=load_1\n"));
    EXPECT_NE(std::string::npos, plain.find("key=secret_key, value=FakeSecret123\n"));

    // A predicate that matches nothing leaves the uri untouched.
    const std::string none = req.debug_string([](std::string_view) { return false; });
    EXPECT_NE(std::string::npos, none.find("uri:/api/update_config?secret_key=FakeSecret123&label=load_1\n"));

    evhttp_request_free(ev_req);
}

} // namespace starrocks
