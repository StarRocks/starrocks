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

#include <gtest/gtest.h>

#include "base/auth/credential_mask.h"
#include "common/logging.h"
#include "http/http_headers.h"
#include "http/http_request.h"
#include "http/utils.h"
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

TEST_F(HttpUtilsTest, debug_string_masks_credential_headers) {
    HttpRequest req(_evhttp_req);
    std::string encoded;
    base64_encode("root:FakePwd123", &encoded);
    req._headers.emplace(HttpHeaders::AUTHORIZATION, "Basic " + encoded);
    // Header names are case-insensitive, so a lower-case name must be masked too.
    req._headers.emplace("proxy-authorization", "Basic cHJveHk6cHJveHlQd2Q=");
    req._headers.emplace(HttpHeaders::COOKIE, "session_id=FakeSessionCookie");
    req._headers.emplace(HttpHeaders::CONTENT_TYPE, "application/json");
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

} // namespace starrocks
