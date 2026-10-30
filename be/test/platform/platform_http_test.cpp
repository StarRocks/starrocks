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
#include <gtest/gtest.h>

#include <cstring>
#include <string>

#include "base/auth/credential_mask.h"
#include "base/url_coding.h"
#include "platform/http/ev_http_server.h"
#include "platform/http/http_auth.h"
#include "platform/http/http_handler.h"
#include "platform/http/http_headers.h"
#include "platform/http/http_method.h"
#include "platform/http/http_parser.h"
#include "platform/http/http_request.h"
#include "platform/http/http_status.h"

namespace starrocks {

namespace {

class DummyHandler final : public HttpHandler {
public:
    void handle(HttpRequest* req) override {}
};

std::string parse_chunked_payload(const std::string& chunked) {
    HttpChunkParseCtx ctx;
    const uint8_t* cursor = reinterpret_cast<const uint8_t*>(chunked.data());
    const uint8_t* end = cursor + chunked.size();
    std::string payload;
    while (cursor < end) {
        auto state = HttpParser::http_parse_chunked(&cursor, end - cursor, &ctx);
        if (state == HttpParser::PARSE_OK) {
            payload.append(reinterpret_cast<const char*>(cursor), ctx.size);
            cursor += ctx.size;
            ctx.size = 0;
            continue;
        }
        if (state == HttpParser::PARSE_DONE) {
            break;
        }
        EXPECT_NE(HttpParser::PARSE_ERROR, state);
    }
    return payload;
}

} // namespace

TEST(PlatformHttpTest, HttpMethodConversion) {
    EXPECT_EQ(HttpMethod::GET, to_http_method("GET"));
    EXPECT_EQ(HttpMethod::POST, to_http_method("POST"));
    EXPECT_EQ(HttpMethod::OPTIONS, to_http_method("OPTIONS"));
    EXPECT_EQ(HttpMethod::UNKNOWN, to_http_method("PATCH"));

    EXPECT_EQ("GET", to_method_desc(HttpMethod::GET));
    EXPECT_EQ("UNKNOWN", to_method_desc(HttpMethod::UNKNOWN));
}

TEST(PlatformHttpTest, HttpStatusReasonAndCode) {
    EXPECT_EQ("200", to_code(HttpStatus::OK));
    EXPECT_EQ("OK", defalut_reason(HttpStatus::OK));
    EXPECT_EQ("No reason", defalut_reason(static_cast<HttpStatus>(999)));
}

TEST(PlatformHttpTest, RequestHeaderLookupIsCaseInsensitive) {
    HttpRequest req(nullptr);
    req._headers.emplace("Content-Type", "application/json");
    req._params.emplace("tablet_id", "12345");

    EXPECT_EQ("application/json", req.header("content-type"));
    EXPECT_EQ("application/json", req.header("CONTENT-TYPE"));
    EXPECT_EQ("12345", req.param("tablet_id"));
    EXPECT_TRUE(req.header("missing").empty());
    EXPECT_TRUE(req.param("missing").empty());
}

TEST(PlatformHttpTest, DebugStringMasksCredentialHeaders) {
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

TEST(PlatformHttpTest, DebugStringMasksSensitiveParams) {
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

TEST(PlatformHttpTest, BasicAuthParsesValidAuthorization) {
    HttpRequest req(nullptr);
    auto auth = encode_basic_auth("starrocks", "passwd");
    req._headers.emplace(HttpHeaders::AUTHORIZATION, auth);

    std::string user;
    std::string passwd;
    EXPECT_TRUE(parse_basic_auth(req, &user, &passwd));
    EXPECT_EQ("starrocks", user);
    EXPECT_EQ("passwd", passwd);
}

TEST(PlatformHttpTest, BasicAuthRejectsInvalidAuthorization) {
    {
        HttpRequest req(nullptr);
        req._headers.emplace(HttpHeaders::AUTHORIZATION, "Basic starrocks:passwd");

        std::string user;
        std::string passwd;
        EXPECT_FALSE(parse_basic_auth(req, &user, &passwd));
    }
    {
        HttpRequest req(nullptr);
        std::string encoded_str;
        base64_encode("starrockspasswd", &encoded_str);
        req._headers.emplace(HttpHeaders::AUTHORIZATION, "Basic " + encoded_str);

        std::string user;
        std::string passwd;
        EXPECT_FALSE(parse_basic_auth(req, &user, &passwd));
    }
    {
        HttpRequest req(nullptr);
        std::string encoded_str;
        base64_encode("starrocks:passwd", &encoded_str);
        req._headers.emplace(HttpHeaders::AUTHORIZATION, "Basic" + encoded_str);

        std::string user;
        std::string passwd;
        EXPECT_FALSE(parse_basic_auth(req, &user, &passwd));
    }
}

TEST(PlatformHttpTest, ChunkParserParsesPayload) {
    const std::string chunked = "4\r\nWiki\r\n5\r\npedia\r\n0\r\nTrailer: value\r\n\r\n";
    EXPECT_EQ("Wikipedia", parse_chunked_payload(chunked));

    HttpChunkParseCtx ctx;
    const std::string invalid = "Z\r\n";
    const uint8_t* cursor = reinterpret_cast<const uint8_t*>(invalid.data());
    EXPECT_EQ(HttpParser::PARSE_ERROR, HttpParser::http_parse_chunked(&cursor, invalid.size(), &ctx));
}

TEST(PlatformHttpTest, FindHandlerResolvesPathParamsAndStaticFallback) {
    EvHttpServer server("127.0.0.1", 0, 1);
    DummyHandler route_handler;
    DummyHandler static_handler;

    ASSERT_TRUE(server.register_handler(HttpMethod::GET, "/api/{db}/{table}", &route_handler));
    ASSERT_FALSE(server.register_handler(HttpMethod::UNKNOWN, "/api/invalid", &route_handler));

    HttpRequest route_req(nullptr);
    route_req._method = HttpMethod::GET;
    route_req._raw_path = "/api/test_db/test_table";
    EXPECT_EQ(&route_handler, server._find_handler(&route_req));
    EXPECT_EQ("test_db", route_req.param("db"));
    EXPECT_EQ("test_table", route_req.param("table"));

    server.register_static_file_handler(&static_handler);
    HttpRequest static_req(nullptr);
    static_req._method = HttpMethod::GET;
    static_req._raw_path = "/static/index.html";
    EXPECT_EQ(&static_handler, server._find_handler(&static_req));
}

} // namespace starrocks
