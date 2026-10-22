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

#include "service/service_be/http_auth_response.h"

#include <event2/buffer.h>
#include <event2/http.h>
#include <gtest/gtest.h>

#include <limits>
#include <string>
#include <vector>

#include "common/config_http_fwd.h"
#include "common/status.h"
#include "common/system/master_info.h"
#include "platform/http/ev_http_server.h"
#include "platform/http/http_auth.h"
#include "platform/http/http_handler.h"
#include "platform/http/http_headers.h"
#include "platform/http/http_request.h"
#include "platform/http/http_status.h"

namespace starrocks {

// `auth_failure_from_fe_status` is the pure mapping that decides what HTTP
// response a BE returns based on what FE's checkAuth came back with. The
// classes are intentionally distinct because each suggests a different
// remedy to the client:
//
//   NOT_AUTHORIZED (401 + WWW-Authenticate): fix your credentials
//   INTERNAL_ERROR (500): a bug on the FE — operator must investigate
//   network/RPC failure: 503, handled by caller (not this helper)
//
// Regression here is silent (e.g. someone collapses the switch and lumps
// all non-OK into 401), so the cases are pinned explicitly.

TEST(AuthFailureFromFeStatusTest, ok_returns_nullopt) {
    auto result = auth_failure_from_fe_status(Status::OK());
    EXPECT_FALSE(result.has_value());
}

static const std::vector<std::string> kBothChallenges = {"Basic realm=\"StarRocks\"",
                                                         "Bearer realm=\"StarRocks\", error=\"invalid_token\""};

TEST(AuthFailureFromFeStatusTest, not_authorized_maps_to_401_with_both_challenges) {
    auto result = auth_failure_from_fe_status(Status::NotAuthorized("bad password"));
    ASSERT_TRUE(result.has_value());
    EXPECT_EQ(HttpStatus::UNAUTHORIZED, result->http_status);
    // Same two lines the FE sends: a password client and a token client each find their scheme.
    EXPECT_EQ(kBothChallenges, result->www_authenticate);
    // Body shape mirrors FE RestBaseResult — verify message round-trips into JSON.
    EXPECT_NE(std::string::npos, result->body.find("FAILED"));
    EXPECT_NE(std::string::npos, result->body.find("bad password"));
}

TEST(AuthFailureFromFeStatusTest, internal_error_maps_to_500_without_basic_challenge) {
    // FE might return InternalError for unknown TPrivilegeRequirement enum
    // values (cross-version mismatch). Client retrying with the same creds
    // would never help, so we must NOT emit a Basic challenge. The body is
    // a generic "auth verification failed" — internal error strings stay
    // server-side (logged at WARNING), never echoed to the HTTP client.
    auto result = auth_failure_from_fe_status(Status::InternalError("unknown required_privilege: 999"));
    ASSERT_TRUE(result.has_value());
    EXPECT_EQ(HttpStatus::INTERNAL_SERVER_ERROR, result->http_status);
    EXPECT_TRUE(result->www_authenticate.empty());
    EXPECT_NE(std::string::npos, result->body.find("auth verification failed"));
    EXPECT_EQ(std::string::npos, result->body.find("unknown required_privilege"));
}

TEST(AuthFailureFromFeStatusTest, other_non_ok_maps_to_500) {
    // Defensive: any non-OK that isn't NOT_AUTHORIZED is treated as server-side.
    // Pick a status that's neither — Aborted is a reasonable stand-in for
    // "something unexpected came back from FE".
    auto result = auth_failure_from_fe_status(Status::Aborted("aborted"));
    ASSERT_TRUE(result.has_value());
    EXPECT_EQ(HttpStatus::INTERNAL_SERVER_ERROR, result->http_status);
    EXPECT_TRUE(result->www_authenticate.empty());
}

TEST(MakeUnauthorizedTest, has_401_and_both_challenges) {
    auto failure = make_unauthorized("missing Basic");
    EXPECT_EQ(HttpStatus::UNAUTHORIZED, failure.http_status);
    EXPECT_EQ(kBothChallenges, failure.www_authenticate);
    EXPECT_NE(std::string::npos, failure.body.find("missing Basic"));
}

TEST(MakeServerErrorTest, respects_caller_supplied_status_and_omits_www_authenticate) {
    auto failure = make_server_error(HttpStatus::SERVICE_UNAVAILABLE, "FE unreachable");
    EXPECT_EQ(HttpStatus::SERVICE_UNAVAILABLE, failure.http_status);
    EXPECT_TRUE(failure.www_authenticate.empty());
    EXPECT_NE(std::string::npos, failure.body.find("FE unreachable"));
}

TEST(BuildAuthFailureBodyTest, contains_failed_status_code_and_message) {
    std::string body = build_auth_failure_body("denied: bad pw");
    // Shape mirrors FE's RestBaseResult — must be parseable JSON with the three
    // expected keys (status / code / message).
    EXPECT_NE(std::string::npos, body.find("\"status\":\"FAILED\""));
    EXPECT_NE(std::string::npos, body.find("\"code\":\"1\""));
    EXPECT_NE(std::string::npos, body.find("\"message\":\"denied: bad pw\""));
}

TEST(BuildAuthFailureBodyTest, escapes_quotes_in_message) {
    // rapidjson must escape user-controlled strings to keep the body parseable.
    std::string body = build_auth_failure_body("he said \"hi\"");
    EXPECT_NE(std::string::npos, body.find("he said \\\"hi\\\""));
}

// --------- to_thrift_privilege ---------
// Every enum value must round-trip to its thrift twin; missing cases trip
// `-Wswitch` at compile time. The unreachable fallback is what catches a future
// enum value that someone adds to the C++ side but forgets to map — pin its
// "fail closed as OPERATE" behavior so we don't silently regress to NONE.

TEST(ToThriftPrivilegeTest, maps_each_enum_value) {
    EXPECT_EQ(TPrivilegeRequirement::NONE, to_thrift_privilege(HttpHandler::RequiredPrivilege::NONE));
    EXPECT_EQ(TPrivilegeRequirement::OPERATE, to_thrift_privilege(HttpHandler::RequiredPrivilege::OPERATE));
    EXPECT_EQ(TPrivilegeRequirement::NODE, to_thrift_privilege(HttpHandler::RequiredPrivilege::NODE));
}

TEST(ToThriftPrivilegeTest, unknown_enum_value_fails_closed_as_operate) {
    // Synthesize an out-of-range RequiredPrivilege to simulate a future enum
    // addition that didn't get a switch case. Fail-closed = OPERATE (still
    // rejects non-operator callers; previously the default branch silently
    // returned NONE, which would have downgraded authorization to AuthN-only).
    auto bogus = static_cast<HttpHandler::RequiredPrivilege>(99);
    EXPECT_EQ(TPrivilegeRequirement::OPERATE, to_thrift_privilege(bogus));
}

// --------- verify_http_auth ---------
// The verifier does flag-gate → Basic-Auth parse → FE master check → RPC.
// Everything up to the RPC is unit-testable. The RPC + result handling are
// covered end-to-end by summary/http_auth/http_auth_smoke_test.py (a real
// cluster is needed to exercise FE's checkAuth).

class VerifyHttpAuthTest : public testing::Test {
public:
    void SetUp() override {
        _saved_flag = config::enable_http_auth;
        _saved_master_info = get_master_info();
        _ev_req = evhttp_request_new(nullptr, nullptr);
        ASSERT_NE(nullptr, _ev_req);
    }
    void TearDown() override {
        config::enable_http_auth = _saved_flag;
        // Restore master_info so later tests in this binary aren't affected.
        // master_info refuses lower-or-equal epoch updates; bump just one above
        // whatever the current epoch is so later tests can still apply their
        // own updates (previously we used int64_max here, which poisoned every
        // subsequent update_master_info call).
        TMasterInfo restored = _saved_master_info;
        restored.epoch = get_master_info().epoch + 1;
        (void)update_master_info(restored);
        if (_ev_req != nullptr) {
            evhttp_request_free(_ev_req);
            _ev_req = nullptr;
        }
    }

protected:
    bool _saved_flag = false;
    TMasterInfo _saved_master_info;
    evhttp_request* _ev_req = nullptr;
};

TEST_F(VerifyHttpAuthTest, flag_off_skips_verification) {
    config::enable_http_auth = false;
    HttpRequest req(_ev_req);
    // No headers populated, no FE configured. With the flag off, none of that
    // matters — must return nullopt unconditionally.
    auto result = verify_http_auth(&req, HttpHandler::RequiredPrivilege::OPERATE);
    EXPECT_FALSE(result.has_value());
}

TEST_F(VerifyHttpAuthTest, missing_authorization_header_returns_401) {
    config::enable_http_auth = true;
    HttpRequest req(_ev_req);
    // No Authorization header → parse_auth fails → 401 with both challenges.
    auto result = verify_http_auth(&req, HttpHandler::RequiredPrivilege::NONE);
    ASSERT_TRUE(result.has_value());
    EXPECT_EQ(HttpStatus::UNAUTHORIZED, result->http_status);
    EXPECT_EQ(kBothChallenges, result->www_authenticate);
}

TEST_F(VerifyHttpAuthTest, unknown_scheme_returns_401) {
    config::enable_http_auth = true;
    HttpRequest req(_ev_req);
    // Neither Basic nor Bearer: nothing on this port speaks Digest.
    req.set_header(HttpHeaders::AUTHORIZATION, "Digest username=\"root\"");
    auto result = verify_http_auth(&req, HttpHandler::RequiredPrivilege::NONE);
    ASSERT_TRUE(result.has_value());
    EXPECT_EQ(HttpStatus::UNAUTHORIZED, result->http_status);
    EXPECT_EQ(kBothChallenges, result->www_authenticate);
}

// --------- parse_auth: the Authorization header as clients actually send it ---------

TEST_F(VerifyHttpAuthTest, parse_auth_reads_bearer_into_passwd_with_auth_type) {
    HttpRequest req(_ev_req);
    req.set_header(HttpHeaders::AUTHORIZATION, "Bearer eyJ.hdr.sig");
    AuthInfo auth;
    ASSERT_TRUE(parse_auth(req, &auth));
    EXPECT_EQ("eyJ.hdr.sig", auth.passwd);
    EXPECT_EQ(AuthInfo::AUTH_TYPE_BEARER, auth.auth_type);
    EXPECT_TRUE(auth.user.empty()) << "a Bearer request declares no user; the FE reads it from the token";
}

TEST_F(VerifyHttpAuthTest, parse_auth_scheme_is_case_insensitive_and_value_is_trimmed) {
    // RFC 7235: the scheme token is case-insensitive and any amount of blank separates it from
    // the credentials. Proxies and hand-written clients produce all of these.
    HttpRequest req(_ev_req);
    req.set_header(HttpHeaders::AUTHORIZATION, "  bearer   eyJ.hdr.sig \t");
    AuthInfo bearer;
    ASSERT_TRUE(parse_auth(req, &bearer));
    EXPECT_EQ("eyJ.hdr.sig", bearer.passwd);
    EXPECT_EQ(AuthInfo::AUTH_TYPE_BEARER, bearer.auth_type);

    req.set_header(HttpHeaders::AUTHORIZATION, "BASIC  cm9vdDpwdw=="); // root:pw
    AuthInfo basic;
    ASSERT_TRUE(parse_auth(req, &basic));
    EXPECT_EQ("root", basic.user);
    EXPECT_EQ("pw", basic.passwd);
    EXPECT_TRUE(basic.auth_type.empty()) << "empty auth_type is what the FE reads as basic";
}

TEST_F(VerifyHttpAuthTest, parse_auth_rejects_a_scheme_without_a_value_or_glued_to_it) {
    HttpRequest req(_ev_req);
    AuthInfo auth;
    req.set_header(HttpHeaders::AUTHORIZATION, "Bearer");
    EXPECT_FALSE(parse_auth(req, &auth));
    req.set_header(HttpHeaders::AUTHORIZATION, "Bearer   ");
    EXPECT_FALSE(parse_auth(req, &auth));
    req.set_header(HttpHeaders::AUTHORIZATION, "Bearertoken");
    EXPECT_FALSE(parse_auth(req, &auth)) << "\"Bearertoken\" is not the Bearer scheme";
    req.set_header(HttpHeaders::AUTHORIZATION, "Basiccm9vdDpwdw==");
    EXPECT_FALSE(parse_auth(req, &auth));
}

TEST_F(VerifyHttpAuthTest, unknown_fe_master_returns_503) {
    config::enable_http_auth = true;
    // Overwrite master info with an empty endpoint. Use a high epoch so we win
    // against any state a prior test in this binary may have stored (the
    // master-info store ignores updates with a lower-or-equal epoch).
    TMasterInfo empty;
    empty.network_address.hostname = "";
    empty.network_address.port = 0;
    empty.epoch = 999999999;
    EXPECT_TRUE(update_master_info(empty));

    HttpRequest req(_ev_req);
    req.set_header(HttpHeaders::AUTHORIZATION, "Basic cm9vdDo="); // root:
    auto result = verify_http_auth(&req, HttpHandler::RequiredPrivilege::OPERATE);
    ASSERT_TRUE(result.has_value());
    EXPECT_EQ(HttpStatus::SERVICE_UNAVAILABLE, result->http_status);
    // Service-side failure ⇒ no WWW-Authenticate (the client's creds may be fine).
    EXPECT_TRUE(result->www_authenticate.empty());
    EXPECT_NE(std::string::npos, result->body.find("FE master address unknown"));
}

} // namespace starrocks
