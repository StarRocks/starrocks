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

#include "service/service_be/arrow_flight_auth_server_middleware.h"

#include <gtest/gtest.h>

#include <map>
#include <memory>
#include <string>

#include "arrow/util/base64.h"

namespace starrocks {

namespace {

// Literals mirror arrow_flight_call_header_utils.h, which defines non-inline functions and so cannot be
// included from a second test translation unit.
constexpr char kAuthHeader[] = "authorization";
constexpr char kBearerPrefix[] = "Bearer ";
constexpr char kBearerDefaultToken[] = "bearertoken";
constexpr char kBasicPrefix[] = "Basic ";

class FakeServerCallContext : public arrow::flight::ServerCallContext {
public:
    explicit FakeServerCallContext(arrow::flight::CallHeaders headers) : _headers(std::move(headers)) {}

    const std::string& peer_identity() const override { return _empty; }
    const std::string& peer() const override { return _empty; }
    void AddHeader(const std::string& key, const std::string& value) const override {}
    void AddTrailer(const std::string& key, const std::string& value) const override {}
    arrow::flight::ServerMiddleware* GetMiddleware(const std::string& key) const override { return nullptr; }
    bool is_cancelled() const override { return false; }
    const arrow::flight::CallHeaders& incoming_headers() const override { return _headers; }

private:
    arrow::flight::CallHeaders _headers;
    std::string _empty;
};

class RecordingAddCallHeaders : public arrow::flight::AddCallHeaders {
public:
    void AddHeader(const std::string& key, const std::string& value) override { headers.emplace(key, value); }

    std::multimap<std::string, std::string> headers;
};

} // namespace

// The bearer middleware is not the authentication boundary (the per-query ticket is), so it must
// accept a call regardless of what bearer token the client sends, and must not add any header.
TEST(ArrowFlightAuthServerMiddlewareTest, BearerFactoryAcceptsAnyToken) {
    NoOpBearerAuthServerMiddlewareFactory factory;
    arrow::flight::CallInfo info{arrow::flight::FlightMethod::DoGet};

    for (const std::string& token : {std::string(kBearerDefaultToken), std::string("some-other-token")}) {
        arrow::flight::CallHeaders headers;
        headers.insert({kAuthHeader, std::string(kBearerPrefix) + token});
        FakeServerCallContext context(std::move(headers));

        std::shared_ptr<arrow::flight::ServerMiddleware> middleware;
        ASSERT_TRUE(factory.StartCall(info, context, &middleware).ok());
        ASSERT_NE(middleware, nullptr);
        ASSERT_EQ(middleware->name(), "NoOpBearerAuthServerMiddleware");

        RecordingAddCallHeaders outgoing;
        middleware->SendingHeaders(&outgoing);
        ASSERT_TRUE(outgoing.headers.empty());
        middleware->CallCompleted(arrow::Status::OK());
    }
}

TEST(ArrowFlightAuthServerMiddlewareTest, BearerFactoryAcceptsMissingHeader) {
    NoOpBearerAuthServerMiddlewareFactory factory;
    arrow::flight::CallInfo info{arrow::flight::FlightMethod::DoGet};
    FakeServerCallContext context({});

    std::shared_ptr<arrow::flight::ServerMiddleware> middleware;
    ASSERT_TRUE(factory.StartCall(info, context, &middleware).ok());
    ASSERT_NE(middleware, nullptr);
}

TEST(ArrowFlightAuthServerMiddlewareTest, HeaderFactorySendsDefaultBearerToken) {
    NoOpHeaderAuthServerMiddlewareFactory factory;
    arrow::flight::CallInfo info{arrow::flight::FlightMethod::Handshake};
    arrow::flight::CallHeaders headers;
    headers.insert({kAuthHeader, std::string(kBasicPrefix) + arrow::util::base64_encode("user:pass")});
    FakeServerCallContext context(std::move(headers));

    std::shared_ptr<arrow::flight::ServerMiddleware> middleware;
    ASSERT_TRUE(factory.StartCall(info, context, &middleware).ok());
    ASSERT_NE(middleware, nullptr);

    RecordingAddCallHeaders outgoing;
    middleware->SendingHeaders(&outgoing);
    auto it = outgoing.headers.find(kAuthHeader);
    ASSERT_NE(it, outgoing.headers.end());
    ASSERT_EQ(it->second, std::string(kBearerPrefix) + kBearerDefaultToken);
}

} // namespace starrocks
