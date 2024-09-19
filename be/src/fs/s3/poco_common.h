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

#pragma once

#include <Poco/Net/HTTPClientSession.h>
#include <Poco/Net/HTTPSClientSession.h>
#include <Poco/Timespan.h>
#include <Poco/Types.h>
#include <Poco/URI.h>
#include <aws/core/http/HttpRequest.h>

#include <memory>

namespace starrocks::poco {

struct ConnectionTimeouts {
    Poco::Timespan connection_timeout;
    Poco::Timespan send_timeout;
    Poco::Timespan receive_timeout;
    Poco::Timespan http_keep_alive_timeout{0};

    ConnectionTimeouts() = default;

    ConnectionTimeouts(const Poco::Timespan& connection_timeout_, const Poco::Timespan& send_timeout_,
                       const Poco::Timespan& receive_timeout_)
            : connection_timeout(connection_timeout_), send_timeout(send_timeout_), receive_timeout(receive_timeout_) {}
};

using HTTPSessionPtr = std::shared_ptr<Poco::Net::HTTPClientSession>;

bool isHTTPS(const Poco::URI& uri);

HTTPSessionPtr makeHTTPSessionImpl(const std::string& host, Poco::UInt16 port, bool https, bool keep_alive,
                                   bool resolve_host = true);

void setTimeouts(Poco::Net::HTTPClientSession& session, const ConnectionTimeouts& timeouts);

HTTPSessionPtr makeHTTPSession(const Poco::URI& uri, const ConnectionTimeouts& timeouts, bool resolve_host);

std::string getCurrentExceptionMessage();

bool checkRequestCanReturn2xxAndErrorInBody(Aws::Http::HttpRequest& request);

enum ResponseCode {
    SUCCESS_RESPONSE_MIN = 200,
    SUCCESS_RESPONSE_MAX = 299,
    TOO_MANY_REQUEST = 429,
    SERVICE_UNAVALIABLE = 503
};

} // namespace starrocks::poco
