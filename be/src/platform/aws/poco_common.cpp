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

#include "platform/aws/poco_common.h"

#include <Poco/Exception.h>
#include <fmt/format.h>

#include <cstdint>
#include <memory>
#include <mutex>
#include <sstream>
#include <unordered_map>

namespace starrocks::poco {

bool isHTTPS(const Poco::URI& uri) {
    if (uri.getScheme() == "https")
        return true;
    else if (uri.getScheme() == "http")
        return false;
    else
        throw std::runtime_error(fmt::format("Unsupported scheme in URI '{}'", uri.toString()));
}

void setTimeouts(Poco::Net::HTTPClientSession& session, const ConnectionTimeouts& timeouts) {
    session.setTimeout(timeouts.connection_timeout, timeouts.send_timeout, timeouts.receive_timeout);
    session.setKeepAliveTimeout(timeouts.http_keep_alive_timeout);
}

// Poco's own send/receive default, read from a freshly constructed session rather than hardcoded
// so it cannot drift from the bundled Poco version.
static const Poco::Timespan& poco_default_timeout() {
    static const Poco::Timespan kDefault = [] {
        Poco::Net::HTTPClientSession probe;
        return probe.getReceiveTimeout();
    }();
    return kDefault;
}

void apply_request_timeouts(Poco::Net::HTTPClientSession& session, const ConnectionTimeouts& timeouts) {
    // Sessions are pooled per endpoint, not per client, and clients that share an endpoint do not
    // share a timeout: a RENAME_FILE client carries object_storage_rename_file_request_timeout_ms
    // (30 s by default) while an ordinary read carries object_storage_request_timeout_ms (unset by
    // default). So this has to leave the session in the state THIS request asked for, never in
    // whatever state the previous borrower left behind -- skipping the call when a request has no
    // timeout would let it inherit another client's.
    //
    // A non-positive value means "unset": object_storage_request_timeout_ms defaults to -1, which
    // arrives as a negative Timespan, and Poco gives no defined meaning to that. Unset restores
    // Poco's default instead of passing the value through.
    const bool has_timeout =
            timeouts.send_timeout.totalMicroseconds() > 0 && timeouts.receive_timeout.totalMicroseconds() > 0;
    const Poco::Timespan& send = has_timeout ? timeouts.send_timeout : poco_default_timeout();
    const Poco::Timespan& receive = has_timeout ? timeouts.receive_timeout : poco_default_timeout();
    session.setTimeout(timeouts.connection_timeout, send, receive);

    // Keep-alive is deliberately not touched: ConnectionTimeouts default-initializes
    // http_keep_alive_timeout to zero and PocoHttpClient never sets it, so writing it through
    // would work against the pool, which exists to reuse connections.
}

std::string getCurrentExceptionMessage() {
    std::stringstream ss;

    try {
        throw;
    } catch (const Poco::Exception& e) {
        ss << fmt::format("Poco::Exception. e.code() = {}, e.displayText() = {}, e.what() = {}", e.code(),
                          e.displayText(), e.what());
    } catch (const std::exception& e) {
        ss << fmt::format("std::exception. type: {}, e.what() = {}", typeid(e).name(), e.what());
    } catch (...) {
        ss << fmt::format("Unknown exception from poco client");
    }

    return ss.str();
}

// 1) https://aws.amazon.com/premiumsupport/knowledge-center/s3-resolve-200-internalerror/
// 2) https://github.com/aws/aws-sdk-cpp/issues/658
bool checkRequestCanReturn2xxAndErrorInBody(Aws::Http::HttpRequest& request) {
    auto query_params = request.GetQueryStringParameters();
    if (request.HasHeader("x-amz-copy-source")) {
        // CopyObject https://docs.aws.amazon.com/AmazonS3/latest/API/API_CopyObject.html
        if (query_params.empty()) return true;

        // UploadPartCopy https://docs.aws.amazon.com/AmazonS3/latest/API/API_UploadPartCopy.html
        if (query_params.contains("partNumber") && query_params.contains("uploadId")) return true;

    } else {
        // CompleteMultipartUpload https://docs.aws.amazon.com/AmazonS3/latest/API/API_CompleteMultipartUpload.html
        if (query_params.size() == 1 && query_params.contains("uploadId")) return true;
    }

    return false;
}

HTTPSessionPtr makeHTTPSessionImpl(const std::string& host, Poco::UInt16 port, bool https, bool keep_alive) {
    HTTPSessionPtr session;

    if (https) {
        session = std::make_shared<Poco::Net::HTTPSClientSession>(host, port);
    } else {
        session = std::make_shared<Poco::Net::HTTPClientSession>(host, port);
    }

    // doesn't work properly without patch
    session->setKeepAlive(keep_alive);
    return session;
}

EndpointHTTPSessionPool::Base::ObjectPtr EndpointHTTPSessionPool::allocObject() {
    auto session = makeHTTPSessionImpl(_host, _port, _is_https, true);
    return session;
}

HTTPSessionPools& HTTPSessionPools::instance() {
    static HTTPSessionPools instance;
    return instance;
}

PooledHTTPSessionPtr HTTPSessionPools::getSession(const Poco::URI& uri, const ConnectionTimeouts& timeouts,
                                                  bool resolve_host) {
    const std::string& host = uri.getHost();
    uint16_t port = uri.getPort();
    bool is_https = isHTTPS(uri);

    const Key key = {.host = host, .port = port, .is_https = is_https};

    EndpointPoolPtr pool = nullptr;
    {
        std::lock_guard lock(_mutex);
        auto item = _endpoint_pools.find(key);
        if (item == _endpoint_pools.end()) {
            std::tie(item, std::ignore) =
                    _endpoint_pools.emplace(key, std::make_shared<EndpointHTTPSessionPool>(host, port, is_https));
        }
        pool = item->second;
    }

    auto session = pool->get(timeouts.connection_timeout.totalMicroseconds());
    // A pooled session is reused, so whatever the previous caller left on it is still in
    // effect -- apply this request's timeouts before handing it out. Without this the send and
    // receive timeouts computed from ClientConfiguration were dropped here, and a read that
    // stopped receiving waited out Poco's built-in default instead of the configured value.
    apply_request_timeouts(*session, timeouts);
    session->attachSessionData({});

    return session;
}

void HTTPSessionPools::shutdown() {
    std::lock_guard lock(_mutex);
    _endpoint_pools.clear();
}

PooledHTTPSessionPtr makeHTTPSession(const Poco::URI& uri, const ConnectionTimeouts& timeouts, bool resolve_host) {
    return HTTPSessionPools::instance().getSession(uri, timeouts, resolve_host);
}

} // namespace starrocks::poco
