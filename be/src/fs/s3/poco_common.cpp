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

#include "fs/s3/poco_common.h"

#include <Poco/Exception.h>
#include <fmt/format.h>

#include <cstdint>
#include <memory>
#include <mutex>
#include <sstream>
#include <unordered_map>

#include "runtime/current_thread.h"

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
    SCOPED_THREAD_LOCAL_MEM_TRACKER_SETTER(_mem_tracker);
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
