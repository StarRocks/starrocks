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

#include <chrono>
#include <cstdint>
#include <memory>
#include <mutex>
#include <sstream>
#include <unordered_map>

#include "base/network/network_util.h"
#include "common/logging.h"
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
    auto session = makeHTTPSessionImpl(_host, _port, _is_https, true);
    return session;
}

HTTPSessionPools& HTTPSessionPools::instance() {
    static HTTPSessionPools instance;
    return instance;
}

// Resolve hostname to IP with TTL cache to avoid blocking DNS on every getSession() and to limit
// _endpoint_pools growth when DNS results change. Caller must not hold _mutex.
std::string HTTPSessionPools::resolveHostWithCache(const std::string& host) {
    auto now = std::chrono::steady_clock::now();
    {
        std::lock_guard lock(_mutex);
        auto it = _resolve_cache.find(host);
        if (it != _resolve_cache.end() && it->second.expiry > now) {
            return it->second.resolved_ip;
        }
    }
    std::string resolved_ip;
    if (!starrocks::hostname_to_ip(host, resolved_ip).ok()) {
        VLOG(2) << "Failed to resolve S3 endpoint host to IP, using hostname as pool key: " << host;
        return host;
    }
    {
        std::lock_guard lock(_mutex);
        _resolve_cache[host] = {
                resolved_ip,
                now + std::chrono::seconds(RESOLVE_CACHE_TTL_SEC),
        };
    }
    return resolved_ip;
}

// When endpoint pool count exceeds cap, evict oldest pools so we don't grow without bound
// (e.g. when resolve_host is on and DNS returns changing IPs). Caller must hold _mutex.
void HTTPSessionPools::evictExcessPoolsLocked() {
    while (_endpoint_pools.size() >= MAX_ENDPOINT_POOLS && !_pool_order.empty()) {
        const Key& old_key = _pool_order.front();
        _endpoint_pools.erase(old_key);
        _pool_order.pop_front();
    }
}

// When resolve_host is true, use resolved IP as the session pool key and connection target.
// Resolved IP is cached with TTL to avoid blocking DNS on every request; pool count is capped
// with eviction to prevent unbounded growth when DNS results change.
// Host-to-IP resolving is applied only for HTTP. For HTTPS we keep the hostname so that TLS
// SNI and certificate hostname verification use the hostname (certs are issued for hostnames,
// not backend IPs); resolving HTTPS to an IP would break verification.
PooledHTTPSessionPtr HTTPSessionPools::getSession(const Poco::URI& uri, const ConnectionTimeouts& timeouts,
                                                  bool resolve_host) {
    std::string host = uri.getHost();
    uint16_t port = uri.getPort();
    bool is_https = isHTTPS(uri);

    if (resolve_host && !is_https && !host.empty() && !starrocks::is_valid_ip(host)) {
        host = resolveHostWithCache(host);
    }

    const Key key = {.host = host, .port = port, .is_https = is_https};

    EndpointPoolPtr pool = nullptr;
    {
        std::lock_guard lock(_mutex);
        evictExcessPoolsLocked();
        auto item = _endpoint_pools.find(key);
        if (item == _endpoint_pools.end()) {
            std::tie(item, std::ignore) =
                    _endpoint_pools.emplace(key, std::make_shared<EndpointHTTPSessionPool>(host, port, is_https));
            _pool_order.push_back(key);
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
    _pool_order.clear();
    _resolve_cache.clear();
}

PooledHTTPSessionPtr makeHTTPSession(const Poco::URI& uri, const ConnectionTimeouts& timeouts, bool resolve_host) {
    return HTTPSessionPools::instance().getSession(uri, timeouts, resolve_host);
}

} // namespace starrocks::poco
