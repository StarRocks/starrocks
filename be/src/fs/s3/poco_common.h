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

#include "fs/s3/pool_base.h"
#include "runtime/exec_env.h"
#include "runtime/mem_tracker.h"

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

using PooledHTTPSessionPtr = PoolBase<Poco::Net::HTTPClientSession>::Entry;
using HTTPSessionPtr = std::shared_ptr<Poco::Net::HTTPClientSession>;

bool isHTTPS(const Poco::URI& uri);

void setTimeouts(Poco::Net::HTTPClientSession& session, const ConnectionTimeouts& timeouts);

HTTPSessionPtr makeHTTPSessionImpl(const std::string& host, Poco::UInt16 port, bool https, bool keep_alive);

PooledHTTPSessionPtr makeHTTPSession(const Poco::URI& uri, const ConnectionTimeouts& timeouts, bool resolve_host);

std::string getCurrentExceptionMessage();

bool checkRequestCanReturn2xxAndErrorInBody(Aws::Http::HttpRequest& request);

enum ResponseCode {
    SUCCESS_RESPONSE_MIN = 200,
    SUCCESS_RESPONSE_MAX = 299,
    TOO_MANY_REQUEST = 429,
    SERVICE_UNAVALIABLE = 503
};

class EndpointHTTPSessionPool : public PoolBase<Poco::Net::HTTPClientSession> {
public:
    using Base = PoolBase<Poco::Net::HTTPClientSession>;
    EndpointHTTPSessionPool(std::string host, uint16_t port, bool is_https)
            : Base(ENDPOINT_POOL_SIZE), _host(std::move(host)), _port(port), _is_https(is_https) {
        _mem_tracker = GlobalEnv::GetInstance()->poco_connection_pool_mem_tracker();
    }

private:
    ObjectPtr allocObject() override;

    static constexpr size_t ENDPOINT_POOL_SIZE = 1024;

    const std::string _host;
    const uint16_t _port;
    const bool _is_https;
    MemTracker* _mem_tracker = nullptr;
};

class HTTPSessionPools {
private:
    struct Key {
        std::string host;
        uint16_t port;
        bool is_https;

        bool operator==(const Key& rhs) const {
            return std::tie(host, port, is_https) == std::tie(rhs.host, rhs.port, rhs.is_https);
        }
    };

    struct Hasher {
        uint32_t operator()(const Key& k) const {
            return std::hash<std::string>()(k.host) ^ std::hash<uint16_t>()(k.port) ^ std::hash<bool>()(k.is_https);
        }
    };

public:
    using EndpointPoolPtr = std::shared_ptr<EndpointHTTPSessionPool>;
    static HTTPSessionPools& instance();

    PooledHTTPSessionPtr getSession(const Poco::URI& uri, const ConnectionTimeouts& timeouts, bool resolve_host);

    void shutdown();

private:
    HTTPSessionPools() = default;

    std::mutex _mutex;
    std::unordered_map<Key, EndpointPoolPtr, Hasher> _endpoint_pools;
};

} // namespace starrocks::poco
