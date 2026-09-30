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

#include "common/brpc/brpc_connection_load.h"

#include <google/protobuf/message.h>

#include <limits>
#include <map>
#include <mutex>
#include <sstream>
#include <tuple>
#include <utility>

namespace starrocks {

namespace {

using ConnectionKey = std::tuple<std::string, std::string, int64_t>;

class BrpcConnectionLoadRegistry {
public:
    BrpcConnectionLoadRegistry() : _cleanup_it(_loads.end()) {}

    std::shared_ptr<BrpcConnectionLoad> get(const butil::EndPoint& endpoint, const std::string& protocol,
                                            int64_t connection_group_seed) {
        std::ostringstream endpoint_stream;
        endpoint_stream << endpoint;
        ConnectionKey key(endpoint_stream.str(), protocol, connection_group_seed);

        std::lock_guard lock(_mutex);
        auto& weak_load = _loads[key];
        auto load = weak_load.lock();
        if (load == nullptr) {
            load = std::make_shared<BrpcConnectionLoad>();
            weak_load = load;
        }
        _cleanup_expired_entries();
        return load;
    }

private:
    void _cleanup_expired_entries() {
        // Bound work under the registry mutex. The cursor eventually visits every entry as new slots are acquired.
        constexpr size_t kEntriesPerCleanup = 8;
        for (size_t i = 0; i < kEntriesPerCleanup && !_loads.empty(); ++i) {
            if (_cleanup_it == _loads.end()) {
                _cleanup_it = _loads.begin();
            }
            auto current = _cleanup_it++;
            if (current->second.expired()) {
                _loads.erase(current);
            }
        }
    }

    std::mutex _mutex;
    std::map<ConnectionKey, std::weak_ptr<BrpcConnectionLoad>> _loads;
    std::map<ConnectionKey, std::weak_ptr<BrpcConnectionLoad>>::iterator _cleanup_it;
};

BrpcConnectionLoadRegistry& connection_load_registry() {
    static BrpcConnectionLoadRegistry registry;
    return registry;
}

} // namespace

BrpcConnectionLoadGuard::BrpcConnectionLoadGuard(std::shared_ptr<BrpcConnectionLoad> load, int64_t payload_bytes)
        : _load(std::move(load)),
          _in_flight_before(_load->_num_in_flight_rpcs.fetch_add(1)),
          _payload_bytes(payload_bytes) {
    _load->_in_flight_payload_bytes.fetch_add(_payload_bytes);
}

BrpcConnectionLoadGuard::~BrpcConnectionLoadGuard() {
    reset();
}

BrpcConnectionLoadGuard::BrpcConnectionLoadGuard(BrpcConnectionLoadGuard&& other) noexcept
        : _load(std::move(other._load)),
          _in_flight_before(other._in_flight_before),
          _payload_bytes(other._payload_bytes) {}

BrpcConnectionLoadGuard& BrpcConnectionLoadGuard::operator=(BrpcConnectionLoadGuard&& other) noexcept {
    if (this != &other) {
        reset();
        _load = std::move(other._load);
        _in_flight_before = other._in_flight_before;
        _payload_bytes = other._payload_bytes;
    }
    return *this;
}

void BrpcConnectionLoadGuard::reset() {
    if (_load != nullptr) {
        _load->_num_in_flight_rpcs.fetch_sub(1);
        _load->_in_flight_payload_bytes.fetch_sub(_payload_bytes);
        _load.reset();
    }
}

std::shared_ptr<BrpcConnectionLoad> get_brpc_connection_load(const butil::EndPoint& endpoint,
                                                             const std::string& protocol,
                                                             int64_t connection_group_seed) {
    return connection_load_registry().get(endpoint, protocol, connection_group_seed);
}

int64_t brpc_request_payload_bytes(const google::protobuf::Message* request,
                                   google::protobuf::RpcController* controller) {
    const size_t request_bytes = request != nullptr ? request->ByteSizeLong() : 0;
    const size_t attachment_bytes =
            controller != nullptr ? static_cast<brpc::Controller*>(controller)->request_attachment().size() : 0;
    constexpr size_t max_payload_bytes = static_cast<size_t>(std::numeric_limits<int64_t>::max());
    if (request_bytes > max_payload_bytes || attachment_bytes > max_payload_bytes - request_bytes) {
        return std::numeric_limits<int64_t>::max();
    }
    return static_cast<int64_t>(request_bytes + attachment_bytes);
}

} // namespace starrocks
