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

#include <atomic>
#include <cstdint>
#include <memory>
#include <string>
#include <utility>

#include "base/brpc/brpc.h"

namespace google::protobuf {
class Message;
class RpcController;
} // namespace google::protobuf

namespace starrocks {

class BrpcConnectionLoad;

class BrpcConnectionLoadGuard {
public:
    BrpcConnectionLoadGuard() = default;
    ~BrpcConnectionLoadGuard();

    BrpcConnectionLoadGuard(BrpcConnectionLoadGuard&& other) noexcept;
    BrpcConnectionLoadGuard& operator=(BrpcConnectionLoadGuard&& other) noexcept;

    BrpcConnectionLoadGuard(const BrpcConnectionLoadGuard&) = delete;
    BrpcConnectionLoadGuard& operator=(const BrpcConnectionLoadGuard&) = delete;

    int64_t in_flight_before() const { return _in_flight_before; }
    void reset();

private:
    friend class BrpcConnectionLoad;

    BrpcConnectionLoadGuard(std::shared_ptr<BrpcConnectionLoad> load, int64_t payload_bytes);

    std::shared_ptr<BrpcConnectionLoad> _load;
    int64_t _in_flight_before{0};
    int64_t _payload_bytes{0};
};

class BrpcConnectionLoad : public std::enable_shared_from_this<BrpcConnectionLoad> {
public:
    BrpcConnectionLoadGuard reserve(int64_t payload_bytes) {
        return BrpcConnectionLoadGuard(shared_from_this(), payload_bytes);
    }

    int64_t num_in_flight_rpcs() const { return _num_in_flight_rpcs.load(); }
    int64_t num_in_flight_payload_bytes() const { return _in_flight_payload_bytes.load(); }

private:
    friend class BrpcConnectionLoadGuard;

    std::atomic<int64_t> _num_in_flight_rpcs{0};
    std::atomic<int64_t> _in_flight_payload_bytes{0};
};

template <typename Guard>
class BrpcInFlightClosure final : public google::protobuf::Closure {
public:
    BrpcInFlightClosure(Guard reservation, google::protobuf::Closure* done)
            : _reservation(std::move(reservation)), _done(done) {}

    void Run() override {
        _reservation.reset();
        _done->Run();
        delete this;
    }

private:
    Guard _reservation;
    google::protobuf::Closure* _done;
};

// Returns the canonical accounting state for a single-connection slot. The registry retains weak references so
// accounting state does not extend the lifetime of cached stubs.
std::shared_ptr<BrpcConnectionLoad> get_brpc_connection_load(const butil::EndPoint& endpoint,
                                                             const std::string& protocol,
                                                             int64_t connection_group_seed);

int64_t brpc_request_payload_bytes(const google::protobuf::Message* request,
                                   google::protobuf::RpcController* controller);

} // namespace starrocks
