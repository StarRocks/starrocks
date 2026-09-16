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

namespace google::protobuf {
class RpcController;
} // namespace google::protobuf

namespace butil {
struct EndPoint;
}

namespace starrocks {

// Bounds the number of RPCs one brpc stub may have in flight.
//
// Under connection_type "pooled" a stub checks out exactly one pooled connection per in-flight RPC
// and returns it when the RPC ends, so bounding in-flight RPCs also bounds the connections the stub
// opens to its peer. The limit is read on every acquire so that it can be changed at runtime.
//
// The counter is incremented even when the limit is disabled, so callers may always pair acquire
// with release and so that the in-flight count stays observable.
class StubInflightLimiter {
public:
    // Returns false without counting the caller when `limit` is positive and already reached.
    bool try_acquire(int32_t limit) {
        const int32_t prev = _inflight.fetch_add(1, std::memory_order_relaxed);
        if (limit > 0 && prev >= limit) {
            _inflight.fetch_sub(1, std::memory_order_relaxed);
            return false;
        }
        return true;
    }

    void release() { _inflight.fetch_sub(1, std::memory_order_relaxed); }

    int32_t inflight() const { return _inflight.load(std::memory_order_relaxed); }

    int64_t rejected() const { return _rejected.load(std::memory_order_relaxed); }

    void add_rejected() { _rejected.fetch_add(1, std::memory_order_relaxed); }

private:
    std::atomic<int32_t> _inflight{0};
    // Cumulative number of RPCs refused by this stub. Without it a saturated stub and a dead peer
    // look the same from the outside.
    std::atomic<int64_t> _rejected{0};
};

// Marks `controller` failed because `endpoint`'s stub is at its in-flight limit.
//
// The caller must still hand the call to brpc afterwards rather than returning early. brpc adopts
// the correlation id and marks the controller used_by_rpc before it notices the failure, then runs
// its own send-failure path, which destroys the id and completes the closure off the caller's
// stack. Short-circuiting instead aborts the process in ~Controller, which CHECKs that a controller
// whose call_id was taken was adopted by an RPC.
void mark_over_inflight_limit(const butil::EndPoint& endpoint, google::protobuf::RpcController* controller);

} // namespace starrocks
