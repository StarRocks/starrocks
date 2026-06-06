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
#include <chrono>
#include <cstdint>

#include "gen_cpp/Types_types.h" // for TUniqueId

namespace starrocks::pipeline {

class QueryRuntimeState {
public:
    QueryRuntimeState() = default;

    void set_query_id(const TUniqueId& query_id) { _query_id = query_id; }
    const TUniqueId& query_id() const { return _query_id; }

    static constexpr int DEFAULT_EXPIRE_SECONDS = 300;

    void set_delivery_expire_seconds(int expire_seconds) { _delivery_expire_seconds.store(expire_seconds); }
    void set_query_expire_seconds(int expire_seconds) { _query_expire_seconds.store(expire_seconds); }
    int get_query_expire_seconds() const { return static_cast<int>(_query_expire_seconds.load()); }

    bool is_delivery_expired() const { return _now_ms() > _delivery_deadline_ms.load(); }
    bool is_query_expired() const { return _now_ms() > _query_deadline_ms.load(); }

    void extend_delivery_lifetime() {
        _delivery_deadline_ms.store(_now_ms() + _delivery_expire_seconds.load() * 1000L);
    }
    void extend_query_lifetime() { _query_deadline_ms.store(_now_ms() + _query_expire_seconds.load() * 1000L); }

private:
    static int64_t _now_ms() {
        return std::chrono::duration_cast<std::chrono::milliseconds>(
                       std::chrono::steady_clock::now().time_since_epoch())
                .count();
    }

    TUniqueId _query_id;
    std::atomic<int64_t> _delivery_deadline_ms{0};
    std::atomic<int64_t> _query_deadline_ms{0};
    std::atomic<int64_t> _delivery_expire_seconds{DEFAULT_EXPIRE_SECONDS};
    std::atomic<int64_t> _query_expire_seconds{DEFAULT_EXPIRE_SECONDS};
};

} // namespace starrocks::pipeline
