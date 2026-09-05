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
#include <cstddef>
#include <cstdint>
#include <memory>
#include <mutex>

#include "base/statusor.h"
#include "compute_env/workgroup/work_group_fwd.h"
#include "platform/llm/ai_http_client.h"
#include "runtime/query_context_lifetime.h"

namespace starrocks {

class MemTracker;
class RuntimeState;

namespace pipeline {

class AIMemoryContextOwner;

// Retains the query tracker and its WorkGroup-owned ancestors for asynchronous AI request/result buffers without
// extending the QueryContext lifetime. The process root is globally owned. The weak query lifetime gates new
// reservations; already reserved bytes can always be released.
class AIQueryMemoryAccount final : public std::enable_shared_from_this<AIQueryMemoryAccount> {
public:
    using Ptr = std::shared_ptr<AIQueryMemoryAccount>;

    static StatusOr<Ptr> create(const RuntimeState& state);

    ~AIQueryMemoryAccount() noexcept;

    AIQueryMemoryAccount(const AIQueryMemoryAccount&) = delete;
    AIQueryMemoryAccount& operator=(const AIQueryMemoryAccount&) = delete;

    // The intrusive context keeps this account and its physical process scope alive as one indivisible contract.
    AIMemoryContext memory_context();

    int64_t reserved_bytes() const noexcept { return _reserved_bytes.load(std::memory_order_relaxed); }

private:
    friend class AIMemoryContextOwner;

    AIQueryMemoryAccount(QueryContextLifetimeWeakPtr query_lifetime, workgroup::WorkGroupPtr workgroup_owner,
                         std::shared_ptr<MemTracker> query_tracker, MemTracker* process_tracker);

    bool _reserve(size_t bytes) noexcept;
    void _release(size_t bytes) noexcept;
    void _drain_residual() noexcept;

    QueryContextLifetimeWeakPtr _query_lifetime;

    // Declaration order is intentional. Members are destroyed in reverse order, so the query tracker must be
    // destroyed before the WorkGroup that owns its raw parent tracker chain.
    workgroup::WorkGroupPtr _workgroup_owner;
    std::shared_ptr<MemTracker> _query_tracker;
    MemTracker* _process_tracker;

    mutable std::mutex _mutex;
    std::atomic<int64_t> _reserved_bytes{0};
};

} // namespace pipeline
} // namespace starrocks
