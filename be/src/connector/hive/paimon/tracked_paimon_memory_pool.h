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

#include <paimon/memory/memory_pool.h>

#include "runtime/current_thread.h"
#include "runtime/mem_tracker.h"

namespace starrocks {

// TrackedPaimonMemoryPool wraps paimon::GetMemoryPool() to solve the cross-thread
// MemTracker asymmetry problem. By setting the TLS MemTracker to a fixed query-level
// tracker before each allocation/deallocation, the jemalloc hook automatically
// consume/release on the correct tracker regardless of which thread executes.
class TrackedPaimonMemoryPool : public paimon::MemoryPool {
public:
    explicit TrackedPaimonMemoryPool(MemTracker* tracker) : _tracker(tracker), _delegate(paimon::GetMemoryPool()) {}

    ~TrackedPaimonMemoryPool() override = default;

    void* Malloc(uint64_t size, uint64_t alignment) override {
        SCOPED_THREAD_LOCAL_MEM_TRACKER_SETTER(_tracker);
        return _delegate->Malloc(size, alignment);
    }

    void* Realloc(void* p, size_t old_size, size_t new_size, uint64_t alignment) override {
        SCOPED_THREAD_LOCAL_MEM_TRACKER_SETTER(_tracker);
        return _delegate->Realloc(p, old_size, new_size, alignment);
    }

    void Free(void* p, uint64_t size) override {
        SCOPED_THREAD_LOCAL_MEM_TRACKER_SETTER(_tracker);
        _delegate->Free(p, size);
    }

    void Free(void* p, uint64_t size, uint64_t alignment) override {
        SCOPED_THREAD_LOCAL_MEM_TRACKER_SETTER(_tracker);
        _delegate->Free(p, size, alignment);
    }

    uint64_t CurrentUsage() const override { return _delegate->CurrentUsage(); }

    uint64_t MaxMemoryUsage() const override { return _delegate->MaxMemoryUsage(); }

private:
    MemTracker* _tracker;
    std::unique_ptr<paimon::MemoryPool> _delegate;
};

} // namespace starrocks
