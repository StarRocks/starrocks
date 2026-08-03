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

#include "connector/hive/paimon/paimon_memory_pool.h"

#include "runtime/current_thread.h"

namespace starrocks {

TrackedPaimonMemoryPool::TrackedPaimonMemoryPool(MemTracker* tracker)
        : _tracker(tracker), _delegate(paimon::GetDefaultPool()) {}

void* TrackedPaimonMemoryPool::Malloc(uint64_t size, uint64_t alignment) {
    SCOPED_THREAD_LOCAL_MEM_TRACKER_SETTER(_tracker);
    return _delegate->Malloc(size, alignment);
}

void* TrackedPaimonMemoryPool::Realloc(void* pointer, size_t old_size, size_t new_size, uint64_t alignment) {
    SCOPED_THREAD_LOCAL_MEM_TRACKER_SETTER(_tracker);
    return _delegate->Realloc(pointer, old_size, new_size, alignment);
}

void TrackedPaimonMemoryPool::Free(void* pointer, uint64_t size) {
    SCOPED_THREAD_LOCAL_MEM_TRACKER_SETTER(_tracker);
    _delegate->Free(pointer, size);
}

void TrackedPaimonMemoryPool::Free(void* pointer, uint64_t size, uint64_t alignment) {
    SCOPED_THREAD_LOCAL_MEM_TRACKER_SETTER(_tracker);
    _delegate->Free(pointer, size, alignment);
}

uint64_t TrackedPaimonMemoryPool::CurrentUsage() const {
    return _delegate->CurrentUsage();
}

uint64_t TrackedPaimonMemoryPool::MaxMemoryUsage() const {
    return _delegate->MaxMemoryUsage();
}

} // namespace starrocks
