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

#include "orc_memory_pool.h"

#include <glog/logging.h>
#include <jemalloc/jemalloc.h>
#include <malloc.h>

#include <orc/OrcFile.hh>

#include "common/compiler_util.h"

namespace starrocks {

orc::MemoryPool* getOrcMemoryPool() {
    static OrcMemoryPool internal;
    return &internal;
}

char* OrcMemoryPool::malloc(uint64_t size) {
    // Return nullptr if size is 0, otherwise debug-enabled jemalloc would fail non-zero size assertion.
    // See https://github.com/jemalloc/jemalloc/issues/2514
    if (UNLIKELY(size == 0)) {
        return nullptr;
    }
    auto p = static_cast<char*>(std::malloc(size));
    if (UNLIKELY(p == nullptr)) {
        LOG(WARNING) << "malloc failed, size=" << size;
        throw std::bad_alloc();
    }
    _bytes_allocated.fetch_add(malloc_usable_size(p), std::memory_order_relaxed);
    return p;
}

void starrocks::OrcMemoryPool::free(char* p) {
    auto size = malloc_usable_size(p);
    std::free(p);
    _bytes_allocated.fetch_sub(size, std::memory_order_relaxed);
}

int64_t OrcMemoryPool::bytes_allocated() const {
    return _bytes_allocated.load(std::memory_order_relaxed);
}

} // namespace starrocks
