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

// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/be/src/runtime/memory/system_allocator.cpp

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "runtime/memory/system_allocator.h"

#include <sys/mman.h>

#include <cstdlib>

#include "common/config.h"
#include "common/logging.h"
#include "runtime/mem_tracker.h"

namespace starrocks {

#define PAGE_SIZE (4 * 1024) // 4K

uint8_t* SystemAllocator::allocate(MemTracker* mem_tracker, size_t length) {
    if (config::use_mmap_allocate_chunk) {
        return allocate_via_mmap(mem_tracker, length);
    } else {
        return allocate_via_malloc(length);
    }
}

void SystemAllocator::free(MemTracker* mem_tracker, uint8_t* ptr, size_t length) {
    if (config::use_mmap_allocate_chunk) {
        auto res = munmap(ptr, length);
        if (res != 0) {
            PLOG(ERROR) << "fail to free memory via munmap";
        }
        if (mem_tracker != nullptr) {
            mem_tracker->release(length);
        }
    } else {
        ::free(ptr);
    }
}

uint8_t* SystemAllocator::allocate_via_malloc(size_t length) {
    void* ptr = nullptr;
    // try to use a whole page instead of parts of one page
    int res = posix_memalign(&ptr, PAGE_SIZE, length);
    if (res != 0) {
        PLOG(ERROR) << "fail to allocate mem via posix_memalign, res=" << res;
        return nullptr;
    }
    return (uint8_t*)ptr;
}

uint8_t* SystemAllocator::allocate_via_mmap(MemTracker* mem_tracker, size_t length) {
    auto ptr = (uint8_t*)mmap(nullptr, length, PROT_READ | PROT_WRITE, MAP_ANONYMOUS | MAP_PRIVATE, -1, 0);
    if (ptr == MAP_FAILED) {
        PLOG(ERROR) << "fail to allocate memory via mmap";
        return nullptr;
    }
    if (mem_tracker != nullptr) {
        mem_tracker->consume(length);
    }
    return ptr;
}

} // namespace starrocks
