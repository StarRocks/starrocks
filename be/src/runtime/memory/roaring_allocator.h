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

#include "runtime/memory/mem_hook_allocator.h"

namespace starrocks {

static MemHookAllocator kDefaultRoaringAllocator = MemHookAllocator{};
inline thread_local Allocator* tls_roaring_allocator = &kDefaultRoaringAllocator;

class ThreadLocalRoaringAllocatorSetter {
public:
    ThreadLocalRoaringAllocatorSetter(Allocator* allocator) {
        _prev = tls_roaring_allocator;
        tls_roaring_allocator = allocator;
    }
    ~ThreadLocalRoaringAllocatorSetter() { tls_roaring_allocator = _prev; }

private:
    Allocator* _prev = nullptr;
};

} // namespace starrocks