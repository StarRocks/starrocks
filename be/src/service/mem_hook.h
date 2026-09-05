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

#include <cstdint>

// Sanitizers need to own malloc/free, and Darwin executable-level allocator
// interposition is unsafe with allocations made inside system/third-party
// dylibs during dyld/global initialization.
#if defined(__APPLE__) || defined(ADDRESS_SANITIZER) || defined(LEAK_SANITIZER) || defined(THREAD_SANITIZER)
#define STARROCKS_ENABLE_JEMALLOC_MEM_HOOK 0
#else
#define STARROCKS_ENABLE_JEMALLOC_MEM_HOOK 1
#endif

namespace starrocks {

int64_t set_large_memory_alloc_failure_threshold(int64_t);

} // namespace starrocks
