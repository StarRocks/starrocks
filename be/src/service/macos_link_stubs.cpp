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

#ifdef __APPLE__

#include <cerrno>
#include <cstddef>

extern "C" {

// brpc weakly probes this gperftools compatibility hook. StarRocks uses jemalloc
// on macOS and does not link the gperftools allocator, so keep the call as a no-op.
void MallocExtension_ReleaseFreeMemory() {}

// brpc weakly probes the unprefixed jemalloc API for its builtin /memory and /pprof/heap
// pages. StarRocks builds jemalloc with the "je" prefix, so report every node as missing.
int mallctl(const char*, void*, size_t*, void*, size_t) {
    return ENOENT;
}
void malloc_stats_print(void (*)(void*, const char*), void*, const char*) {}

} // extern "C"

#endif
