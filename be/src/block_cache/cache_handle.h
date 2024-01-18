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

#ifdef WITH_STARCACHE
#include "starcache/obj_handle.h"
#endif

namespace starrocks {

class DummyCacheHandle {
public:
    DummyCacheHandle() = default;
    ~DummyCacheHandle() = default;

    const void* ptr() { return nullptr; }
    void release() {}
};

// We use the `starcache::ObjectHandle` directly because implementing a new one seems unnecessary.
// Importing the starcache headers here is not graceful, but the `cachelib` doesn't support
// object cache and we'll deprecate it for some performance reasons. Now there is no need to
// pay too much attention to the compatibility and upper-level abstraction of the cachelib interface.
#ifdef WITH_STARCACHE
using CacheHandle = starcache::ObjectHandle;
#else
using CacheHandle = DummyCacheHandle;
#endif

} // namespace starrocks
