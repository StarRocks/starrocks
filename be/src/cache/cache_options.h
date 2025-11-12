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

namespace starrocks {

struct DataCacheOptions {
    bool enable_datacache = false;
    bool enable_cache_select = false;
    bool enable_populate_datacache = false;
    bool enable_datacache_async_populate_mode = false;
    bool enable_datacache_io_adaptor = false;
    int64_t modification_time = 0;
    int32_t datacache_evict_probability = 100;
    int8_t datacache_priority = 0;
    int64_t datacache_ttl_seconds = 0;
};

} // namespace starrocks
