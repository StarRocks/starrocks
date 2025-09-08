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

#include "common/status.h"
#include "util/threadpool.h"

namespace starrocks {

class SegmentCachePopulator {
public:
    static SegmentCachePopulator* instance();

    // Sync version - does the actual work
    static Status populate_segment_to_cache(const std::string& segment_path);

    // Async version - submits to thread pool
    Status populate_segment_to_cache_async(const std::string& segment_path);

private:
    SegmentCachePopulator();
    ~SegmentCachePopulator();

    std::unique_ptr<ThreadPool> _cache_populate_pool;
};

} // namespace starrocks
