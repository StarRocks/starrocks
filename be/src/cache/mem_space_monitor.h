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

#include <atomic>

#include "cache/datacache.h"

namespace starrocks {
class MemSpaceMonitor {
public:
    MemSpaceMonitor(DataCache* datacache) : _datacache(datacache) {}

    void start();
    void stop();

private:
    void _adjust_datacache_callback();
    void _evict_datacache(int64_t bytes_to_dec);

    DataCache* _datacache = nullptr;
    std::thread _adjust_datacache_thread;
    std::atomic<bool> _stopped = false;
};
} // namespace starrocks
