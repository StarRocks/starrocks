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

namespace starrocks {

// Set once at startup, before any lake read, when the remote I/O scheduler of starlet was enabled
// (remote_io_scheduler_enable); it never changes afterwards. While it is set, the lake scans read ahead through
// SegmentPrefetchWindow, and the read-ahead strategies it replaces are off: io_coalesce_lake_read_enable was set to
// false, and the starlet buffer prefetch is forced off.
inline std::atomic<bool> g_remote_io_scheduler_enabled{false};

inline bool remote_io_scheduler_enabled() {
    return g_remote_io_scheduler_enabled.load(std::memory_order_relaxed);
}

} // namespace starrocks
