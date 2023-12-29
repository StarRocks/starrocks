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

#include <mutex>

#include "common/status.h"
#include "gutil/macros.h"

namespace starrocks {
class HeapProf {
public:
    static HeapProf& getInstance() {
        static HeapProf profiler;
        return profiler;
    }

    // set enable to profiler
    void enable_prof();
    // set disable to profiler
    void disable_prof();
    // return true if jemalloc enable heap profile
    bool has_enable();
    // return heap dump filename
    std::string snapshot();
    // convert raw heap dump to dot format
    std::string to_dot_format(const std::string& heapdump_filename);

    std::string dump_dot_snapshot() { return to_dot_format(snapshot()); }

    DISALLOW_COPY_AND_MOVE(HeapProf);

private:
    HeapProf() = default;

private:
    std::mutex _mutex;
};
} // namespace starrocks