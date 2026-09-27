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
//   https://github.com/apache/incubator-doris/blob/master/be/src/util/mem_info.h

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

#pragma once

#include <cstdint>
#include <string>

#include "common/logging.h"

namespace starrocks {

// Provides the amount of physical memory available.
// Populated from /proc/meminfo.
// TODO: Combine mem-info, cpu-info and disk-info into hardware-info?
class MemInfo {
public:
    // Initialize MemInfo.
    static void init();

    // Get total physical memory in bytes
    static int64_t physical_mem() {
        DCHECK(_s_initialized);
        return _s_physical_mem;
    }

    // This process's resident set size in bytes, or -1 when it cannot be read.
    //
    // Unlike physical_mem(), this is read on every call rather than cached by init(), because it
    // is the number that moves: it counts pages the allocator has freed but not yet returned to
    // the kernel, which a memory tracker counting requested bytes does not see, so the two drift
    // apart by however much the allocator is holding on to.
    //
    // It is this process's residency and nothing more. A cgroup's memory.max is enforced against
    // the cgroup's aggregate charge, which also covers page cache and kernel memory and spans
    // every process in the cgroup, and the OOM killer's score adds swap entries and page tables
    // on top of RSS. Do not read this as the usage either of them enforces.
    //
    // Reads /proc/self/statm, which the kernel serves from counters it already maintains -- no
    // VMA walk, unlike /proc/self/smaps -- so it is cheap enough to poll. Returns -1 on any
    // platform without that file, macOS included.
    static int64_t process_resident_bytes();

    static std::string debug_string();

private:
    static void set_memlimit_if_container();

    static bool _s_initialized;
    static int64_t _s_physical_mem;
};

} // namespace starrocks
