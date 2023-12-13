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

#include "common/prof/heap_prof.h"

#include <unistd.h>

#include <cstdlib>
#include <fstream>
#include <mutex>

#include "common/config.h"
#include "fmt/format.h"
#include "jemalloc/jemalloc.h"

namespace starrocks {
// detail implements for allocator
static int set_jemalloc_profiling(bool enable) {
    int ret = je_mallctl("prof.active", nullptr, nullptr, &enable, 1);
    ret |= je_mallctl("prof.thread_active_init", nullptr, nullptr, &enable, 1);
    return ret;
}

static int has_enable_heap_profile() {
    int value = 0;
    size_t size = sizeof(value);
    je_mallctl("prof.active", &value, &size, nullptr, 0);
    return value;
}

bool dump_snapshot(const std::string& filename) {
    const char* fname = filename.c_str();
    return je_mallctl("prof.dump", nullptr, nullptr, &fname, sizeof(const char*)) == 0;
}

// declare exec from script
std::string exec(const std::string& cmd);

void HeapProf::enable_prof() {
    LOG(INFO) << "try to enable the heap profiling";
    std::lock_guard guard(_mutex);
    set_jemalloc_profiling(true);
}

void HeapProf::disable_prof() {
    LOG(INFO) << "try to disable the heap profiling";
    std::lock_guard guard(_mutex);
    set_jemalloc_profiling(false);
}

bool HeapProf::has_enable() {
    return has_enable_heap_profile();
}

std::string HeapProf::snapshot() {
    std::string output_name = fmt::format("{}/heap_profile.{}.{}", config::pprof_profile_dir, getpid(), rand());
    LOG(INFO) << "try to dump the heap profile " << output_name;
    //
    std::lock_guard guard(_mutex);
    if (dump_snapshot(output_name)) {
        return output_name;
    } else {
        return "";
    }
}

std::string HeapProf::to_dot_format(const std::string& heapdump_filename) {
    LOG(INFO) << "converting heap snapshot:" << heapdump_filename << " to dot format";
    std::lock_guard guard(_mutex);
    // jeprof --dot be/lib/starrocks_be b.heap > a.dot
    auto base_home = getenv("STARROCKS_HOME");
    std::string jeprof = fmt::format("{}/bin/jeprof", base_home);
    std::string binary = fmt::format("{}/lib/starrocks_be", base_home);
    return exec(fmt::format("{} --dot {} {}", jeprof, binary, heapdump_filename));
}

} // namespace starrocks