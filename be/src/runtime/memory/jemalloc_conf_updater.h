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

#include <map>
#include <mutex>
#include <set>
#include <string>
#include <string_view>

#include "common/statusor.h"

namespace starrocks {

// Parsed form of a jemalloc option string such as
// "percpu_arena:percpu,dirty_decay_ms:5000,prof_active:false". A duplicated key
// follows jemalloc's own rule that the last assignment wins.
using JemallocOptions = std::map<std::string, std::string>;

StatusOr<JemallocOptions> parse_jemalloc_conf(std::string_view conf);

// Applies the runtime-mutable subset of the `jemalloc_conf` config.
//
// Most jemalloc options are frozen once the process is initialized, because their
// `opt.*` mallctl nodes are read-only. So an update is accepted only when every
// option that actually changed has a writable mallctl counterpart; otherwise it is
// rejected and ConfigUpdateRegistry rolls the config value back.
class JemallocConfUpdater {
public:
    static JemallocConfUpdater& instance();

    // Seeds the baseline with the option string the process was started with.
    void init(std::string_view startup_conf);

    // Diffs `new_conf` against the options applied so far and pushes the changed
    // ones into jemalloc. Returns an error without touching jemalloc if any option
    // outside the mutable set was added, removed or changed.
    Status update(std::string_view new_conf);

    // The options that have a writable mallctl counterpart.
    static const std::set<std::string>& mutable_options();

    JemallocOptions applied_options();

private:
    JemallocConfUpdater() = default;

    // `prof.active` can also be toggled through HeapProf (`ADMIN EXECUTE`), which
    // bypasses this config. Refresh the baseline from the live value so that the
    // config becomes authoritative again instead of drifting away from jemalloc.
    static void refresh_prof_active(JemallocOptions* options);

    std::mutex _mutex;
    JemallocOptions _applied;
};

} // namespace starrocks
