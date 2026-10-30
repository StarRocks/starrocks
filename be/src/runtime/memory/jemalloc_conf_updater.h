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

// The option string jemalloc initialized itself from: the JEMALLOC_CONF environment variable
// when it is set, and `config_value` otherwise. bin/start_backend.sh normally exports the
// config value into that variable, but it leaves an already-set variable alone and forces its
// own string under --jemalloc_debug and --check_mem_leak, so the two can differ.
std::string startup_jemalloc_conf(std::string_view config_value);

// Renders `options` back into a jemalloc option string. Option order is the map's, so a string
// that went through parse_jemalloc_conf() comes back normalized rather than byte identical.
std::string serialize_jemalloc_conf(const JemallocOptions& options);

// `conf` with its `prof_active` set to `active`, inserting the option when `conf` does not carry
// it -- --jemalloc_debug starts the BE without it. Only a `conf` that cannot be parsed fails
// here; whether profiling is armed at all is checked when the option is applied.
StatusOr<std::string> jemalloc_conf_with_prof_active(std::string_view conf, bool active);

// Turns heap profiling on or off by updating the `jemalloc_conf` config, so that the config
// stays the single place that says whether profiling is on. This is what the HeapProf script
// bindings call; it must never be reached from HeapProf itself, because the config update hook
// calls into HeapProf and a call back the other way would deadlock on HeapProf's own mutex.
Status set_prof_active_via_config(bool active);

// Applies the runtime-mutable subset of the `jemalloc_conf` config.
//
// Most jemalloc options are frozen once the process is initialized, because their
// `opt.*` mallctl nodes are read-only. So an update is accepted only when every
// option that actually changed has a writable mallctl counterpart; otherwise it is
// rejected and ConfigUpdateRegistry rolls the config value back.
class JemallocConfUpdater {
public:
    static JemallocConfUpdater& instance();

    // Seeds the baseline with the option string jemalloc actually started with, and rewrites
    // `jemalloc_conf` to it when the config says something else, so that the value shown by
    // information_schema.be_configs is the one an update is diffed against.
    void init(std::string_view config_value);

    // Diffs `new_conf` against the options applied so far and pushes the changed
    // ones into jemalloc. Returns an error without touching jemalloc if any option
    // outside the mutable set was added, removed or changed.
    Status update(std::string_view new_conf);

    // The options that have a writable mallctl counterpart.
    static const std::set<std::string>& mutable_options();

    JemallocOptions applied_options();

private:
    JemallocConfUpdater() = default;

    std::mutex _mutex;
    JemallocOptions _applied;
};

} // namespace starrocks
