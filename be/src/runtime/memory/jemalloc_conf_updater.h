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
#include <optional>
#include <set>
#include <string>
#include <string_view>

#include "common/statusor.h"

namespace starrocks {

class ThreadPool;

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

// Sets dirty/muzzy decay on every automatic arena. 0 purges each arena's backlog synchronously
// as it is applied; a positive value only restarts the decay backlog and leaves the work to the
// background threads.
//
// `pool` spreads the walk over its threads, one task per arena. A null pool walks in the calling
// thread, which is also what happens for any arena the pool refuses. Walking serially is worth
// avoiding when the decay is 0: the cost is proportional to how much is dirty, so every arena
// after the current one stays without backpressure for as long as the walk takes, and with
// ~65GiB of dirty over 32 arenas the walk did not finish before the process was killed.
//
// Not serialized against a `jemalloc_conf` update, which writes the same per-arena nodes. The
// two can interleave and leave the arenas holding a mix for a scan interval or two; what makes
// that safe is decay_write_generation(), which lets a caller holding the arenas at a decay of
// its own notice that an update happened and derive the decay again. Serializing instead would
// make a config update wait out a walk -- seconds, for a decay of 0 -- and it is an operator
// under memory pressure who is most likely to be issuing one.
Status set_jemalloc_decay_ms(bool dirty, ssize_t decay_ms, ThreadPool* pool);

// Counts the times a `jemalloc_conf` update has written a decay to the arenas.
//
// A caller that keeps arenas at a decay of its own -- the memory purge daemon's ladder -- cannot
// tell from its own state whether the arenas still hold what it wrote, because an update
// overwrites it. Reading this before a write and comparing it later says whether anything came
// in between, so the caller can drop its assumption and derive the decay again from the current
// resident size and the current baseline.
uint64_t decay_write_generation();

// The dirty/muzzy decay the arenas are currently running with: the value last applied through
// the `jemalloc_conf` config, or nullopt when the config carries no decay for `dirty` or the
// process started with a `jemalloc_conf` that could not be parsed.
//
// Deliberately not opt.dirty_decay_ms. That node is read-only and holds what the process was
// started with, while `dirty_decay_ms` is one of the three options a running BE may change, so
// reading it would make anything derived from it disagree with what the arenas actually run
// with the moment an operator updates the config.
//
// Must not be called while holding the serialization the two setters above take -- read the
// baseline first, then write.
std::optional<ssize_t> configured_decay_ms(bool dirty);

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
