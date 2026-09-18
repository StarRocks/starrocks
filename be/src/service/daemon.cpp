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
//   https://github.com/apache/incubator-doris/blob/master/be/src/common/daemon.cpp

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

#include "service/daemon.h"

#include <gflags/gflags.h>

#include "cache/datacache.h"
#include "column/column_helper.h"
#include "common/config_diagnostic_fwd.h"
#include "common/config_memory_allocator_fwd.h"
#include "common/config_metrics_fwd.h"
#include "common/config_path_fwd.h"
#include "common/metrics/process_metrics_registry.h"
#include "common/process_exit.h"
#include "common/util/minidump.h"
#include "compute_env/workgroup/work_group.h"
#ifdef USE_STAROS
#include "compute_env/staros/staros_worker_runtime.h"
#include "fslib/star_cache_handler.h"
#endif
#include <fmt/ranges.h>

#include <chrono>
#include <csignal>
#include <cstdio>
#include <cstring>
#include <memory>
#include <optional>
#include <thread>
#include <utility>

#include "base/concurrency/stopwatch.hpp"
#include "base/time/monotime.h"
#include "base/time/time.h"
#include "base/time/timezone_utils.h"
#include "common/glog_init.h"
#include "common/system/cpu_info.h"
#include "common/system/disk_info.h"
#include "common/system/mem_info.h"
#include "common/thread/thread.h"
#include "common/thread/threadpool.h"
#include "common/util/debug_util.h"
#include "common/util/misc.h"
#include "common/util/thrift_util.h"
#include "compute_env/query/query_scan_metrics.h"
#include "exec/exec_env.h"
#include "fs/encrypt_file.h"
#include "gutil/cpu.h"
#include "jemalloc/jemalloc.h"
#include "platform/platform_metrics.h"
#include "platform/user_function_cache.h"
#include "runtime/memory/jemalloc_conf_updater.h"
#include "runtime/memory/memory_lock.h"
#include "runtime/process_memory_metrics.h"
#include "runtime/remote_arrow_queue_mgr.h"
#include "runtime/remote_chunk_queue_mgr.h"
#include "runtime/remote_scan_token_mgr.h"
#include "runtime/runtime_env.h"
#include "runtime/runtime_metrics.h"
#include "service/backend_metrics_initializer.h"
#include "service/failure_handler.h"
#include "service/mem_hook.h"
#include "service/mem_purge_policy.h"
#include "storage/storage_engine.h"
#include "storage/storage_metrics.h"
#include "types/time_types.h"

namespace starrocks {
DEFINE_bool(cn, false, "start as compute node");

std::string dump_memory_tracker();

/*
 * This thread will calculate some metrics at a fix interval(15 sec)
 * 1. push bytes per second
 * 2. scan bytes per second
 * 3. max io util of all disks
 * 4. max network send bytes rate
 * 5. max network receive bytes rate
 * 6. datacache memory usage
 */
void calculate_metrics(Daemon* daemon, ProcessMetricsRegistry* process_metrics_registry) {
    int64_t last_ts = -1L;
    int64_t lst_push_bytes = -1;
    int64_t lst_query_bytes = -1;

    std::map<std::string, int64_t> lst_disks_io_time;
    std::map<std::string, int64_t> lst_net_send_bytes;
    std::map<std::string, int64_t> lst_net_receive_bytes;

    while (!daemon->stopped()) {
        process_metrics_registry->root_registry()->trigger_hook();

        if (last_ts == -1L) {
            last_ts = MonotonicSeconds();
            lst_push_bytes = StorageMetrics::instance()->push_request_write_bytes.value();
            lst_query_bytes = QueryScanMetrics::instance()->query_scan_bytes.value();
            PlatformMetrics::instance()->get_disks_io_time(&lst_disks_io_time);
            PlatformMetrics::instance()->get_network_traffic(&lst_net_send_bytes, &lst_net_receive_bytes);
        } else {
            int64_t current_ts = MonotonicSeconds();
            long interval = (current_ts - last_ts);
            last_ts = current_ts;

            // 1. push bytes per second.
            int64_t current_push_bytes = StorageMetrics::instance()->push_request_write_bytes.value();
            int64_t pps = (current_push_bytes - lst_push_bytes) / (interval == 0 ? 1 : interval);
            StorageMetrics::instance()->push_request_write_bytes_per_second.set_value(pps < 0 ? 0 : pps);
            lst_push_bytes = current_push_bytes;

            // 2. query bytes per second.
            int64_t current_query_bytes = QueryScanMetrics::instance()->query_scan_bytes.value();
            int64_t qps = (current_query_bytes - lst_query_bytes) / (interval == 0 ? 1 : interval);
            QueryScanMetrics::instance()->query_scan_bytes_per_second.set_value(qps < 0 ? 0 : qps);
            lst_query_bytes = current_query_bytes;

            // 3. max disk io util.
            RuntimeMetrics::instance()->max_disk_io_util_percent.set_value(
                    PlatformMetrics::instance()->get_max_io_util(lst_disks_io_time, 15));
            // Update lst map.
            PlatformMetrics::instance()->get_disks_io_time(&lst_disks_io_time);

            // 4. max network traffic.
            int64_t max_send = 0;
            int64_t max_receive = 0;
            PlatformMetrics::instance()->get_max_net_traffic(lst_net_send_bytes, lst_net_receive_bytes, 15, &max_send,
                                                             &max_receive);
            RuntimeMetrics::instance()->max_network_send_bytes_rate.set_value(max_send);
            RuntimeMetrics::instance()->max_network_receive_bytes_rate.set_value(max_receive);
            // update lst map
            PlatformMetrics::instance()->get_network_traffic(&lst_net_send_bytes, &lst_net_receive_bytes);
        }

        LOG(INFO) << dump_memory_tracker();

        process_metrics_registry->table_metrics_mgr()->cleanup();
        nap_sleep(15, [daemon] { return daemon->stopped(); });
    }
}

/*
 * Periodically reclaim expired remote-scan sessions: drop expired tokens and cancel their
 * result queues (routed by transport). This runs regardless of read traffic, so abandoned
 * sessions whose consumer never fetches are still reclaimed instead of leaking their token and
 * buffered queue data until an unrelated fetch happens to sweep them.
 */
void remote_scan_token_cleanup(Daemon* daemon) {
    constexpr int kCleanupIntervalSec = 30;
    while (!daemon->stopped()) {
        auto* exec_env = ExecEnv::GetInstance();
        auto* token_mgr = exec_env != nullptr ? exec_env->remote_scan_token_mgr() : nullptr;
        if (token_mgr != nullptr) {
            for (const auto& expired : token_mgr->cleanup_expired_tokens(UnixMillis())) {
                if (expired.transport == TStarRocksScanTransport::STARROCKS_ARROW_FLIGHT) {
                    WARN_IF_ERROR(exec_env->remote_arrow_queue_mgr()->cancel(expired.fragment_instance_id),
                                  "Failed to cancel expired remote scan arrow queue");
                } else if (expired.transport == TStarRocksScanTransport::STARROCKS_BRPC_CHUNK) {
                    WARN_IF_ERROR(exec_env->remote_chunk_queue_mgr()->cancel(expired.fragment_instance_id),
                                  "Failed to cancel expired remote scan chunk queue");
                }
            }
        }
        nap_sleep(kCleanupIntervalSec, [daemon] { return daemon->stopped(); });
    }
}

#ifndef __APPLE__

struct JemallocStats {
    int64_t allocated = 0;
    int64_t active = 0;
    int64_t metadata = 0;
    int64_t resident = 0;
    int64_t mapped = 0;
    int64_t retained = 0;
};

static void retrieve_jemalloc_stats(JemallocStats* stats) {
    // On macOS, jemalloc may define je_mallctl as mallctl via macro in jemalloc.h
    uint64_t epoch = 1;
    size_t sz = sizeof(epoch);
    je_mallctl("epoch", &epoch, &sz, &epoch, sz);

    int64_t value = 0;
    sz = sizeof(value);
    if (je_mallctl("stats.allocated", &value, &sz, nullptr, 0) == 0) {
        stats->allocated = value;
    }
    if (je_mallctl("stats.active", &value, &sz, nullptr, 0) == 0) {
        stats->active = value;
    }
    if (je_mallctl("stats.metadata", &value, &sz, nullptr, 0) == 0) {
        stats->metadata = value;
    }
    if (je_mallctl("stats.resident", &value, &sz, nullptr, 0) == 0) {
        stats->resident = value;
    }
    if (je_mallctl("stats.mapped", &value, &sz, nullptr, 0) == 0) {
        stats->mapped = value;
    }
    if (je_mallctl("stats.retained", &value, &sz, nullptr, 0) == 0) {
        stats->retained = value;
    }
}

// The process memory tracker counts the bytes the BE asked for, while the OOM killer counts
// the pages the kernel still holds. When jemalloc cannot reuse what the BE freed the two drift
// apart: the tracker stays well under its limit and keeps admitting queries while the resident
// size runs away.
//
// Waiting for the limit and then switching to eager reclaim in one step does not work. Setting
// a decay of 0 purges each arena's backlog synchronously as it goes, so the arenas only come
// under backpressure one after another -- measured as 1.8s to cover all 32, during which a
// load allocating ~18GiB/s overshot by another 18GiB and settled at 97% of the machine.
//
// So tighten the decay in steps instead. Draining early keeps the backlog small, which is what
// makes the final switch fast, and the resident size is nudged down long before anything has to
// be forced. The rungs and the rules for moving between them are in service/mem_purge_policy.h;
// what is left here is reading the resident size, applying a decay, and waiting.
void mem_purge_daemon(void* arg_this) {
    auto* daemon = static_cast<Daemon*>(arg_this);
    // Both configs below are mutable and so are read on every pass, and both have values outside
    // which the loop breaks rather than merely tunes.
    //
    // The scan interval cannot go under 10ms -- a non-positive one spins, and a few milliseconds
    // buys nothing because the cheapest decay transition already costs ~5ms -- nor over a second.
    // At the ~16GiB/s this was built against, a second is more than the 15GiB between two rungs,
    // so an interval above it can step over a rung entirely; it is also what a sleep holds up
    // shutdown by, since Daemon::stop() sets the flag and then joins.
    //
    // The step-down hold can be 0 -- relax as soon as the resident size is under the rung,
    // leaving only the ratio gap to damp it -- but not negative, and not so long that the ladder
    // stays tight for an hour after the pressure has gone.
    constexpr int32_t kMinScanIntervalMs = 10;
    constexpr int32_t kMaxScanIntervalMs = 1000;
    constexpr int32_t kMinStepDownHoldMs = 0;
    constexpr int32_t kMaxStepDownHoldMs = 300000;
    // Each floor doubles as "nothing refused yet" for its own warning: it is inside the accepted
    // range and so can never be a refused value -- unlike a fixed 0, which is the value a
    // misconfigured interval most often has.
    int32_t warned_scan_interval_ms = kMinScanIntervalMs;
    int32_t warned_step_down_hold_ms = kMinStepDownHoldMs;

    // The baseline is read at every transition rather than captured once here. `dirty_decay_ms`
    // and `muzzy_decay_ms` are two of the three jemalloc options a running BE may change, so a
    // baseline captured at thread start goes stale the moment an operator updates
    // `jemalloc_conf`: the ladder would scale from a value the arenas no longer run with -- a
    // rung derived from 5000ms is looser than a BE actually running at 1000ms -- and would later
    // "restore" a value the operator had already replaced.
    //
    // A read is a snapshot, not a reservation: an update can land between it and the write that
    // follows, leaving the arenas on a rung derived from a baseline that is already gone. That
    // is what decay_write_generation() is watched for -- the next pass sees the update, drops
    // the rung and derives it again.
    constexpr ssize_t kDefaultDecayMs = 5000;

    // Level 0 puts back the configured decay, whatever it is now; level N applies a fraction of
    // it. Restoring and scaling are not the same value: restoring hands back exactly what is
    // configured, -1 ("never purge") included, while scaling needs a finite positive duration
    // and falls back to the default when -1 or an unreadable config offers none -- rather than
    // refusing to engage on the configuration most likely to need it. Each kind scales from its
    // own baseline, since the two may be configured differently.
    auto decay_of = [&](int level) -> std::pair<ssize_t, ssize_t> {
        const ssize_t restore_dirty = configured_decay_ms(/*dirty=*/true).value_or(kDefaultDecayMs);
        const ssize_t restore_muzzy = configured_decay_ms(/*dirty=*/false).value_or(kDefaultDecayMs);
        if (level <= 0) {
            return {restore_dirty, restore_muzzy};
        }
        return {decay_ms_at_level(level, decay_ladder_reference_ms(restore_dirty, kDefaultDecayMs)),
                decay_ms_at_level(level, decay_ladder_reference_ms(restore_muzzy, kDefaultDecayMs))};
    };
    // A decay of 0 purges every arena's backlog as it is applied, and walking the arenas one
    // at a time leaves the ones at the end of the walk without backpressure for as long as the
    // walk takes -- measured as a walk that never finished. Spread it over a pool; each arena
    // has its own lock.
    //
    // min_threads is 1 because this pool is idle for the whole life of a BE that never comes
    // under pressure, and the feature is off by default. The threads it does grow are kept for
    // kPurgePoolIdleMs, long enough to span one pressure episode: the ladder climbs through its
    // levels within seconds and a step down cannot happen for at least the step-down hold, so
    // the transitions of an episode share the threads the first of them paid for.
    constexpr int64_t kPurgePoolIdleMs = 30000;
    const int purge_parallelism = std::max(4, static_cast<int>(std::thread::hardware_concurrency()) / 2);
    std::unique_ptr<ThreadPool> purge_pool;
    if (Status st = ThreadPoolBuilder("mem_purge")
                            .set_min_threads(1)
                            .set_max_threads(purge_parallelism)
                            .set_idle_timeout(MonoDelta::FromMilliseconds(kPurgePoolIdleMs))
                            .build(&purge_pool);
        !st.ok()) {
        // Not fatal: a null pool makes the walk serial, which is what it was before the pool.
        LOG(WARNING) << "could not build the arena purge pool, the walk will be serial: " << st;
        purge_pool.reset();
    }
    auto write_decay = [&purge_pool](ssize_t dirty_ms, ssize_t muzzy_ms) {
        Status st = set_jemalloc_decay_ms(/*dirty=*/true, dirty_ms, purge_pool.get());
        if (!st.ok()) {
            LOG(WARNING) << "failed to set dirty_decay_ms to " << dirty_ms << ": " << st;
        }
        st = set_jemalloc_decay_ms(/*dirty=*/false, muzzy_ms, purge_pool.get());
        if (!st.ok()) {
            LOG(WARNING) << "failed to set muzzy_decay_ms to " << muzzy_ms << ": " << st;
        }
    };

    // What the arenas were left holding, as far as this thread knows. Only as far as it knows: a
    // `jemalloc_conf` update writes the same per-arena nodes, so an update makes this stale
    // until the rung is written again. `seen_decay_generation` is what says one has landed.
    int level = 0;
    uint64_t seen_decay_generation = decay_write_generation();
    MonotonicStopWatch below_level;

    auto apply_rung = [&](int target) {
        const auto [dirty_ms, muzzy_ms] = decay_of(target);
        write_decay(dirty_ms, muzzy_ms);
    };

    while (!daemon->stopped()) {
        // A `jemalloc_conf` update writes the same per-arena nodes, so once one lands the arenas
        // no longer hold what this thread put there. Write the current rung again rather than
        // work out what was clobbered: the write is what makes the two agree, and deriving it
        // from the baseline picks up the operator's new value at the same time.
        //
        // Unconditionally: at level 0 as well as at a rung, and whether the feature is enabled or
        // not. Both exceptions look safe and are not. At level 0 the arenas may hold a baseline
        // this thread restored just before the update landed. Disabled is the same case -- the
        // restore that turning it off performs is itself a write that can land last -- so
        // skipping the rewrite there strands the arenas on a decay the operator has replaced,
        // with level already 0 and nothing else that would ever write again.
        //
        // No attempt to prove the write was not itself raced: whatever order two writes land in,
        // the last update bumps the generation last, so the last rewrite is the one derived from
        // the settled baseline. When nothing was wrong this costs one extra walk per config
        // change, which is a rare, deliberate act.
        if (const uint64_t generation = decay_write_generation(); generation != seen_decay_generation) {
            seen_decay_generation = generation;
            LOG(INFO) << "a jemalloc_conf update rewrote the arenas' decay; applying level " << level
                      << " again from the new baseline";
            apply_rung(level);
        }

        const int32_t interval_ms =
                config_ms_in_range("jemalloc_decay_rss_scan_interval_ms", config::jemalloc_decay_rss_scan_interval_ms,
                                   kMinScanIntervalMs, kMaxScanIntervalMs, &warned_scan_interval_ms);
        const int32_t step_down_hold_ms =
                config_ms_in_range("jemalloc_decay_rss_step_down_hold_ms", config::jemalloc_decay_rss_step_down_hold_ms,
                                   kMinStepDownHoldMs, kMaxStepDownHoldMs, &warned_step_down_hold_ms);

        if (!config::enable_jemalloc_decay_under_rss_pressure) {
            // The config is mutable, so this is also the path taken when it is turned off at
            // runtime. Hand jemalloc back the configured decay before going idle; leaving the
            // arenas at whatever level the last scan set would make the switch look like it
            // had no effect until the next restart.
            if (level != 0) {
                LOG(INFO) << "enable_jemalloc_decay_under_rss_pressure turned off at level " << level
                          << "; restoring the configured jemalloc decay";
                apply_rung(0);
                level = 0;
            }
            below_level.reset();
            std::this_thread::sleep_for(std::chrono::milliseconds(interval_ms));
            continue;
        }

        auto* tracker = RuntimeEnv::GetInstance()->process_mem_tracker();
        const int64_t limit = tracker != nullptr ? tracker->limit() : -1;
        // MemInfo::process_resident_bytes() rather than jemalloc's stats.resident: the latter
        // needs an epoch refresh that walks every arena, far too heavy to poll at this interval,
        // and it is not what the kernel kills on anyway.
        const int64_t rss = MemInfo::process_resident_bytes();

        if (limit > 0 && rss > 0) {
            const int up_target = decay_level_to_enter(rss, limit);
            const int down_target = decay_level_to_hold(rss, limit);

            if (up_target > level) {
                LOG(INFO) << "rss " << rss << " reached " << kDecayLevels[up_target - 1].enter_ratio * 100
                          << "% of the process mem limit " << limit << " while the tracker only counted "
                          << tracker->consumption() << "; tightening jemalloc decay to level " << up_target;
                apply_rung(up_target);
                level = up_target;
                below_level.reset();
            } else if (down_target < level) {
                below_level.start();
                // The floor guarantees step_down_hold_ms is not negative, so the cast is safe;
                // elapsed_time() is unsigned nanoseconds.
                if (below_level.elapsed_time() / 1000000 >= static_cast<uint64_t>(step_down_hold_ms)) {
                    LOG(INFO) << "rss " << rss << " back under the level " << level
                              << " threshold of the process mem limit " << limit << "; relaxing jemalloc decay to "
                              << "level " << down_target;
                    apply_rung(down_target);
                    level = down_target;
                    below_level.reset();
                }
            } else {
                below_level.reset();
            }
        }

        std::this_thread::sleep_for(std::chrono::milliseconds(interval_ms));
    }

    if (level != 0) {
        apply_rung(0);
    }
}

// Tracker the memory usage of jemalloc
void jemalloc_tracker_daemon(void* arg_this) {
    auto* daemon = static_cast<Daemon*>(arg_this);
    while (!daemon->stopped()) {
        JemallocStats stats;
        retrieve_jemalloc_stats(&stats);

        // Jemalloc metadata
        if (RuntimeEnv::GetInstance()->jemalloc_metadata_traker() && stats.metadata > 0) {
            auto tracker = RuntimeEnv::GetInstance()->jemalloc_metadata_traker();
            int64_t delta = stats.metadata - tracker->consumption();
            tracker->consume(delta);
        }

        nap_sleep(1, [daemon] { return daemon->stopped(); });
    }
}
#endif

#define DUMP_METRIC(name, value_expr) fmt::format_to(std::back_inserter(buffer), " " #name "({})", value_expr);
std::string dump_memory_tracker() {
    auto* mem_metrics = RuntimeEnv::GetInstance()->process_memory_metrics();

    fmt::memory_buffer buffer;
    fmt::format_to(std::back_inserter(buffer), "Current memory statistics:");

    DUMP_METRIC(process, mem_metrics->process_mem_bytes.value())
    DUMP_METRIC(query_pool, mem_metrics->query_mem_bytes.value())
    DUMP_METRIC(load, mem_metrics->load_mem_bytes.value())
    DUMP_METRIC(metadata, mem_metrics->metadata_mem_bytes.value())
    DUMP_METRIC(compaction, mem_metrics->compaction_mem_bytes.value())
    DUMP_METRIC(schema_change, mem_metrics->schema_change_mem_bytes.value())
    DUMP_METRIC(page_cache, mem_metrics->storage_page_cache_mem_bytes.value())
    DUMP_METRIC(update, mem_metrics->update_mem_bytes.value())
    DUMP_METRIC(passthrough, mem_metrics->passthrough_mem_bytes.value())
    DUMP_METRIC(clone, mem_metrics->clone_mem_bytes.value())
    DUMP_METRIC(consistency, mem_metrics->consistency_mem_bytes.value())
    DUMP_METRIC(datacache, mem_metrics->datacache_mem_bytes.value())
    DUMP_METRIC(jit, mem_metrics->jit_cache_mem_bytes.value())
    DUMP_METRIC(brpc_iobuf, mem_metrics->brpc_iobuf_mem_bytes.value())
    DUMP_METRIC(replication, mem_metrics->replication_mem_bytes.value())
    DUMP_METRIC(vector_index, mem_metrics->vector_index_mem_bytes.value())

    DUMP_METRIC(jemalloc_active, mem_metrics->jemalloc_active_bytes.value())
    DUMP_METRIC(jemalloc_allocated, mem_metrics->jemalloc_allocated_bytes.value())
    DUMP_METRIC(jemalloc_metadata, mem_metrics->jemalloc_metadata_bytes.value())
    DUMP_METRIC(jemalloc_rss, mem_metrics->jemalloc_resident_bytes.value())
    DUMP_METRIC(jemalloc_mapped, mem_metrics->jemalloc_mapped_bytes.value())
    DUMP_METRIC(jemalloc_retained, mem_metrics->jemalloc_retained_bytes.value())
    DUMP_METRIC(jemalloc_dirty, mem_metrics->jemalloc_dirty_bytes.value())
    DUMP_METRIC(jemalloc_muzzy, mem_metrics->jemalloc_muzzy_bytes.value())

    return fmt::to_string(buffer);
}

static void init_starrocks_metrics(const std::vector<StorePath>& store_paths,
                                   ProcessMetricsRegistry* process_metrics_registry) {
    std::vector<std::string> paths;
    paths.reserve(store_paths.size());
    for (auto& store_path : store_paths) {
        paths.emplace_back(store_path.path);
    }
    auto options = BackendMetricsInitializer::from_config(std::move(paths));
    BackendMetricsInitializer::initialize(process_metrics_registry, options);
}

Slice get_process_comm(pid_t pid, char* buffer, int max_size) {
    std::string path = fmt::format("/proc/{}/comm", static_cast<int>(pid));

    int fd = open(path.c_str(), O_RDONLY);
    if (fd < 0) {
        return {};
    }

    ssize_t n = read(fd, buffer, max_size - 1);
    close(fd);

    if (n <= 0) {
        buffer[0] = '\0';
        return {};
    }

    buffer[n] = '\0';

    // trim '\n'
    if (n > 0 && buffer[n - 1] == '\n') {
        buffer[--n] = '\0';
    }

    return Slice(buffer, static_cast<size_t>(n));
}

// Ideally, we should avoid calling any non-signal-safe functions within signal handler, such as malloc, open, or log.
// This could potentially cause deadlocks or unexpected recursion.
// Typically, the main thread receives SIGTERM, and under normal circumstances,
// the main thread remains in a sleep state. Therefore, the current implementation is safe.
void sigterm_handler(int signo, siginfo_t* info, void* context) {
    if (info == nullptr) {
        LOG(ERROR) << "got signal: " << strsignal(signo) << "from unknown pid, is going to exit";
    } else {
        char buffer[1024];
        Slice process_comm = get_process_comm(info->si_pid, buffer, sizeof(buffer));
        LOG(ERROR) << "got signal: " << strsignal(signo) << " from pid: " << info->si_pid << "(" << process_comm << ")"
                   << ", is going to exit";

        DataCache::GetInstance()->update_mem_trackers();
        RuntimeEnv::GetInstance()->process_memory_metrics()->update_memory_metrics();
        LOG(ERROR) << dump_memory_tracker();
    }
#ifdef USE_STAROS
    set_starlet_in_shutdown();
#endif
    set_process_exit();
}

int install_signal(int signo, void (*handler)(int sig, siginfo_t* info, void* context)) {
    struct sigaction sa;
    memset(&sa, 0, sizeof(struct sigaction));
    sa.sa_sigaction = handler;
    sa.sa_flags = SA_SIGINFO;
    sigemptyset(&sa.sa_mask);
    auto ret = sigaction(signo, &sa, nullptr);
    if (ret != 0) {
        PLOG(ERROR) << "install signal failed, signo=" << signo;
    }
    return ret;
}

void init_signals() {
    auto ret = install_signal(SIGINT, sigterm_handler);
    if (ret < 0) {
        exit(-1);
    }
    ret = install_signal(SIGTERM, sigterm_handler);
    if (ret < 0) {
        exit(-1);
    }
    signal(SIGPIPE, SIG_IGN);
}

void init_minidump() {
#ifdef __x86_64__
    if (config::sys_minidump_enable) {
        LOG(INFO) << "Minidump is enabled";
        Minidump::init();
    } else {
        LOG(INFO) << "Minidump is disabled";
    }
#else
    LOG(INFO) << "Minidump is disabled on non-x86_64 arch";
#endif
}

void Daemon::init(bool as_cn, const std::vector<StorePath>& paths, ProcessMetricsRegistry* process_metrics_registry) {
    DCHECK(process_metrics_registry != nullptr);
    if (as_cn) {
        init_glog("cn", true);
    } else {
        init_glog("be", true);
    }
    init_runtime_logging_hooks();

    LOG(INFO) << get_version_string(false);

#if !defined(BE_TEST) && !defined(__APPLE__)
    starrocks::mlock_modules();
#endif

    init_thrift_logging();
    CpuInfo::init();
    DiskInfo::init();
    MemInfo::init();
    LOG(INFO) << CpuInfo::debug_string();
    LOG(INFO) << DiskInfo::debug_string();
    LOG(INFO) << MemInfo::debug_string();
    LOG(INFO) << base::CPU::instance()->debug_string();
    LOG(INFO) << "openssl aesni support: " << openssl_supports_aesni();
    auto unsupported_flags = CpuInfo::unsupported_cpu_flags_from_current_env();
    if (!unsupported_flags.empty()) {
        LOG(FATAL) << fmt::format(
                "CPU flags check failed! The following instruction sets are enabled during compiling but not supported "
                "in current running env: {}!",
                fmt::join(unsupported_flags, ","));
        std::abort();
    }

    CHECK(UserFunctionCache::instance()->init(config::user_function_dir).ok());

    date::init_date_cache();

    TimezoneUtils::init_time_zones();

    init_starrocks_metrics(paths, process_metrics_registry);

#ifndef __APPLE__
    if (config::enable_metric_calculator) {
        std::thread calculate_metrics_thread(calculate_metrics, this, process_metrics_registry);
        Thread::set_thread_name(calculate_metrics_thread, "metrics_daemon");
        _daemon_threads.emplace_back(std::move(calculate_metrics_thread));
    }

    if (config::enable_jemalloc_memory_tracker) {
        std::thread jemalloc_tracker_thread(jemalloc_tracker_daemon, this);
        Thread::set_thread_name(jemalloc_tracker_thread, "jemalloc_track");
        _daemon_threads.emplace_back(std::move(jemalloc_tracker_thread));
    }

    {
        std::thread mem_purge_thread(mem_purge_daemon, this);
        Thread::set_thread_name(mem_purge_thread, "mem_purge");
        _daemon_threads.emplace_back(std::move(mem_purge_thread));
    }
#endif

    {
        std::thread remote_scan_token_cleanup_thread(remote_scan_token_cleanup, this);
        Thread::set_thread_name(remote_scan_token_cleanup_thread, "rscan_token_gc");
        _daemon_threads.emplace_back(std::move(remote_scan_token_cleanup_thread));
    }

    init_signals();
    init_minidump();
#if defined(__SANITIZE_ADDRESS__) || defined(ADDRESS_SANITIZER)
#else
#ifndef __APPLE__
    // Don't bother set the limit if the process is running with very limited memory capacity
    if (MemInfo::physical_mem() > 1024 * 1024 * 1024) {
        // set mem hook to reject the memory allocation if large than available physical memory detected.
        set_large_memory_alloc_failure_threshold(MemInfo::physical_mem());
    }
#endif
#endif
}

void Daemon::stop() {
    _stopped.store(true, std::memory_order_release);
    size_t thread_size = _daemon_threads.size();
    for (size_t i = 0; i < thread_size; ++i) {
        if (_daemon_threads[i].joinable()) {
            _daemon_threads[i].join();
        }
    }
}

bool Daemon::stopped() {
    return _stopped.load(std::memory_order_consume);
}

} // namespace starrocks
