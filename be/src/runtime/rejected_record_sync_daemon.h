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

#include <pthread.h>

#include <atomic>
#include <cstdint>
#include <future>
#include <string>
#include <vector>

#include "common/status.h"

namespace starrocks {

class ExecEnv;

// Phase 3 of the rejected records system table feature.
//
// RejectedRecordSyncDaemon periodically scans the local JSON Lines files
// produced by RejectedRecordWriter (Phase 2) and ships them to the FE
// `_statistics_.rejected_records` system table via a merge-commit Stream
// Load. The daemon is opt-in during the phased rollout: it is started by
// ExecEnv only when `config::enable_rejected_record_sync` is true.
//
// Lifecycle:
//   * init()   -- called from ExecEnv::init(). Spawns the background thread.
//   * stop()   -- called from ExecEnv::stop() (or dtor). Signals the thread
//                 to drain and exit; joins it.
//
// Design choices:
//   * Reuses the `pthread_create` + `std::promise<bool>::wait_for` pattern
//     from LoadPathMgr::cleaner so agents familiar with one can read the
//     other. The daemon thread name is `rr_sync` (8 chars, fits Linux
//     task comm).
//   * Disk-to-wire in two phases: `scan_once()` enumerates candidate files
//     and `flush_batch()` posts one batch to FE. Both are exposed as
//     protected methods so unit tests can drive them directly without
//     needing a live ExecEnv.
//   * The post-to-FE call is factored into `post_to_stream_load()` which
//     is the only method that talks HTTP. Tests override it to capture
//     the payload in memory; production ships it over HttpClient.
//
// `post_to_stream_load()` issues a real HttpClient PUT with merge-commit
// headers and parses the FE response; tests override it to capture the
// payload in memory. FE discovery / auth / retry semantics are handled by
// the shared HttpClient layer.
class RejectedRecordSyncDaemon {
public:
    explicit RejectedRecordSyncDaemon(ExecEnv* env);
    virtual ~RejectedRecordSyncDaemon();

    RejectedRecordSyncDaemon(const RejectedRecordSyncDaemon&) = delete;
    RejectedRecordSyncDaemon& operator=(const RejectedRecordSyncDaemon&) = delete;

    // Spawn the background thread. Idempotent; returns OK if already running.
    Status init();

    // Signal the thread to exit and join it. Safe to call multiple times
    // and safe to call even if init() was never invoked (no-op).
    void stop();

    // Stats surfaced for metrics and tests.
    int64_t files_scanned() const { return _files_scanned.load(std::memory_order_relaxed); }
    int64_t records_flushed() const { return _records_flushed.load(std::memory_order_relaxed); }
    int64_t sync_failures() const { return _sync_failures.load(std::memory_order_relaxed); }
    // Files deleted by `garbage_collect_stale_files` because their mtime
    // is older than `rejected_record_local_retention_hours`. These
    // represent rejected rows that never successfully shipped to the FE
    // (because the FE was down for longer than the retention window), so
    // the counter measures silent data loss due to prolonged outages.
    int64_t files_dropped_by_gc() const { return _files_dropped_by_gc.load(std::memory_order_relaxed); }
    // Per-file transient open failures. These files are kept on disk so
    // the next tick can retry, rather than deleted outright.
    int64_t open_failures() const { return _open_failures.load(std::memory_order_relaxed); }

protected:
    // Returns the list of store-path root directories that scan_once() should
    // walk. Production returns one entry per element in _env->store_paths().
    // Tests override this to inject a temporary directory without needing a
    // live ExecEnv, which allows the real scan_once() / collect_jsonl() /
    // is_claimable() logic to execute under test control.
    virtual std::vector<std::string> store_path_roots() const;

    // Read-only enumeration of `.jsonl` and `.jsonl.syncing.<digits>`
    // files under every store-path's rejected_record tree. Does not
    // rename or modify anything. Used by `garbage_collect_stale_files`
    // to inspect mtimes without flipping files into the current tick's
    // `.syncing.<id>`; `run_one_tick` uses the rename-ful `scan_once`
    // below to adopt files for this tick.
    virtual std::vector<std::string> list_once() const;

    // Enumerate .jsonl files under every store path's rejected_record tree
    // and rename each one into this tick's `.syncing.<id>`. Files that were
    // left behind by a previous failed tick (already carry a `.syncing.<id>`
    // suffix) are re-claimed without adding a second nested suffix, so the
    // filename never grows past `<original>.jsonl.syncing.<id>` regardless
    // of how many retry rounds happen.
    virtual std::vector<std::string> scan_once();

    // Test-only entry to the read-concat-post-delete loop. Drives the
    // shared `process_files` implementation with an unbounded row cap
    // so a caller that supplies N files gets a single merge-commit
    // Stream Load. Returns non-OK iff the underlying post failed (the
    // `.syncing.<tick>` files are left behind in that case, matching
    // the production retry-on-next-tick semantics). Production
    // traffic goes through `run_one_tick` instead.
    virtual Status flush_batch(const std::vector<std::string>& files);

    // Post a raw JSON Lines payload to the FE `_statistics_.rejected_records`
    // Stream Load endpoint with `enable_merge_commit=true`. Virtual so
    // tests can substitute a capturing stub. NOTE: current implementation
    // returns NotSupported -- the FE Stream Load client wiring is deferred
    // to a follow-up commit.
    virtual Status post_to_stream_load(const std::string& payload);

    // Delete files older than rejected_record_local_retention_hours that
    // we have repeatedly failed to sync. Called from the tick loop.
    void garbage_collect_stale_files();

    std::future<bool>& stop_future() { return _stop_future; }

protected:
    // Exposed as protected so tests can invoke one synchronous tick
    // without running the background thread.
    void run_one_tick();

    // Core read-post-delete loop shared by `run_one_tick` (production,
    // caps each commit at the configured per-batch row / byte limit)
    // and `flush_batch` (tests, unbounded so the call ships in one
    // post). Both `max_rows` and `max_bytes` are enforced per-line
    // inside a file, so a single giant file won't exceed either cap.
    void process_files(const std::vector<std::string>& files, int64_t max_rows, int64_t max_bytes);

private:
    static void* tick_thread_entry(void* self);
    void tick_loop();

    ExecEnv* _env;
    std::promise<bool> _stop;
    std::future<bool> _stop_future;
    pthread_t _thread_id = 0;
    std::atomic<bool> _started{false};
    std::atomic<bool> _stopped{false};

    std::atomic<int64_t> _files_scanned{0};
    std::atomic<int64_t> _records_flushed{0};
    std::atomic<int64_t> _sync_failures{0};
    std::atomic<int64_t> _files_dropped_by_gc{0};
    std::atomic<int64_t> _open_failures{0};
};

} // namespace starrocks
