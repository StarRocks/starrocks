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

#include "runtime/rejected_record_sync_daemon.h"

#include <chrono>
#include <filesystem>
#include <fstream>
#include <sstream>
#include <system_error>

#include "common/config.h"
#include "common/logging.h"
#include "runtime/exec_env.h"

namespace starrocks {

namespace {

constexpr const char* kJsonlExtension = ".jsonl";
constexpr const char* kRejectedRecordDir = "/rejected_record";

// Best-effort delete; logs on failure but never throws. Missing files are
// not an error (another tick may have raced us).
void remove_file(const std::string& path) {
    std::error_code ec;
    std::filesystem::remove(path, ec);
    if (ec && ec != std::errc::no_such_file_or_directory) {
        LOG(WARNING) << "RejectedRecordSyncDaemon: failed to remove " << path << ": " << ec.message();
    }
}

// Recursively walk `root` and collect paths ending in `.jsonl`. Symlinks
// are not followed so a malicious / misconfigured link cannot pull the
// daemon out of the store path.
void collect_jsonl(const std::string& root, std::vector<std::string>* out) {
    std::error_code ec;
    if (!std::filesystem::exists(root, ec) || ec) {
        return;
    }
    std::filesystem::recursive_directory_iterator it(root,
                                                     std::filesystem::directory_options::skip_permission_denied, ec);
    if (ec) {
        LOG(WARNING) << "RejectedRecordSyncDaemon: cannot iterate " << root << ": " << ec.message();
        return;
    }
    for (; it != std::filesystem::recursive_directory_iterator(); it.increment(ec)) {
        if (ec) {
            LOG(WARNING) << "RejectedRecordSyncDaemon: directory iteration error under " << root << ": "
                         << ec.message();
            break;
        }
        if (!it->is_regular_file(ec) || ec) {
            continue;
        }
        const auto& p = it->path();
        if (p.extension() == kJsonlExtension) {
            out->push_back(p.string());
        }
    }
}

} // namespace

RejectedRecordSyncDaemon::RejectedRecordSyncDaemon(ExecEnv* env) : _env(env) {
    _stop_future = _stop.get_future();
}

RejectedRecordSyncDaemon::~RejectedRecordSyncDaemon() {
    stop();
}

Status RejectedRecordSyncDaemon::init() {
    bool expected = false;
    if (!_started.compare_exchange_strong(expected, true)) {
        return Status::OK(); // already started
    }
    int rc = pthread_create(&_thread_id, nullptr, &RejectedRecordSyncDaemon::tick_thread_entry, this);
    if (rc != 0) {
        _started.store(false);
        return Status::InternalError("Failed to create RejectedRecordSyncDaemon thread");
    }
#if defined(__linux__)
    // pthread_setname_np truncates at 15 chars + NUL; "rr_sync" fits.
    pthread_setname_np(_thread_id, "rr_sync");
#endif
    LOG(INFO) << "RejectedRecordSyncDaemon started";
    return Status::OK();
}

void RejectedRecordSyncDaemon::stop() {
    if (!_started.load()) {
        return;
    }
    bool expected = false;
    if (!_stopped.compare_exchange_strong(expected, true)) {
        return;
    }
    _stop.set_value(true);
    if (_thread_id != 0) {
        pthread_join(_thread_id, nullptr);
        _thread_id = 0;
    }
    LOG(INFO) << "RejectedRecordSyncDaemon stopped";
}

void* RejectedRecordSyncDaemon::tick_thread_entry(void* self) {
    static_cast<RejectedRecordSyncDaemon*>(self)->tick_loop();
    return nullptr;
}

void RejectedRecordSyncDaemon::tick_loop() {
    while (true) {
        int interval_sec = std::max(1, config::rejected_record_sync_interval_sec);
        auto status = _stop_future.wait_for(std::chrono::seconds(interval_sec));
        if (status == std::future_status::ready) {
            // stop() was called; drain nothing and exit. A final sync would
            // race the ExecEnv tear-down so we intentionally skip it.
            return;
        }
        // status == std::future_status::timeout -> time for another tick.
        if (!config::enable_rejected_record_sync) {
            // Feature flag flipped off at runtime; behave as a no-op.
            continue;
        }
        run_one_tick();
    }
}

void RejectedRecordSyncDaemon::run_one_tick() {
    std::vector<std::string> files = scan_once();
    _files_scanned.fetch_add(static_cast<int64_t>(files.size()), std::memory_order_relaxed);
    if (files.empty()) {
        return;
    }
    int max_rows = std::max(1, config::rejected_record_sync_max_batch_rows);
    // Chunk into batches sized so each Stream Load stays under the configured
    // row cap. We use file count as a rough proxy for row count (one line per
    // record); a more precise split would require reading each file first.
    std::vector<std::string> batch;
    batch.reserve(std::min<size_t>(files.size(), 128));
    int row_budget = max_rows;
    auto commit = [&]() {
        if (batch.empty()) return;
        auto st = flush_batch(batch);
        if (!st.ok()) {
            _sync_failures.fetch_add(1, std::memory_order_relaxed);
            LOG(WARNING) << "RejectedRecordSyncDaemon: flush_batch failed (" << batch.size()
                         << " files): " << st.message() << "; leaving files on disk for retry.";
        }
        batch.clear();
        row_budget = max_rows;
    };
    for (const auto& f : files) {
        // Without reading the file ahead of time we can't know the exact row
        // count, so fall back to a conservative cap: at most max_rows files
        // per batch. Oversized files still get shipped -- the cap is a
        // soft limit to bound merge-commit transaction size.
        batch.push_back(f);
        if (--row_budget <= 0 || batch.size() >= static_cast<size_t>(max_rows)) {
            commit();
        }
    }
    commit();
    garbage_collect_stale_files();
}

std::vector<std::string> RejectedRecordSyncDaemon::scan_once() {
    std::vector<std::string> out;
    if (_env == nullptr) {
        return out;
    }
    for (const auto& sp : _env->store_paths()) {
        std::string rejected_root = sp.path + kRejectedRecordDir;
        collect_jsonl(rejected_root, &out);
    }
    return out;
}

Status RejectedRecordSyncDaemon::flush_batch(const std::vector<std::string>& files) {
    // Concatenate the JSON Lines contents of each file into a single
    // payload. rapidjson / simdjson parsing on the FE side will split lines.
    std::ostringstream payload;
    int64_t row_count = 0;
    for (const auto& f : files) {
        std::ifstream in(f);
        if (!in.is_open()) {
            LOG(WARNING) << "RejectedRecordSyncDaemon: cannot open " << f << " for reading; skipping.";
            continue;
        }
        std::string line;
        while (std::getline(in, line)) {
            if (line.empty()) continue;
            payload << line << '\n';
            ++row_count;
        }
    }
    if (row_count == 0) {
        // Every file was empty or unreadable. Still remove them so we don't
        // scan them repeatedly.
        for (const auto& f : files) {
            remove_file(f);
        }
        return Status::OK();
    }
    RETURN_IF_ERROR(post_to_stream_load(payload.str()));
    _records_flushed.fetch_add(row_count, std::memory_order_relaxed);
    // Post succeeded -- delete the files we just shipped. A crash between
    // post and delete means those records are duplicated into the system
    // table; the PK on `id` (UUID) dedups them transparently.
    for (const auto& f : files) {
        remove_file(f);
    }
    return Status::OK();
}

Status RejectedRecordSyncDaemon::post_to_stream_load(const std::string& payload) {
    // TODO(rejected_records Phase 3 follow-up): wire this up to the FE Stream
    // Load endpoint. Implementation plan:
    //   1. Locate the master FE via `MasterInfo` (be/src/agent/master_info.h)
    //      or `BackendServiceClient` -- we need the FE's HTTP host + port.
    //   2. Build a PUT against
    //        http://<fe_host>:<fe_http_port>/api/_statistics_/rejected_records/_stream_load
    //      with headers:
    //        - Expect: 100-continue
    //        - format: json
    //        - strip_outer_array: false (one record per line)
    //        - enable_merge_commit: true
    //        - Authorization: Basic <internal service credentials>
    //   3. Use HttpClient (be/src/http/http_client.h) to issue the PUT.
    //   4. Check the JSON response for Status=="Success" or "Publish Timeout"
    //      (the latter is still a successful commit).
    //
    // Until that lands this method returns NotSupported so the daemon keeps
    // local files around for a future binary to pick up. The error is
    // counted via `_sync_failures` and surfaces in metrics.
    LOG_EVERY_N(WARNING, 100) << "RejectedRecordSyncDaemon::post_to_stream_load is a stub; "
                              << payload.size() << " bytes staged locally, awaiting Phase 3 follow-up wiring";
    return Status::NotSupported("RejectedRecordSyncDaemon::post_to_stream_load is not yet wired");
}

void RejectedRecordSyncDaemon::garbage_collect_stale_files() {
    int retention_hours = std::max(1, config::rejected_record_local_retention_hours);
    auto cutoff = std::filesystem::file_time_type::clock::now() - std::chrono::hours(retention_hours);
    std::vector<std::string> files = scan_once();
    for (const auto& f : files) {
        std::error_code ec;
        auto mtime = std::filesystem::last_write_time(f, ec);
        if (ec) continue;
        if (mtime < cutoff) {
            LOG(INFO) << "RejectedRecordSyncDaemon: dropping stale rejected-record file " << f;
            remove_file(f);
        }
    }
}

} // namespace starrocks
