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

#include <fmt/format.h>

#include <chrono>
#include <filesystem>
#include <fstream>
#include <sstream>
#include <system_error>

#include "agent/master_info.h"
#include "common/config.h"
#include "common/logging.h"
#include "gen_cpp/HeartbeatService_types.h"
#include "http/http_client.h"
#include "http/http_common.h"
#include "rapidjson/document.h"
#include "runtime/exec_env.h"

namespace starrocks {

namespace {

constexpr const char* kJsonlExtension = ".jsonl";
constexpr const char* kRejectedRecordDir = "/rejected_record";
// Files the daemon has claimed for a flush get this suffix appended so
// concurrent writers never see them as the active `.jsonl` path.
// Making it per-tick via a UUID prevents a crashed daemon from leaving a
// `.syncing` file that the next daemon boot might interpret as "already
// being processed" and skip forever.
constexpr const char* kSyncingSuffix = ".syncing";

// Best-effort delete; logs on failure but never throws. Missing files are
// not an error (another tick may have raced us).
void remove_file(const std::string& path) {
    std::error_code ec;
    std::filesystem::remove(path, ec);
    if (ec && ec != std::errc::no_such_file_or_directory) {
        LOG(WARNING) << "RejectedRecordSyncDaemon: failed to remove " << path << ": " << ec.message();
    }
}

// Does this path name a live `.jsonl` writer file OR a dangling
// `.jsonl.syncing.<id>` left over by a crashed previous tick?
bool is_claimable(const std::filesystem::path& p) {
    if (p.extension() == kJsonlExtension) {
        return true;
    }
    // Match `foo.jsonl.syncing.<anything>`: the ".jsonl" needs to still
    // be in the filename (as an interior extension) before ".syncing.*".
    // extension() only returns the last component, so walk once.
    const std::string name = p.filename().string();
    auto pos = name.find(".jsonl.syncing");
    return pos != std::string::npos;
}

// Recursively walk `root` and collect `.jsonl` / `.jsonl.syncing.*`
// paths. Symlinks are not followed so a malicious / misconfigured link
// cannot pull the daemon out of the store path.
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
        if (is_claimable(it->path())) {
            out->push_back(it->path().string());
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
    // scan_once now atomically renames `.jsonl` files to
    // `.jsonl.syncing.<tick-uuid>`. Any writer still holding the
    // `.jsonl` path will implicitly create a fresh inode on its next
    // open-append-close cycle (the writer no longer holds a long-lived
    // fd -- see Fix 3.3 in the feature branch). That fresh inode will
    // be picked up by a subsequent tick rather than racing this one.
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
    // Per-tick suffix so a crash mid-tick leaves recoverable files --
    // the next boot sees a dangling `.syncing.<old-tick>` file and
    // re-includes it (adopt_stale_syncing below).
    const std::string tick_suffix =
            std::string(kSyncingSuffix) + "." + std::to_string(static_cast<uint64_t>(
                    std::chrono::duration_cast<std::chrono::nanoseconds>(
                            std::chrono::steady_clock::now().time_since_epoch())
                            .count()));

    std::vector<std::string> candidates;
    for (const auto& sp : _env->store_paths()) {
        std::string rejected_root = sp.path + kRejectedRecordDir;
        // Pick up both live `.jsonl` (rename them into this tick) and
        // orphaned `.syncing.*` from previous crashed / killed ticks
        // (re-claim them by rename into this tick's suffix, so we don't
        // stomp on a genuinely-in-flight previous tick -- in normal
        // operation ticks don't overlap because the daemon is single-
        // threaded, but a fresh BE boot after a crash will find
        // leftovers here).
        collect_jsonl(rejected_root, &candidates);
    }

    for (const auto& src : candidates) {
        std::string dst = src + tick_suffix;
        std::error_code ec;
        std::filesystem::rename(src, dst, ec);
        if (ec) {
            // ENOENT = another tick already claimed it; any other error
            // is transient and we'll retry on the next tick.
            if (ec != std::errc::no_such_file_or_directory) {
                LOG(WARNING) << "RejectedRecordSyncDaemon: rename " << src << " -> " << dst
                             << " failed: " << ec.message();
            }
            continue;
        }
        out.push_back(std::move(dst));
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
    TMasterInfo master_info = get_master_info();
    if (master_info.network_address.hostname.empty() || master_info.http_port <= 0) {
        return Status::InternalError(
                "RejectedRecordSyncDaemon: master FE address not yet known (no heartbeat received?)");
    }

    // Stream Load PUT path for an internal system table. The FE serves
    // loads to any table under /api/<db>/<table>/_stream_load; we target
    // `_statistics_.rejected_records` directly.
    std::ostringstream url;
    url << "http://" << master_info.network_address.hostname << ":" << master_info.http_port
        << "/api/_statistics_/rejected_records/_stream_load";

    HttpClient client;
    RETURN_IF_ERROR(client.init(url.str()));
    client.set_method(PUT);
    client.set_content_type("application/json");
    client.set_basic_auth(config::rejected_record_sync_user, config::rejected_record_sync_password);

    // Merge-commit so N BEs writing concurrently collapse into one FE
    // transaction. Format is json-lines (one JSON object per line); the
    // StarRocks Stream Load parses this when `strip_outer_array=false`
    // (the default) with `format=json`.
    client.set_header(HTTP_ENABLE_MERGE_COMMIT, "true");
    client.set_header(HTTP_FORMAT_KEY, "json");
    client.set_header(HTTP_STRIP_OUTER_ARRAY, "false");
    // The FE 307-redirects large PUTs; opt into 100-continue so curl
    // negotiates before uploading the payload body.
    client.set_header("Expect", "100-continue");
    // Follow the redirect with credentials preserved; the FE issues one.
    client.set_unrestricted_auth(1);

    client.set_payload(payload);
    client.set_timeout_ms(static_cast<int64_t>(std::max(1, config::rejected_record_sync_post_timeout_sec)) * 1000);

    std::string response;
    RETURN_IF_ERROR(client.execute(&response));

    long http_status = client.get_http_status();
    if (http_status < 200 || http_status >= 300) {
        return Status::InternalError(fmt::format("Stream Load HTTP {} from FE: {}", http_status, response));
    }

    // The Stream Load response is a JSON object; "Status" is either
    // "Success" or "Publish Timeout" on a successful commit, and the
    // latter is still committed data per the existing semantics in
    // StreamLoadContext.
    rapidjson::Document doc;
    if (doc.Parse(response.c_str(), response.size()).HasParseError() || !doc.IsObject() ||
        !doc.HasMember("Status") || !doc["Status"].IsString()) {
        return Status::InternalError(
                fmt::format("Stream Load response from FE is not valid JSON: {}", response));
    }
    std::string status = doc["Status"].GetString();
    if (status == "Success" || status == "Publish Timeout") {
        return Status::OK();
    }
    // The response typically carries a "Message" field with details; bubble
    // it up so operators see the actual reason in the BE log.
    std::string message;
    if (doc.HasMember("Message") && doc["Message"].IsString()) {
        message = doc["Message"].GetString();
    }
    return Status::InternalError(
            fmt::format("Stream Load for _statistics_.rejected_records rejected: Status={} Message={}",
                        status, message));
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
