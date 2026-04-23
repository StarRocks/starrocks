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
#include <limits>
#include <sstream>
#include <system_error>

#include "common/config.h"
#include "common/logging.h"
#include "common/system/master_info.h"
#include "gen_cpp/HeartbeatService_types.h"
#include "http/http_client.h"
#include "http/http_common.h"
#include "rapidjson/document.h"
#include "runtime/exec_env.h"
#include "storage/store_path.h"

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
// `.jsonl.syncing.<numeric-tick-id>` left over by a crashed previous
// tick?
//
// The nested-suffix check is strict: we accept exactly one `.syncing.`
// segment followed by digits. This prevents two problems:
//   1. Coincidental names like `foo.jsonl.syncing_backup` being picked up.
//   2. A file that already carries `.syncing.<id>` from a failed post
//      being re-claimed recursively, which would grow the filename by
//      one segment every tick and eventually hit NAME_MAX.
bool is_claimable(const std::filesystem::path& p) {
    const std::string name = p.filename().string();
    if (p.extension() == kJsonlExtension) {
        // Accept a live writer file (exactly ends in `.jsonl`).
        return true;
    }
    // Accept `<name>.jsonl.syncing.<digits>` and nothing else. Anything
    // that already carries more than one `.syncing.` is a retry leftover
    // we don't want to rename again (that would balloon the filename).
    static constexpr std::string_view kSyncingToken = ".jsonl.syncing.";
    auto pos = name.find(kSyncingToken);
    if (pos == std::string::npos) {
        return false;
    }
    // Reject nested suffixes: a second `.syncing.` after the first means
    // we already renamed this file on a prior failed tick.
    if (name.find(kSyncingToken, pos + kSyncingToken.size()) != std::string::npos) {
        return false;
    }
    // Tail after `.syncing.` must be all digits (our tick id is a
    // nanosecond timestamp).
    std::string_view tail(name.data() + pos + kSyncingToken.size(),
                          name.size() - pos - kSyncingToken.size());
    if (tail.empty()) {
        return false;
    }
    for (char c : tail) {
        if (c < '0' || c > '9') {
            return false;
        }
    }
    return true;
}

// Recursively walk `root` and collect `.jsonl` / `.jsonl.syncing.*`
// paths. Symlinks are not followed so a malicious / misconfigured link
// cannot pull the daemon out of the store path.
void collect_jsonl(const std::string& root, std::vector<std::string>* out) {
    std::error_code ec;
    if (!std::filesystem::exists(root, ec) || ec) {
        return;
    }
    std::filesystem::recursive_directory_iterator it(root, std::filesystem::directory_options::skip_permission_denied,
                                                     ec);
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
        const int base_interval = std::max(1, config::rejected_record_sync_interval_sec);
        const int max_backoff = std::max(base_interval, config::rejected_record_sync_max_backoff_sec);
        // Double-and-cap backoff on consecutive failures. The goal is to
        // stop hammering a dead FE with a retry every `base_interval`
        // seconds when an outage lasts minutes or hours: after 5
        // consecutive failures at 30s base we'd be at 480s, at 10
        // consecutive failures capped at max_backoff (default 600s).
        int interval_sec = base_interval;
        if (_consecutive_failures > 0) {
            // Use int64 arithmetic so the shift can't overflow; then clamp.
            const int shift = std::min(_consecutive_failures, 20); // 2^20 already well past any max
            int64_t backoff = static_cast<int64_t>(base_interval) << shift;
            if (backoff > max_backoff) backoff = max_backoff;
            interval_sec = static_cast<int>(backoff);
        }
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
        const int64_t failures_before = _sync_failures.load(std::memory_order_relaxed);
        run_one_tick();
        const int64_t failures_after = _sync_failures.load(std::memory_order_relaxed);
        if (failures_after > failures_before) {
            ++_consecutive_failures;
        } else {
            _consecutive_failures = 0;
        }
    }
}

void RejectedRecordSyncDaemon::run_one_tick() {
    const auto tick_start = std::chrono::steady_clock::now();
    // scan_once now atomically renames `.jsonl` files to
    // `.jsonl.syncing.<tick-uuid>`. Any writer still holding the
    // `.jsonl` path will implicitly create a fresh inode on its next
    // open-append-close cycle (the writer no longer holds a long-lived
    // fd -- see Fix 3.3 in the feature branch). That fresh inode will
    // be picked up by a subsequent tick rather than racing this one.
    std::vector<std::string> files = scan_once();
    _files_scanned.fetch_add(static_cast<int64_t>(files.size()), std::memory_order_relaxed);
    if (files.empty()) {
        garbage_collect_stale_files();
        _last_tick_duration_us.store(
                std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::steady_clock::now() - tick_start)
                        .count(),
                std::memory_order_relaxed);
        return;
    }
    const int max_rows = std::max(1, config::rejected_record_sync_max_batch_rows);
    const int64_t max_bytes = std::max<int64_t>(1024 * 1024, config::rejected_record_sync_max_batch_bytes);
    process_files(files, max_rows, max_bytes);
    garbage_collect_stale_files();
    _last_tick_duration_us.store(
            std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::steady_clock::now() - tick_start).count(),
            std::memory_order_relaxed);
}

// Shared read-post-delete loop used by both `run_one_tick` (the
// production caller, with `max_rows` = configured cap) and
// `flush_batch` (the test-only caller, with `max_rows = INT_MAX` so
// the whole list ships as one post). Keeping both callers on the
// same implementation means any future fix to the
// accumulate/commit/retry semantics shows up consistently in both
// paths instead of silently drifting.
//
// Contract:
//   * Files that cannot be opened are immediately removed (keeping
//     them would make them re-claimable forever without ever being
//     readable).
//   * Empty files are swept up in the same way -- the commit deletes
//     them alongside readable siblings even when the batch as a
//     whole posts zero rows.
//   * On a successful post, every source file in that commit is
//     deleted.
//   * On a failed post, source files are LEFT on disk. They retain
//     their `.syncing.<tick>` suffix so scan_once's adopt-stale path
//     reclaims them on a subsequent tick.
void RejectedRecordSyncDaemon::process_files(const std::vector<std::string>& files, int64_t max_rows,
                                              int64_t max_bytes) {
    std::ostringstream payload;
    // Files fully read into the current payload. Only these get deleted
    // on a successful commit. A file that triggers a mid-file commit
    // stays out of `batch` until its `while(getline)` loop completes,
    // so partial files are never deleted prematurely.
    std::vector<std::string> batch;
    int64_t batch_rows = 0;
    int64_t batch_bytes = 0;

    auto commit = [&]() {
        if (batch_rows == 0 && batch.empty()) {
            return; // nothing accumulated
        }
        if (batch_rows == 0) {
            // Empty files in the batch -- nothing to post, but do
            // delete them so they don't get re-scanned forever.
            for (const auto& f : batch) {
                remove_file(f);
            }
            batch.clear();
            payload.str("");
            payload.clear();
            batch_bytes = 0;
            return;
        }
        auto st = post_to_stream_load(payload.str());
        if (!st.ok()) {
            if (st.is_uninitialized()) {
                // Master FE not yet known (early boot, master re-election
                // in flight). Don't count it as a failure and don't spam
                // the log at WARN; a VLOG trace is enough for operators
                // who are debugging the startup sequence.
                // Surface -1 as the "no HTTP exchange happened" sentinel so
                // dashboards can distinguish "FE not reachable yet" from a
                // real 4xx/5xx response.
                _last_http_status.store(-1, std::memory_order_relaxed);
                VLOG(1) << "RejectedRecordSyncDaemon: skipping tick because master FE is not yet reachable; "
                        << batch.size() << " files left on disk for retry";
            } else {
                _sync_failures.fetch_add(1, std::memory_order_relaxed);
                LOG(WARNING) << "RejectedRecordSyncDaemon: post_to_stream_load failed (" << batch.size()
                             << " full files, " << batch_rows << " rows, " << batch_bytes
                             << " bytes): " << st.message() << "; leaving files on disk for retry.";
            }
        } else {
            _records_flushed.fetch_add(batch_rows, std::memory_order_relaxed);
            _bytes_flushed.fetch_add(batch_bytes, std::memory_order_relaxed);
            // Only fully-drained files get removed; any file that triggered a
            // mid-file commit is still being read by the outer for-loop and
            // will be pushed into `batch` when its while(getline) exits.
            for (const auto& f : batch) {
                remove_file(f);
            }
        }
        payload.str("");
        payload.clear();
        batch.clear();
        batch_rows = 0;
        batch_bytes = 0;
    };

    for (const auto& f : files) {
        std::ifstream in(f);
        if (!in.is_open()) {
            _open_failures.fetch_add(1, std::memory_order_relaxed);
            LOG(WARNING) << "RejectedRecordSyncDaemon: cannot open " << f
                         << " for reading; keeping on disk for retry. If the file is truly unrecoverable"
                         << " it will eventually be garbage-collected by the retention policy.";
            // Don't remove: a transient EMFILE / I/O error shouldn't
            // silently destroy the rejected records. The retention-based
            // GC will clear it out if the failure is permanent.
            continue;
        }
        std::string line;
        while (std::getline(in, line)) {
            // Skip lines that are empty OR contain only whitespace. A
            // truncated file or a record writer that crashed mid-write
            // can leave behind a blank-ish trailing line; shipping it as
            // its own record would make the FE parse it as invalid JSON
            // and could force the entire merge-commit batch to fail.
            bool all_whitespace = true;
            for (char c : line) {
                if (c != ' ' && c != '\t' && c != '\r') {
                    all_whitespace = false;
                    break;
                }
            }
            if (all_whitespace) {
                continue;
            }
            const int64_t line_bytes = static_cast<int64_t>(line.size()) + 1; // +1 for '\n'
            // Enforce the cap BEFORE appending. A fresh line that would push
            // the accumulator past max_rows or max_bytes commits the current
            // batch first, then joins the new one. This matters for two
            // scenarios the old "check after append" logic handled badly:
            //   1. A single file with many rows - the loop used to append the
            //      whole file before checking, so one 1M-row file became one
            //      1M-row PUT regardless of max_rows.
            //   2. A row whose raw_record / error_message is unusually large -
            //      no byte budget at all, so a giant line could blow FE's
            //      streaming_load_max_mb regardless of the row count.
            if (batch_rows > 0 && (batch_rows + 1 > max_rows || batch_bytes + line_bytes > max_bytes)) {
                commit();
            }
            payload << line << '\n';
            batch_bytes += line_bytes;
            ++batch_rows;
        }
        // File fully read: safe to enroll for deletion on the next commit.
        batch.push_back(f);
    }
    commit();
}

std::vector<std::string> RejectedRecordSyncDaemon::store_path_roots() const {
    std::vector<std::string> roots;
    if (_env != nullptr) {
        for (const auto& sp : _env->store_paths()) {
            roots.push_back(sp.path);
        }
    }
    return roots;
}

std::vector<std::string> RejectedRecordSyncDaemon::list_once() const {
    std::vector<std::string> out;
    const std::vector<std::string> roots = store_path_roots();
    for (const auto& root : roots) {
        std::string rejected_root = root + kRejectedRecordDir;
        collect_jsonl(rejected_root, &out);
    }
    return out;
}

std::vector<std::string> RejectedRecordSyncDaemon::scan_once() {
    std::vector<std::string> out;
    // Per-tick suffix so a crash mid-tick leaves recoverable files --
    // the next boot sees a dangling `.syncing.<old-tick>` file and
    // re-includes it via `is_claimable`'s nested-suffix check.
    const std::string tick_suffix =
            std::string(kSyncingSuffix) + "." +
            std::to_string(static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::nanoseconds>(
                                                         std::chrono::steady_clock::now().time_since_epoch())
                                                         .count()));

    // Pick up both live `.jsonl` (rename them into this tick) and
    // orphaned `.syncing.<digits>` from previous crashed / killed ticks
    // (re-claim them by rename into this tick's suffix, so we don't
    // stomp on a genuinely-in-flight previous tick -- in normal
    // operation ticks don't overlap because the daemon is single-
    // threaded, but a fresh BE boot after a crash will find
    // leftovers here).
    const std::vector<std::string> candidates = list_once();

    for (const auto& src : candidates) {
        // For already-claimed leftovers (path ends in `.syncing.<digits>`),
        // strip the old tick suffix before re-appending the current one.
        // Otherwise a file that previously failed to sync would gain a
        // fresh `.syncing.<new>` suffix every tick and the filename would
        // grow unbounded toward NAME_MAX.
        std::string rename_base = src;
        const std::string name = std::filesystem::path(src).filename().string();
        static constexpr std::string_view kSyncingToken = ".jsonl.syncing.";
        auto pos = name.find(kSyncingToken);
        if (pos != std::string::npos) {
            // Trim `.syncing.<digits>` back to the `.jsonl` boundary.
            const size_t trim_from = src.size() - (name.size() - pos) + std::string(kJsonlExtension).size();
            rename_base = src.substr(0, trim_from);
        }
        std::string dst = rename_base + tick_suffix;
        if (dst == src) {
            // Shouldn't happen with the trim above, but guard against a
            // no-op rename that would otherwise burn a syscall.
            out.push_back(src);
            continue;
        }
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
    // Test entry point. Production traffic goes through `run_one_tick`
    // which caps each post at `rejected_record_sync_max_batch_rows`
    // and split-commits oversized backlogs. flush_batch drives the
    // same underlying `process_files` implementation with an
    // effectively-infinite cap so the whole `files` list ships as
    // one post, mirroring the simpler shape that unit tests want.
    // Keeping both callers on the shared implementation means
    // accumulate / commit / retry semantics stay in sync.
    if (files.empty()) {
        return Status::OK();
    }
    const int64_t prior_failures = _sync_failures.load(std::memory_order_relaxed);
    process_files(files, std::numeric_limits<int64_t>::max(), std::numeric_limits<int64_t>::max());
    // process_files updates _sync_failures / _records_flushed directly.
    // Surface the outcome via this entry point's Status return so the
    // existing tests that assert on "post failed => non-OK" keep
    // passing.
    if (_sync_failures.load(std::memory_order_relaxed) > prior_failures) {
        return Status::InternalError("RejectedRecordSyncDaemon::flush_batch: post_to_stream_load failed");
    }
    return Status::OK();
}

Status RejectedRecordSyncDaemon::post_to_stream_load(const std::string& payload) {
    TMasterInfo master_info = get_master_info();
    if (master_info.network_address.hostname.empty() || master_info.http_port <= 0) {
        // Master FE not yet known is a transient startup condition, not a
        // sync failure. Returning a dedicated NotReady-shaped status lets
        // process_files() skip the failure counter and retry-log for this
        // case -- otherwise early BE boot (before the first heartbeat) and
        // any master-elected transition generate a steady stream of
        // "post failed" log lines and metric spikes that don't reflect
        // real errors.
        return Status::Uninitialized(
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
    // Use set_custom_method("PUT") instead of set_method(PUT) which
    // sets CURLOPT_UPLOAD. CURLOPT_UPLOAD conflicts with set_payload's
    // CURLOPT_POSTFIELDS -- the former expects a read callback while the
    // latter provides inline data. set_custom_method uses
    // CURLOPT_CUSTOMREQUEST which overrides the method string without
    // changing curl's transfer semantics, so the payload is correctly
    // sent as the PUT body through the FE 307 redirect to the CN.
    client.set_custom_method("PUT");
    client.set_content_type("application/json");
    client.set_basic_auth(config::rejected_record_sync_user, config::rejected_record_sync_password);

    // Format is json-lines (one JSON object per line); the StarRocks
    // Stream Load parses this when `strip_outer_array=false` (the
    // default) with `format=json`.
    client.set_header(HTTP_FORMAT_KEY, "json");
    client.set_header(HTTP_STRIP_OUTER_ARRAY, "false");
    // The FE 307-redirects large PUTs; opt into 100-continue so curl
    // negotiates before uploading the payload body.
    client.set_header("Expect", "100-continue");
    // Follow the redirect with credentials preserved; the FE issues one.
    //
    // Security note: CURLOPT_UNRESTRICTED_AUTH = 1 makes libcurl include
    // the Basic auth header on the host the FE returned in the Location
    // header. That host is always another node in the same cluster
    // (FE-owned redirect to a BE serving the Stream Load), so the
    // credential never leaves the VPC. Operators running an FE behind a
    // non-cluster reverse proxy should ensure that proxy only redirects
    // inside their trust boundary; otherwise the
    // rejected_record_sync_password would leak to whatever host the
    // proxy named in the Location.
    client.set_unrestricted_auth(1);

    client.set_payload(payload);
    client.set_timeout_ms(static_cast<int64_t>(std::max(1, config::rejected_record_sync_post_timeout_sec)) * 1000);

    std::string response;
    RETURN_IF_ERROR(client.execute(&response));

    long http_status = client.get_http_status();
    _last_http_status.store(http_status, std::memory_order_relaxed);
    if (http_status < 200 || http_status >= 300) {
        return Status::InternalError(fmt::format("Stream Load HTTP {} from FE: {}", http_status, response));
    }

    // The Stream Load response is a JSON object; "Status" is either
    // "Success" or "Publish Timeout" on a successful commit, and the
    // latter is still committed data per the existing semantics in
    // StreamLoadContext.
    rapidjson::Document doc;
    if (doc.Parse(response.c_str(), response.size()).HasParseError() || !doc.IsObject() || !doc.HasMember("Status") ||
        !doc["Status"].IsString()) {
        return Status::InternalError(fmt::format("Stream Load response from FE is not valid JSON: {}", response));
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
    return Status::InternalError(fmt::format(
            "Stream Load for _statistics_.rejected_records rejected: Status={} Message={}", status, message));
}

void RejectedRecordSyncDaemon::garbage_collect_stale_files() {
    int retention_hours = std::max(1, config::rejected_record_local_retention_hours);
    auto cutoff = std::filesystem::file_time_type::clock::now() - std::chrono::hours(retention_hours);
    // Use the read-only enumeration here; GC must never rename files.
    // Renaming would flip them into this tick's `.syncing.<id>`,
    // causing the next `run_one_tick` to re-adopt them under a second
    // nested suffix -- the filename-balloon bug. A pure read also
    // halves the directory-walk cost.
    const std::vector<std::string> files = list_once();
    int64_t dropped_this_cycle = 0;
    for (const auto& f : files) {
        std::error_code ec;
        auto mtime = std::filesystem::last_write_time(f, ec);
        if (ec) continue;
        if (mtime < cutoff) {
            // WARN rather than INFO: each dropped file is a batch of
            // rejected rows that never made it to the FE, i.e. data loss
            // visible to operators. The counter below feeds metrics so
            // dashboards can alert on it.
            LOG(WARNING) << "RejectedRecordSyncDaemon: dropping stale rejected-record file " << f
                         << " (retention " << retention_hours << "h exceeded); rejected rows in this file"
                         << " are lost. Check FE availability and rejected_record_sync_user / "
                            "rejected_record_sync_password if this repeats.";
            remove_file(f);
            ++dropped_this_cycle;
        }
    }
    if (dropped_this_cycle > 0) {
        _files_dropped_by_gc.fetch_add(dropped_this_cycle, std::memory_order_relaxed);
    }
}

} // namespace starrocks
