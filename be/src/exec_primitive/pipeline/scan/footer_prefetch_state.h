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

#include <algorithm>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

namespace starrocks::pipeline {

// Connector-supplied, self-contained context to open one root file the same cache-wrapped
// way the real scan does (FS/cloud config + DataCacheOptions). Defined by the connector;
// opaque to the state machine. Shared because it is identical across a scan's files.
struct FooterOpenContext;

// One root file the footer prefetcher may warm. A COPY is handed to each metadata task,
// which may outlive operator/node close, so it must hold no provider / RuntimeState /
// ObjectPool pointers -- only copied values and the shared open context.
struct FooterPrefetchItem {
    // Stable identity, derivable from a TScanRange at both build time and chunk-source
    // creation (so the real scan can mark this file Started without re-resolving paths).
    std::string key;
    std::string path; // normalized native path, for opening the file
    int64_t file_size = 0;
    int64_t modification_time = 0; // per-file; fills DataCacheOptions.modification_time on warm
    std::shared_ptr<const FooterOpenContext> open_ctx;
};

// Shared, factory-owned state for stall-time footer prefetch. Tracks an explicit
// contiguous frontier of root files the real scan has started, how far the prefetcher has
// submitted ahead, the in-flight task count, and a cancel flag. Every method is per-file
// (root start, task submit/finish, incremental append) -- never per-row -- so a single
// mutex is cheap. See handbook/plans/local/scan-footer-prefetch.md.
class FooterPrefetchState {
public:
    // lead_distance = scan_dop * connector_io_tasks_per_scan_operator * 2 (files ahead).
    // max_in_flight = connector_io_tasks_per_scan_operator (concurrent warm tasks per node).
    // metacache_on / datacache_populate_on: which caches can hold a warmed footer; if neither
    // is set the prefetcher never warms.
    FooterPrefetchState(std::vector<FooterPrefetchItem> files, int lead_distance, int max_in_flight, bool metacache_on,
                        bool datacache_populate_on)
            : _lead_distance(lead_distance < 0 ? 0 : static_cast<size_t>(lead_distance)),
              _max_in_flight(max_in_flight < 1 ? 1 : max_in_flight),
              _metacache_on(metacache_on),
              _datacache_populate_on(datacache_populate_on) {
        _set_files(std::move(files));
    }

    bool warmable() const { return _metacache_on || _datacache_populate_on; }
    bool metacache_on() const { return _metacache_on; }
    bool datacache_populate_on() const { return _datacache_populate_on; }

    // The real scan started a root file (split_context == nullptr): mark it Started and
    // advance the contiguous Started prefix. No-op if the key is unknown. O(1) amortized.
    void mark_started(const std::string& key) {
        std::lock_guard<std::mutex> l(_mu);
        auto it = _index_of.find(key);
        if (it == _index_of.end()) {
            return;
        }
        _started[it->second] = 1;
        while (_frontier < _started.size() && _started[_frontier] != 0) {
            ++_frontier;
        }
    }

    // Incremental scan ranges: append new root files at the tail (monotonic indices). Safe
    // for the contiguous-prefix frontier -- the frontier extends into them as the real scan
    // reaches them. Deduped by key: a file already in the sidecar (e.g. another offset split
    // of the same path, or re-delivered) is dropped, so mark_started always resolves and the
    // frontier never stalls on an un-markable duplicate index.
    void append(std::vector<FooterPrefetchItem> more) {
        std::lock_guard<std::mutex> l(_mu);
        _files.reserve(_files.size() + more.size());
        for (auto& item : more) {
            if (_index_of.emplace(item.key, _files.size()).second) {
                _files.emplace_back(std::move(item));
                _started.emplace_back(0);
            }
        }
    }

    // Pick the next file to warm within (frontier, frontier + lead_distance], if under the
    // in-flight cap and not cancelled. On success copies it into *out, increments in_flight,
    // and returns true. The caller loops until this returns false. The submit cursor never
    // falls behind the real-scan frontier, and already-Started files are skipped, so a file
    // the scan overtook during an unblock is never re-warmed.
    bool try_take_next(FooterPrefetchItem* out) {
        std::lock_guard<std::mutex> l(_mu);
        if (_cancelled.load(std::memory_order_relaxed) || _in_flight >= _max_in_flight) {
            return false;
        }
        if (_submit_cursor < _frontier) {
            _submit_cursor = _frontier;
        }
        const size_t limit = std::min(_files.size(), _frontier + _lead_distance);
        while (_submit_cursor < limit && _started[_submit_cursor] != 0) {
            ++_submit_cursor; // skip files the real scan already started
        }
        if (_submit_cursor >= limit) {
            return false;
        }
        *out = _files[_submit_cursor];
        ++_submit_cursor;
        ++_in_flight;
        return true;
    }

    void on_task_done() {
        std::lock_guard<std::mutex> l(_mu);
        if (_in_flight > 0) {
            --_in_flight;
        }
    }

    // Called from the connector scan operator factory's close: queued tasks cannot be
    // removed from the scan executor, so they drain by observing this flag and returning.
    void cancel() { _cancelled.store(true, std::memory_order_relaxed); }
    bool cancelled() const { return _cancelled.load(std::memory_order_relaxed); }

    // Stall gate: the prefetch loop only runs while the scan is back-pressured (row buffer full).
    // The operator sets this from is_buffer_full() at its scheduling points; the self-sustaining
    // loop stops re-submitting once it clears, so prefetch does not contend with row io-tasks
    // after the scan resumes.
    void set_stalled(bool v) { _stalled.store(v, std::memory_order_relaxed); }
    bool stalled() const { return _stalled.load(std::memory_order_relaxed); }

    // Per-query (node-wide) counts of footers this prefetcher actually wrote into each cache;
    // surfaced on the scan profile at close.
    void record_warm(bool wrote_pagecache, bool wrote_blockcache) {
        if (wrote_pagecache) _warmed_pagecache.fetch_add(1, std::memory_order_relaxed);
        if (wrote_blockcache) _warmed_blockcache.fetch_add(1, std::memory_order_relaxed);
    }
    int64_t warmed_pagecache() const { return _warmed_pagecache.load(std::memory_order_relaxed); }
    int64_t warmed_blockcache() const { return _warmed_blockcache.load(std::memory_order_relaxed); }

private:
    void _set_files(std::vector<FooterPrefetchItem> files) {
        _index_of.reserve(files.size());
        _files.reserve(files.size());
        for (auto& item : files) {
            // Dedup by key: collapse offset-splits of the same path / re-delivered ranges, so
            // every index in _files has a distinct key that mark_started can resolve.
            if (_index_of.emplace(item.key, _files.size()).second) {
                _files.emplace_back(std::move(item));
                _started.emplace_back(0);
            }
        }
    }

    mutable std::mutex _mu;
    std::vector<FooterPrefetchItem> _files;            // append-only
    std::vector<uint8_t> _started;                     // per-file STARTED flag, parallel to _files
    std::unordered_map<std::string, size_t> _index_of; // key -> file index
    size_t _frontier = 0;                              // contiguous Started prefix
    size_t _submit_cursor = 0;                         // how far we have submitted
    int _in_flight = 0;
    const size_t _lead_distance;
    const int _max_in_flight;
    const bool _metacache_on;
    const bool _datacache_populate_on;
    std::atomic<bool> _cancelled{false};
    std::atomic<bool> _stalled{false};
    std::atomic<int64_t> _warmed_pagecache{0};
    std::atomic<int64_t> _warmed_blockcache{0};
};

} // namespace starrocks::pipeline
