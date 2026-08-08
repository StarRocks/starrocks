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
#include <unordered_set>
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

// Shared, factory-owned state for footer prefetch. Tracks an explicit contiguous frontier of
// root files the real scan has started, how far the prefetcher has submitted ahead, and a
// cancel flag. Concurrency is bounded by each operator's free io-task slots, not here. Every
// method is per-file (root start, submit, incremental append) -- never per-row -- so a single
// mutex is cheap.
class FooterPrefetchState {
public:
    // lead_distance (files ahead) bounds how far the warm cursor may run past the real-scan frontier
    // -- cache footprint / wasted reads if the scan terminates early. The scan node derives it as
    // scan_dop * connector_footer_prefetch_max_inflight * connector_footer_prefetch_lead_multiplier.
    // Concurrency is NOT bounded here -- each operator caps its own warm tasks against its free
    // io-task slots (data + warm <= connector_io_tasks_per_scan_operator).
    // metacache_on / datacache_populate_on: which caches can hold a warmed footer; if neither
    // is set the prefetcher never warms.
    FooterPrefetchState(std::vector<FooterPrefetchItem> files, int lead_distance, bool metacache_on,
                        bool datacache_populate_on)
            : _lead_distance(lead_distance < 0 ? 0 : static_cast<size_t>(lead_distance)),
              _metacache_on(metacache_on),
              _datacache_populate_on(datacache_populate_on) {
        _set_files(std::move(files));
    }

    bool warmable() const { return _metacache_on || _datacache_populate_on; }
    bool metacache_on() const { return _metacache_on; }
    bool datacache_populate_on() const { return _datacache_populate_on; }

    // The real scan started a root file (split_context == nullptr): mark it Started and advance the
    // contiguous Started prefix. If the key is not in the sidecar yet (its range has not reached
    // append()), remember it so append() applies the start. O(1) amortized.
    void mark_started(const std::string& key) {
        std::lock_guard<std::mutex> l(_mu);
        auto it = _index_of.find(key);
        if (it == _index_of.end()) {
            // Incremental delivery can publish a morsel (and start its file) before that range
            // reaches append(). Remember the start so append() applies it, instead of dropping it and
            // freezing the frontier behind a file the scan already started.
            _pending_started.insert(key);
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
                // Apply a start the scan recorded before this range arrived (see mark_started).
                uint8_t started = 0;
                auto pit = _pending_started.find(item.key);
                if (pit != _pending_started.end()) {
                    started = 1;
                    _pending_started.erase(pit);
                }
                _files.emplace_back(std::move(item));
                _started.emplace_back(started);
            }
        }
        // A pending start may have landed at the frontier; extend the contiguous Started prefix.
        while (_frontier < _started.size() && _started[_frontier] != 0) {
            ++_frontier;
        }
    }

    // Pick the next file to warm within (frontier, frontier + lead_distance], if not cancelled. On
    // success copies it into *out, advances the submit cursor, and returns true. The caller (one per
    // scan operator) loops until this returns false or it runs out of free io-task slots. The submit
    // cursor never falls behind the real-scan frontier, and already-Started files are skipped, so a
    // file the scan overtook is never re-warmed. Shared across operators; the mutex + monotonic
    // cursor hand each concurrent caller a distinct file.
    bool try_take_next(FooterPrefetchItem* out) {
        std::lock_guard<std::mutex> l(_mu);
        if (_cancelled.load(std::memory_order_relaxed)) {
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
        return true;
    }

    // Give a taken file back when its warm task could not be submitted (executor backpressure), so
    // the prefetcher re-offers it. Best-effort: rolls back only if no other caller took past it (the
    // cursor still sits just after this key). If it cannot roll back, only the footer PREFETCH for
    // that file is skipped -- the real scan still opens the file and reads its footer normally, so no
    // query data is lost (a missed prefetch just means one cold footer read).
    void untake(const std::string& key) {
        std::lock_guard<std::mutex> l(_mu);
        if (_submit_cursor > _frontier && _submit_cursor <= _files.size() && _files[_submit_cursor - 1].key == key) {
            --_submit_cursor;
        }
    }

    // Called from the connector scan operator factory's close: queued tasks cannot be
    // removed from the scan executor, so they drain by observing this flag and returning.
    void cancel() { _cancelled.store(true, std::memory_order_relaxed); }
    bool cancelled() const { return _cancelled.load(std::memory_order_relaxed); }

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
    std::unordered_set<std::string> _pending_started;  // started before their range reached append()
    size_t _frontier = 0;                              // contiguous Started prefix
    size_t _submit_cursor = 0;                         // how far we have submitted
    const size_t _lead_distance;
    const bool _metacache_on;
    const bool _datacache_populate_on;
    std::atomic<bool> _cancelled{false};
    std::atomic<int64_t> _warmed_pagecache{0};
    std::atomic<int64_t> _warmed_blockcache{0};
};

} // namespace starrocks::pipeline
