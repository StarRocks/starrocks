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

#include "cache/scan/shared_buffered_input_stream.h"

#include <gutil/strings/substitute.h>

#include <algorithm>
#include <mutex>

#include "base/string/parse_util.h"
#include "common/config_scan_io_fwd.h"
#include "common/runtime_profile.h"
#include "common/thread/threadpool.h"
#include "gutil/strings/fastmem.h"
#include "runtime/current_thread.h"
#include "runtime/mem_tracker.h"

namespace starrocks {

// Global thread pool for parallel reads (lazy singleton).
static std::once_flag s_parallel_read_pool_init_flag;
static std::unique_ptr<ThreadPool> s_parallel_read_thread_pool;

static ThreadPool* get_parallel_read_thread_pool() {
    std::call_once(s_parallel_read_pool_init_flag, []() {
        int max_threads = config::parallel_io_thread_pool_size;
        if (max_threads <= 0) {
            return;
        }
        std::unique_ptr<ThreadPool> pool;
        Status status = ThreadPoolBuilder("parallel_io_pool")
                                .set_min_threads(0)
                                .set_max_threads(max_threads)
                                .set_max_queue_size(max_threads * 4)
                                .set_idle_timeout(MonoDelta::FromMilliseconds(5000))
                                .build(&pool);
        if (status.ok()) {
            s_parallel_read_thread_pool = std::move(pool);
            LOG(INFO) << "Parallel IO thread pool initialized with max_threads=" << max_threads;
        } else {
            LOG(WARNING) << "Failed to create parallel IO thread pool: " << status.message();
        }
    });
    return s_parallel_read_thread_pool.get();
}

// Global counter for parallel-read buffer memory usage. Reservation is acquired in
// _ensure_sb_initialized via CAS; refund is performed in ~SharedBuffer so the budget is
// tied to actual buffer lifetime (workers and async cache writes extend that lifetime
// via the captured SharedBufferPtr).
static std::atomic<int64_t> s_parallel_read_buffer_used{0};

// Process memory limit injected at startup by the service layer (see set_process_mem_limit).
// The cache layer must not reach into the GlobalEnv/ExecEnv singletons, so the percentage-based
// parallel_io_buffer_limit is resolved against this value. Zero means "not injected" (e.g. unit
// tests), in which case we fall back to a fixed budget.
static std::atomic<int64_t> s_process_mem_limit{0};

static int64_t get_parallel_read_buffer_limit() {
    constexpr int64_t kOneGB = 1024L * 1024 * 1024;
    int64_t be_mem = s_process_mem_limit.load(std::memory_order_relaxed);
    if (be_mem <= 0) {
        return kOneGB;
    }
    auto result = ParseUtil::parse_mem_spec(config::parallel_io_buffer_limit, be_mem);
    if (!result.ok() || result.value() <= 0) {
        return kOneGB;
    }
    return result.value();
}

void SharedBufferedInputStream::set_process_mem_limit(int64_t bytes) {
    s_process_mem_limit.store(bytes, std::memory_order_relaxed);
}

int64_t SharedBufferedInputStream::TEST_get_parallel_read_buffer_used() {
    return s_parallel_read_buffer_used.load(std::memory_order_relaxed);
}

int64_t SharedBufferedInputStream::TEST_get_parallel_read_buffer_limit() {
    return get_parallel_read_buffer_limit();
}

// --- SharedBuffer -----------------------------------------------------------

SharedBufferedInputStream::SharedBuffer::~SharedBuffer() {
    if (reserved_bytes > 0) {
        s_parallel_read_buffer_used.fetch_sub(reserved_bytes, std::memory_order_relaxed);
    }
}

void SharedBufferedInputStream::SharedBuffer::align(int64_t align_size, int64_t file_size) {
    if (align_size != 0) {
        offset = raw_offset / align_size * align_size;
        int64_t end = std::min((raw_offset + raw_size + align_size - 1) / align_size * align_size, file_size);
        size = end - offset;
    } else {
        offset = raw_offset;
        size = raw_size;
    }
}

std::string SharedBufferedInputStream::SharedBuffer::debug_string() const {
    return strings::Substitute("SharedBuffer raw_offset=$0, raw_size=$1, offset=$2, size=$3, ref_count=$4, state=$5",
                               raw_offset, raw_size, offset, size, ref_count, static_cast<int>(state));
}

// --- ActiveReadGuard --------------------------------------------------------

SharedBufferedInputStream::ActiveReadGuard::~ActiveReadGuard() {
    _sbi->_active_read_count.fetch_sub(1, std::memory_order_release);
    {
        std::lock_guard<std::mutex> lock(_sbi->_map_mu);
        _sbi->_parallel_read_cv.notify_all();
    }
    // Try to schedule the next chunk. _schedule() short-circuits if _shutting_down.
    _sbi->_schedule();
}

// --- Construction / Destruction --------------------------------------------

SharedBufferedInputStream::SharedBufferedInputStream(std::shared_ptr<io::SeekableInputStream> stream,
                                                     std::string filename, size_t file_size)
        : _stream(std::move(stream)), _filename(std::move(filename)), _file_size(file_size) {}

SharedBufferedInputStream::~SharedBufferedInputStream() {
    std::vector<SharedBufferPtr> drained;
    {
        std::lock_guard<std::mutex> lock(_map_mu);
        _shutting_down = true;
        drained.reserve(_map.size());
        for (auto& [_, sb] : _map) drained.push_back(sb);
        _map.clear();
    }
    for (auto& sb : drained) {
        std::lock_guard<std::mutex> sl(sb->mu);
        if (sb->state != SharedBuffer::State::Filled && sb->state != SharedBuffer::State::Failed) {
            sb->state = SharedBuffer::State::Abandoned;
            sb->cv.notify_all();
        }
    }
    // Workers capture shared_ptr<SBI> via `self`, so they should already keep us alive until
    // they finish. This wait is the formal backstop in case captures change in the future.
    std::unique_lock<std::mutex> lock(_map_mu);
    _parallel_read_cv.wait(lock, [this]() { return _active_read_count.load(std::memory_order_acquire) == 0; });
}

// --- Coalesce / map mutation (under _map_mu) -------------------------------

Status SharedBufferedInputStream::_sort_and_check_overlap(std::vector<IORange>& ranges) {
    // Specify compare function: zero-sized ranges like [351,351] vs [351,356] must order by size.
    std::sort(ranges.begin(), ranges.end(), [](const IORange& a, const IORange& b) {
        if (a.offset != b.offset) {
            return a.offset < b.offset;
        }
        return a.size < b.size;
    });

    for (size_t i = 1; i < ranges.size(); i++) {
        if (ranges[i].offset < (ranges[i - 1].offset + ranges[i - 1].size)) {
            LOG(WARNING) << "io ranges are overlapped" << ranges[i].offset << " "
                         << ranges[i - 1].offset + ranges[i - 1].size;
            return Status::RuntimeError("io ranges are overlapped");
        }
    }

    return Status::OK();
}

void SharedBufferedInputStream::_merge_small_ranges_locked(const std::vector<IORange>& small_ranges, bool is_lazy) {
    if (small_ranges.empty()) return;
    auto update_map = [&](size_t from, size_t to) {
        int64_t ref_count = (to - from + 1);
        int64_t end = (small_ranges[to].offset + small_ranges[to].size);
        SharedBufferPtr sb =
                std::make_shared<SharedBuffer>(small_ranges[from].offset, end - small_ranges[from].offset, ref_count);
        sb->align(_align_size, _file_size);
        sb->is_lazy_only = is_lazy;
        _map.insert(std::make_pair(sb->raw_offset + sb->raw_size, sb));
    };

    size_t unmerge = 0;
    for (size_t i = 1; i < small_ranges.size(); i++) {
        const auto& prev = small_ranges[i - 1];
        const auto& now = small_ranges[i];
        size_t now_end = now.offset + now.size;
        size_t prev_end = prev.offset + prev.size;
        if (((now_end - small_ranges[unmerge].offset) <= (size_t)_options.max_buffer_size) &&
            (now.offset - prev_end) <= (size_t)_options.max_dist_size) {
            continue;
        } else {
            update_map(unmerge, i - 1);
            unmerge = i;
        }
    }
    update_map(unmerge, small_ranges.size() - 1);
}

Status SharedBufferedInputStream::_set_io_ranges_all_columns_locked(const std::vector<IORange>& ranges) {
    if (ranges.empty()) return Status::OK();

    std::vector<IORange> check(ranges);
    RETURN_IF_ERROR(_sort_and_check_overlap(check));

    std::vector<IORange> small_ranges;
    for (const IORange& r : check) {
        if (r.size > _options.max_buffer_size) {
            SharedBufferPtr sb = std::make_shared<SharedBuffer>(r.offset, r.size, 1);
            sb->align(_align_size, _file_size);
            _map.insert(std::make_pair(sb->raw_offset + sb->raw_size, sb));
        } else {
            small_ranges.emplace_back(r);
        }
    }

    // _set_io_ranges_all_columns_locked is called when the caller doesn't distinguish
    // active/lazy. Treat everything as eligible for prefetch (is_lazy=false).
    _merge_small_ranges_locked(small_ranges, /*is_lazy=*/false);
    _update_estimated_mem_usage_locked();
    return Status::OK();
}

Status SharedBufferedInputStream::_set_io_ranges_active_and_lazy_columns_locked(const std::vector<IORange>& ranges) {
    if (ranges.empty()) return Status::OK();

    std::vector<IORange> check(ranges);
    RETURN_IF_ERROR(_sort_and_check_overlap(check));

    std::vector<IORange> small_active_ranges;
    std::vector<bool> small_lazy_flag(ranges.size(), false);
    for (size_t index = 0; index < check.size(); ++index) {
        const IORange& r = check[index];
        if (r.size > _options.max_buffer_size) {
            SharedBufferPtr sb = std::make_shared<SharedBuffer>(r.offset, r.size, 1);
            sb->align(_align_size, _file_size);
            sb->is_lazy_only = !r.is_active;
            _map.insert(std::make_pair(sb->raw_offset + sb->raw_size, sb));
        } else {
            if (r.is_active) {
                small_active_ranges.emplace_back(r);
            } else {
                small_lazy_flag[index] = true;
            }
        }
    }

    if (!small_active_ranges.empty()) {
        _merge_small_ranges_locked(small_active_ranges, /*is_lazy=*/false);
    }

    std::vector<IORange> small_lazy_batch_ranges;
    for (size_t index = 0; index < small_lazy_flag.size(); ++index) {
        if (!small_lazy_flag[index]) continue;
        const IORange& r = check[index];
        auto iter = _map.upper_bound(r.offset);
        if (iter != _map.end()) {
            SharedBufferPtr& sb = iter->second;
            if (sb->offset <= r.offset && sb->offset + sb->size >= r.offset + r.size) {
                sb->ref_count++;
                continue;
            }
        }
        small_lazy_batch_ranges.emplace_back(r);
        if (index + 1 >= small_lazy_flag.size() || !small_lazy_flag[index + 1]) {
            _merge_small_ranges_locked(small_lazy_batch_ranges, /*is_lazy=*/true);
            small_lazy_batch_ranges.clear();
        }
    }

    _update_estimated_mem_usage_locked();
    return Status::OK();
}

void SharedBufferedInputStream::_update_estimated_mem_usage_locked() {
    int64_t mem_usage = 0;
    for (const auto& [_, sb] : _map) {
        mem_usage += sb->size;
    }
    // Data is typically compressed; add 50% overhead for decompression.
    mem_usage += mem_usage / 2;
    _estimated_mem_usage = std::max(mem_usage, _estimated_mem_usage);
}

// --- Public API: set_io_ranges, find_shared_buffer, release ----------------

Status SharedBufferedInputStream::set_io_ranges(const std::vector<IORange>& ranges, bool coalesce_lazy_column) {
    {
        std::lock_guard<std::mutex> lock(_map_mu);
        if (_shutting_down) return Status::Aborted("SBI shutting down");
        if (coalesce_lazy_column || !config::io_coalesce_adaptive_lazy_active) {
            RETURN_IF_ERROR(_set_io_ranges_all_columns_locked(ranges));
        } else {
            RETURN_IF_ERROR(_set_io_ranges_active_and_lazy_columns_locked(ranges));
        }
    }
    _schedule();
    return Status::OK();
}

StatusOr<SharedBufferedInputStream::SharedBufferPtr> SharedBufferedInputStream::find_shared_buffer(size_t offset,
                                                                                                   size_t count) {
    std::lock_guard<std::mutex> lock(_map_mu);
    auto iter = _map.upper_bound(offset);
    if (iter == _map.end()) {
        return Status::RuntimeError("failed to find shared buffer based on offset");
    }
    const SharedBufferPtr& sb = iter->second;
    if (((size_t)sb->offset > offset) || ((size_t)sb->offset + (size_t)sb->size) < (offset + count)) {
        return Status::RuntimeError("bad construction of shared buffer");
    }
    return sb;
}

int64_t SharedBufferedInputStream::current_range_ref_sum() const {
    std::lock_guard<std::mutex> lock(_map_mu);
    int64_t ref = 0;
    for (const auto& [_, sb] : _map) {
        ref += sb->ref_count;
    }
    return ref;
}

void SharedBufferedInputStream::release() {
    std::vector<SharedBufferPtr> drained;
    {
        std::lock_guard<std::mutex> lock(_map_mu);
        drained.reserve(_map.size());
        for (auto& [_, sb] : _map) drained.push_back(sb);
        _map.clear();
    }
    for (auto& sb : drained) {
        std::lock_guard<std::mutex> sl(sb->mu);
        if (sb->state != SharedBuffer::State::Filled && sb->state != SharedBuffer::State::Failed) {
            sb->state = SharedBuffer::State::Abandoned;
            sb->cv.notify_all();
        }
    }
    // shared_ptr refs drop; ~SharedBuffer refunds reserved_bytes when last reference goes.
}

void SharedBufferedInputStream::release_to_offset(int64_t offset) {
    std::vector<SharedBufferPtr> drained;
    {
        std::lock_guard<std::mutex> lock(_map_mu);
        auto it = _map.upper_bound(offset);
        for (auto cur = _map.begin(); cur != it; ++cur) drained.push_back(cur->second);
        _map.erase(_map.begin(), it);
    }
    for (auto& sb : drained) {
        std::lock_guard<std::mutex> sl(sb->mu);
        if (sb->state != SharedBuffer::State::Filled && sb->state != SharedBuffer::State::Failed) {
            sb->state = SharedBuffer::State::Abandoned;
            sb->cv.notify_all();
        }
    }
}

// --- Parallel-read state machine -------------------------------------------

bool SharedBufferedInputStream::_try_reserve_global_budget(int64_t bytes) {
    int64_t limit = get_parallel_read_buffer_limit();
    int64_t current = s_parallel_read_buffer_used.load(std::memory_order_relaxed);
    while (true) {
        if (current + bytes > limit) return false;
        if (s_parallel_read_buffer_used.compare_exchange_weak(current, current + bytes, std::memory_order_relaxed,
                                                              std::memory_order_relaxed)) {
            return true;
        }
    }
}

void SharedBufferedInputStream::_refund_global_budget(int64_t bytes) {
    s_parallel_read_buffer_used.fetch_sub(bytes, std::memory_order_relaxed);
}

std::vector<SharedBufferedInputStream::ChunkInfo> SharedBufferedInputStream::_build_chunks_unlocked(
        const SharedBufferPtr& sb) {
    std::vector<ChunkInfo> chunks;
    int64_t chunk_size = _options.max_buffer_size;
    // Single chunk if total size <= chunk_size * 1.5 (don't split tiny tails).
    if (sb->size <= chunk_size + chunk_size / 2) {
        chunks.push_back(ChunkInfo{.file_offset = sb->offset, .buffer_offset = 0, .size = sb->size});
        return chunks;
    }
    int64_t off = 0;
    while (off < sb->size) {
        int64_t remaining = sb->size - off;
        int64_t this_size;
        if (remaining <= chunk_size) {
            this_size = remaining;
        } else if (remaining < chunk_size + chunk_size / 2) {
            this_size = remaining;
        } else {
            this_size = chunk_size;
        }
        chunks.push_back(ChunkInfo{.file_offset = sb->offset + off, .buffer_offset = off, .size = this_size});
        off += this_size;
    }
    return chunks;
}

void SharedBufferedInputStream::_ensure_sb_initialized(const SharedBufferPtr& sb) {
    {
        std::unique_lock<std::mutex> L(sb->mu);
        // Another thread may already be initializing — wait it out.
        sb->cv.wait(L, [&] { return sb->state != SharedBuffer::State::Initializing; });
        if (sb->state != SharedBuffer::State::Cold) return;
        sb->state = SharedBuffer::State::Initializing;
    }

    // Allocation/CAS outside sb->mu.
    int64_t want = sb->size;
    bool budget_ok = (want > 0) && _try_reserve_global_budget(want);
    std::vector<uint8_t> buf;
    std::vector<ChunkInfo> chunks;
    bool alloc_ok = false;
    if (budget_ok) {
        try {
            buf.resize(want);
            chunks = _build_chunks_unlocked(sb);
            alloc_ok = true;
        } catch (...) {
            _refund_global_budget(want);
            budget_ok = false;
        }
    }

    {
        std::lock_guard<std::mutex> L(sb->mu);
        // Abandoned wins races: refund any reservation we took and stay terminal.
        if (sb->state == SharedBuffer::State::Abandoned) {
            if (budget_ok) _refund_global_budget(want);
            sb->cv.notify_all();
            return;
        }
        if (alloc_ok) {
            sb->buffer = std::move(buf);
            sb->chunks = std::move(chunks);
            sb->reserved_bytes = want;
            sb->state = SharedBuffer::State::Ready;
        } else {
            sb->state = SharedBuffer::State::SyncOnly;
        }
        sb->cv.notify_all();
    }
}

int32_t SharedBufferedInputStream::_compute_window(ThreadPool* pool) {
    int32_t pool_size = config::parallel_io_thread_pool_size;
    int32_t base_per_file = config::parallel_io_workers_per_file;
    int32_t active = pool->active_threads();
    int32_t available = std::max(0, pool_size - active);
    return std::max(base_per_file, available / 4);
}

void SharedBufferedInputStream::_schedule() {
    if (!_stream->is_thread_safe_positional_read()) return;
    auto self = weak_from_this().lock();
    if (!self) return;
    auto* pool = get_parallel_read_thread_pool();
    if (pool == nullptr) return;

    // Snapshot _map under brief lock; do not hold _map_mu during init/submit.
    std::vector<SharedBufferPtr> snapshot;
    {
        std::lock_guard<std::mutex> lock(_map_mu);
        if (_shutting_down) return;
        snapshot.reserve(_map.size());
        for (auto& [_, sb] : _map) snapshot.push_back(sb);
    }

    for (auto& sb : snapshot) {
        if (sb->size <= 0) continue;
        // Lazy-only buffers may be skipped by predicate filters; don't prefetch them speculatively.
        // Demand consumers will drive the fill via _ensure_sb_filled (Cold → sync init → fill).
        if (sb->is_lazy_only) continue;
        _ensure_sb_initialized(sb);
    }

    int32_t window = _compute_window(pool);
    for (auto& sb : snapshot) {
        if (sb->is_lazy_only) continue;
        std::vector<size_t> claimed;
        {
            std::lock_guard<std::mutex> L(sb->mu);
            if (sb->state != SharedBuffer::State::Ready && sb->state != SharedBuffer::State::Filling) continue;
            for (size_t i = 0; i < sb->chunks.size(); ++i) {
                if (_active_read_count.load(std::memory_order_relaxed) >= window) break;
                if (sb->chunks[i].state != ChunkInfo::State::NotQueued) continue;
                sb->chunks[i].state = ChunkInfo::State::Queued;
                sb->queued_chunks++;
                sb->state = SharedBuffer::State::Filling;
                _active_read_count.fetch_add(1, std::memory_order_relaxed);
                claimed.push_back(i);
            }
        }
        for (size_t idx : claimed) {
            _submit_chunk_outside_locks(sb, idx, self);
        }
    }
}

void SharedBufferedInputStream::_submit_chunk_outside_locks(const SharedBufferPtr& sb, size_t chunk_idx,
                                                            std::shared_ptr<SharedBufferedInputStream> self) {
    auto stream = _stream;
    auto task = [self, sb, chunk_idx, stream]() {
        // RAII: unconditional decrement + notify + reschedule on exit.
        ActiveReadGuard guard(self.get());

        // Transition Queued → Running (or short-circuit if Abandoned).
        {
            std::lock_guard<std::mutex> L(sb->mu);
            if (sb->state == SharedBuffer::State::Abandoned) {
                sb->chunks[chunk_idx].state = ChunkInfo::State::NotQueued;
                sb->queued_chunks--;
                sb->cv.notify_all();
                return;
            }
            sb->chunks[chunk_idx].state = ChunkInfo::State::Running;
            sb->queued_chunks--;
            sb->running_chunks++;
        }

        Status s = stream->read_at_fully(sb->chunks[chunk_idx].file_offset,
                                         sb->buffer.data() + sb->chunks[chunk_idx].buffer_offset,
                                         sb->chunks[chunk_idx].size);

        std::lock_guard<std::mutex> L(sb->mu);
        // First-error-wins status BEFORE the finish counter so the terminal transition sees it.
        if (!s.ok() && sb->status.ok()) sb->status = s;
        sb->running_chunks--;
        sb->finished_chunks++;
        sb->chunks[chunk_idx].state = s.ok() ? ChunkInfo::State::Done : ChunkInfo::State::Failed;
        if (sb->state == SharedBuffer::State::Abandoned) {
            sb->cv.notify_all();
            return;
        }
        if (sb->finished_chunks == (int32_t)sb->chunks.size()) {
            sb->state = sb->status.ok() ? SharedBuffer::State::Filled : SharedBuffer::State::Failed;
        }
        sb->cv.notify_all();
    };

    Status submit_status = get_parallel_read_thread_pool()->submit_func(std::move(task));
    if (!submit_status.ok()) {
        // Rollback the Queued claim. ActiveReadGuard never ran (task lambda didn't execute).
        std::lock_guard<std::mutex> L(sb->mu);
        sb->chunks[chunk_idx].state = ChunkInfo::State::NotQueued;
        sb->queued_chunks--;
        _active_read_count.fetch_sub(1, std::memory_order_relaxed);
        {
            std::lock_guard<std::mutex> M(_map_mu);
            _parallel_read_cv.notify_all();
        }
        // Only revert to Ready when no chunk has ever started. Otherwise stay Filling and let
        // _ensure_sb_filled() sync-fill the remaining NotQueued chunks on consumer demand.
        if (sb->state == SharedBuffer::State::Filling && sb->running_chunks == 0 && sb->finished_chunks == 0 &&
            sb->queued_chunks == 0) {
            sb->state = SharedBuffer::State::Ready;
        }
        sb->cv.notify_all();
    }
}

Status SharedBufferedInputStream::_ensure_sb_filled(const SharedBufferPtr& sb) {
    std::unique_lock<std::mutex> L(sb->mu);
    while (true) {
        switch (sb->state) {
        case SharedBuffer::State::Filled:
            return Status::OK();
        case SharedBuffer::State::Failed:
            return sb->status;
        case SharedBuffer::State::Abandoned:
            return Status::Aborted("SharedBuffer abandoned");
        case SharedBuffer::State::Cold: {
            // Consumer demand without prior _schedule() succeeding: init inline.
            L.unlock();
            _ensure_sb_initialized(sb);
            L.lock();
            continue;
        }
        case SharedBuffer::State::Initializing: {
            sb->cv.wait(L, [&] { return sb->state != SharedBuffer::State::Initializing; });
            continue;
        }
        case SharedBuffer::State::SyncOnly: {
            // Claim the sync fill; concurrent consumers wait on cv via SyncFilling.
            sb->state = SharedBuffer::State::SyncFilling;
            L.unlock();
            std::vector<uint8_t>* buf_ptr = &sb->buffer;
            int64_t off = sb->offset;
            int64_t sz = sb->size;
            if (buf_ptr->empty()) buf_ptr->resize(sz);
            Status s = _stream->read_at_fully(off, buf_ptr->data(), sz);
            L.lock();
            if (!s.ok() && sb->status.ok()) sb->status = s;
            // Abandoned wins.
            if (sb->state != SharedBuffer::State::Abandoned) {
                sb->state = s.ok() ? SharedBuffer::State::Filled : SharedBuffer::State::Failed;
            }
            sb->cv.notify_all();
            continue;
        }
        case SharedBuffer::State::SyncFilling: {
            sb->cv.wait(L, [&] { return sb->state != SharedBuffer::State::SyncFilling; });
            continue;
        }
        case SharedBuffer::State::Ready:
        case SharedBuffer::State::Filling: {
            // Demand path: sync-fill any NotQueued chunks on consumer thread, then wait on cv
            // for already Queued/Running chunks. Each chunk is claimed under the lock before IO.
            std::vector<size_t> to_sync_fill;
            for (size_t i = 0; i < sb->chunks.size(); ++i) {
                if (sb->chunks[i].state == ChunkInfo::State::NotQueued) {
                    sb->chunks[i].state = ChunkInfo::State::Running;
                    sb->running_chunks++;
                    sb->state = SharedBuffer::State::Filling;
                    to_sync_fill.push_back(i);
                }
            }
            if (!to_sync_fill.empty()) {
                L.unlock();
                for (size_t i : to_sync_fill) {
                    Status s =
                            _stream->read_at_fully(sb->chunks[i].file_offset,
                                                   sb->buffer.data() + sb->chunks[i].buffer_offset, sb->chunks[i].size);
                    std::lock_guard<std::mutex> CL(sb->mu);
                    if (!s.ok() && sb->status.ok()) sb->status = s;
                    sb->running_chunks--;
                    sb->finished_chunks++;
                    sb->chunks[i].state = s.ok() ? ChunkInfo::State::Done : ChunkInfo::State::Failed;
                }
                L.lock();
                if (sb->state != SharedBuffer::State::Abandoned && sb->finished_chunks == (int32_t)sb->chunks.size()) {
                    sb->state = sb->status.ok() ? SharedBuffer::State::Filled : SharedBuffer::State::Failed;
                }
                sb->cv.notify_all();
                continue;
            }
            // Nothing to sync-fill; wait for in-flight chunks.
            sb->cv.wait(L, [&] {
                return sb->state == SharedBuffer::State::Filled || sb->state == SharedBuffer::State::Failed ||
                       sb->state == SharedBuffer::State::Abandoned;
            });
            continue;
        }
        }
    }
}

// --- Public consume API ----------------------------------------------------

Status SharedBufferedInputStream::get_bytes(const uint8_t** buffer, size_t offset, size_t count,
                                            SharedBufferPtr shared_buffer) {
    if (!shared_buffer) {
        ASSIGN_OR_RETURN(shared_buffer, find_shared_buffer(offset, count));
    }
    SCOPED_RAW_TIMER(&_shared_io_timer);
    bool was_unfilled;
    {
        std::lock_guard<std::mutex> L(shared_buffer->mu);
        was_unfilled = shared_buffer->state != SharedBuffer::State::Filled &&
                       shared_buffer->state != SharedBuffer::State::Failed;
    }
    RETURN_IF_ERROR(_ensure_sb_filled(shared_buffer));
    if (was_unfilled) {
        _shared_io_count += 1;
        _shared_io_bytes += shared_buffer->size;
        if (shared_buffer->size > shared_buffer->raw_size) {
            _shared_align_io_bytes += shared_buffer->size - shared_buffer->raw_size;
        }
    }
    *buffer = shared_buffer->buffer.data() + offset - shared_buffer->offset;
    return Status::OK();
}

Status SharedBufferedInputStream::read_at_fully(int64_t offset, void* out, int64_t count) {
    auto st = find_shared_buffer(offset, count);
    if (!st.ok()) {
        SCOPED_RAW_TIMER(&_direct_io_timer);
        _direct_io_count += 1;
        _direct_io_bytes += count;
        RETURN_IF_ERROR(_stream->read_at_fully(offset, out, count));
        return Status::OK();
    }
    const uint8_t* buffer = nullptr;
    RETURN_IF_ERROR(get_bytes(&buffer, offset, count, st.value()));
    strings::memcpy_inlined(out, buffer, count);
    return Status::OK();
}

StatusOr<int64_t> SharedBufferedInputStream::get_size() {
    return _file_size;
}

StatusOr<int64_t> SharedBufferedInputStream::read(void* data, int64_t count) {
    auto n = _stream->read_at(_offset, data, count);
    RETURN_IF_ERROR(n);
    _offset += n.value();
    return n;
}

StatusOr<std::string_view> SharedBufferedInputStream::peek(int64_t count) {
    auto found = find_shared_buffer(_offset, count);
    if (!found.ok()) return Status::NotSupported("peek shared buffer empty");
    const uint8_t* buf = nullptr;
    RETURN_IF_ERROR(get_bytes(&buf, _offset, count, found.value()));
    return std::string_view((const char*)buf, count);
}

StatusOr<std::string_view> SharedBufferedInputStream::peek_shared_buffer(int64_t count,
                                                                         SharedBufferPtr* shared_buffer) {
    ASSIGN_OR_RETURN(auto sb, find_shared_buffer(_offset, count));
    RETURN_IF_ERROR(_ensure_sb_filled(sb));
    if (shared_buffer) *shared_buffer = sb;
    const uint8_t* buf = sb->buffer.data() + _offset - sb->offset;
    return std::string_view((const char*)buf, count);
}

// Out-of-line so gcov can attribute coverage to a concrete `.cpp` line.
io::IoStatsSnapshot SharedBufferedInputStream::get_io_stats_snapshot() const {
    return _stream->get_io_stats_snapshot();
}

} // namespace starrocks
