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

#include <atomic>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <string_view>
#include <vector>

#include "common/status.h"
#include "io/seekable_input_stream.h"

namespace starrocks {

class ThreadPool;

class SharedBufferedInputStream : public io::SeekableInputStream,
                                  public std::enable_shared_from_this<SharedBufferedInputStream> {
public:
    struct IORange {
        IORange(const int64_t offset, const int64_t size, const bool is_active = true)
                : offset(offset), size(size), is_active(is_active) {}
        int64_t offset;
        int64_t size;
        bool is_active = true;
        bool operator<(const IORange& x) const { return offset < x.offset; }
    };
    struct CoalesceOptions {
        static constexpr int64_t MB = 1024 * 1024;
        int64_t max_dist_size = 1 * MB;
        int64_t max_buffer_size = 8 * MB;
    };

    struct ChunkInfo {
        // Per-chunk lifecycle. Transitions are made under SharedBuffer::mu.
        enum class State : uint8_t {
            NotQueued, // initial; not yet handed to the pool
            Queued,    // pool->submit_func accepted; worker hasn't started
            Running,   // worker has begun read_at_fully
            Done,      // worker succeeded
            Failed,    // worker failed; sb->status holds the first error
        };
        int64_t file_offset;   // offset in file
        int64_t buffer_offset; // offset in buffer
        int64_t size;          // chunk size
        State state = State::NotQueued;
    };

    struct SharedBuffer {
        // Coalesced range (set at construction; immutable once state passes Cold).
        int64_t raw_offset;
        int64_t raw_size;
        int64_t offset;
        int64_t size;
        int64_t ref_count;
        std::vector<uint8_t> buffer;

        // SharedBuffer lifecycle. All transitions are under `mu`; every transition into a
        // terminal state (Filled/Failed/Abandoned) notifies `cv`.
        enum class State : uint8_t {
            Cold,         // newly inserted; no buffer, no chunks
            Initializing, // exactly one thread is allocating/chunking; others wait on cv
            Ready,        // buffer allocated, chunks built, none submitted yet
            Filling,      // at least one chunk has been claimed (Queued or Running) or finished
            SyncOnly,     // budget exhausted / no pool / not shared-owned; consumer fills inline
            SyncFilling,  // one consumer is doing the sync fill; others wait on cv
            Filled,       // terminal: all chunks succeeded
            Failed,       // terminal: first IO error; `status` holds it
            Abandoned,    // terminal: release()/destructor observed; workers finish quietly
        };

        mutable std::mutex mu;
        mutable std::condition_variable cv;
        State state = State::Cold;
        Status status;              // first-error-wins; meaningful only when state == Failed
        int64_t reserved_bytes = 0; // RAII-refunded by ~SharedBuffer
        int32_t queued_chunks = 0;
        int32_t running_chunks = 0;
        int32_t finished_chunks = 0;
        // Buffer holds only lazy-column ranges (predicates may skip these). Set at coalesce
        // time; _schedule() does NOT prefetch lazy-only buffers so we don't waste S3 IO on
        // columns that filtering may discard. Consumers still pull them via _ensure_sb_filled
        // (sync fallback) when actually needed.
        bool is_lazy_only = false;
        std::vector<ChunkInfo> chunks;

        SharedBuffer() = default;
        SharedBuffer(int64_t raw_off, int64_t raw_sz, int64_t ref)
                : raw_offset(raw_off), raw_size(raw_sz), offset(raw_off), size(raw_sz), ref_count(ref) {}
        ~SharedBuffer();

        void align(int64_t align_size, int64_t file_size);
        std::string debug_string() const;
    };
    using SharedBufferPtr = std::shared_ptr<SharedBuffer>;

    SharedBufferedInputStream(std::shared_ptr<io::SeekableInputStream> stream, std::string filename, size_t file_size);
    ~SharedBufferedInputStream() override;

    Status seek(int64_t position) override {
        _offset = position;
        return _stream->seek(position);
    }
    StatusOr<int64_t> position() override { return _offset; }
    StatusOr<int64_t> read(void* data, int64_t count) override;
    Status read_at_fully(int64_t offset, void* out, int64_t count) override;
    StatusOr<int64_t> get_size() override;
    Status skip(int64_t count) override {
        _offset += count;
        return _stream->skip(count);
    }

    StatusOr<SharedBufferPtr> find_shared_buffer(size_t offset, size_t count);
    // Get bytes from shared buffer or remote storage. When shared_buffer is not NULL the
    // function uses it directly instead of finding it again.
    Status get_bytes(const uint8_t** buffer, size_t offset, size_t count, SharedBufferPtr shared_buffer);

    StatusOr<std::unique_ptr<io::NumericStatistics>> get_numeric_statistics() override {
        return _stream->get_numeric_statistics();
    }

    io::IoStatsSnapshot get_io_stats_snapshot() const override;

    Status set_io_ranges(const std::vector<IORange>& ranges, bool coalesce_lazy_column = true);
    void release_to_offset(int64_t offset);
    void release();
    void set_coalesce_options(const CoalesceOptions& options) { _options = options; }
    void set_align_size(int64_t size) { _align_size = size; }

    int64_t shared_io_count() const { return _shared_io_count; }
    int64_t shared_io_bytes() const { return _shared_io_bytes; }
    int64_t shared_align_io_bytes() const { return _shared_align_io_bytes; }
    int64_t shared_io_timer() const { return _shared_io_timer; }
    int64_t direct_io_count() const { return _direct_io_count; }
    int64_t direct_io_bytes() const { return _direct_io_bytes; }
    int64_t direct_io_timer() const { return _direct_io_timer; }
    int64_t estimated_mem_usage() const { return _estimated_mem_usage; }
    int64_t current_range_ref_sum() const;

    StatusOr<std::string_view> peek(int64_t count) override;
    const std::string& filename() const override { return _filename; }
    bool is_cache_hit() const override { return false; }
    StatusOr<std::string_view> peek_shared_buffer(int64_t count, SharedBufferPtr* shared_buffer);

    // Inject the process memory limit used to resolve the percentage-based
    // parallel_io_buffer_limit. Called once at startup from the service layer, which owns
    // GlobalEnv access; the cache layer must not depend on the GlobalEnv/ExecEnv singletons.
    static void set_process_mem_limit(int64_t bytes);

    // For testing
    static int64_t TEST_get_parallel_read_buffer_used();
    static int64_t TEST_get_parallel_read_buffer_limit();

private:
    // RAII for _active_read_count. Constructed once per submitted task; destructor unconditionally
    // decrements + notifies _parallel_read_cv + kicks _schedule(). Used inside the worker lambda
    // so an early-return path (Abandoned, exception) cannot leak the slot.
    class ActiveReadGuard {
    public:
        explicit ActiveReadGuard(SharedBufferedInputStream* sbi) : _sbi(sbi) {}
        ~ActiveReadGuard();

    private:
        SharedBufferedInputStream* _sbi;
    };

    void _update_estimated_mem_usage_locked(); // requires _map_mu held by caller
    Status _sort_and_check_overlap(std::vector<IORange>& ranges);
    // requires _map_mu held. `is_lazy` propagates onto newly-created SharedBuffers so the
    // scheduler can skip prefetching lazy-only buffers.
    void _merge_small_ranges_locked(const std::vector<IORange>& ranges, bool is_lazy);
    Status _set_io_ranges_all_columns_locked(const std::vector<IORange>& ranges);
    Status _set_io_ranges_active_and_lazy_columns_locked(const std::vector<IORange>& ranges);

    // Parallel-read state machine
    void _schedule();
    void _ensure_sb_initialized(const SharedBufferPtr& sb);
    Status _ensure_sb_filled(const SharedBufferPtr& sb);
    void _submit_chunk_outside_locks(const SharedBufferPtr& sb, size_t chunk_idx,
                                     std::shared_ptr<SharedBufferedInputStream> self);
    std::vector<ChunkInfo> _build_chunks_unlocked(const SharedBufferPtr& sb);
    bool _try_reserve_global_budget(int64_t bytes);
    void _refund_global_budget(int64_t bytes);
    int32_t _compute_window(starrocks::ThreadPool* pool);

    const std::shared_ptr<io::SeekableInputStream> _stream;
    const std::string _filename;
    std::map<int64_t, SharedBufferPtr> _map;
    CoalesceOptions _options;
    int64_t _offset = 0;
    int64_t _file_size = 0;
    int64_t _shared_io_count = 0;
    int64_t _shared_io_bytes = 0;
    int64_t _shared_align_io_bytes = 0;
    int64_t _shared_io_timer = 0;
    int64_t _direct_io_count = 0;
    int64_t _direct_io_bytes = 0;
    int64_t _direct_io_timer = 0;
    int64_t _align_size = 0;
    int64_t _estimated_mem_usage = 0;

    // Protects _map and _shutting_down only.
    mutable std::mutex _map_mu;
    // Signaled when _active_read_count decrements; destructor waits on this.
    std::condition_variable _parallel_read_cv;
    std::atomic<int32_t> _active_read_count{0};
    bool _shutting_down = false;
};

} // namespace starrocks
