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

#include "storage/index/vector/vector_index_load_scheduler.h"

#include <algorithm>

#include "base/testutil/sync_point.h"
#include "base/utility/defer_op.h"
#include "common/config_vector_index_fwd.h"
#include "common/logging.h"
#include "common/thread/threadpool.h"
#include "runtime/runtime_env.h"

namespace starrocks {

namespace {

void merge_io_stats(io::IoStatsSnapshot* dst, const io::IoStatsSnapshot& src) {
    dst->bytes_read_local_disk += src.bytes_read_local_disk;
    dst->bytes_read_remote += src.bytes_read_remote;
    dst->io_count_local_disk += src.io_count_local_disk;
    dst->io_count_remote += src.io_count_remote;
    dst->io_ns_read_local_disk += src.io_ns_read_local_disk;
    dst->io_ns_read_remote += src.io_ns_read_remote;
    dst->prefetch_hit_count += src.prefetch_hit_count;
}

} // namespace

// ==================== VectorIndexLoadRequest ====================

VectorIndexLoadRequest::VectorIndexLoadRequest(const FileInfo& file_info, const RandomAccessFileOptions& opts,
                                               int64_t offset, char* dst, int64_t length, int64_t chunk_bytes)
        : _file_info(file_info),
          _opts(opts),
          _offset(offset),
          _dst(dst),
          _length(length),
          _chunk_bytes(chunk_bytes),
          _nchunks((length + chunk_bytes - 1) / chunk_bytes) {}

void VectorIndexLoadRequest::record_failure(const Status& st) {
    std::lock_guard<std::mutex> g(_mu);
    // Keep the first error: later ones are usually this one's consequence.
    if (_first_error.ok()) {
        _first_error = st;
    }
    _failed.store(true, std::memory_order_release);
    _cv.notify_all();
}

Status VectorIndexLoadRequest::status() const {
    std::lock_guard<std::mutex> g(_mu);
    return _first_error;
}

bool VectorIndexLoadRequest::try_join(int32_t cap) {
    // Joining tests the cap in the same step: checking it separately would let two workers
    // both see room and both take it. The count is also what wait_until_settled() uses to
    // know nobody can still be writing into `dst`, which is why a worker must join before
    // the scheduler lock is released -- see pick_locked().
    int32_t seen = _in_flight.load(std::memory_order_relaxed);
    do {
        if (cap != kUncapped && seen >= cap) {
            return false;
        }
    } while (!_in_flight.compare_exchange_weak(seen, seen + 1, std::memory_order_acq_rel, std::memory_order_relaxed));
    return true;
}

void VectorIndexLoadRequest::leave() {
    if (_in_flight.fetch_sub(1, std::memory_order_acq_rel) == 1) {
        std::lock_guard<std::mutex> g(_mu);
        _cv.notify_all();
    }
}

VectorIndexReaderSlot VectorIndexReaderSlot::try_take(VectorIndexLoadRequest* r, int32_t cap) {
    return r->try_join(cap) ? VectorIndexReaderSlot(r) : VectorIndexReaderSlot();
}

VectorIndexReaderSlot::~VectorIndexReaderSlot() {
    if (_request != nullptr) {
        _request->leave();
    }
}

void VectorIndexLoadRequest::read_chunks() {
    // One stream for as long as this reader stays on the file. A stream cannot be shared
    // between threads -- starlet's only has seek() -- and opening one per chunk would
    // cost more than the chunk.
    auto stream_or = _file_info.fs->new_random_access_file(_opts, _file_info);
    if (!stream_or.ok()) {
        record_failure(stream_or.status());
        return;
    }
    auto stream = std::move(*stream_or);
    // The counters live on the stream, so they have to be folded in before it closes.
    DeferOp fold_io([&] {
        if (auto s = stream->stream(); s) {
            std::lock_guard<std::mutex> g(_mu);
            merge_io_stats(&_io, s->get_io_stats_snapshot());
        }
    });

    while (!_failed.load(std::memory_order_relaxed)) {
        const int64_t i = _claimed.fetch_add(1, std::memory_order_relaxed);
        if (i >= _nchunks) {
            return;
        }
        const int64_t begin = i * _chunk_bytes;
        const int64_t len = std::min(_chunk_bytes, _length - begin);
        if (auto st = stream->read_at_fully(_offset + begin, _dst + begin, len); !st.ok()) {
            record_failure(st);
            return;
        }
        if (_completed.fetch_add(1, std::memory_order_acq_rel) + 1 == _nchunks) {
            std::lock_guard<std::mutex> g(_mu);
            _cv.notify_all();
        }
    }
}

void VectorIndexLoadRequest::wait_until_settled() {
    std::unique_lock<std::mutex> lk(_mu);
    _cv.wait(lk, [this] {
        // No worker may still be touching `dst`, and either every chunk landed or the
        // read has failed.
        if (_in_flight.load(std::memory_order_acquire) != 0) {
            return false;
        }
        return _failed.load(std::memory_order_relaxed) || _completed.load(std::memory_order_acquire) == _nchunks;
    });
}

// ==================== VectorIndexLoadScheduler ====================

VectorIndexLoadScheduler& VectorIndexLoadScheduler::instance() {
    static VectorIndexLoadScheduler s;
    return s;
}

int32_t VectorIndexLoadScheduler::max_readers_per_file() const {
    const int32_t v = config::vector_index_load_parallel_threads;
    // 0 or 1 means no helper at all: the loading thread reads the file by itself, which is
    // the plain streamed read this replaced.
    return v > 1 ? v : 1;
}

int32_t VectorIndexLoadScheduler::max_workers() const {
    // The pool's own width is the budget. It follows the config through the update hook,
    // so there is nothing to recompute here.
    ThreadPool* pool = RuntimeEnv::GetInstance()->vector_index_load_thread_pool();
    return pool != nullptr ? pool->max_threads() : 0;
}

bool VectorIndexLoadScheduler::has_work_locked() const {
    const int32_t cap = max_readers_per_file();
    return std::any_of(_active.begin(), _active.end(),
                       [cap](const VectorIndexLoadRequest* r) { return r->could_serve(cap); });
}

VectorIndexReaderSlot VectorIndexLoadScheduler::pick_locked() const {
    // FIFO, skipping anything that cannot take another reader. Starvation is impossible
    // -- every request has its own calling thread -- so this only decides who gets helped
    // first, and staying on one file reuses its stream.
    const int32_t cap = max_readers_per_file();
    for (auto* r : _active) {
        if (!r->could_serve(cap)) {
            continue;
        }
        // could_serve() above is only a hint; taking the slot is what enforces the cap,
        // atomically, and what makes the request safe to hand out.
        if (auto slot = VectorIndexReaderSlot::try_take(r, cap)) {
            return slot;
        }
    }
    return {};
}

// Submitting a task here means "one more worker", not "one more piece of work": the
// closure carries no chunk, it goes and picks one. So the count to submit is how many
// workers this request could keep busy, bounded by how many the budget still allows.
void VectorIndexLoadScheduler::spawn_workers_locked(int64_t wanted) {
    ThreadPool* pool = RuntimeEnv::GetInstance()->vector_index_load_thread_pool();
    if (pool == nullptr) {
        return;
    }
    const int64_t room = std::min<int64_t>(wanted, pool->max_threads() - _live_workers);
    for (int64_t i = 0; i < room; ++i) {
        ++_live_workers;
        if (auto st = pool->submit_func([this] { worker_task(); }); !st.ok()) {
            --_live_workers;
            break; // The calling thread still finishes the read on its own.
        }
    }
}

void VectorIndexLoadScheduler::register_request(VectorIndexLoadRequest* r) {
    std::lock_guard<std::mutex> g(_mu);
    _active.push_back(r);
    // One of the readers is the calling thread, and a request cannot use more readers
    // than its own cap -- asking for more only creates workers that bounce off it.
    spawn_workers_locked(std::min<int64_t>(r->nchunks(), max_readers_per_file()) - 1);
}

void VectorIndexLoadScheduler::unregister_request(VectorIndexLoadRequest* r) {
    std::lock_guard<std::mutex> g(_mu);
    _active.erase(std::remove(_active.begin(), _active.end(), r), _active.end());
}

void VectorIndexLoadScheduler::on_worker_exit() {
    std::lock_guard<std::mutex> g(_mu);
    --_live_workers;
    // A worker may be leaving at the very moment a request registers: that register saw
    // this worker still counted and hired one fewer, and nothing else will make up the
    // difference. Look again so a request with room but nobody serving it cannot persist.
    // This only asks whether work exists -- pick_locked() would join, and a join nobody
    // drains would keep the request from ever settling.
    if (has_work_locked()) {
        spawn_workers_locked(1);
    }
}

VectorIndexReaderSlot VectorIndexLoadScheduler::pick() {
    std::lock_guard<std::mutex> g(_mu);
    return pick_locked();
}

void VectorIndexLoadScheduler::worker_task() {
    // Returns as soon as there is nothing to do rather than parking: the pool creates
    // threads on demand and retires them after its idle timeout, and a task that never
    // returns would pin a thread and defeat both.
    // The slot pick() returns is what keeps its request alive for the read below, and
    // gives the place back at the end of the iteration.
    while (auto slot = pick()) {
        // Marks the gap the slot closes: a worker holding a request it is not yet counted
        // on. Compiled out outside BE_TEST.
        TEST_SYNC_POINT("VectorIndexLoadScheduler::worker_task:picked");
        slot->read_chunks();
    }
    on_worker_exit();
}

Status VectorIndexLoadScheduler::read(const FileInfo& file_info, const RandomAccessFileOptions& opts, int64_t offset,
                                      char* dst, int64_t length, int64_t chunk_bytes, io::IoStatsSnapshot* io_out) {
    if (file_info.fs == nullptr) {
        return Status::InvalidArgument("a parallel vector index read needs a FileSystem");
    }
    if (length <= 0) {
        return Status::OK();
    }

    VectorIndexLoadRequest request(file_info, opts, offset, dst, length, std::max<int64_t>(1, chunk_bytes));

    register_request(&request);
    {
        // The calling thread is worker 0 and never stops early: with no helpers this is
        // the single-stream read, byte for byte. Uncapped, so the slot is always taken.
        auto slot = VectorIndexReaderSlot::try_take(&request, VectorIndexLoadRequest::kUncapped);
        slot->read_chunks();
    }
    // Helpers may still hold chunks claimed before this thread ran out.
    request.wait_until_settled();
    unregister_request(&request);

    if (io_out != nullptr) {
        merge_io_stats(io_out, request.io_stats());
    }
    return request.status();
}

} // namespace starrocks
