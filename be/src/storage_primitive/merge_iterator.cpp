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

#include "storage_primitive/merge_iterator.h"

#include <atomic>
#include <climits>
#include <condition_variable>
#include <deque>
#include <future>
#include <limits>
#include <memory>
#include <mutex>
#include <queue>
#include <vector>

#include "base/time/time.h"
#include "base/utility/defer_op.h"
#include "column/chunk.h"
#include "column/chunk_factory.h"
#include "column/sorting/sorting.h"
#include "common/config_compaction_fwd.h"
#include "common/logging.h"
#include "common/thread/threadpool.h"
#include "gutil/strings/substitute.h"
#include "runtime/current_thread.h"

namespace starrocks {

// Compare the row of index |m| in |lhs|, with the row of index |n| in |rhs|.
// The `compare_at` result is multiplied by `sort_order`, so the null hint must be `null_first`, which already
// carries the `sort_order` factor. Using `nan_direction()` here would cancel that factor out and place the NULLs
// at the opposite end of a descending run, disagreeing with how the runs themselves were sorted.
inline int compare_column(const ColumnPtr& lc, size_t m, const ColumnPtr& rc, size_t n, const SortDesc* sort_desc) {
    int sort_order = 1;
    int null_first = -1;
    if (sort_desc) {
        sort_order = sort_desc->sort_order;
        null_first = sort_desc->null_first;
    }
    return lc->compare_at(m, n, *rc, null_first) * sort_order;
}

// Compare the row of index |m| in |lhs|, with the row of index |n| in |rhs|.
inline int compare_chunk(size_t key_columns, const std::vector<uint32_t>& sort_key_idxes,
                         const std::shared_ptr<SortDescs>& sort_descs, const Chunk& lhs, size_t m, const Chunk& rhs,
                         size_t n, const std::string& merge_condition) {
    for (size_t pos = 0; pos < sort_key_idxes.size(); ++pos) {
        uint32_t sort_key_idx = sort_key_idxes[pos];
        const ColumnPtr& lc = lhs.get_column_by_index(sort_key_idx);
        const ColumnPtr& rc = rhs.get_column_by_index(sort_key_idx);
        SortDesc* sort_desc = nullptr;
        if (sort_descs && pos < sort_descs->descs.size()) {
            sort_desc = &sort_descs->descs[pos];
        }
        if (int r = compare_column(lc, m, rc, n, sort_desc); r != 0) {
            return r;
        }
    }

    // we append merge_condition into schema in rowset writer, so here we use key_columns as
    // update condition column index
    if (!merge_condition.empty() && lhs.columns().size() > key_columns) {
        const ColumnPtr& lc = lhs.get_column_by_index(key_columns);
        const ColumnPtr& rc = rhs.get_column_by_index(key_columns);
        if (int r = compare_column(lc, m, rc, n, nullptr); r != 0) {
            return r;
        }
    }

    return 0;
}

// MergingChunk contains a chunk for merge and an index of compared row.
class MergingChunk {
public:
    MergingChunk() = default;
    explicit MergingChunk(Chunk* chunk) : _chunk(chunk) {}

    size_t compared_row() const { return _compared_row; }

    void advance(size_t row) { _compared_row += row; }

    size_t remaining_rows() const { return _chunk->num_rows() - _compared_row; }

protected:
    friend class MaskMergeIterator;

    Chunk* _chunk = nullptr;
    // use uint16_t for better heap merge performance
    uint16_t _compared_row = 0;
};

// Compare two chunks by the one specific row of each other.
class ComparableChunk : public MergingChunk {
public:
    explicit ComparableChunk(Chunk* chunk, size_t order, size_t key_columns, std::vector<uint32_t> sort_key_idxes,
                             std::shared_ptr<SortDescs> sort_descs, std::string merge_condition)
            : MergingChunk(chunk),
              _order(order),
              _key_columns(key_columns),
              _sort_key_idxes(std::move(sort_key_idxes)),
              _sort_descs(std::move(sort_descs)),
              _merge_condition(std::move(merge_condition)) {}

    explicit ComparableChunk(Chunk* chunk, size_t order, size_t key_columns, std::vector<uint32_t> sort_key_idxes,
                             std::shared_ptr<SortDescs> sort_descs, std::string merge_condition,
                             std::shared_ptr<std::vector<uint64_t>> rssid_rowids)
            : ComparableChunk(chunk, order, key_columns, std::move(sort_key_idxes), std::move(sort_descs),
                              std::move(merge_condition)) {
        _rssid_rowids = std::move(rssid_rowids);
    }

    bool operator>(const ComparableChunk& rhs) const {
        DCHECK_EQ(_key_columns, rhs._key_columns);
        int r = compare_chunk(_key_columns, _sort_key_idxes, _sort_descs, *_chunk, _compared_row, *rhs._chunk,
                              rhs._compared_row, _merge_condition);
        return (r > 0) | ((r == 0) & (_order > rhs._order));
    }

    // return true iff all rows in |this| chunk are less than those in |rhs|, i.e, if
    // last row in |this| chunk is less than the first row in |rhs|.
    // assume both |this| and |rhs| are not empty.
    bool less_than_all(const ComparableChunk& rhs) {
        size_t last_row = _chunk->num_rows() - 1;
        return less_than(last_row, rhs);
    }

    // return the next row number of last row whose key value is less than all values in |rhs|
    size_t last_row_less_than(const ComparableChunk& rhs, size_t limit_num) {
        // As we previously pop this chunk from the heap top, `_compared_row` in this chunk
        // must be less than all rows in rhs, thus here we start comparision from _compared_row + 1;
        size_t next_compare_row = _compared_row + 1;
        size_t upper_bound = std::min(_compared_row + limit_num, _chunk->num_rows());
        while (next_compare_row < upper_bound && less_than(next_compare_row, rhs)) {
            next_compare_row++;
        }
        return next_compare_row;
    }

    bool less_than(size_t lhs_row, const ComparableChunk& rhs) {
        int r = compare_chunk(_key_columns, _sort_key_idxes, _sort_descs, *_chunk, lhs_row, *rhs._chunk,
                              rhs._compared_row, _merge_condition);
        return (r < 0) | ((r == 0) & (_order < rhs._order));
    }

private:
    friend class HeapMergeIterator;

    // used to determinate the order of two rows when their key columns are all equals.
    uint16_t _order;
    uint16_t _key_columns;
    std::vector<uint32_t> _sort_key_idxes;
    std::shared_ptr<SortDescs> _sort_descs;
    std::string _merge_condition;
    std::shared_ptr<std::vector<uint64_t>> _rssid_rowids;
};

namespace {

// The prefill borrows no existing pool on purpose: the compaction workers are the callers here,
// and the ingestion pools serve latency-sensitive writes, so either choice would make this
// contend with unrelated work. Built once, lazily, so a BE that never enables the config never
// pays for it.
ThreadPool* merge_prefill_pool() {
    static std::unique_ptr<ThreadPool> pool = []() -> std::unique_ptr<ThreadPool> {
        std::unique_ptr<ThreadPool> p;
        // The pool is shared by every concurrent merge; size it above the per-task in-flight
        // limit so tasks do not dilute each other, and never below that limit.
        int max_threads = std::max({1, config::compaction_parallel_merge_init_threads,
                                    config::compaction_parallel_merge_init_pool_threads});
        Status st = ThreadPoolBuilder("merge_prefill")
                            .set_min_threads(0)
                            .set_max_threads(max_threads)
                            .set_max_queue_size(INT_MAX)
                            .set_idle_timeout(MonoDelta::FromMilliseconds(10000))
                            .build(&p);
        if (!st.ok()) {
            LOG(WARNING) << "Failed to create merge prefill thread pool, fall back to serial merge init: " << st;
            return nullptr;
        }
        return p;
    }();
    return pool.get();
}

// A background read may itself drive a nested merge iterator. Letting the nested level submit into
// the same bounded pool and then block on it would deadlock once the pool is saturated, so a
// nested merge always reads inline.
thread_local bool tls_in_merge_prefill = false;

} // namespace

class MergeIterator : public ChunkIterator {
public:
    explicit MergeIterator(std::vector<ChunkIteratorPtr> children)
            : ChunkIterator(children[0]->schema(), children[0]->chunk_size()), _children(std::move(children)) {
        _bufs.reserve(_children.size());
        for (size_t i = 0; i < _children.size(); i++) {
            _bufs.push_back(std::make_unique<ChildBuffer>());
        }
#ifndef NDEBUG
        // ensure that the children's schemas are all the same.
        for (size_t i = 1; i < _children.size(); i++) {
            CHECK_EQ(_schema.num_fields(), _children[i]->schema().num_fields());
            for (size_t j = 0; j < _schema.num_fields(); j++) {
                CHECK_EQ(_schema.field(j)->to_string(), _children[i]->schema().field(j)->to_string());
            }
        }
#endif
    }

    ~MergeIterator() override { close(); }

    void close() override;

    size_t merged_rows() const override { return _merged_rows; }

    Status init_encoded_schema(ColumnIdToGlobalDictMap& dict_maps) override {
        RETURN_IF_ERROR(ChunkIterator::init_encoded_schema(dict_maps));
        for (auto& i : _children) {
            RETURN_IF_ERROR(i->init_encoded_schema(dict_maps));
        }
        return Status::OK();
    }

    Status init_output_schema(const std::unordered_set<uint32_t>& unused_output_column_ids) override {
        RETURN_IF_ERROR(ChunkIterator::init_output_schema(unused_output_column_ids));
        for (auto& i : _children) {
            RETURN_IF_ERROR(i->init_output_schema(unused_output_column_ids));
        }
        return Status::OK();
    }

protected:
    static constexpr size_t kNoSlot = std::numeric_limits<size_t>::max();

    // Per-child ring of chunks. With a single slot this is the original behaviour: the merge holds
    // the only chunk, and refilling it is a blocking read that stalls the merge for a full round
    // trip. With more slots a background pump keeps the free ones filled while the merge consumes
    // the held one, so that round trip overlaps the merge instead of stopping it.
    //
    // A child iterator is sequential, so at most one read per child may ever be in flight: the pump
    // is that single reader, and it fills the free slots one after another, in order.
    struct ChildBuffer {
        std::vector<ChunkPtr> slots;
        // Read status per slot, produced by read_slot and interpreted by commit_slot.
        std::vector<Status> st;
        std::deque<size_t> freelist; // slots the merge is done with
        std::deque<size_t> ready;    // slots the pump has filled, in read order
        std::mutex mu;
        std::condition_variable cv;
        bool pumping = false;   // a pump is filling this child right now
        bool exhausted = false; // the last read hit end-of-file or an error; stop reading
        size_t held = kNoSlot;  // the slot the merge state currently points at; merge thread only
    };

    Status init();
    void close_child(size_t child);

    // The read half touches only this child's own iterator and its own slot, so different children
    // may run it concurrently. The commit half mutates shared merge state (the heap, or the
    // per-child slot) and must stay serial and in read order, otherwise the merge would consume a
    // different order than the serial path.
    virtual Status read_slot(size_t child, size_t slot) = 0;
    virtual Status commit_slot(size_t child, size_t slot) = 0;

    // Called when the merge has consumed everything in the child's held slot.
    Status refill(size_t child);

    Status parallel_prefill();
    void start_pump(size_t child);
    void submit_pump(size_t child);
    void pump(size_t child);
    void stop_pump(size_t child);

    // Whether the background pump is in play at all. Without it every read is inline and the
    // extra slots would only waste memory, so they are not even allocated.
    bool pipelined() const { return _pool != nullptr && _buffers > 1; }

    std::vector<ChunkIteratorPtr> _children;
    std::vector<std::unique_ptr<ChildBuffer>> _bufs;
    // Set during the prefill for a child whose prefetch() made its whole scan locally available:
    // from then on its reads are decode-only, and they run on the merge thread -- never on the
    // pool, whose job under the IO/decode split is the IO half alone.
    std::vector<uint8_t> _bytes_resident;
    ThreadPool* _pool = nullptr;
    MemTracker* _mem_tracker = nullptr;
    size_t _buffers = 1;
    size_t _merged_rows = 0;
    bool _inited = false;
};

inline Status MergeIterator::init() {
    DCHECK(_chunk_size > 0);
    DCHECK_EQ(_children.size(), _bufs.size());
    _mem_tracker = tls_thread_status.mem_tracker();
    _buffers = std::max(1, config::compaction_merge_child_buffers);
    _pool = tls_in_merge_prefill ? nullptr : merge_prefill_pool();

    const size_t nslots = pipelined() ? _buffers : 1;
    for (auto& buf : _bufs) {
        buf->slots.resize(nslots);
        buf->st.assign(nslots, Status::OK());
        for (size_t s = 0; s < nslots; s++) {
            // No need to reserve, because it's already reserved in segment interators.
            // If we reserve here, for small segment files, it will consume large memory then need.
            buf->slots[s] = ChunkFactory::new_chunk(output_schema(), 0);
            if (s > 0) {
                buf->freelist.push_back(s);
            }
        }
    }

    _bytes_resident.assign(_children.size(), 0);
    const bool parallel = config::enable_compaction_parallel_merge_init && _children.size() > 1 && _pool != nullptr;
    const int64_t start_ns = MonotonicNanos();
    if (parallel) {
        RETURN_IF_ERROR(parallel_prefill());
    } else {
        for (size_t i = 0; i < _children.size(); i++) {
            RETURN_IF_ERROR(read_slot(i, 0));
            RETURN_IF_ERROR(commit_slot(i, 0));
        }
    }
    // A prefill this slow is a real stall -- every child is read before the merge emits a row --
    // so it is worth a line, and it is rare enough not to be noise.
    int64_t cost_ms = (MonotonicNanos() - start_ns) / 1000000;
    LOG_IF(INFO, cost_ms >= 1000) << "slow merge iterator prefill: children=" << _children.size()
                                  << ", parallel=" << parallel << ", cost=" << cost_ms << "ms";

    // Slot 0 of every surviving child is now in the merge state; start reading ahead into the
    // rest. A resident child gets no pump: its refills are decode-only and the merge thread runs
    // them inline.
    if (pipelined()) {
        for (size_t i = 0; i < _children.size(); i++) {
            if (!_bytes_resident[i]) {
                start_pump(i);
            }
        }
    }
    _inited = true;
    return Status::OK();
}

// Reads slot 0 of every child concurrently, then commits them serially in child order. The commit
// order is what makes this equivalent to the serial prefill: the heap and the per-child slots see
// the same sequence of updates, and a child that fails still surfaces its error at the same point
// the serial path would.
inline Status MergeIterator::parallel_prefill() {
    const size_t n = _children.size();
    std::vector<std::future<void>> futures;
    futures.reserve(n);

    // The tasks capture `this`, so every submitted task must finish before returning -- a task
    // still running after the iterator is destroyed would touch freed memory.
    DeferOp wait_all([&futures]() {
        for (auto& f : futures) {
            if (f.valid()) f.wait();
        }
    });

    auto* mem_tracker = _mem_tracker;
    // One residency allowance for the whole merge, drawn from by every child's prefetch: it caps
    // what this merge may hold in prefetched buffers no matter how many children it has. Children
    // past the budget fall back to the pre-split path, a graceful degradation for merges whose
    // scans are too big to hold. Joined before this function returns, so a stack slot is safe.
    std::atomic<int64_t> prefetch_budget{config::compaction_parallel_merge_prefetch_bytes};
    // Per-iterator in-flight limit: the shared pool is sized for concurrent merges, so without
    // this cap one task with many children would occupy the whole pool and past-the-knee
    // concurrency slows the task itself down (measured: 64 threads slower than 16 for one task).
    const int inflight_cap = std::max(1, config::compaction_parallel_merge_init_threads);
    auto gate = std::make_shared<std::pair<std::mutex, std::condition_variable>>();
    auto inflight = std::make_shared<int>(0);
    for (size_t i = 0; i < n; i++) {
        {
            std::unique_lock<std::mutex> l(gate->first);
            gate->second.wait(l, [&]() { return *inflight < inflight_cap; });
            ++*inflight;
        }
        auto done = [gate, inflight]() {
            std::lock_guard<std::mutex> l(gate->first);
            --*inflight;
            gate->second.notify_one();
        };
        auto task = std::make_shared<std::packaged_task<void()>>([this, i, mem_tracker, done, &prefetch_budget]() {
            // Memory tracking is thread local: without re-installing the caller's tracker the
            // bytes read here would vanish from the task's account and escape its memory limit.
            SCOPED_THREAD_LOCAL_MEM_TRACKER_SETTER(mem_tracker);
            tls_in_merge_prefill = true;
            DeferOp reset_flag([done]() {
                tls_in_merge_prefill = false;
                done();
            });
            // The IO half first: a child whose prefetch makes its bytes locally available keeps
            // its decode half on the merge thread, so this pool thread only ever waits on IO and
            // the task's CPU stays on its own worker.
            auto covered = _children[i]->prefetch(&prefetch_budget);
            if (!covered.ok()) {
                // Land the error in the slot for commit_slot to interpret in child order, at the
                // same point a serial read would have surfaced its error. Retrying via read_slot
                // instead would re-run the child's init on a half-initialized iterator and mask
                // the real error. commit_slot treats end-of-file as a clean child close, so an
                // EOF-status here (which no prefetch should produce) must not slip through as one.
                _bufs[i]->st[0] = covered.status().is_end_of_file()
                                          ? Status::InternalError("unexpected EOF from prefetch")
                                          : covered.status();
                return;
            }
            if (*covered) {
                _bytes_resident[i] = 1;
                return;
            }
            // This child cannot be covered up front (non-lake file, complex column, cache off,
            // or over budget): keep the pre-split behaviour, a full read on the pool.
            (void)read_slot(i, 0);
        });
        auto st = _pool->submit_func([task]() { (*task)(); });
        if (!st.ok()) {
            // Pool refused the task: read this child inline. Still correct, just not overlapped.
            done();
            (void)read_slot(i, 0);
            continue;
        }
        futures.push_back(task->get_future());
    }
    for (auto& f : futures) {
        f.wait();
    }
    futures.clear();

    // Decode half of the resident children, on this thread and in child order, then commit in
    // child order. End-of-file and read errors are interpreted by the same branching the serial
    // path uses, so a caller cannot tell the paths apart.
    for (size_t i = 0; i < n; i++) {
        if (_bytes_resident[i]) {
            (void)read_slot(i, 0);
        }
        RETURN_IF_ERROR(commit_slot(i, 0));
    }
    return Status::OK();
}

inline Status MergeIterator::refill(size_t child) {
    ChildBuffer& b = *_bufs[child];
    if (!pipelined() || _bytes_resident[child]) {
        // One slot, or the child's bytes are already resident: read straight back into the slot
        // the merge just released. For a resident child this is decode-only work, and it belongs
        // here on the merge thread -- handing it to the pump would put CPU back on the IO pool.
        RETURN_IF_ERROR(read_slot(child, b.held));
        return commit_slot(child, b.held);
    }

    size_t slot;
    {
        std::lock_guard<std::mutex> l(b.mu);
        if (b.held != kNoSlot) {
            b.freelist.push_back(b.held);
            b.held = kNoSlot;
        }
    }
    // The slot just freed may be the one the pump was waiting for, so offer it before blocking.
    start_pump(child);
    {
        std::unique_lock<std::mutex> l(b.mu);
        while (b.ready.empty() && b.pumping) {
            b.cv.wait(l);
        }
        if (b.ready.empty()) {
            // The pump stopped without producing anything more: nothing left in this child.
            l.unlock();
            close_child(child);
            return Status::OK();
        }
        slot = b.ready.front();
        b.ready.pop_front();
    }
    return commit_slot(child, slot);
}

inline void MergeIterator::start_pump(size_t child) {
    ChildBuffer& b = *_bufs[child];
    {
        std::lock_guard<std::mutex> l(b.mu);
        // A closed child has no slots left, and its freelist indices are stale.
        if (b.slots.empty() || b.pumping || b.exhausted || b.freelist.empty()) {
            return;
        }
        b.pumping = true;
    }
    submit_pump(child);
}

inline void MergeIterator::submit_pump(size_t child) {
    auto* mem_tracker = _mem_tracker;
    auto st = _pool->submit_func([this, child, mem_tracker]() {
        SCOPED_THREAD_LOCAL_MEM_TRACKER_SETTER(mem_tracker);
        tls_in_merge_prefill = true;
        DeferOp reset_flag([]() { tls_in_merge_prefill = false; });
        pump(child);
    });
    if (!st.ok()) {
        // Pool refused the task: read inline. Correct, just not overlapped.
        pump(child);
    }
}

// Fills this child's free slots one at a time until there are none left or the child runs out of
// rows. Exactly one pump runs per child, which is what keeps the child iterator single-threaded.
inline void MergeIterator::pump(size_t child) {
    ChildBuffer& b = *_bufs[child];
    for (;;) {
        size_t slot;
        {
            std::lock_guard<std::mutex> l(b.mu);
            if (b.exhausted || b.freelist.empty()) {
                b.pumping = false;
                b.cv.notify_all();
                return;
            }
            slot = b.freelist.front();
            b.freelist.pop_front();
        }
        // The status lands in b.st[slot]; commit_slot on the merge thread interprets it.
        (void)read_slot(child, slot);
        {
            std::lock_guard<std::mutex> l(b.mu);
            if (!b.st[slot].ok()) {
                b.exhausted = true;
            }
            b.ready.push_back(slot);
            b.cv.notify_all();
        }
    }
}

// Blocks until this child has no reader left, so its chunks can be released.
inline void MergeIterator::stop_pump(size_t child) {
    ChildBuffer& b = *_bufs[child];
    std::unique_lock<std::mutex> l(b.mu);
    b.exhausted = true;
    while (b.pumping) {
        b.cv.wait(l);
    }
    b.freelist.clear();
    b.ready.clear();
    b.held = kNoSlot;
}

inline void MergeIterator::close_child(size_t child) {
    if (_bufs[child] == nullptr || _bufs[child]->slots.empty()) {
        return;
    }
    if (pipelined()) {
        stop_pump(child);
    }
    _bufs[child]->slots.clear();
    _merged_rows += _children[child]->merged_rows();
    _children[child]->close();
    _children[child].reset();
}

inline void MergeIterator::close() {
    DCHECK_EQ(_children.size(), _bufs.size());
    for (size_t i = 0; i < _children.size(); i++) {
        close_child(i);
    }
    _children.clear();
    _bufs.clear();
}

class HeapMergeIterator final : public MergeIterator {
public:
    explicit HeapMergeIterator(std::vector<ChunkIteratorPtr> children)
            : MergeIterator(std::move(children)), _rssid_slots(_children.size()) {}

    std::string merge_condition;

    // In PK table compaction, we need to get chunk and each row's rssid & rowid
    bool need_rssid_rowids = false;

protected:
    Status do_get_next(Chunk* chunk, std::vector<RowSourceMask>* source_masks,
                       std::vector<uint64_t>* rssid_rowids) override;
    Status do_get_next(Chunk* chunk) override { return do_get_next(chunk, nullptr, nullptr); }
    Status do_get_next(Chunk* chunk, std::vector<RowSourceMask>* source_masks) override {
        return do_get_next(chunk, source_masks, nullptr);
    }
    Status do_get_next(Chunk* chunk, std::vector<uint64_t>* rssid_rowids) override {
        return do_get_next(chunk, nullptr, rssid_rowids);
    }
    Status read_slot(size_t child, size_t slot) override;
    Status commit_slot(size_t child, size_t slot) override;

private:
    // rssid/rowid buffer per (child, slot), produced by read_slot and handed to the heap in
    // commit_slot. Only one reader touches a given child at a time, so the inner vector needs no
    // lock of its own.
    std::vector<std::vector<std::shared_ptr<vector<uint64_t>>>> _rssid_slots;

    template <typename T, typename Container = std::vector<T>>
    using MinPriorityQueue = std::priority_queue<T, Container, std::greater<T>>;
    using ChunkHeap = MinPriorityQueue<ComparableChunk>;

    ChunkHeap _heap;
};

inline Status HeapMergeIterator::do_get_next(Chunk* chunk, std::vector<RowSourceMask>* source_masks,
                                             std::vector<uint64_t>* rssid_rowids) {
    if (!_inited) {
        RETURN_IF_ERROR(init());
    }
    size_t rows = 0;
    Status st;

    while (!_heap.empty() && rows < _chunk_size) {
        ComparableChunk min_chunk = _heap.top();
        _heap.pop();
        DCHECK_GT(min_chunk.remaining_rows(), 0);

        size_t offset = min_chunk.compared_row();
        size_t append_row_num = 0;
        bool less_than_all = _heap.empty() || min_chunk.less_than_all(_heap.top());

        if (less_than_all) {
            if (offset == 0) {
                // all keys in |min_chunk| are less than heap top and |min_chunk|'s current offset is 0,
                // so here we swap the whole min_chunk out.
                if (rows == 0) {
                    chunk->swap_chunk(*min_chunk._chunk);
                    if (rssid_rowids != nullptr && need_rssid_rowids) {
                        // insert into `rssid_rowids` only when need_rssid_rowids is true.
                        DCHECK(min_chunk._rssid_rowids != nullptr);
                        rssid_rowids->insert(rssid_rowids->end(), min_chunk._rssid_rowids->begin(),
                                             min_chunk._rssid_rowids->end());
                    }
                    if (source_masks) {
                        source_masks->insert(source_masks->end(), chunk->num_rows(),
                                             RowSourceMask{min_chunk._order, false});
                    }
                    return refill(min_chunk._order);
                } else {
                    // retrieve |min_chunk| next time to avoid memory copy.
                    _heap.push(min_chunk);
                    break;
                }
            } else {
                // all keys in |min_chunk| are less than heap top, but |min_chunk|'s current offset is larger than 0
                // here we append the remaining rows in |min_chunk| to the chunk.
                size_t remain_row_num = min_chunk.remaining_rows();
                if (rows + remain_row_num <= _chunk_size) {
                    append_row_num = remain_row_num;
                } else {
                    append_row_num = _chunk_size - rows;
                }
            }
        } else {
            // find the last row in |min_chunk| whose key is less than all values in _heap.top(),
            // subtract it with the offset to get the append_row_num
            append_row_num = min_chunk.last_row_less_than(_heap.top(), _chunk_size - rows) - offset;
        }

        DCHECK_GT(append_row_num, 0);

        chunk->append(*min_chunk._chunk, offset, append_row_num);
        if (rssid_rowids != nullptr && need_rssid_rowids) {
            // insert into `rssid_rowids` only when need_rssid_rowids is true.
            DCHECK(min_chunk._rssid_rowids != nullptr);
            rssid_rowids->insert(rssid_rowids->end(), min_chunk._rssid_rowids->begin() + offset,
                                 min_chunk._rssid_rowids->begin() + offset + append_row_num);
        }
        min_chunk.advance(append_row_num);
        rows += append_row_num;

        DCHECK_LE(rows, _chunk_size);

        if (source_masks) {
            source_masks->insert(source_masks->end(), append_row_num, RowSourceMask{min_chunk._order, false});
        }
        if (min_chunk.remaining_rows() > 0) {
            _heap.push(min_chunk);
        } else {
            st = refill(min_chunk._order);
            if (!st.ok()) {
                break;
            }
        }
    }
    if (!st.ok()) {
        return st;
    } else if (rows > 0) {
        return Status::OK();
    } else {
        return Status::EndOfFile("End of heap merge iterator");
    }
}

inline Status HeapMergeIterator::read_slot(size_t child, size_t slot) {
    ChildBuffer& b = *_bufs[child];
    Chunk* chunk = b.slots[slot].get();
    chunk->reset();
    if (need_rssid_rowids) {
        auto& v = _rssid_slots[child];
        if (v.size() <= slot) {
            v.resize(slot + 1);
        }
        v[slot] = std::make_shared<vector<uint64_t>>();
        b.st[slot] = _children[child]->get_next(chunk, v[slot].get());
    } else {
        b.st[slot] = _children[child]->get_next(chunk);
    }
    return Status::OK();
}

inline Status HeapMergeIterator::commit_slot(size_t child, size_t slot) {
    ChildBuffer& b = *_bufs[child];
    Chunk* chunk = b.slots[slot].get();
    const Status st = b.st[slot];
    if (st.ok()) {
        size_t num_rows = chunk->num_rows();
        DCHECK_GT(num_rows, 0u);
        if (num_rows > max_merge_chunk_size) {
            return Status::InternalError(strings::Substitute(
                    "Merge iterator only supports merging chunks with rows less than $0", max_merge_chunk_size));
        }
        if (need_rssid_rowids) {
            _heap.emplace(chunk, child, _schema.num_key_fields(), _schema.sort_key_idxes(), _schema.sort_descs(),
                          merge_condition, std::move(_rssid_slots[child][slot]));
        } else {
            _heap.emplace(chunk, child, _schema.num_key_fields(), _schema.sort_key_idxes(), _schema.sort_descs(),
                          merge_condition);
        }
        b.held = slot;
    } else if (st.is_end_of_file()) {
        // ignore Status::EndOfFile.
        close_child(child);
    } else {
        close_child(child);
        return st;
    }
    return Status::OK();
}

ChunkIteratorPtr new_heap_merge_iterator(const std::vector<ChunkIteratorPtr>& children) {
    DCHECK(!children.empty());
    if (children.size() == 1) {
        return children[0];
    }

    // The `ComparableChunk` is using `uint16_t` to save the chunk order, if the size of
    // children is greater than UINT16_MAX, the value of order will overflow.
    const static size_t kMaxChildrenSize = std::numeric_limits<uint16_t>::max();

    if (children.size() <= kMaxChildrenSize) {
        return std::make_shared<HeapMergeIterator>(children);
    }
    std::vector<ChunkIteratorPtr> sub_merge_iterators;
    sub_merge_iterators.reserve((children.size() + kMaxChildrenSize - 1) / kMaxChildrenSize);
    for (size_t i = 0; i < children.size(); i += kMaxChildrenSize) {
        size_t j = std::min(i + kMaxChildrenSize, children.size());
        std::vector<ChunkIteratorPtr> v(children.begin() + i, children.begin() + j);
        sub_merge_iterators.emplace_back(new_heap_merge_iterator(v));
    }
    return new_heap_merge_iterator(sub_merge_iterators);
}

ChunkIteratorPtr new_heap_merge_iterator(const std::vector<ChunkIteratorPtr>& children,
                                         const std::string& merge_condition) {
    DCHECK(!children.empty());
    if (children.size() == 1) {
        return children[0];
    }

    // The `ComparableChunk` is using `uint16_t` to save the chunk order, if the size of
    // children is greater than UINT16_MAX, the value of order will overflow.
    const static size_t kMaxChildrenSize = std::numeric_limits<uint16_t>::max();

    if (children.size() <= kMaxChildrenSize) {
        auto heapMergeIterator = std::make_shared<HeapMergeIterator>(children);
        heapMergeIterator->merge_condition = merge_condition;
        return heapMergeIterator;
    }
    std::vector<ChunkIteratorPtr> sub_merge_iterators;
    sub_merge_iterators.reserve((children.size() + kMaxChildrenSize - 1) / kMaxChildrenSize);
    for (size_t i = 0; i < children.size(); i += kMaxChildrenSize) {
        size_t j = std::min(i + kMaxChildrenSize, children.size());
        std::vector<ChunkIteratorPtr> v(children.begin() + i, children.begin() + j);
        sub_merge_iterators.emplace_back(new_heap_merge_iterator(v, merge_condition));
    }
    return new_heap_merge_iterator(sub_merge_iterators, merge_condition);
}

ChunkIteratorPtr new_heap_merge_iterator(const std::vector<ChunkIteratorPtr>& children, const bool need_rssid_rowids) {
    DCHECK(!children.empty());
    if (children.size() == 1) {
        return children[0];
    }

    // The `ComparableChunk` is using `uint16_t` to save the chunk order, if the size of
    // children is greater than UINT16_MAX, the value of order will overflow.
    const static size_t kMaxChildrenSize = std::numeric_limits<uint16_t>::max();

    if (children.size() <= kMaxChildrenSize) {
        auto heapMergeIterator = std::make_shared<HeapMergeIterator>(children);
        heapMergeIterator->need_rssid_rowids = need_rssid_rowids;
        return heapMergeIterator;
    }
    std::vector<ChunkIteratorPtr> sub_merge_iterators;
    sub_merge_iterators.reserve((children.size() + kMaxChildrenSize - 1) / kMaxChildrenSize);
    for (size_t i = 0; i < children.size(); i += kMaxChildrenSize) {
        size_t j = std::min(i + kMaxChildrenSize, children.size());
        std::vector<ChunkIteratorPtr> v(children.begin() + i, children.begin() + j);
        sub_merge_iterators.emplace_back(new_heap_merge_iterator(v, need_rssid_rowids));
    }
    return new_heap_merge_iterator(sub_merge_iterators, need_rssid_rowids);
}

// Merge iterator based on source masks.
// The order of rows is determinate by mask sequence.
class MaskMergeIterator final : public MergeIterator {
public:
    explicit MaskMergeIterator(std::vector<ChunkIteratorPtr> children, RowSourceMaskBuffer* mask_buffer,
                               RowSourceMaskBuffer* selection_buffer)
            : MergeIterator(std::move(children)),
              _chunks(_children.size()),
              _mask_buffer(mask_buffer),
              _selection_buffer(selection_buffer) {
        DCHECK(_mask_buffer);
    }

protected:
    Status do_get_next(Chunk* chunk) override { return do_get_next(chunk, nullptr); }
    Status do_get_next(Chunk* chunk, std::vector<RowSourceMask>* source_masks) override;
    Status read_slot(size_t child, size_t slot) override;
    Status commit_slot(size_t child, size_t slot) override;

private:
    std::vector<MergingChunk> _chunks;
    RowSourceMaskBuffer* _mask_buffer = nullptr;
    // Optional row-selection stream aligned one-to-one with _mask_buffer. A non-zero
    // source number means keep the row; zero means consume it from the source iterator
    // without appending it. Vertical UNSHARE compaction records this stream while
    // processing the key group and replays it for every value group.
    RowSourceMaskBuffer* _selection_buffer = nullptr;
};

inline Status MaskMergeIterator::do_get_next(Chunk* chunk, std::vector<RowSourceMask>* source_masks) {
    if (!_inited) {
        RETURN_IF_ERROR(init());
    }
    size_t rows = 0;
    Status st;

    auto st_or = _mask_buffer->has_remaining();
    if (!st_or.ok()) {
        return st_or.status();
    }
    if (_selection_buffer != nullptr) {
        ASSIGN_OR_RETURN(bool selection_remaining, _selection_buffer->has_remaining());
        if (selection_remaining != st_or.value()) {
            return Status::InternalError("row-source mask and selection buffers have different lengths");
        }
    }
    while (st_or.value() && rows < _chunk_size) {
        RowSourceMask mask = _mask_buffer->current();
        uint16_t child = mask.get_source_num();
        auto& min_chunk = _chunks[child];
        if (min_chunk._chunk == nullptr) {
            return Status::InternalError(strings::Substitute(
                    "Mask buffer expects more rows from child $0, but child iterator is exhausted", child));
        }
        DCHECK_GT(min_chunk.remaining_rows(), 0);

        // A filtered row is still represented in the source mask so every value-group
        // child advances over exactly the same physical rows as the key group. It is not
        // emitted to the output chunk.
        if (_selection_buffer != nullptr && _selection_buffer->current().get_source_num() == 0) {
            min_chunk.advance(1);
            _mask_buffer->advance();
            _selection_buffer->advance();
            if (min_chunk.remaining_rows() == 0) {
                st = fill(child);
                if (!st.ok() && !st.is_end_of_file()) {
                    return st;
                }
            }
            st_or = _mask_buffer->has_remaining();
            if (!st_or.ok()) {
                return st_or.status();
            }
            ASSIGN_OR_RETURN(bool selection_remaining, _selection_buffer->has_remaining());
            if (selection_remaining != st_or.value()) {
                return Status::InternalError("row-source mask and selection buffers have different lengths");
            }
            continue;
        }

        size_t offset = min_chunk.compared_row();
        size_t min_chunk_num_rows = min_chunk._chunk->num_rows();
        size_t append_row_num = 0;
        size_t max_same_source_count = _mask_buffer->max_same_source_count(child, min_chunk.remaining_rows());
        if (_selection_buffer != nullptr) {
            max_same_source_count = std::min(max_same_source_count, _selection_buffer->max_same_source_count(
                                                                            /*source=*/1, max_same_source_count));
        }
        if (max_same_source_count == min_chunk_num_rows) {
            DCHECK(offset == 0);
            // all rows in |min_chunk| are from the same source chunk and |min_chunk|'s current offset is 0,
            // so here we swap the whole min_chunk out.
            if (rows == 0) {
                chunk->swap_chunk(*min_chunk._chunk);
                for (int i = 0; i < min_chunk_num_rows; ++i) {
                    if (source_masks) {
                        source_masks->emplace_back(_mask_buffer->current());
                    }
                    _mask_buffer->advance();
                    if (_selection_buffer != nullptr) {
                        _selection_buffer->advance();
                    }
                }
                return refill(child);
            } else {
                // retrieve |min_chunk| next time to avoid memory copy.
                break;
            }
        } else {
            // `max_same_source_count` rows in |min_chunk| are from the same source chunk,
            // here we append the `max_same_source_count` in |min_chunk| to the chunk.
            if (rows + max_same_source_count <= _chunk_size) {
                append_row_num = max_same_source_count;
            } else {
                append_row_num = _chunk_size - rows;
            }
        }

        DCHECK_GT(append_row_num, 0);
        chunk->append(*min_chunk._chunk, offset, append_row_num);
        min_chunk.advance(append_row_num);
        rows += append_row_num;
        for (size_t i = 0; i < append_row_num; ++i) {
            if (source_masks) {
                source_masks->emplace_back(_mask_buffer->current());
            }
            _mask_buffer->advance();
            if (_selection_buffer != nullptr) {
                _selection_buffer->advance();
            }
        }

        DCHECK_LE(rows, _chunk_size);

        if (min_chunk.remaining_rows() == 0) {
            st = refill(child);
            if (!st.ok()) {
                break;
            }
        }

        st_or = _mask_buffer->has_remaining();
        if (!st_or.ok()) {
            return st_or.status();
        }
        if (_selection_buffer != nullptr) {
            ASSIGN_OR_RETURN(bool selection_remaining, _selection_buffer->has_remaining());
            if (selection_remaining != st_or.value()) {
                return Status::InternalError("row-source mask and selection buffers have different lengths");
            }
        }
    }
    if (!st.ok()) {
        return st;
    } else if (rows > 0) {
        return Status::OK();
    } else {
        for (auto& buf : _bufs) {
            DCHECK(buf->slots.empty());
        }
        return Status::EndOfFile("End of mask merge iterator");
    }
}

inline Status MaskMergeIterator::read_slot(size_t child, size_t slot) {
    ChildBuffer& b = *_bufs[child];
    Chunk* chunk = b.slots[slot].get();
    chunk->reset();
    b.st[slot] = _children[child]->get_next(chunk);
    return Status::OK();
}

inline Status MaskMergeIterator::commit_slot(size_t child, size_t slot) {
    ChildBuffer& b = *_bufs[child];
    Chunk* chunk = b.slots[slot].get();
    const Status st = b.st[slot];
    if (st.ok()) {
        size_t num_rows = chunk->num_rows();
        DCHECK_GT(num_rows, 0u);
        if (num_rows > max_merge_chunk_size) {
            return Status::InternalError(strings::Substitute(
                    "Merge iterator only supports merging chunks with rows less than $0", max_merge_chunk_size));
        }
        _chunks[child] = MergingChunk(chunk);
        b.held = slot;
    } else if (st.is_end_of_file()) {
        // ignore Status::EndOfFile.
        close_child(child);
        _chunks[child]._chunk = nullptr;
    } else {
        close_child(child);
        _chunks[child]._chunk = nullptr;
        return st;
    }
    return Status::OK();
}

ChunkIteratorPtr new_mask_merge_iterator(const std::vector<ChunkIteratorPtr>& children,
                                         RowSourceMaskBuffer* mask_buffer, RowSourceMaskBuffer* selection_buffer) {
    if (children.size() == 1 && selection_buffer == nullptr) {
        return children[0];
    }
    DCHECK(!children.empty() && children.size() <= RowSourceMask::MAX_SOURCES);
    return std::make_shared<MaskMergeIterator>(children, mask_buffer, selection_buffer);
}

} // namespace starrocks
