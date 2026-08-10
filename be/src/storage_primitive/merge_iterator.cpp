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

#include <climits>
#include <future>
#include <memory>
#include <queue>
#include <vector>

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
inline int compare_column(const ColumnPtr& lc, size_t m, const ColumnPtr& rc, size_t n, const SortDesc* sort_desc) {
    int sort_order = 1;
    int nan_direction = -1;
    if (sort_desc) {
        sort_order = sort_desc->sort_order;
        nan_direction = sort_desc->nan_direction();
    }
    return lc->compare_at(m, n, *rc, nan_direction) * sort_order;
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

class MergeIterator : public ChunkIterator {
public:
    explicit MergeIterator(std::vector<ChunkIteratorPtr> children)
            : ChunkIterator(children[0]->schema(), children[0]->chunk_size()),
              _children(std::move(children)),
              _chunk_pool(_children.size()) {
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
    Status init();
    void close_child(size_t child);

    virtual Status fill(size_t child) = 0;

    // `fill()` split in two so the prologue can overlap the reads. `fill_read` only touches
    // this child's own iterator and chunk, so different children may run it concurrently;
    // `fill_commit` mutates shared merge state (heap / per-child slots) and must stay serial
    // and in child order, otherwise the merge would consume a different order than the serial
    // path. Both halves together are exactly `fill()`.
    virtual Status fill_read(size_t child) = 0;
    virtual Status fill_commit(size_t child) = 0;

    Status parallel_prefill();

    std::vector<ChunkIteratorPtr> _children;
    std::vector<ChunkPtr> _chunk_pool;
    // Status of each child's prologue read, produced by fill_read and consumed by fill_commit.
    std::vector<Status> _prefill_st;
    size_t _merged_rows = 0;
    bool _inited = false;
};

inline Status MergeIterator::init() {
    DCHECK(_chunk_size > 0);
    DCHECK_EQ(_children.size(), _chunk_pool.size());
    _prefill_st.assign(_children.size(), Status::OK());
    for (size_t i = 0; i < _children.size(); i++) {
        // No need to reserve, because it's already reserved in segment interators.
        // If we reserve here, for small segment files, it will consume large memory then need.
        _chunk_pool[i] = ChunkFactory::new_chunk(output_schema(), 0);
    }
    if (config::enable_compaction_parallel_merge_init && _children.size() > 1) {
        RETURN_IF_ERROR(parallel_prefill());
    } else {
        for (size_t i = 0; i < _children.size(); i++) {
            RETURN_IF_ERROR(fill(i));
        }
    }
    _inited = true;
    return Status::OK();
}

namespace {

// The prologue borrows no existing pool on purpose: the compaction workers are the callers here,
// and the ingestion pools serve latency-sensitive writes, so either choice would make this
// contend with unrelated work. Built once, lazily, so a BE that never enables the config never
// pays for it.
ThreadPool* merge_prefill_pool() {
    static std::unique_ptr<ThreadPool> pool = []() -> std::unique_ptr<ThreadPool> {
        std::unique_ptr<ThreadPool> p;
        int max_threads = std::max(1, config::compaction_parallel_merge_init_threads);
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

// A prefill task may itself drive a nested merge iterator. Letting the nested level submit into
// the same bounded pool and then block on it would deadlock once the pool is saturated, so a
// nested prologue always runs serially.
thread_local bool tls_in_merge_prefill = false;

} // namespace

// Reads every child concurrently, then commits them serially in child order. The commit order is
// what makes this equivalent to the serial prologue: the heap and the per-child slots see the same
// sequence of updates, and a child that fails still surfaces its error at the same point the
// serial path would.
inline Status MergeIterator::parallel_prefill() {
    ThreadPool* pool = tls_in_merge_prefill ? nullptr : merge_prefill_pool();
    if (pool == nullptr) {
        for (size_t i = 0; i < _children.size(); i++) {
            RETURN_IF_ERROR(fill(i));
        }
        return Status::OK();
    }

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

    auto* mem_tracker = tls_thread_status.mem_tracker();
    for (size_t i = 0; i < n; i++) {
        auto task = std::make_shared<std::packaged_task<void()>>([this, i, mem_tracker]() {
            // Memory tracking is thread local: without re-installing the caller's tracker the
            // bytes read here would vanish from the task's account and escape its memory limit.
            SCOPED_THREAD_LOCAL_MEM_TRACKER_SETTER(mem_tracker);
            tls_in_merge_prefill = true;
            DeferOp reset_flag([]() { tls_in_merge_prefill = false; });
            (void)fill_read(i);
        });
        auto st = pool->submit_func([task]() { (*task)(); });
        if (!st.ok()) {
            // Pool refused the task: read this child inline. Still correct, just not overlapped.
            (void)fill_read(i);
            continue;
        }
        futures.push_back(task->get_future());
    }
    for (auto& f : futures) {
        f.wait();
    }
    futures.clear();

    // Commit in child order. End-of-file and read errors are interpreted here, by the same
    // branching the serial path uses, so a caller cannot tell the two paths apart.
    for (size_t i = 0; i < n; i++) {
        RETURN_IF_ERROR(fill_commit(i));
    }
    return Status::OK();
}

inline void MergeIterator::close_child(size_t child) {
    if (_chunk_pool[child] == nullptr) {
        return;
    }
    _chunk_pool[child].reset();
    _merged_rows += _children[child]->merged_rows();
    _children[child]->close();
    _children[child].reset();
}

inline void MergeIterator::close() {
    DCHECK_EQ(_children.size(), _chunk_pool.size());
    for (size_t i = 0; i < _children.size(); i++) {
        close_child(i);
    }
    _children.clear();
    _chunk_pool.clear();
}

class HeapMergeIterator final : public MergeIterator {
public:
    explicit HeapMergeIterator(std::vector<ChunkIteratorPtr> children)
            : MergeIterator(std::move(children)), _prefill_rssid_rowids(_children.size()) {}

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
    Status fill(size_t child) override;
    Status fill_read(size_t child) override;
    Status fill_commit(size_t child) override;

private:
    // Per-child rssid/rowid buffer produced by fill_read, handed to the heap in fill_commit.
    std::vector<std::shared_ptr<vector<uint64_t>>> _prefill_rssid_rowids;

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
                    return fill(min_chunk._order);
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
            st = fill(min_chunk._order);
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

inline Status HeapMergeIterator::fill_read(size_t child) {
    Chunk* chunk = _chunk_pool[child].get();
    chunk->reset();
    if (need_rssid_rowids) {
        _prefill_rssid_rowids[child] = std::make_shared<vector<uint64_t>>();
        _prefill_st[child] = _children[child]->get_next(chunk, _prefill_rssid_rowids[child].get());
    } else {
        _prefill_st[child] = _children[child]->get_next(chunk);
    }
    return Status::OK();
}

inline Status HeapMergeIterator::fill_commit(size_t child) {
    Chunk* chunk = _chunk_pool[child].get();
    const Status& st = _prefill_st[child];
    if (st.ok()) {
        size_t num_rows = chunk->num_rows();
        DCHECK_GT(num_rows, 0u);
        if (num_rows > max_merge_chunk_size) {
            return Status::InternalError(strings::Substitute(
                    "Merge iterator only supports merging chunks with rows less than $0", max_merge_chunk_size));
        }
        if (need_rssid_rowids) {
            _heap.emplace(chunk, child, _schema.num_key_fields(), _schema.sort_key_idxes(), _schema.sort_descs(),
                          merge_condition, std::move(_prefill_rssid_rowids[child]));
        } else {
            _heap.emplace(chunk, child, _schema.num_key_fields(), _schema.sort_key_idxes(), _schema.sort_descs(),
                          merge_condition);
        }
    } else if (st.is_end_of_file()) {
        // ignore Status::EndOfFile.
        close_child(child);
    } else {
        close_child(child);
        return st;
    }
    return Status::OK();
}

inline Status HeapMergeIterator::fill(size_t child) {
    RETURN_IF_ERROR(fill_read(child));
    return fill_commit(child);
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
    explicit MaskMergeIterator(std::vector<ChunkIteratorPtr> children, RowSourceMaskBuffer* mask_buffer)
            : MergeIterator(std::move(children)), _chunks(_children.size()), _mask_buffer(mask_buffer) {
        DCHECK(_mask_buffer);
    }

protected:
    Status do_get_next(Chunk* chunk) override { return do_get_next(chunk, nullptr); }
    Status do_get_next(Chunk* chunk, std::vector<RowSourceMask>* source_masks) override;
    Status fill(size_t child) override;
    Status fill_read(size_t child) override;
    Status fill_commit(size_t child) override;

private:
    std::vector<MergingChunk> _chunks;
    RowSourceMaskBuffer* _mask_buffer = nullptr;
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
    while (st_or.value() && rows < _chunk_size) {
        RowSourceMask mask = _mask_buffer->current();
        uint16_t child = mask.get_source_num();
        auto& min_chunk = _chunks[child];
        if (min_chunk._chunk == nullptr) {
            return Status::InternalError(strings::Substitute(
                    "Mask buffer expects more rows from child $0, but child iterator is exhausted", child));
        }
        DCHECK_GT(min_chunk.remaining_rows(), 0);

        size_t offset = min_chunk.compared_row();
        size_t min_chunk_num_rows = min_chunk._chunk->num_rows();
        size_t append_row_num = 0;
        size_t max_same_source_count = _mask_buffer->max_same_source_count(child, min_chunk.remaining_rows());
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
                }
                return fill(child);
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
        }

        DCHECK_LE(rows, _chunk_size);

        if (min_chunk.remaining_rows() == 0) {
            st = fill(child);
            if (!st.ok()) {
                break;
            }
        }

        st_or = _mask_buffer->has_remaining();
        if (!st_or.ok()) {
            return st_or.status();
        }
    }
    if (!st.ok()) {
        return st;
    } else if (rows > 0) {
        return Status::OK();
    } else {
        for (auto& chunk : _chunk_pool) {
            DCHECK(chunk == nullptr);
        }
        return Status::EndOfFile("End of mask merge iterator");
    }
}

inline Status MaskMergeIterator::fill_read(size_t child) {
    Chunk* chunk = _chunk_pool[child].get();
    chunk->reset();
    _prefill_st[child] = _children[child]->get_next(chunk);
    return Status::OK();
}

inline Status MaskMergeIterator::fill_commit(size_t child) {
    Chunk* chunk = _chunk_pool[child].get();
    const Status& st = _prefill_st[child];
    if (st.ok()) {
        size_t num_rows = chunk->num_rows();
        DCHECK_GT(num_rows, 0u);
        if (num_rows > max_merge_chunk_size) {
            return Status::InternalError(strings::Substitute(
                    "Merge iterator only supports merging chunks with rows less than $0", max_merge_chunk_size));
        }
        _chunks[child] = MergingChunk(chunk);
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

inline Status MaskMergeIterator::fill(size_t child) {
    RETURN_IF_ERROR(fill_read(child));
    return fill_commit(child);
}

ChunkIteratorPtr new_mask_merge_iterator(const std::vector<ChunkIteratorPtr>& children,
                                         RowSourceMaskBuffer* mask_buffer) {
    if (children.size() == 1) {
        return children[0];
    }
    DCHECK(children.size() > 1 && children.size() <= RowSourceMask::MAX_SOURCES);
    return std::make_shared<MaskMergeIterator>(children, mask_buffer);
}

} // namespace starrocks
