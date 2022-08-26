// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#pragma once

#include <algorithm>
#include <atomic>
#include <memory>

#include "column/chunk.h"
#include "column/vectorized_fwd.h"
#include "exec/pipeline/context_with_dependency.h"
#include "exec/vectorized/chunks_sorter.h"
#include "exec/vectorized/chunks_sorter_full_sort.h"
#include "exec/vectorized/chunks_sorter_topn.h"

namespace starrocks {
namespace vectorized {
class ChunksSorter;
}

namespace pipeline {
using namespace vectorized;
class SortContext;
using SortContextPtr = std::shared_ptr<SortContext>;
using SortContexts = std::vector<SortContextPtr>;
class SortContext final : public ContextWithDependency {
public:
    explicit SortContext(RuntimeState* state, int64_t offset, int64_t limit, const int32_t num_right_sinkers,
                         const std::vector<bool>& is_asc_order, const std::vector<bool>& is_null_first)
            : _state(state),
              _offset(offset),
              _limit(limit),
              _num_partition_sinkers(num_right_sinkers),
              _comparer(limit, is_asc_order, is_null_first) {
        _chunks_sorter_partions.reserve(num_right_sinkers);
        _data_segment_heaps.reserve(num_right_sinkers);
    }

    void close(RuntimeState* state) override { _chunks_sorter_partions.clear(); }

    void finish_partition(uint64_t partition_rows) {
        _total_rows.fetch_add(partition_rows, std::memory_order_relaxed);
        _num_partition_finished.fetch_add(1, std::memory_order_release);
    }

    bool is_partition_sort_finished() const {
        if (_is_partions_finish) {
            return true;
        }

        // Used to drive merge sort.
        auto is_partions_finish = _num_partition_finished.load(std::memory_order_acquire) == _num_partition_sinkers;
        if (is_partions_finish) {
            _require_rows = ((_limit < 0) ? _total_rows.load(std::memory_order_relaxed)
                                          : std::min(_limit + _offset, _total_rows.load(std::memory_order_relaxed)));
            _heapify_chunks_sorter();
            _is_partions_finish = true;
        }

        return is_partions_finish;
    }

    bool is_output_finished() const { return _next_output_row >= _require_rows; }

    // Dispatch logic for full sort and topn,
    // provide different index parrterns through lambda expression.
    ChunkPtr pull_chunk() {
        if (_limit < 0) {
            return pull_chunk([](DataSegment* min_heap_entry) -> uint32_t {
                return (*min_heap_entry->_sorted_permutation)[min_heap_entry->_next_output_row++].index_in_chunk;
            });
        } else {
            return pull_chunk(
                    [](DataSegment* min_heap_entry) -> uint32_t { return min_heap_entry->_next_output_row++; });
        }
    }

    /*
     * Output the result data in a streaming manner,
     * And dynamically adjust the heap.
     */
    // uint32_t UpdateFunc(DataSegment* min_heap_entry)
    template <class UpdateFunc>
    ChunkPtr pull_chunk(UpdateFunc&& get_and_update_min_entry_func) {
        // Get appropriate size
        uint32_t needed_rows = std::min((uint64_t)_state->chunk_size(), _require_rows - _next_output_row);

        uint32_t rows_number = 0;
        if (rows_number >= needed_rows) {
            return std::make_shared<vectorized::Chunk>();
        }

        // for data range from one DataSegment, used to collect chunks.
        std::vector<uint32_t> selective_values;
        selective_values.reserve(needed_rows);
        auto min_heap_entry = _data_segment_heaps[0];
        ChunkPtr result_chunk = min_heap_entry->chunk->clone_empty_with_slot(needed_rows);

        // Optimization for single thread.
        if (_data_segment_heaps.size() == 1) {
            for (; rows_number < needed_rows; ++rows_number) {
                selective_values.push_back(get_and_update_min_entry_func(min_heap_entry));
            }
            _update_result_chunk(result_chunk, min_heap_entry->chunk, selective_values);
            _next_output_row += rows_number;
            return result_chunk;
        }

        // get the first data
        selective_values.push_back(get_and_update_min_entry_func(min_heap_entry));
        _adjust_heap();
        ++rows_number;

        while (rows_number < needed_rows) {
            if (min_heap_entry == _data_segment_heaps[0]) {
                // data from the same data segment, just add selective value.
                selective_values.push_back(get_and_update_min_entry_func(min_heap_entry));
            } else {
                // data from different data segment, just copy datas to reuslt chunk.
                _update_result_chunk(result_chunk, min_heap_entry->chunk, selective_values);
                // re-select min-heap entry.
                min_heap_entry = _data_segment_heaps[0];
                selective_values.clear();
                selective_values.push_back(get_and_update_min_entry_func(min_heap_entry));
            }

            _adjust_heap();
            ++rows_number;
        }

        _next_output_row += rows_number;
        // last copy of data
        _update_result_chunk(result_chunk, min_heap_entry->chunk, selective_values);
        return result_chunk;
    }

    void add_partition_chunks_sorter(std::shared_ptr<ChunksSorter> chunks_sorter) {
        _chunks_sorter_partions.push_back(chunks_sorter);
    }

private:
    RuntimeState* _state;
    int64_t _offset;
    const int64_t _limit;
    // size of all chunks from all partitions.
    std::atomic<int64_t> _total_rows = 0;

    // Data that actually needs to be processed, it
    // is computed from _limit and _total_rows.
    mutable int64_t _require_rows = 0;

    const int32_t _num_partition_sinkers;
    std::atomic<int32_t> _num_partition_finished = 0;

    // Is used to gather all partition sorted chunks,
    // partition per ChunksSorter.
    std::vector<std::shared_ptr<ChunksSorter>> _chunks_sorter_partions;
    class Comparer {
    public:
        Comparer(int64_t limit, const std::vector<bool>& is_asc_order, const std::vector<bool>& is_null_first)
                : _is_topn(limit >= 0) {
            size_t col_num = is_asc_order.size();
            _sort_order_flag.resize(col_num);
            _null_first_flag.resize(col_num);
            for (size_t i = 0; i < is_asc_order.size(); ++i) {
                _sort_order_flag[i] = is_asc_order.at(i) ? 1 : -1;
                if (is_asc_order.at(i)) {
                    _null_first_flag[i] = is_null_first.at(i) ? -1 : 1;
                } else {
                    _null_first_flag[i] = is_null_first.at(i) ? 1 : -1;
                }
            }
        }

        inline bool operator()(const DataSegment* a, const DataSegment* b) {
            // We used different index pattern for topn and full sort.
            if (_is_topn) {
                return a->compare_at(a->_next_output_row, *b, b->_next_output_row, _sort_order_flag, _null_first_flag) >
                       0;
            } else {
                return a->compare_at((*a->_sorted_permutation)[a->_next_output_row].index_in_chunk, *b,
                                     (*b->_sorted_permutation)[b->_next_output_row].index_in_chunk, _sort_order_flag,
                                     _null_first_flag) > 0;
            }
        }

    private:
        const bool _is_topn;

        // This is inherited from TopNNode.
        std::vector<int> _sort_order_flag; // 1 for ascending, -1 for descending.
        std::vector<int> _null_first_flag; // 1 for greatest, -1 for least.
    };
    Comparer _comparer;

    mutable bool _is_partions_finish = false;

    // It's better to use DataSegment than ChunksSorter as heap's element.
    mutable std::vector<DataSegment*> _data_segment_heaps;

    // Construct heap for DataSegment through _data_segment_heaps.
    // DataSegment per ChunksSorter.
    void _heapify_chunks_sorter() const {
        auto num_chunks_sorter = _num_partition_sinkers;
        for (int i = 0; i < num_chunks_sorter; ++i) {
            auto data_segment = _chunks_sorter_partions[i]->get_result_data_segment();
            if (data_segment != nullptr) {
                // get size from ChunksSorter into DataSegment.
                data_segment->_partitions_rows = _chunks_sorter_partions[i]->get_partition_rows();
                // _sorted_permutation is just used for full sort to index data,
                // and topn is needn't it.
                data_segment->_sorted_permutation = _chunks_sorter_partions[i]->get_permutation();
                if (data_segment->_partitions_rows > 0) {
                    _data_segment_heaps.emplace_back(data_segment);
                }
            }
        }

        // _data_segment_heaps[0] is the toppest entry.
        std::make_heap(_data_segment_heaps.begin(), _data_segment_heaps.end(), _comparer);
    }

    // Minimum entry of heap is updated by get_and_update_min_entry_func and may be not the minimum entry anymore,
    // so pop it from the heap and push it to heap again, to make sure _data_segment_heaps as a complete heap.
    void _adjust_heap() {
        auto* old_min_heap_entry = _data_segment_heaps[0];

        // Swaps the value in the position first and the value in the position last-1
        // and makes the subrange [first, last-1) into a heap.
        std::pop_heap(_data_segment_heaps.begin(), _data_segment_heaps.end(), _comparer);

        if (old_min_heap_entry->has_next()) {
            // Inserts the element at the position last-1 into the max heap defined by the range [first, last-1).
            std::push_heap(_data_segment_heaps.begin(), _data_segment_heaps.end(), _comparer);
        } else {
            // Remove the empty data segment.
            _data_segment_heaps.pop_back();
        }
    }

    void _update_result_chunk(ChunkPtr& result, ChunkPtr& src, const std::vector<uint32_t>& selective_values) {
        if (_offset >= selective_values.size()) {
            // all data should be skipped
            _offset -= selective_values.size();
        } else {
            // skip the first `_offset` rows
            result->append_selective(*src, selective_values.data(), _offset, selective_values.size() - _offset);
        }
    }

    size_t _next_output_row = 0;
};
class SortContextFactory;
using SortContextFactoryPtr = std::shared_ptr<SortContextFactory>;
class SortContextFactory {
public:
    SortContextFactory(RuntimeState* state, bool is_merging, int64_t offset, int64_t limit, int32_t num_right_sinkers,
                       const std::vector<bool>& _is_asc_order, const std::vector<bool>& is_null_first);

    SortContextPtr create(int32_t idx);

private:
    RuntimeState* _state;
    // _is_merging is true means to merge multiple output streams of PartitionSortSinkOperators into a common
    // LocalMergeSortSourceOperator that will produce a total order output stream.
    // _is_merging is false means to pipe each output stream of PartitionSortSinkOperators to an independent
    // LocalMergeSortSourceOperator respectively for scenarios of AnalyticNode with partition by.
    const bool _is_merging;
    SortContexts _sort_contexts;
    const int64_t _offset;
    const int64_t _limit;
    const int32_t _num_right_sinkers;
    std::vector<bool> _is_asc_order;
    std::vector<bool> _is_null_first;
};
} // namespace pipeline
} // namespace starrocks
