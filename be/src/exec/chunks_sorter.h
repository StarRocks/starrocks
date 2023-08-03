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

#include <memory>

#include "column/column_helper.h"
#include "column/vectorized_fwd.h"
#include "common/object_pool.h"
#include "exec/pipeline/spill_process_channel.h"
#include "exec/sort_exec_exprs.h"
#include "exec/sorting/sort_permute.h"
#include "exec/sorting/sorting.h"
#include "exec/spill/executor.h"
#include "exec/spill/spiller.h"
#include "exprs/expr_context.h"
#include "exprs/runtime_filter.h"
#include "runtime/descriptors.h"
#include "runtime/runtime_state.h"
#include "util/runtime_profile.h"

namespace starrocks {

struct DataSegment {
    static const uint8_t SMALLER_THAN_MIN_OF_SEGMENT = 2;
    static const uint8_t INCLUDE_IN_SEGMENT = 1;
    static const uint8_t LARGER_THAN_MAX_OF_SEGMENT = 0;

    ChunkPtr chunk;
    Columns order_by_columns;

    DataSegment() : chunk(std::make_shared<Chunk>()) {}

    DataSegment(const std::vector<ExprContext*>* sort_exprs, const ChunkPtr& cnk) { init(sort_exprs, cnk); }

    int64_t mem_usage() const { return chunk->memory_usage(); }

    void init(const std::vector<ExprContext*>* sort_exprs, const ChunkPtr& cnk);

    // There is two compares in the method,
    // the first is:
    //     compare every row in every DataSegment of data_segments with `rows_to_sort - 1` row of this DataSegment,
    //     obtain every row compare result in compare_results_array, if <= 0, mark it with `INCLUDE_IN_SEGMENT`.
    // the second is:
    //     compare every row in compare_results_array that <= 0 (i.e. `INCLUDE_IN_SEGMENT` part) with the first row of this DataSegment,
    //     if < 0, then mark it with `SMALLER_THAN_MIN_OF_SEGMENT`
    Status get_filter_array(std::vector<DataSegment>& data_segments, size_t rows_to_sort,
                            std::vector<std::vector<uint8_t>>& filter_array, const SortDescs& sort_order_flags,
                            uint32_t& least_num, uint32_t& middle_num);

    void clear() {
        chunk.reset(std::make_unique<Chunk>().release());
        order_by_columns.clear();
    }

    // Return value:
    //  < 0: current row precedes the row in the other chunk;
    // == 0: current row is equal to the row in the other chunk;
    //  > 0: current row succeeds the row in the other chunk;
    int compare_at(size_t index_in_chunk, const DataSegment& other, size_t index_in_other_chunk,
                   const std::vector<int>& sort_order_flag, const std::vector<int>& null_first_flag) const {
        size_t col_number = order_by_columns.size();
        for (size_t col_index = 0; col_index < col_number; ++col_index) {
            const auto& left_col = order_by_columns[col_index];
            const auto& right_col = other.order_by_columns[col_index];
            int c = left_col->compare_at(index_in_chunk, index_in_other_chunk, *right_col, null_first_flag[col_index]);
            if (c != 0) {
                return c * sort_order_flag[col_index];
            }
        }
        return 0;
    }
};
using DataSegments = std::vector<DataSegment>;

class SortedRuns;
class ChunksSorter;
using ChunksSorterPtr = std::shared_ptr<ChunksSorter>;
using ChunksSorters = std::vector<ChunksSorterPtr>;

// Sort Chunks in memory with specified order by rules.
class ChunksSorter {
public:
    static constexpr int USE_HEAP_SORTER_LIMIT_SZ = 1024;

    /**
     * Constructor.
     * @param sort_exprs            The order-by columns or columns with expression. This sorter will use but not own the object.
     * @param is_asc_order          Orders on each column.
     * @param is_null_first         NULL values should at the head or tail.
     * @param size_of_chunk_batch   In the case of a positive limit, this parameter limits the size of the batch in Chunk unit.
     */
    ChunksSorter(RuntimeState* state, const std::vector<ExprContext*>* sort_exprs,
                 const std::vector<bool>* is_asc_order, const std::vector<bool>* is_null_first, std::string sort_keys,
                 const bool is_topn);
    virtual ~ChunksSorter();

    static StatusOr<ChunkPtr> materialize_chunk_before_sort(Chunk* chunk, TupleDescriptor* materialized_tuple_desc,
                                                            const SortExecExprs& sort_exec_exprs,
                                                            const std::vector<OrderByType>& order_by_types);

    virtual void setup_runtime(RuntimeProfile* profile, MemTracker* parent_mem_tracker);

    void set_spiller(std::shared_ptr<spill::Spiller> spiller) { _spiller = std::move(spiller); }

    void set_spill_channel(SpillProcessChannelPtr channel) { _spill_channel = std::move(channel); }
    const SpillProcessChannelPtr& spill_channel() { return _spill_channel; }
    auto& io_executor() { return *spill_channel()->io_executor(); }

    // Append a Chunk for sort.
    virtual Status update(RuntimeState* state, const ChunkPtr& chunk) = 0;
    // Finish seeding Chunk, and get sorted data with top OFFSET rows have been skipped.
    virtual Status do_done(RuntimeState* state) = 0;

    Status done(RuntimeState* state);

    // get_next only works after done().
    virtual Status get_next(ChunkPtr* chunk, bool* eos) = 0;

    // RuntimeFilter generate by ChunkSorter only works in TopNSorter and HeapSorter
    virtual std::vector<JoinRuntimeFilter*>* runtime_filters(ObjectPool* pool) { return nullptr; }

    // Return accurate output rows of this operator
    virtual size_t get_output_rows() const = 0;

    virtual int64_t mem_usage() const = 0;

    virtual bool is_full() { return false; }

    virtual bool has_pending_data() { return false; }

    const std::shared_ptr<spill::Spiller>& spiller() const { return _spiller; }

    size_t revocable_mem_bytes() const { return _revocable_mem_bytes; }
    void set_spill_stragety(spill::SpillStrategy stragety) { _spill_strategy = stragety; }

    virtual size_t reserved_bytes(const ChunkPtr& chunk) { return chunk != nullptr ? chunk->memory_usage() : 0; }

    virtual void cancel() {}

protected:
    size_t _get_number_of_order_by_columns() const { return _sort_exprs->size(); }

    RuntimeState* _state;

    // sort rules
    const std::vector<ExprContext*>* _sort_exprs;
    const SortDescs _sort_desc;
    const std::string _sort_keys;
    const bool _is_topn;

    size_t _next_output_row = 0;

    RuntimeProfile::Counter* _build_timer = nullptr;
    RuntimeProfile::Counter* _sort_timer = nullptr;
    RuntimeProfile::Counter* _merge_timer = nullptr;
    RuntimeProfile::Counter* _output_timer = nullptr;

    size_t _revocable_mem_bytes = 0;
    spill::SpillStrategy _spill_strategy = spill::SpillStrategy::NO_SPILL;
    std::shared_ptr<spill::Spiller> _spiller;
    SpillProcessChannelPtr _spill_channel;
};

namespace detail {
struct SortRuntimeFilterBuilder {
    template <LogicalType ltype>
    JoinRuntimeFilter* operator()(ObjectPool* pool, const ColumnPtr& column, int rid, bool asc,
                                  bool is_close_interval) {
        auto data_column = ColumnHelper::get_data_column(column.get());
        auto runtime_data_column = down_cast<RunTimeColumnType<ltype>*>(data_column);
        auto data = runtime_data_column->get_data()[rid];
        if (asc) {
            return RuntimeBloomFilter<ltype>::template create_with_range<false>(pool, data, is_close_interval);
        } else {
            return RuntimeBloomFilter<ltype>::template create_with_range<true>(pool, data, is_close_interval);
        }
    }
};

struct SortRuntimeFilterUpdater {
    template <LogicalType ltype>
    std::nullptr_t operator()(JoinRuntimeFilter* filter, const ColumnPtr& column, int rid, bool asc) {
        auto data_column = ColumnHelper::get_data_column(column.get());
        auto runtime_data_column = down_cast<RunTimeColumnType<ltype>*>(data_column);
        auto data = runtime_data_column->get_data()[rid];
        if (asc) {
            down_cast<RuntimeBloomFilter<ltype>*>(filter)->template update_min_max<false>(data);
        } else {
            down_cast<RuntimeBloomFilter<ltype>*>(filter)->template update_min_max<true>(data);
        }
        return nullptr;
    }
};
} // namespace detail

} // namespace starrocks
