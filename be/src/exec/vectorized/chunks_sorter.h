// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#pragma once

#include "column/vectorized_fwd.h"
#include "exec/sort_exec_exprs.h"
#include "exec/vectorized/sorting/sort_permute.h"
#include "exec/vectorized/sorting/sorting.h"
#include "exprs/expr_context.h"
#include "runtime/descriptors.h"
#include "util/runtime_profile.h"

namespace starrocks::vectorized {

struct DataSegment {
    static const uint8_t BEFORE_LAST_RESULT = 2;
    static const uint8_t IN_LAST_RESULT = 1;

    ChunkPtr chunk;
    Columns order_by_columns;

    DataSegment() : chunk(std::make_shared<Chunk>()) {}

    DataSegment(const std::vector<ExprContext*>* sort_exprs, const ChunkPtr& cnk) { init(sort_exprs, cnk); }

    int64_t mem_usage() const { return chunk->memory_usage(); }

    void init(const std::vector<ExprContext*>* sort_exprs, const ChunkPtr& cnk) {
        chunk = cnk;
        order_by_columns.reserve(sort_exprs->size());
        for (ExprContext* expr_ctx : (*sort_exprs)) {
            order_by_columns.push_back(EVALUATE_NULL_IF_ERROR(expr_ctx, expr_ctx->root(), chunk.get()));
        }
    }

    // there is two compares in the method,
    // the first is:
    //     compare every row in every DataSegment of data_segments with number_of_rows_to_sort - 1 row of this DataSegment,
    //     obtain every row compare result in compare_results_array, if < 0, use it to set IN at filter_array.
    // the second is:
    //     compare every row in compare_results_array that less than 0, use it to compare with first row of this DataSegment,
    //     as the first step, we set BEFORE_LAST_RESULT at filter_array.
    //
    // Actually, we Count the results in the first compare for the second compare.
    Status get_filter_array(std::vector<DataSegment>& data_segments, size_t number_of_rows_to_sort,
                            std::vector<std::vector<uint8_t>>& filter_array, const std::vector<int>& sort_order_flags,
                            const std::vector<int>& null_first_flags, uint32_t& least_num, uint32_t& middle_num);

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
                 const std::vector<bool>* is_asc_order, const std::vector<bool>* is_null_first,
                 const std::string& sort_keys, const bool is_topn);
    virtual ~ChunksSorter();

    static StatusOr<vectorized::ChunkPtr> materialize_chunk_before_sort(vectorized::Chunk* chunk,
                                                                        TupleDescriptor* materialized_tuple_desc,
                                                                        const SortExecExprs& sort_exec_exprs,
                                                                        const std::vector<OrderByType>& order_by_types);

    virtual void setup_runtime(RuntimeProfile* profile);

    // Append a Chunk for sort.
    virtual Status update(RuntimeState* state, const ChunkPtr& chunk) = 0;
    // Finish seeding Chunk, and get sorted data with top OFFSET rows have been skipped.
    virtual Status done(RuntimeState* state) = 0;
    // get_next only works after done().
    virtual void get_next(ChunkPtr* chunk, bool* eos) = 0;

    // Return sorted data in multiple runs(Avoid merge them into a big chunk)
    virtual SortedRuns get_sorted_runs() = 0;

    // Return accurate output rows of this operator
    virtual size_t get_output_rows() const = 0;

    Status finish(RuntimeState* state);

    bool sink_complete();

    // Pull chunk version for pipeline.
    // Return false if there is no more chunks
    virtual bool pull_chunk(ChunkPtr* chunk) = 0;

    virtual int64_t mem_usage() const = 0;

    // For test only
    void set_compare_strategy(CompareStrategy cmp) { _compare_strategy = cmp; }

protected:
    size_t _get_number_of_order_by_columns() const { return _sort_exprs->size(); }

    RuntimeState* _state;

    // sort rules
    const std::vector<ExprContext*>* _sort_exprs;
    // TODO: refactor it with SortDesc
    std::vector<int> _sort_order_flag; // 1 for ascending, -1 for descending.
    std::vector<int> _null_first_flag; // 1 for greatest, -1 for least.
    const std::string _sort_keys;
    const bool _is_topn;

    size_t _next_output_row = 0;

    RuntimeProfile::Counter* _build_timer = nullptr;
    RuntimeProfile::Counter* _sort_timer = nullptr;
    RuntimeProfile::Counter* _merge_timer = nullptr;
    RuntimeProfile::Counter* _output_timer = nullptr;

    std::atomic<bool> _is_sink_complete = false;

    CompareStrategy _compare_strategy = Default;
};

} // namespace starrocks::vectorized
