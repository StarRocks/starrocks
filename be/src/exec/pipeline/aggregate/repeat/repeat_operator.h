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

#include "column/column_helper.h"
#include "column/vectorized_fwd.h"
#include "common/global_types.h"
#include "exec/pipeline/operator.h"
#include "exprs/expr_context.h"
#include "runtime/runtime_state.h"

namespace starrocks {
class TupleDescriptor;
namespace pipeline {
class RepeatOperator : public Operator {
public:
    RepeatOperator(OperatorFactory* factory, int32_t id, int32_t plan_node_id, int32_t driver_sequence,
                   const std::vector<std::vector<SlotId>>& null_slot_ids, uint64_t repeat_times_required,
                   uint64_t repeat_times_last, const std::vector<std::vector<int64_t>>& grouping_list,
                   const TupleDescriptor* tuple_desc, const std::vector<ExprContext*>& conjunct_ctxs)
            : Operator(factory, id, "repeat", plan_node_id, driver_sequence),
              _null_slot_ids(null_slot_ids),
              _repeat_times_required(repeat_times_required),
              _repeat_times_last(repeat_times_last),
              _grouping_list(grouping_list),
              _tuple_desc(tuple_desc),
              _conjunct_ctxs(conjunct_ctxs) {}
    ~RepeatOperator() override = default;

    bool has_output() const override;
    bool need_input() const override {
        // For every chunk, we could produce _repeat_times_required copys,
        // _repeat_times_last >= _repeat_times_required means should get next chunk.
        return _repeat_times_last >= _repeat_times_required;
    }
    bool is_finished() const override;
    Status set_finishing(RuntimeState* state) override;

    Status prepare(RuntimeState* state) override;

    StatusOr<ChunkPtr> pull_chunk(RuntimeState* state) override;

    Status push_chunk(RuntimeState* state, const ChunkPtr& chunk) override;

    Status reset_state(starrocks::RuntimeState* state, const std::vector<ChunkPtr>& refill_chunks) override;

private:
    static ColumnPtr generate_repeat_column(int64_t value, int64_t num_rows) {
        auto column = RunTimeColumnType<TYPE_BIGINT>::create();
        column->append_datum(Datum(value));
        return ConstColumn::create(column, num_rows);
    }

    void extend_and_update_columns(ChunkPtr* curr_chunk);

    /*
     * _curr_chunk
     * _curr_columns
     * This 2 fields is used privately.
     */
    // accessing chunk.
    ChunkPtr _curr_chunk;

    /*
     * _null_slot_ids
     * _repeat_times_required
     * _repeat_times_last
     * _grouping_list
     * _tuple_desc
     * 
     * This 11 fields is referenced from factory, and that is moved from RepeatNode.
     */

    const std::vector<std::vector<SlotId>>& _null_slot_ids;
    // needed repeat times
    const uint64_t _repeat_times_required;
    // repeat timer for chunk. 0 <=  _repeat_times_last < _repeat_times_required.
    uint64_t _repeat_times_last;
    // _grouping_list for grouping_id'value and grouping()/grouping_id()'s value.
    // It's a two dimensional array.
    // first is grouping index and second is repeat index.
    const std::vector<std::vector<int64_t>>& _grouping_list;
    const TupleDescriptor* _tuple_desc;

    // used for expr's compute.
    const std::vector<ExprContext*>& _conjunct_ctxs;
    // Whether prev operator has no output
    bool _is_finished = false;
};

class RepeatOperatorFactory final : public OperatorFactory {
public:
    RepeatOperatorFactory(int32_t id, int32_t plan_node_id, std::vector<std::set<SlotId>>&& slot_id_set_list,
                          std::set<SlotId>&& all_slot_ids, std::vector<std::vector<SlotId>>&& null_slot_ids,
                          std::vector<int64_t>&& repeat_id_list, uint64_t repeat_times_required,
                          uint64_t repeat_times_last, ColumnPtr&& column_null,
                          std::vector<std::vector<ColumnPtr>>&& grouping_columns,
                          std::vector<std::vector<int64_t>>&& grouping_list, TupleId output_tuple_id,
                          const TupleDescriptor* tuple_desc, std::vector<ExprContext*>&& conjunct_ctxs)
            : OperatorFactory(id, "repeat", plan_node_id),
              _slot_id_set_list(std::move(slot_id_set_list)),
              _all_slot_ids(std::move(all_slot_ids)),
              _null_slot_ids(std::move(null_slot_ids)),
              _repeat_id_list(std::move(repeat_id_list)),
              _repeat_times_required(repeat_times_required),
              _repeat_times_last(repeat_times_last),
              _column_null(std::move(column_null)),
              _grouping_columns(std::move(grouping_columns)),
              _grouping_list(std::move(grouping_list)),
              _tuple_desc(tuple_desc),
              _conjunct_ctxs(std::move(conjunct_ctxs)) {}

    ~RepeatOperatorFactory() override = default;

    OperatorPtr create(int32_t degree_of_parallelism, int32_t driver_sequence) override {
        return std::make_shared<RepeatOperator>(this, _id, _plan_node_id, driver_sequence, _null_slot_ids,
                                                _repeat_times_required, _repeat_times_last, _grouping_list, _tuple_desc,
                                                _conjunct_ctxs);
    }

    Status prepare(RuntimeState* state) override;

private:
    // Fields moved from RepeatNode.
    std::vector<std::set<SlotId>> _slot_id_set_list;
    std::set<SlotId> _all_slot_ids;
    std::vector<std::vector<SlotId>> _null_slot_ids;
    std::vector<int64_t> _repeat_id_list;
    const uint64_t _repeat_times_required;
    uint64_t _repeat_times_last;
    ColumnPtr _column_null;
    std::vector<std::vector<ColumnPtr>> _grouping_columns;
    std::vector<std::vector<int64_t>> _grouping_list;
    const TupleDescriptor* _tuple_desc;
    std::vector<ExprContext*> _conjunct_ctxs;
};
} // namespace pipeline
} // namespace starrocks
