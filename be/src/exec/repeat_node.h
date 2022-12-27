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
#include "exec/exec_node.h"

namespace starrocks {
class DescriptorTbl;
class SlotDescriptor;
class TupleDescriptor;
} // namespace starrocks

namespace starrocks {
class RepeatNode final : public ExecNode {
public:
    RepeatNode(ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs);
    Status prepare(RuntimeState* state) override;
    Status open(RuntimeState* state) override;
    Status get_next(RuntimeState* state, ChunkPtr* chunk, bool* eos) override;
    Status close(RuntimeState* state) override;

    std::vector<std::shared_ptr<pipeline::OperatorFactory>> decompose_to_pipeline(
            pipeline::PipelineBuilderContext* context) override;

private:
    static ColumnPtr generate_null_column(int64_t num_rows) {
        auto nullable_column = NullableColumn::create(Int8Column::create(), NullColumn::create());
        nullable_column->append_nulls(1);
        return ConstColumn::create(nullable_column, num_rows);
    }

    static ColumnPtr generate_repeat_column(int64_t value, int64_t num_rows) {
        auto ptr = RunTimeColumnType<TYPE_BIGINT>::create();
        ptr->append_datum(Datum(value));
        return ConstColumn::create(ptr, num_rows);
    }

    void extend_and_update_columns(ChunkPtr* curr_chunk, ChunkPtr* chunk);

    // Slot id set used to indicate those slots need to set to null.
    std::vector<std::set<SlotId>> _slot_id_set_list;
    // all slot id
    std::set<SlotId> _all_slot_ids;
    std::vector<std::vector<SlotId>> _null_slot_ids;
    // An integer bitmap list, it indicates the bit position of the exprs not null.
    std::vector<int64_t> _repeat_id_list;

    // needed repeat times
    uint64_t _repeat_times_required;

    // repeat timer for chunk. 0 <=  _repeat_times_last < _repeat_times_required.
    uint64_t _repeat_times_last;

    // accessing chunk.
    ChunkPtr _curr_chunk;

    // only null columns for reusing, It has chunk_size rows.
    ColumnPtr _column_null;

    // column for grouping_id and virtual columns for grouping()/grouping_id() for reusing.
    // It has chunk_size rows.
    std::vector<std::vector<ColumnPtr>> _grouping_columns;

    // _grouping_list for gourping_id'value and grouping()/grouping_id()'s value.
    // It's a two dimensional array.
    // first is grouping index and second is repeat index.
    std::vector<std::vector<int64_t>> _grouping_list;

    // Tulple id used for output, it has new slots.
    TupleId _output_tuple_id;
    const TupleDescriptor* _tuple_desc;

    // time to append columns for grouping_id column and grouping()/grouping_id()'s virtual columns.
    RuntimeProfile::Counter* _extend_column_timer = nullptr;

    // time to copy/assign/move columns between chunk and columns.
    RuntimeProfile::Counter* _copy_column_timer = nullptr;

    // time to update columns for grouping_id column and grouping()/grouping_id()'s virtual columns.
    // and
    // time to set null_column for unneed colums.
    RuntimeProfile::Counter* _update_column_timer = nullptr;
};

} // namespace starrocks
