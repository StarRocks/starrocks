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

#include "exec/pipeline/aggregate/repeat/repeat_operator.h"

#include <gtest/gtest.h>

#include <set>
#include <vector>

#include "base/testutil/assert.h"
#include "column/chunk.h"
#include "column/fixed_length_column.h"
#include "column/nullable_column.h"
#include "common/config_exec_fwd.h"
#include "common/object_pool.h"
#include "gen_cpp/Descriptors_types.h"
#include "runtime/descriptors.h"
#include "runtime/runtime_state.h"

namespace starrocks::pipeline {

// GROUPING SETS ((c0, c1), (c0), ()) over two INT columns: every input chunk is emitted three
// times, each time blanking out a different subset of the data columns and stamping three virtual
// grouping columns.
//
// The layout mirrors what RepeatNode hands the factory:
//   repeat 0 -> keeps {c0, c1}, blanks {}
//   repeat 1 -> keeps {c0},     blanks {c1}
//   repeat 2 -> keeps {},       blanks {c0, c1}
class RepeatOperatorTest : public ::testing::Test {
protected:
    static constexpr uint64_t kRepeatTimes = 3;
    static constexpr SlotId kFirstGroupingSlotId = 2;
    static constexpr int kNumRows = 3;

    void SetUp() override {
        TDescriptorTable t_desc_table;

        TTableDescriptor t_table_desc;
        t_table_desc.id = 0;
        t_table_desc.tableType = TTableType::MYSQL_TABLE;
        t_table_desc.numCols = 0;
        t_table_desc.numClusteringCols = 0;
        t_desc_table.tableDescriptors.push_back(t_table_desc);
        t_desc_table.__isset.tableDescriptors = true;

        // The output tuple RepeatNode declares; its slots are the virtual grouping columns, and
        // RepeatOperator addresses them positionally as _tuple_desc->slots()[i].
        TTupleDescriptor t_tuple_desc;
        t_tuple_desc.id = 1;
        t_desc_table.tupleDescriptors.push_back(t_tuple_desc);

        TTypeDesc type;
        {
            TTypeNode node;
            node.__set_type(TTypeNodeType::SCALAR);
            TScalarType scalar_type;
            scalar_type.__set_type(TPrimitiveType::BIGINT);
            node.__set_scalar_type(scalar_type);
            type.types.push_back(node);
        }
        for (uint64_t i = 0; i < kRepeatTimes; ++i) {
            TSlotDescriptor slot_desc;
            slot_desc.id = kFirstGroupingSlotId + i;
            slot_desc.parent = 1;
            slot_desc.slotType = type;
            t_desc_table.slotDescriptors.push_back(slot_desc);
        }

        ASSERT_TRUE(
                DescriptorTbl::create(&_state, &_obj_pool, t_desc_table, &_desc_tbl, config::vector_chunk_size).ok());
        _state.set_desc_tbl(_desc_tbl);
        _tuple_desc = _desc_tbl->get_tuple_descriptor(1);
        ASSERT_TRUE(_tuple_desc != nullptr);
        ASSERT_EQ(kRepeatTimes, _tuple_desc->slots().size());
    }

    RepeatOperatorFactory make_factory() {
        std::vector<std::set<SlotId>> slot_id_set_list = {{0, 1}, {0}, {}};
        std::set<SlotId> all_slot_ids = {0, 1};
        std::vector<std::vector<SlotId>> null_slot_ids = {{}, {1}, {0, 1}};
        std::vector<int64_t> repeat_id_list = {0, 1, 3};
        // One row per virtual grouping column, one column per repeat.
        std::vector<std::vector<int64_t>> grouping_list = {{0, 1, 3}, {0, 0, 1}, {0, 1, 3}};

        auto null_data = NullableColumn::create(Int8Column::create(), NullColumn::create());
        null_data->append_nulls(1);
        ColumnPtr column_null = ConstColumn::create(std::move(null_data), config::vector_chunk_size);

        return RepeatOperatorFactory(/*id=*/0, /*plan_node_id=*/0, std::move(slot_id_set_list), std::move(all_slot_ids),
                                     std::move(null_slot_ids), std::move(repeat_id_list), kRepeatTimes, kRepeatTimes,
                                     std::move(column_null), std::vector<Columns>{}, std::move(grouping_list),
                                     /*output_tuple_id=*/1, _tuple_desc, std::vector<ExprContext*>{});
    }

    // c0 is a plain INT column, c1 a nullable one, so that both branches of
    // generate_null_column() are exercised when they are blanked out.
    static ChunkPtr build_chunk() {
        auto c0 = Int32Column::create();
        auto c1_data = Int32Column::create();
        auto c1_null = NullColumn::create();
        for (int i = 0; i < kNumRows; ++i) {
            c0->append(i);
            c1_data->append(10 * i);
            c1_null->append(0);
        }

        auto chunk = std::make_shared<Chunk>();
        chunk->append_column(std::move(c0), 0);
        chunk->append_column(NullableColumn::create(std::move(c1_data), std::move(c1_null)), 1);
        chunk->owner_info().set_owner_id(7, /*is_last_chunk=*/true);
        return chunk;
    }

    static void expect_all_null(const ColumnPtr& column) {
        ASSERT_EQ(kNumRows, column->size());
        for (int i = 0; i < kNumRows; ++i) {
            EXPECT_TRUE(column->is_null(i));
        }
    }

    RuntimeState _state;
    ObjectPool _obj_pool;
    DescriptorTbl* _desc_tbl = nullptr;
    const TupleDescriptor* _tuple_desc = nullptr;
};

// One chunk in, three chunks out; each carries the right data columns, the right blanked-out
// columns and the right grouping values.
TEST_F(RepeatOperatorTest, RepeatsEveryGroupingSet) {
    auto factory = make_factory();
    auto op = factory.create(/*dop=*/1, /*driver_seq=*/0);

    auto input = build_chunk();
    ASSERT_TRUE(op->need_input());
    ASSERT_OK(op->push_chunk(&_state, input));

    const std::vector<std::vector<SlotId>> expected_null_slots = {{}, {1}, {0, 1}};
    const std::vector<std::vector<int64_t>> grouping_list = {{0, 1, 3}, {0, 0, 1}, {0, 1, 3}};

    for (uint64_t repeat = 0; repeat < kRepeatTimes; ++repeat) {
        ASSERT_TRUE(op->has_output()) << "repeat " << repeat;
        ASSERT_FALSE(op->need_input()) << "repeat " << repeat;
        ASSIGN_OR_ABORT(auto out, op->pull_chunk(&_state));
        ASSERT_NE(nullptr, out);

        // Two data columns plus one column per virtual grouping slot.
        EXPECT_EQ(2 + kRepeatTimes, out->num_columns());
        EXPECT_EQ(kNumRows, out->num_rows());

        const auto& nulled = expected_null_slots[repeat];
        for (SlotId slot_id : {0, 1}) {
            const auto& column = out->get_column_by_slot_id(slot_id);
            if (std::find(nulled.begin(), nulled.end(), slot_id) != nulled.end()) {
                expect_all_null(column);
            } else {
                for (int i = 0; i < kNumRows; ++i) {
                    EXPECT_FALSE(column->is_null(i));
                    EXPECT_EQ(slot_id == 0 ? i : 10 * i, column->get(i).get_int32());
                }
                // A surviving column must be a copy: the chunk is filtered in place downstream,
                // and the source chunk still has to serve the remaining repeats.
                EXPECT_NE(input->get_column_by_slot_id(slot_id).get(), column.get());
            }
        }

        for (uint64_t i = 0; i < kRepeatTimes; ++i) {
            const auto& column = out->get_column_by_slot_id(kFirstGroupingSlotId + i);
            ASSERT_EQ(kNumRows, column->size());
            EXPECT_EQ(grouping_list[i][repeat], column->get(0).get_int64());
        }

        // Bookkeeping that used to ride along on Chunk::clone_unique().
        EXPECT_EQ(7, out->owner_info().owner_id());
        EXPECT_TRUE(out->owner_info().is_last_chunk());
    }

    EXPECT_FALSE(op->has_output());
    EXPECT_TRUE(op->need_input());

    // The source chunk is untouched by the three repeats.
    EXPECT_EQ(kNumRows, input->num_rows());
    EXPECT_EQ(2, input->num_columns());
    for (int i = 0; i < kNumRows; ++i) {
        EXPECT_EQ(i, input->get_column_by_slot_id(0)->get(i).get_int32());
        EXPECT_FALSE(input->get_column_by_slot_id(1)->is_null(i));
        EXPECT_EQ(10 * i, input->get_column_by_slot_id(1)->get(i).get_int32());
    }
}

// The grouping set that blanks out every data column is the one where the output chunk is made
// entirely of columns the copy step skipped, so nothing is left to read a row count from.
TEST_F(RepeatOperatorTest, AllColumnsBlankedOutKeepsRowCount) {
    auto factory = make_factory();
    auto op = factory.create(/*dop=*/1, /*driver_seq=*/0);

    ASSERT_OK(op->push_chunk(&_state, build_chunk()));
    for (uint64_t repeat = 0; repeat < kRepeatTimes - 1; ++repeat) {
        ASSIGN_OR_ABORT(auto skipped, op->pull_chunk(&_state));
        ASSERT_NE(nullptr, skipped);
    }

    ASSIGN_OR_ABORT(auto out, op->pull_chunk(&_state));
    ASSERT_NE(nullptr, out);
    EXPECT_EQ(kNumRows, out->num_rows());
    expect_all_null(out->get_column_by_slot_id(0));
    expect_all_null(out->get_column_by_slot_id(1));
}

// A second chunk restarts the repeat cursor and is expanded from scratch.
TEST_F(RepeatOperatorTest, SecondChunkRestartsTheCursor) {
    auto factory = make_factory();
    auto op = factory.create(/*dop=*/1, /*driver_seq=*/0);

    for (int round = 0; round < 2; ++round) {
        ASSERT_TRUE(op->need_input()) << "round " << round;
        ASSERT_OK(op->push_chunk(&_state, build_chunk()));

        uint64_t produced = 0;
        while (op->has_output()) {
            ASSIGN_OR_ABORT(auto out, op->pull_chunk(&_state));
            ASSERT_NE(nullptr, out);
            EXPECT_EQ(kNumRows, out->num_rows());
            ++produced;
        }
        EXPECT_EQ(kRepeatTimes, produced) << "round " << round;
    }
}

} // namespace starrocks::pipeline
