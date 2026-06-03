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

#include "exec/analytor.h"

#include <gtest/gtest.h>

#include "column/column_helper.h"
#include "column/fixed_length_column.h"
#include "column/nullable_column.h"
#include "common/config_exec_fwd.h"
#include "gen_cpp/PlanNodes_types.h"

namespace starrocks {
class AnalytorTest : public ::testing::Test {
public:
    void SetUp() override { config::vector_chunk_size = 1024; }
};

// NOLINTNEXTLINE
TEST_F(AnalytorTest, find_peer_group_end) {
    TPlanNode plan_node;
    RowDescriptor row_desc;
    Analytor analytor(plan_node, row_desc, nullptr, false);

    int32_t v;
    auto c1 = Int32Column::create();
    v = 1;
    c1->append_value_multiple_times(&v, 10);
    v = 2;
    c1->append_value_multiple_times(&v, 10);

    analytor._input_rows += 20;
    analytor._order_columns.emplace_back(std::move(c1));
    analytor._partition.is_real = true;
    analytor._partition.end = 20;

    analytor._find_peer_group_end();
    ASSERT_TRUE(analytor._peer_group.is_real);
    ASSERT_EQ(analytor._peer_group.end, 10);
}

// NOLINTNEXTLINE
TEST_F(AnalytorTest, reset_state_for_next_partition) {
    TPlanNode plan_node;
    RowDescriptor row_desc;
    Analytor analytor(plan_node, row_desc, nullptr, false);

    analytor._partition.start = 10;
    analytor._partition.is_real = true;
    analytor._partition.end = 20;
    analytor._reset_state_for_next_partition();
    ASSERT_EQ(analytor._partition.start, 20);
    ASSERT_EQ(analytor._partition.end, 20);
    ASSERT_EQ(analytor._current_row_position, 20);
}

// NOLINTNEXTLINE
TEST_F(AnalytorTest, find_partition_end) {
    TPlanNode plan_node;
    RowDescriptor row_desc;
    Analytor analytor1(plan_node, row_desc, nullptr, false);

    int32_t v;
    auto c1 = Int32Column::create();
    v = 1;
    c1->append_value_multiple_times(&v, 10);
    v = 2;
    c1->append_value_multiple_times(&v, 10);

    auto c2 = Int32Column::create();
    v = 3;
    c2->append_value_multiple_times(&v, 5);
    v = 4;
    c2->append_value_multiple_times(&v, 15);

    analytor1._input_rows += 20;
    analytor1._input_eos = true;
    analytor1._partition_columns.emplace_back(std::move(c1));
    analytor1._partition_columns.emplace_back(std::move(c2));

    analytor1._current_row_position = analytor1._partition.end;
    analytor1._find_partition_end();
    ASSERT_TRUE(analytor1._partition.is_real);
    ASSERT_EQ(analytor1._partition.end, 5);

    analytor1._reset_state_for_next_partition();

    analytor1._current_row_position = analytor1._partition.end;
    analytor1._find_partition_end();
    ASSERT_TRUE(analytor1._partition.is_real);
    ASSERT_EQ(analytor1._partition.end, 10);

    analytor1._reset_state_for_next_partition();

    analytor1._current_row_position = analytor1._partition.end;
    analytor1._find_partition_end();
    ASSERT_TRUE(analytor1._partition.is_real);
    ASSERT_EQ(analytor1._partition.end, 20);

    // partition columns is empty
    Analytor analytor2(plan_node, row_desc, nullptr, false);
    analytor2._input_rows += 20;
    analytor1._input_eos = true;

    analytor2._current_row_position = analytor2._partition.end;
    analytor2._find_partition_end();
    ASSERT_FALSE(analytor2._partition.is_real);
    ASSERT_EQ(analytor2._partition.end, 20);

    // input rows = 0
    Analytor analytor3(plan_node, row_desc, nullptr, false);
    analytor3._input_rows = 0;
    analytor1._input_eos = true;

    analytor2._current_row_position = analytor2._partition.end;
    analytor3._find_partition_end();
    ASSERT_FALSE(analytor3._partition.is_real);
    ASSERT_EQ(analytor3._partition.end, 0);
}

// Regression for the BE crash in enable_push_down_pre_agg_with_rank: the merge-count input state buffer is
// non-nullable (serialized partial counts are non-nullable by contract), but the FE may mark the intermediate
// slot nullable so the evaluated input arrives as a no-null NullableColumn. _append_column must unwrap it into
// the bare destination; appending the NullableColumn directly would down_cast NullableColumn -> Int64Column and
// crash (SIGSEGV in release / down_cast assert in ASAN).
// NOLINTNEXTLINE
TEST_F(AnalytorTest, append_nullable_src_into_non_nullable_dst_unwraps) {
    TPlanNode plan_node;
    RowDescriptor row_desc;
    Analytor analytor(plan_node, row_desc, nullptr, false);

    // src: NullableColumn(BIGINT) with no actual nulls, values {10, 20, 30}.
    auto src_mut = ColumnHelper::create_column(TypeDescriptor(TYPE_BIGINT), /*nullable=*/true);
    src_mut->append_datum(Datum(int64_t{10}));
    src_mut->append_datum(Datum(int64_t{20}));
    src_mut->append_datum(Datum(int64_t{30}));
    ColumnPtr src = std::move(src_mut);
    ASSERT_TRUE(src->is_nullable());
    ASSERT_FALSE(down_cast<const NullableColumn*>(src.get())->has_null());

    // dst: bare BIGINT column (the non-nullable merge-count state buffer).
    auto dst = ColumnHelper::create_column(TypeDescriptor(TYPE_BIGINT), /*nullable=*/false);

    analytor._append_column(3, dst.get(), src);

    // Unwrapped into the bare buffer without crashing; values preserved, dst stays non-nullable.
    ASSERT_FALSE(dst->is_nullable());
    ASSERT_EQ(3, dst->size());
    ASSERT_EQ(10, dst->get(0).get_int64());
    ASSERT_EQ(20, dst->get(1).get_int64());
    ASSERT_EQ(30, dst->get(2).get_int64());
}

// Guard that the unwrap branch does not hijack the normal nullable -> nullable path: when the destination is
// nullable, the source NullableColumn (including its null mask) must be appended verbatim.
// NOLINTNEXTLINE
TEST_F(AnalytorTest, append_nullable_src_into_nullable_dst_preserves_nulls) {
    TPlanNode plan_node;
    RowDescriptor row_desc;
    Analytor analytor(plan_node, row_desc, nullptr, false);

    // src: NullableColumn(BIGINT) {1, NULL, 3}.
    auto src_mut = ColumnHelper::create_column(TypeDescriptor(TYPE_BIGINT), /*nullable=*/true);
    src_mut->append_datum(Datum(int64_t{1}));
    src_mut->append_nulls(1);
    src_mut->append_datum(Datum(int64_t{3}));
    ColumnPtr src = std::move(src_mut);
    ASSERT_TRUE(down_cast<const NullableColumn*>(src.get())->has_null());

    // dst: empty nullable BIGINT column.
    auto dst = ColumnHelper::create_column(TypeDescriptor(TYPE_BIGINT), /*nullable=*/true);

    analytor._append_column(3, dst.get(), src);

    ASSERT_TRUE(dst->is_nullable());
    ASSERT_EQ(3, dst->size());
    ASSERT_FALSE(dst->is_null(0));
    ASSERT_TRUE(dst->is_null(1));
    ASSERT_FALSE(dst->is_null(2));
}

} // namespace starrocks
