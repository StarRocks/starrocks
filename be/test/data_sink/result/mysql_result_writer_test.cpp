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

#include "data_sink/result/mysql_result_writer.h"

#include <gtest/gtest.h>

#include <limits>

#include "base/utility/defer_op.h"
#include "column/chunk.h"
#include "column/column_helper.h"
#include "column/nullable_column.h"
#include "column/struct_column.h"
#include "common/config_network_fwd.h"
#include "common/config_thrift_server_fwd.h"
#include "common/object_pool.h"
#include "common/runtime_profile.h"
#include "common/util/thrift_util.h"
#include "compute_env/result/buffer_control_block.h"
#include "exprs/expr.h"
#include "exprs/expr_context.h"
#include "gen_cpp/Data_types.h"
#include "gen_cpp/Types_types.h"
#include "runtime/runtime_state.h"
#include "types/logical_type.h"

namespace starrocks {

namespace {

constexpr size_t kTestResultBatchFixedOverhead = 24;
constexpr size_t kTestBinaryLengthOverhead = sizeof(uint32_t);

} // namespace

class ConstColumnExpr final : public Expr {
public:
    ConstColumnExpr(const TypeDescriptor& type_desc, ColumnPtr column) : Expr(type_desc), _column(std::move(column)) {}

    Expr* clone(ObjectPool* pool) const override { return pool->add(new ConstColumnExpr(type(), _column)); }

    StatusOr<ColumnPtr> evaluate_checked(ExprContext* context, Chunk* chunk) override { return _column; }

private:
    ColumnPtr _column;
};

class MysqlResultWriterTest : public testing::Test {
protected:
    std::vector<ExprContext*> make_expr_ctxs(const std::vector<std::pair<TypeDescriptor, ColumnPtr>>& columns) {
        std::vector<ExprContext*> ctxs;
        ctxs.reserve(columns.size());
        for (const auto& [type, column] : columns) {
            auto* expr = _pool.add(new ConstColumnExpr(type, column));
            ctxs.emplace_back(_pool.add(new ExprContext(expr)));
        }
        return ctxs;
    }

    ObjectPool _pool;
};

TEST(MysqlResultWriterSizingTest, fixed_batch_overhead_matches_binary_thrift_encoding) {
    TResultBatch batch;
    batch.rows = {"", "a", "three"};

    ThriftSerializer serializer(false, 64);
    uint32_t serialized_size = 0;
    uint8_t* serialized_data = nullptr;
    ASSERT_TRUE(serializer.serialize(&batch, &serialized_size, &serialized_data).ok());
    ASSERT_NE(nullptr, serialized_data);

    size_t expected_size = kTestResultBatchFixedOverhead;
    for (const auto& row : batch.rows) {
        expected_size += kTestBinaryLengthOverhead + row.size();
    }
    EXPECT_EQ(expected_size, serialized_size);
}

TEST_F(MysqlResultWriterTest, should_set_binary_null_bit_after_fallback_column) {
    constexpr int kNumRows = 2;

    // Typed BIGINT column.
    auto bigint_col = Int64Column::create();
    bigint_col->append(11);
    bigint_col->append(22);

    // Fallback STRUCT column (non-nullable).
    auto struct_field = BinaryColumn::create();
    struct_field->append("first");
    struct_field->append("second");
    Columns struct_fields{struct_field};
    std::vector<std::string> field_names{"f"};
    auto struct_col = StructColumn::create(struct_fields, field_names);

    // Nullable BIGINT column that is NULL for the first row.
    auto nullable_data = Int64Column::create();
    nullable_data->append(999);
    nullable_data->append(888);
    auto null_flags = NullColumn::create();
    null_flags->append(1);
    null_flags->append(0);
    auto nullable_col = NullableColumn::create(nullable_data, null_flags);

    std::vector<std::pair<TypeDescriptor, ColumnPtr>> expr_columns;
    expr_columns.emplace_back(TypeDescriptor::from_logical_type(TYPE_BIGINT), bigint_col);
    expr_columns.emplace_back(
            TypeDescriptor::create_struct_type(field_names, {TypeDescriptor::from_logical_type(TYPE_VARCHAR)}),
            struct_col);
    expr_columns.emplace_back(TypeDescriptor::from_logical_type(TYPE_BIGINT), nullable_col);
    auto expr_ctxs = make_expr_ctxs(expr_columns);

    // Dummy chunk that only carries row count information.
    Chunk chunk;
    auto dummy_col = ColumnHelper::create_column(TypeDescriptor::from_logical_type(TYPE_INT), false);
    dummy_col->append_default();
    dummy_col->append_default();
    chunk.append_column(dummy_col, 0);
    chunk.set_num_rows(kNumRows);

    // Writer setup (binary protocol).
    TUniqueId query_id;
    query_id.hi = 0;
    query_id.lo = 0;
    BufferControlBlock sinker(query_id, 1024);
    ASSERT_TRUE(sinker.init().ok());

    RuntimeProfile profile("mysql_result_writer_test");
    MysqlResultWriter writer(&sinker, expr_ctxs, true, &profile);
    RuntimeState dummy_state;
    ASSERT_TRUE(writer.init(&dummy_state).ok());

    // ExprContext::evaluate() DCHECKs that each context was prepared and opened, and
    // process_chunk() evaluates them, so prepare/open before feeding the writer.
    for (auto* ctx : expr_ctxs) {
        ASSERT_TRUE(ctx->prepare(&dummy_state).ok());
        ASSERT_TRUE(ctx->open(&dummy_state).ok());
    }

    auto result_or = writer.process_chunk(&chunk);
    ASSERT_TRUE(result_or.ok());
    const auto& result_ptrs = result_or.value();
    ASSERT_EQ(1, result_ptrs.size());

    const auto& rows = result_ptrs[0]->result_batch.rows;
    ASSERT_EQ(kNumRows, rows.size());

    auto get_null_byte = [](const std::string& row_bytes) -> uint8_t {
        CHECK_GE(row_bytes.size(), size_t{2});
        return static_cast<uint8_t>(row_bytes[1]);
    };

    // Column indexes: 0 -> bigint, 1 -> struct (fallback), 2 -> nullable bigint
    const uint8_t struct_bit = 1 << ((1 + 2) & 7);   // bit mask for column index 1
    const uint8_t nullable_bit = 1 << ((2 + 2) & 7); // bit mask for column index 2

    uint8_t first_row_null_map = get_null_byte(rows[0]);
    EXPECT_EQ(nullable_bit, first_row_null_map & nullable_bit);
    EXPECT_EQ(0, first_row_null_map & struct_bit);

    uint8_t second_row_null_map = get_null_byte(rows[1]);
    EXPECT_EQ(0, second_row_null_map & nullable_bit);
    EXPECT_EQ(0, second_row_null_map & struct_bit);

    for (auto* ctx : expr_ctxs) {
        ctx->close(&dummy_state);
    }
}

TEST_F(MysqlResultWriterTest, split_pipeline_results_at_transport_boundaries) {
    const int32_t saved_thrift_limit = config::thrift_max_message_size;
    const int64_t saved_brpc_limit = config::brpc_max_body_size;
    DeferOp restore_limits([&]() {
        config::thrift_max_message_size = saved_thrift_limit;
        config::brpc_max_body_size = saved_brpc_limit;
    });

    auto varchar_col = BinaryColumn::create();
    varchar_col->append("a");
    varchar_col->append("b");
    auto expr_ctxs = make_expr_ctxs({{TypeDescriptor::create_varchar_type(1), varchar_col}});

    Chunk chunk;
    auto dummy_col = Int32Column::create();
    dummy_col->append_default(2);
    chunk.append_column(dummy_col, 0);
    chunk.set_num_rows(2);

    BufferControlBlock sinker(TUniqueId(), 1024);
    ASSERT_TRUE(sinker.init().ok());
    RuntimeProfile profile("mysql_result_writer_transport_boundary_test");
    MysqlResultWriter writer(&sinker, expr_ctxs, false, &profile);
    RuntimeState state;
    ASSERT_TRUE(writer.init(&state).ok());
    for (auto* ctx : expr_ctxs) {
        ASSERT_TRUE(ctx->prepare(&state).ok());
        ASSERT_TRUE(ctx->open(&state).ok());
    }
    DeferOp close_contexts([&]() {
        for (auto* ctx : expr_ctxs) {
            ctx->close(&state);
        }
    });

    config::thrift_max_message_size = std::numeric_limits<int32_t>::max();
    config::brpc_max_body_size = std::numeric_limits<int32_t>::max();
    auto baseline_or = writer.process_chunk(&chunk);
    ASSERT_TRUE(baseline_or.ok()) << baseline_or.status();
    ASSERT_EQ(1, baseline_or.value().size());
    const auto expected_rows = baseline_or.value()[0]->result_batch.rows;
    ASSERT_EQ(2, expected_rows.size());

    const size_t first_serialized_size = kTestBinaryLengthOverhead + expected_rows[0].size();
    const size_t second_serialized_size = kTestBinaryLengthOverhead + expected_rows[1].size();
    ASSERT_EQ(first_serialized_size, second_serialized_size);
    const int32_t exact_two_row_limit =
            static_cast<int32_t>(kTestResultBatchFixedOverhead + first_serialized_size + second_serialized_size);

    // The lower Thrift limit controls the budget, and equality stays in one batch.
    config::thrift_max_message_size = exact_two_row_limit;
    config::brpc_max_body_size = std::numeric_limits<int32_t>::max();
    auto exact_thrift_or = writer.process_chunk(&chunk);
    ASSERT_TRUE(exact_thrift_or.ok()) << exact_thrift_or.status();
    ASSERT_EQ(1, exact_thrift_or.value().size());
    EXPECT_EQ(expected_rows, exact_thrift_or.value()[0]->result_batch.rows);

    // The lower BRPC limit is treated identically.
    config::thrift_max_message_size = std::numeric_limits<int32_t>::max();
    config::brpc_max_body_size = exact_two_row_limit;
    auto exact_brpc_or = writer.process_chunk(&chunk);
    ASSERT_TRUE(exact_brpc_or.ok()) << exact_brpc_or.status();
    ASSERT_EQ(1, exact_brpc_or.value().size());
    EXPECT_EQ(expected_rows, exact_brpc_or.value()[0]->result_batch.rows);

    // One byte below the cumulative size splits before the second row.
    config::brpc_max_body_size = exact_two_row_limit - 1;
    auto split_or = writer.process_chunk(&chunk);
    ASSERT_TRUE(split_or.ok()) << split_or.status();
    ASSERT_EQ(2, split_or.value().size());
    ASSERT_EQ(1, split_or.value()[0]->result_batch.rows.size());
    ASSERT_EQ(1, split_or.value()[1]->result_batch.rows.size());
    EXPECT_EQ(expected_rows[0], split_or.value()[0]->result_batch.rows[0]);
    EXPECT_EQ(expected_rows[1], split_or.value()[1]->result_batch.rows[0]);

    // A row exactly equal to the standalone budget succeeds; each row forms its own batch.
    const int32_t exact_one_row_limit = static_cast<int32_t>(kTestResultBatchFixedOverhead + first_serialized_size);
    config::brpc_max_body_size = exact_one_row_limit;
    auto exact_one_row_or = writer.process_chunk(&chunk);
    ASSERT_TRUE(exact_one_row_or.ok()) << exact_one_row_or.status();
    ASSERT_EQ(2, exact_one_row_or.value().size());
}

TEST_F(MysqlResultWriterTest, reject_pipeline_row_one_byte_over_transport_budget) {
    const int32_t saved_thrift_limit = config::thrift_max_message_size;
    const int64_t saved_brpc_limit = config::brpc_max_body_size;
    DeferOp restore_limits([&]() {
        config::thrift_max_message_size = saved_thrift_limit;
        config::brpc_max_body_size = saved_brpc_limit;
    });

    auto varchar_col = BinaryColumn::create();
    varchar_col->append("payload");
    auto expr_ctxs = make_expr_ctxs({{TypeDescriptor::create_varchar_type(7), varchar_col}});

    Chunk chunk;
    auto dummy_col = Int32Column::create();
    dummy_col->append_default();
    chunk.append_column(dummy_col, 0);
    chunk.set_num_rows(1);

    BufferControlBlock sinker(TUniqueId(), 1024);
    ASSERT_TRUE(sinker.init().ok());
    RuntimeProfile profile("mysql_result_writer_reject_boundary_test");
    MysqlResultWriter writer(&sinker, expr_ctxs, false, &profile);
    RuntimeState state;
    ASSERT_TRUE(writer.init(&state).ok());
    for (auto* ctx : expr_ctxs) {
        ASSERT_TRUE(ctx->prepare(&state).ok());
        ASSERT_TRUE(ctx->open(&state).ok());
    }
    DeferOp close_contexts([&]() {
        for (auto* ctx : expr_ctxs) {
            ctx->close(&state);
        }
    });

    config::thrift_max_message_size = std::numeric_limits<int32_t>::max();
    config::brpc_max_body_size = std::numeric_limits<int32_t>::max();
    auto baseline_or = writer.process_chunk(&chunk);
    ASSERT_TRUE(baseline_or.ok()) << baseline_or.status();
    ASSERT_EQ(1, baseline_or.value().size());
    ASSERT_EQ(1, baseline_or.value()[0]->result_batch.rows.size());
    const size_t serialized_row_size = kTestBinaryLengthOverhead + baseline_or.value()[0]->result_batch.rows[0].size();

    config::brpc_max_body_size = static_cast<int64_t>(kTestResultBatchFixedOverhead + serialized_row_size - 1);
    auto rejected_or = writer.process_chunk(&chunk);
    ASSERT_FALSE(rejected_or.ok());
    EXPECT_TRUE(rejected_or.status().is_not_supported()) << rejected_or.status();
    EXPECT_NE(std::string::npos, rejected_or.status().message().find("exceeds batch rows budget"));
}

} // namespace starrocks
