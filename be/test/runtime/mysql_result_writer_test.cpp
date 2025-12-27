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

#include "runtime/mysql_result_writer.h"

#include <gtest/gtest.h>

#include "column/chunk.h"
#include "column/column_helper.h"
#include "column/nullable_column.h"
#include "column/struct_column.h"
#include "common/object_pool.h"
#include "exprs/expr.h"
#include "exprs/expr_context.h"
#include "gen_cpp/Types_types.h"
#include "runtime/buffer_control_block.h"
#include "runtime/runtime_profile.h"
#include "types/logical_type.h"

namespace starrocks {

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
    ASSERT_TRUE(writer.init(nullptr).ok());

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

    Expr::close(expr_ctxs);
}

} // namespace starrocks
