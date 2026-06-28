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

#include <arrow/array.h>
#include <arrow/memory_pool.h>
#include <arrow/record_batch.h>
#include <arrow/type.h>
#include <gtest/gtest.h>

#include "base/testutil/assert.h"
#include "column/chunk.h"
#include "column/fixed_length_column.h"
#include "exec/arrow/result_to_arrow_converter.h"
#include "exprs/column_ref.h"
#include "exprs/expr_context.h"
#include "runtime/descriptors.h"
#include "runtime/runtime_state.h"
#include "types/type_descriptor.h"

namespace starrocks {

TEST(ResultToArrowConverterExecPrimitiveTest, BuildsSchemaAndConvertsChunkThroughExprs) {
    constexpr TupleId kTupleId = 2;
    constexpr SlotId kSlotId = 5;

    ColumnRef ref(TypeDescriptor(TYPE_INT), kSlotId);
    ref.set_tuple_id(kTupleId);
    ExprContext expr_context(&ref);
    RuntimeState runtime_state{TQueryGlobals()};
    ASSERT_OK(expr_context.prepare(&runtime_state));
    ASSERT_OK(expr_context.open(&runtime_state));
    std::vector<ExprContext*> output_expr_ctxs{&expr_context};

    RowDescriptor row_desc;
    std::unordered_map<int64_t, std::string> id_to_col_name{{(int64_t{kTupleId} << 32) | kSlotId, "answer"}};
    std::shared_ptr<arrow::Schema> schema;
    ASSERT_OK(convert_to_arrow_schema(row_desc, id_to_col_name, &schema, output_expr_ctxs));
    ASSERT_NE(nullptr, schema);
    ASSERT_EQ(1, schema->num_fields());
    EXPECT_EQ("answer", schema->field(0)->name());
    EXPECT_EQ(arrow::Type::INT32, schema->field(0)->type()->id());

    auto column = Int32Column::create();
    column->append(42);
    column->append(43);
    Chunk chunk;
    chunk.append_column(std::move(column), kSlotId);

    std::shared_ptr<arrow::RecordBatch> batch;
    ASSERT_OK(convert_chunk_to_arrow_batch(&chunk, output_expr_ctxs, schema, arrow::default_memory_pool(), &batch));
    ASSERT_NE(nullptr, batch);
    ASSERT_EQ(2, batch->num_rows());
    auto array = std::static_pointer_cast<arrow::Int32Array>(batch->column(0));
    EXPECT_EQ(42, array->Value(0));
    EXPECT_EQ(43, array->Value(1));
}

TEST(ResultToArrowConverterExecPrimitiveTest, ConvertsChunkBySlotIds) {
    constexpr SlotId kSlotId = 7;

    auto column = Int32Column::create();
    column->append(11);
    column->append(12);
    Chunk chunk;
    chunk.append_column(std::move(column), kSlotId);

    auto schema = arrow::schema({arrow::field("v", arrow::int32(), false)});
    TypeDescriptor type(TYPE_INT);
    std::vector<const TypeDescriptor*> slot_types{&type};
    std::vector<SlotId> slot_ids{kSlotId};

    std::shared_ptr<arrow::RecordBatch> batch;
    ASSERT_OK(convert_chunk_to_arrow_batch(&chunk, slot_types, slot_ids, schema, arrow::default_memory_pool(), &batch));
    ASSERT_NE(nullptr, batch);
    ASSERT_EQ(2, batch->num_rows());
    auto array = std::static_pointer_cast<arrow::Int32Array>(batch->column(0));
    EXPECT_EQ(11, array->Value(0));
    EXPECT_EQ(12, array->Value(1));
}

} // namespace starrocks
