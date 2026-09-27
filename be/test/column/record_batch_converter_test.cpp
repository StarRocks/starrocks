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

#include "column/arrow/record_batch_converter.h"

#include <arrow/array.h>
#include <arrow/memory_pool.h>
#include <arrow/record_batch.h>
#include <arrow/type.h>
#include <gtest/gtest.h>

#include "base/testutil/assert.h"
#include "column/fixed_length_column.h"
#include "types/type_descriptor.h"

namespace starrocks {

TEST(RecordBatchConverterTest, ConvertsColumnsAndSerializesBatches) {
    auto column = Int32Column::create();
    column->append(7);
    column->append(8);

    Columns columns;
    columns.emplace_back(std::move(column));
    TypeDescriptor type_descs[] = {TypeDescriptor(TYPE_INT)};
    auto schema = arrow::schema({arrow::field("c0", arrow::int32(), false)});

    std::shared_ptr<arrow::RecordBatch> batch;
    ASSERT_OK(convert_columns_to_arrow_batch(2, columns, arrow::default_memory_pool(), type_descs, schema, &batch));
    ASSERT_NE(nullptr, batch);
    ASSERT_EQ(2, batch->num_rows());
    ASSERT_EQ(1, batch->num_columns());
    auto array = std::static_pointer_cast<arrow::Int32Array>(batch->column(0));
    EXPECT_EQ(7, array->Value(0));
    EXPECT_EQ(8, array->Value(1));

    std::string payload;
    ASSERT_OK(serialize_record_batch(*batch, &payload));
    EXPECT_FALSE(payload.empty());

    std::string schema_payload;
    ASSERT_OK(serialize_arrow_schema(&schema, &schema_payload));
    EXPECT_FALSE(schema_payload.empty());
}

} // namespace starrocks
