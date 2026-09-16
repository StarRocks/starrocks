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

#include "exprs/udf/java/arrow_udf_jni.h"

#include <arrow/c/abi.h>
#include <gtest/gtest.h>

#include "base/testutil/assert.h"
#include "column/fixed_length_column.h"
#include "types/type_descriptor.h"

namespace starrocks {

// ExportedArrowBatch exports StarRocks columns to the Arrow C Data Interface without needing a JVM,
// so the export side of the vectorized UDF path is unit-testable directly.
TEST(ExportedArrowBatchTest, ExportColumnsPopulatesCStructs) {
    auto column = Int32Column::create();
    column->append(1);
    column->append(2);
    column->append(3);
    Columns columns;
    columns.emplace_back(std::move(column));
    TypeDescriptor type_descs[] = {TypeDescriptor(TYPE_INT)};

    ExportedArrowBatch batch;
    ASSERT_OK(batch.export_columns(3, columns, type_descs));

    auto* array = reinterpret_cast<ArrowArray*>(batch.array_addr());
    auto* schema = reinterpret_cast<ArrowSchema*>(batch.schema_addr());
    EXPECT_EQ(3, array->length);
    // The exporter installs release callbacks; the destructor invokes them (leak-guard).
    EXPECT_NE(nullptr, array->release);
    EXPECT_NE(nullptr, schema->release);
}

// The raw-pointer overload (used by the UDAF path, which receives const Column**) builds a
// non-owning view internally and delegates to the Columns overload.
TEST(ExportedArrowBatchTest, ExportRawColumnPointers) {
    auto column = Int32Column::create();
    column->append(7);
    column->append(8);
    TypeDescriptor type_descs[] = {TypeDescriptor(TYPE_INT)};
    const Column* raw[] = {column.get()};

    ExportedArrowBatch batch;
    ASSERT_OK(batch.export_columns(2, raw, 1, type_descs));
    EXPECT_EQ(2, reinterpret_cast<ArrowArray*>(batch.array_addr())->length);
}

} // namespace starrocks
