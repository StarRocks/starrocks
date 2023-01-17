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

#include "exprs/struct_functions.h"

#include <gtest/gtest.h>

#include "column/column_helper.h"
#include "column/struct_column.h"
#include "testutil/parallel_test.h"

namespace starrocks {

PARALLEL_TEST(StructFunctionsTest, test_row) {
    Columns input_columns;
    for (int i = 0; i < 5; ++i) {
        TypeDescriptor type;
        type.type = LogicalType::TYPE_INT;
        input_columns.emplace_back(ColumnHelper::create_column(type, true));
    }

    // append 0,1,2,3,4
    for (int i = 0; i < 5; ++i) {
        input_columns[i]->append_datum({i});
    }
    // append NULL,1,NULL,3,NULL
    for (int i = 0; i < 5; ++i) {
        if ((i % 2) == 0) {
            input_columns[i]->append_nulls(1);
        } else {
            input_columns[i]->append_datum({i});
        }
    }
    // append 5,4,3,2,1
    for (int i = 0; i < 5; ++i) {
        input_columns[i]->append_datum({5 - i});
    }

    auto ret = StructFunctions::row(nullptr, input_columns);
    ASSERT_TRUE(ret.ok());
    auto struct_col = std::move(ret).value();
    ASSERT_EQ(3, struct_col->size());

    ASSERT_STREQ("{0,1,2,3,4}", struct_col->debug_item(0).c_str());
    ASSERT_STREQ("{NULL,1,NULL,3,NULL}", struct_col->debug_item(1).c_str());
    ASSERT_STREQ("{5,4,3,2,1}", struct_col->debug_item(2).c_str());
}

} // namespace starrocks
