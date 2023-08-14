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

PARALLEL_TEST(StructFunctionsTest, test_new_struct) {
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

    FunctionContext::TypeDesc ret_type;
    FunctionContext::TypeDesc cint;
    std::vector<FunctionContext::TypeDesc> arg_types;
    cint.type = starrocks::TYPE_INT;
    ret_type.type = starrocks::TYPE_STRUCT;
    for (int i = 0; i < 5; ++i) {
        ret_type.field_names.emplace_back(fmt::format("col{}", (i + 1)));
        ret_type.children.emplace_back(cint);
        arg_types.emplace_back(cint);
    }

    FunctionContext* context = FunctionContext::create_context(nullptr, nullptr, ret_type, arg_types);

    auto ret = StructFunctions::new_struct(context, input_columns);
    delete context;
    ASSERT_TRUE(ret.ok());
    auto struct_col = std::move(ret).value();
    ASSERT_EQ(3, struct_col->size());

    ASSERT_STREQ("{col1:0,col2:1,col3:2,col4:3,col5:4}", struct_col->debug_item(0).c_str());
    ASSERT_STREQ("{col1:NULL,col2:1,col3:NULL,col4:3,col5:NULL}", struct_col->debug_item(1).c_str());
    ASSERT_STREQ("{col1:5,col2:4,col3:3,col4:2,col5:1}", struct_col->debug_item(2).c_str());
}

PARALLEL_TEST(StructFunctionsTest, test_named_struct) {
    Columns input_columns;
    for (int i = 0; i < 6; ++i) {
        TypeDescriptor type;
        type.type = LogicalType::TYPE_INT;
        input_columns.emplace_back(ColumnHelper::create_column(type, true));
    }

    for (int i = 0; i < 6; ++i) {
        input_columns[i]->append_datum({i});
    }
    for (int i = 0; i < 6; ++i) {
        if ((i % 2) == 0) {
            input_columns[i]->append_nulls(1);
        } else {
            input_columns[i]->append_datum({1});
        }
    }
    for (int i = 0; i < 6; ++i) {
        input_columns[i]->append_nulls(1);
    }

    FunctionContext::TypeDesc ret_type;
    FunctionContext::TypeDesc cint;
    std::vector<FunctionContext::TypeDesc> arg_types;
    cint.type = starrocks::TYPE_INT;
    ret_type.type = starrocks::TYPE_STRUCT;
    for (int i = 0; i < 3; ++i) {
        ret_type.field_names.emplace_back(fmt::format("col{}", (i + 1)));
        ret_type.children.emplace_back(cint);
    }
    for (int i = 0; i < 6; ++i) {
        arg_types.emplace_back(cint);
    }

    FunctionContext* context = FunctionContext::create_context(nullptr, nullptr, ret_type, arg_types);

    auto ret = StructFunctions::named_struct(context, input_columns);
    delete context;
    ASSERT_TRUE(ret.ok());
    auto struct_col = std::move(ret).value();
    ASSERT_EQ(3, struct_col->size());

    ASSERT_STREQ("{col1:1,col2:3,col3:5}", struct_col->debug_item(0).c_str());
    ASSERT_STREQ("{col1:1,col2:1,col3:1}", struct_col->debug_item(1).c_str());
    ASSERT_STREQ("{col1:NULL,col2:NULL,col3:NULL}", struct_col->debug_item(2).c_str());
}

} // namespace starrocks
