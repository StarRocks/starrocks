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

#include "runtime/agg_state_desc.h"

#include <gtest/gtest.h>

#include <string>

#include "runtime/mem_pool.h"

namespace starrocks {

TEST(AggStateDescTest, Basic) {
    {
        std::vector<TypeDescriptor> arg_types = {TypeDescriptor::from_logical_type(TYPE_VARCHAR),
                                                 TypeDescriptor::from_logical_type(TYPE_INT)};
        auto return_type = TypeDescriptor::from_logical_type(TYPE_ARRAY);
        AggStateDesc desc("array_agg2", return_type, arg_types, true, 1);
        ASSERT_EQ("array_agg2", desc.get_func_name());
        ASSERT_EQ(true, desc.is_result_nullable());
        ASSERT_EQ(1, desc.get_func_version());
        auto* agg_func = AggStateDesc::get_agg_state_func(&desc);
        ASSERT_TRUE(agg_func != nullptr);
    }
    {
        std::vector<TypeDescriptor> arg_types = {TypeDescriptor::from_logical_type(TYPE_VARCHAR)};
        auto return_type = TypeDescriptor::from_logical_type(TYPE_VARCHAR);
        AggStateDesc desc("group_concat", return_type, arg_types, true, 1);
        ASSERT_EQ("group_concat", desc.get_func_name());
        ASSERT_EQ(true, desc.is_result_nullable());
        ASSERT_EQ(1, desc.get_func_version());
        auto* agg_func = AggStateDesc::get_agg_state_func(&desc);
        ASSERT_TRUE(agg_func != nullptr);
    }
    {
        std::vector<TypeDescriptor> arg_types = {TypeDescriptor::from_logical_type(TYPE_SMALLINT)};
        auto return_type = TypeDescriptor::from_logical_type(TYPE_DOUBLE);
        AggStateDesc desc("avg", return_type, arg_types, true, 1);
        ASSERT_EQ("avg", desc.get_func_name());
        ASSERT_EQ(true, desc.is_result_nullable());
        ASSERT_EQ(1, desc.get_func_version());
        auto* agg_func = AggStateDesc::get_agg_state_func(&desc);
        ASSERT_TRUE(agg_func != nullptr);
    }
}

} // namespace starrocks
