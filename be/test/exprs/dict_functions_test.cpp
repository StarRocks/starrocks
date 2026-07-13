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

#include "exprs/dict_functions.h"

#include <gtest/gtest.h>

#include <limits>
#include <memory>
#include <string>
#include <vector>

#include "column/column_helper.h"
#include "column/vectorized_fwd.h"
#include "runtime/global_dict/config.h"
#include "runtime/global_dict/parser.h"
#include "runtime/global_dict/types.h"
#include "runtime/runtime_state.h"
#include "runtime/types.h"
#include "types/logical_type.h"
#include "util/slice.h"

namespace starrocks {

class EncodeDictTest : public ::testing::Test {
public:
    void SetUp() override {
        _dict_strings = {"a", "b", "c"};
        GlobalDictMap forward;
        RGlobalDictMap reverse;
        for (size_t i = 0; i < _dict_strings.size(); ++i) {
            auto code = static_cast<int32_t>(i + 1);
            forward.emplace(Slice(_dict_strings[i]), code);
            reverse.emplace(code, Slice(_dict_strings[i]));
        }
        GlobalDictMaps* dict_maps = _state.mutable_query_global_dict_map();
        dict_maps->emplace(kSlotId, std::make_pair(std::move(forward), std::move(reverse)));
        _state.mutable_dict_optimize_parser()->set_mutable_dict_maps(&_state, dict_maps);
    }

protected:
    static constexpr int32_t kSlotId = 1;
    static constexpr DictId kNoMatch = std::numeric_limits<DictId>::max();

    std::unique_ptr<FunctionContext> makeContext(const TypeDescriptor& arg0_type, RuntimeState* rs,
                                                 const Columns& constant_columns) {
        std::vector<TypeDescriptor> arg_types = {arg0_type, TypeDescriptor(TYPE_INT)};
        std::unique_ptr<FunctionContext> ctx(
                FunctionContext::create_test_context(std::move(arg_types), TypeDescriptor(TYPE_INT)));
        ctx->set_runtime_state(rs);
        ctx->set_constant_columns(constant_columns);
        return ctx;
    }

    std::vector<std::string> _dict_strings;
    RuntimeState _state;
};

TEST_F(EncodeDictTest, scalar_value_in_dict) {
    Columns columns = {ColumnHelper::create_const_column<TYPE_VARCHAR>(Slice("b"), 1),
                       ColumnHelper::create_const_column<TYPE_INT>(kSlotId, 1)};
    auto ctx = makeContext(TypeDescriptor(TYPE_VARCHAR), &_state, columns);

    ASSERT_TRUE(DictFunctions::dict_encode_prepare(ctx.get(), FunctionContext::FRAGMENT_LOCAL).ok());
    ColumnPtr result = DictFunctions::dict_encode(ctx.get(), columns).value();
    EXPECT_EQ(2, ColumnHelper::get_const_value<TYPE_INT>(result));
    ASSERT_TRUE(DictFunctions::dict_encode_close(ctx.get(), FunctionContext::FRAGMENT_LOCAL).ok());
}

TEST_F(EncodeDictTest, scalar_value_absent_uses_no_match_code) {
    Columns columns = {ColumnHelper::create_const_column<TYPE_VARCHAR>(Slice("z"), 1),
                       ColumnHelper::create_const_column<TYPE_INT>(kSlotId, 1)};
    auto ctx = makeContext(TypeDescriptor(TYPE_VARCHAR), &_state, columns);

    ASSERT_TRUE(DictFunctions::dict_encode_prepare(ctx.get(), FunctionContext::FRAGMENT_LOCAL).ok());
    ColumnPtr result = DictFunctions::dict_encode(ctx.get(), columns).value();
    EXPECT_EQ(kNoMatch, ColumnHelper::get_const_value<TYPE_INT>(result));
    ASSERT_TRUE(DictFunctions::dict_encode_close(ctx.get(), FunctionContext::FRAGMENT_LOCAL).ok());
}

TEST_F(EncodeDictTest, scalar_null_value_returns_null) {
    Columns columns = {ColumnHelper::create_const_null_column(1),
                       ColumnHelper::create_const_column<TYPE_INT>(kSlotId, 1)};
    auto ctx = makeContext(TypeDescriptor(TYPE_VARCHAR), &_state, columns);

    ASSERT_TRUE(DictFunctions::dict_encode_prepare(ctx.get(), FunctionContext::FRAGMENT_LOCAL).ok());
    ColumnPtr result = DictFunctions::dict_encode(ctx.get(), columns).value();
    EXPECT_TRUE(result->only_null());
    ASSERT_TRUE(DictFunctions::dict_encode_close(ctx.get(), FunctionContext::FRAGMENT_LOCAL).ok());
}

} // namespace starrocks
