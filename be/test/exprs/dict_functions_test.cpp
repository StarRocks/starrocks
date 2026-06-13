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

#include <memory>
#include <string>
#include <vector>

#include "base/string/slice.h"
#include "column/column_helper.h"
#include "column/const_column.h"
#include "column/vectorized_fwd.h"
#include "runtime/global_dict/fragment_dict_state.h"
#include "runtime/global_dict/types.h"
#include "runtime/runtime_state.h"
#include "types/datum.h"
#include "types/logical_type.h"
#include "types/type_descriptor.h"

namespace starrocks {

// Dictionary on slot 1: {"a" -> 1, "b" -> 2, "c" -> 3} (codes are the contiguous 1..N range).
// So the "no match" sentinel for an absent value is size + 1 = 4.
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
        _dict_state = std::make_unique<FragmentDictState>();
        _dict_state->mutable_query_global_dicts()->emplace(kSlotId,
                                                           std::make_pair(std::move(forward), std::move(reverse)));
        _state.set_fragment_dict_state(_dict_state.get());
    }

protected:
    static constexpr int32_t kSlotId = 1;
    static constexpr int32_t kNoMatch = 4; // dict size (3) + 1

    // Build a prepared FunctionContext for dict_encode(arg0_type, INT) bound to `rs`.
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
    std::unique_ptr<FragmentDictState> _dict_state;
    RuntimeState _state;
    RuntimeState _state_without_dict; // fragment_dict_state() == nullptr
};

TEST_F(EncodeDictTest, scalar_value_in_dict) {
    Columns columns = {ColumnHelper::create_const_column<TYPE_VARCHAR>(Slice("b"), 1),
                       ColumnHelper::create_const_column<TYPE_INT>(kSlotId, 1)};
    auto ctx = makeContext(TypeDescriptor(TYPE_VARCHAR), &_state, columns);

    ASSERT_TRUE(DictFunctions::dict_encode_prepare(ctx.get(), FunctionContext::FRAGMENT_LOCAL).ok());
    ColumnPtr result = DictFunctions::dict_encode(ctx.get(), columns).value();
    EXPECT_EQ(2, ColumnHelper::get_const_value<TYPE_INT>(result)); // "b" -> 2
    ASSERT_TRUE(DictFunctions::dict_encode_close(ctx.get(), FunctionContext::FRAGMENT_LOCAL).ok());
}

TEST_F(EncodeDictTest, scalar_value_absent_uses_no_match_code) {
    Columns columns = {ColumnHelper::create_const_column<TYPE_VARCHAR>(Slice("z"), 1),
                       ColumnHelper::create_const_column<TYPE_INT>(kSlotId, 1)};
    auto ctx = makeContext(TypeDescriptor(TYPE_VARCHAR), &_state, columns);

    ASSERT_TRUE(DictFunctions::dict_encode_prepare(ctx.get(), FunctionContext::FRAGMENT_LOCAL).ok());
    ColumnPtr result = DictFunctions::dict_encode(ctx.get(), columns).value();
    EXPECT_EQ(kNoMatch, ColumnHelper::get_const_value<TYPE_INT>(result)); // "z" absent -> size + 1
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

TEST_F(EncodeDictTest, array_encodes_each_element) {
    // ['b', NULL, 'z'] -> [2, NULL, no_match]
    TypeDescriptor array_varchar = TypeDescriptor::create_array_type(TypeDescriptor(TYPE_VARCHAR));
    MutableColumnPtr array = ColumnHelper::create_column(array_varchar, false);
    DatumArray elements = {Datum(Slice("b")), Datum(), Datum(Slice("z"))};
    Datum array_datum;
    array_datum.set_array(elements);
    array->append_datum(array_datum);
    ColumnPtr value = ConstColumn::create(std::move(array), 1);

    Columns columns = {value, ColumnHelper::create_const_column<TYPE_INT>(kSlotId, 1)};
    auto ctx = makeContext(array_varchar, &_state, columns);

    ASSERT_TRUE(DictFunctions::dict_encode_prepare(ctx.get(), FunctionContext::FRAGMENT_LOCAL).ok());
    ColumnPtr result = DictFunctions::dict_encode(ctx.get(), columns).value();

    DatumArray encoded = result->get(0).get_array();
    ASSERT_EQ(3, encoded.size());
    EXPECT_EQ(2, encoded[0].get_int32());        // "b" -> 2
    EXPECT_TRUE(encoded[1].is_null());           // NULL element stays NULL
    EXPECT_EQ(kNoMatch, encoded[2].get_int32()); // "z" absent -> no match
    ASSERT_TRUE(DictFunctions::dict_encode_close(ctx.get(), FunctionContext::FRAGMENT_LOCAL).ok());
}

TEST_F(EncodeDictTest, missing_dict_state_is_error) {
    Columns columns = {ColumnHelper::create_const_column<TYPE_VARCHAR>(Slice("b"), 1),
                       ColumnHelper::create_const_column<TYPE_INT>(kSlotId, 1)};
    // Runtime state has no fragment dict state, so the dict for the slot cannot be resolved.
    auto ctx = makeContext(TypeDescriptor(TYPE_VARCHAR), &_state_without_dict, columns);

    Status status = DictFunctions::dict_encode_prepare(ctx.get(), FunctionContext::FRAGMENT_LOCAL);
    EXPECT_FALSE(status.ok());
    (void)DictFunctions::dict_encode_close(ctx.get(), FunctionContext::FRAGMENT_LOCAL);
}

} // namespace starrocks
