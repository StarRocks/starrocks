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

#include <gtest/gtest.h>

#include <memory>

#include "exprs/agg/base_aggregate_test.h"

namespace starrocks {

class ArrayAggTest : public testing::Test {
public:
    void SetUp() override {
        utils = new FunctionUtils();
        ctx = utils->get_fn_ctx();
        _allocator = std::make_unique<CountingAllocatorWithHook>();
        tls_agg_state_allocator = _allocator.get();
    }
    void TearDown() override {
        delete utils;
        tls_agg_state_allocator = nullptr;
        _allocator.reset();
    }

protected:
    FunctionUtils* utils{};
    FunctionContext* ctx{};
    std::unique_ptr<CountingAllocatorWithHook> _allocator;
};

TEST_F(ArrayAggTest, test_array_aggV2) {
    std::vector<TypeDescriptor> arg_types = {TYPE_VARCHAR_DESC, TYPE_INT_DESC};
    auto return_type = TypeDescriptor::from_logical_type(TYPE_ARRAY);
    std::unique_ptr<RuntimeState> runtime_state = std::make_unique<RuntimeState>();
    std::unique_ptr<FunctionContext> local_ctx(FunctionContext::create_test_context(std::move(arg_types), return_type));
    std::vector<bool> is_asc_order{false};
    std::vector<bool> nulls_first{true};
    local_ctx->set_is_asc_order(is_asc_order);
    local_ctx->set_nulls_first(nulls_first);
    local_ctx->set_runtime_state(runtime_state.get());

    const AggregateFunction* array_agg_func = get_aggregate_function("array_agg2", TYPE_BIGINT, TYPE_ARRAY, false);
    auto state = ManagedAggrState::create(local_ctx.get(), array_agg_func);

    // nullable columns input
    {
        MutableColumnPtr char_column = ColumnHelper::create_column(TYPE_VARCHAR_DESC, true);
        char_column->append_datum(Datum());
        char_column->append_datum("bcd");
        char_column->append_datum("cdrdfe");
        char_column->append_datum(Datum());
        char_column->append_datum("esfg");

        auto int_type = TYPE_INT_DESC;
        MutableColumnPtr int_column = ColumnHelper::create_column(int_type, true);
        int_column->append_datum(Datum());
        int_column->append_datum(9);
        int_column->append_datum(Datum());
        int_column->append_datum(7);
        int_column->append_datum(6);

        std::vector<const Column*> raw_columns;
        Columns columns;
        columns.emplace_back(char_column);
        columns.emplace_back(int_column);
        raw_columns.resize(2);
        raw_columns[0] = char_column.get();
        raw_columns[1] = int_column.get();

        // test update
        array_agg_func->update_batch_single_state(local_ctx.get(), int_column->size(), raw_columns.data(),
                                                  state->state());
        auto agg_state = (ArrayAggAggregateStateV2*)(state->state());
        ASSERT_EQ(agg_state->data_columns.size(), 2);
        // data_columns in state are nullable
        ASSERT_EQ((agg_state->data_columns)[0]->debug_string(), char_column->debug_string());
        ASSERT_EQ((agg_state->data_columns)[1]->debug_string(), int_column->debug_string());

        TypeDescriptor type_array_char;
        type_array_char.type = LogicalType::TYPE_ARRAY;
        type_array_char.children.emplace_back(LogicalType::TYPE_VARCHAR);

        TypeDescriptor type_array_int;
        type_array_int.type = LogicalType::TYPE_ARRAY;
        type_array_int.children.emplace_back(LogicalType::TYPE_INT);

        TypeDescriptor type_struct_char_int;
        type_struct_char_int.type = LogicalType::TYPE_STRUCT;
        type_struct_char_int.children.emplace_back(type_array_char);
        type_struct_char_int.children.emplace_back(type_array_int);
        type_struct_char_int.field_names.emplace_back("vchar");
        type_struct_char_int.field_names.emplace_back("int");
        MutableColumnPtr res_struct_col = ColumnHelper::create_column(type_struct_char_int, true);
        array_agg_func->serialize_to_column(local_ctx.get(), state->state(), res_struct_col.get());
        ASSERT_EQ(strcmp(res_struct_col->debug_string().c_str(),
                         "[{vchar:[NULL,'bcd','cdrdfe',NULL,'esfg'],int:[NULL,9,NULL,7,6]}]"),
                  0);

        state = ManagedAggrState::create(local_ctx.get(), array_agg_func);
        array_agg_func->merge_batch_single_state(local_ctx.get(), state->state(), res_struct_col.get(), 0,
                                                 res_struct_col->size());

        res_struct_col->resize(0);
        array_agg_func->convert_to_serialize_format(local_ctx.get(), columns, int_column->size(), res_struct_col);
        ASSERT_EQ(strcmp(res_struct_col->debug_string().c_str(),
                         "[{vchar:[NULL],int:[NULL]}, {vchar:['bcd'],int:[9]}, {vchar:['cdrdfe'],int:[NULL]}, "
                         "{vchar:[NULL],int:[7]}, {vchar:['esfg'],int:[6]}]"),
                  0);

        MutableColumnPtr res_array_col = ColumnHelper::create_column(type_array_char, false);
        array_agg_func->finalize_to_column(local_ctx.get(), state->state(), res_array_col.get());
        ASSERT_EQ(strcmp(res_array_col->debug_string().c_str(), "[[NULL,'cdrdfe','bcd',NULL,'esfg']]"), 0);
    }
    // not nullable columns input
    state = ManagedAggrState::create(local_ctx.get(), array_agg_func);
    {
        MutableColumnPtr char_column = ColumnHelper::create_column(TYPE_VARCHAR_DESC, false);
        char_column->append_datum("");
        char_column->append_datum("bcd");
        char_column->append_datum("cdrdfe");
        char_column->append_datum("Datum()");
        char_column->append_datum("esfg");

        auto int_type = TYPE_INT_DESC;
        MutableColumnPtr int_column = ColumnHelper::create_column(int_type, false);
        int_column->append_datum(2);
        int_column->append_datum(9);
        int_column->append_datum(5);
        int_column->append_datum(7);
        int_column->append_datum(6);

        std::vector<const Column*> raw_columns;
        Columns columns;
        columns.emplace_back(char_column);
        columns.emplace_back(int_column);
        raw_columns.resize(2);
        raw_columns[0] = char_column.get();
        raw_columns[1] = int_column.get();

        // test update
        array_agg_func->update_batch_single_state(local_ctx.get(), int_column->size(), raw_columns.data(),
                                                  state->state());
        auto agg_state = (ArrayAggAggregateStateV2*)(state->state());
        ASSERT_EQ(agg_state->data_columns.size(), 2);
        // data_columns in state are nullable
        ASSERT_EQ((agg_state->data_columns)[0]->debug_string(), char_column->debug_string());
        ASSERT_EQ((agg_state->data_columns)[1]->debug_string(), int_column->debug_string());

        TypeDescriptor type_array_char;
        type_array_char.type = LogicalType::TYPE_ARRAY;
        type_array_char.children.emplace_back(LogicalType::TYPE_VARCHAR);

        TypeDescriptor type_array_int;
        type_array_int.type = LogicalType::TYPE_ARRAY;
        type_array_int.children.emplace_back(LogicalType::TYPE_INT);

        TypeDescriptor type_struct_char_int;
        type_struct_char_int.type = LogicalType::TYPE_STRUCT;
        type_struct_char_int.children.emplace_back(type_array_char);
        type_struct_char_int.children.emplace_back(type_array_int);
        type_struct_char_int.field_names.emplace_back("vchar");
        type_struct_char_int.field_names.emplace_back("int");
        MutableColumnPtr res_struct_col = ColumnHelper::create_column(type_struct_char_int, true);
        array_agg_func->serialize_to_column(local_ctx.get(), state->state(), res_struct_col.get());
        ASSERT_EQ(strcmp(res_struct_col->debug_string().c_str(),
                         "[{vchar:['','bcd','cdrdfe','Datum()','esfg'],int:[2,9,5,7,6]}]"),
                  0);

        state = ManagedAggrState::create(local_ctx.get(), array_agg_func);
        array_agg_func->merge_batch_single_state(local_ctx.get(), state->state(), res_struct_col.get(), 0,
                                                 res_struct_col->size());

        res_struct_col->resize(0);
        array_agg_func->convert_to_serialize_format(local_ctx.get(), columns, int_column->size(), res_struct_col);
        ASSERT_EQ(strcmp(res_struct_col->debug_string().c_str(),
                         "[{vchar:[''],int:[2]}, {vchar:['bcd'],int:[9]}, {vchar:['cdrdfe'],int:[5]}, "
                         "{vchar:['Datum()'],int:[7]}, {vchar:['esfg'],int:[6]}]"),
                  0);
        // should not finalize due to following appends
    }

    // append only column + const column
    {
        MutableColumnPtr char_column = ColumnHelper::create_const_null_column(2);
        auto int_column = ColumnHelper::create_const_column<TYPE_INT>(3, 2);

        std::vector<const Column*> raw_columns;
        Columns columns;
        columns.emplace_back(char_column);
        columns.emplace_back(int_column);
        raw_columns.resize(2);
        raw_columns[0] = char_column.get();
        raw_columns[1] = int_column.get();

        // test update
        array_agg_func->update_batch_single_state(local_ctx.get(), int_column->size(), raw_columns.data(),
                                                  state->state());
        auto agg_state = (ArrayAggAggregateStateV2*)(state->state());
        ASSERT_EQ(agg_state->data_columns.size(), 2);
        // data_columns in state are nullable
        ASSERT_EQ(strcmp((agg_state->data_columns)[0]->debug_string().c_str(),
                         "['', 'bcd', 'cdrdfe', 'Datum()', 'esfg', NULL, NULL]"),
                  0);
        ASSERT_EQ(strcmp((agg_state->data_columns)[1]->debug_string().c_str(), "[2, 9, 5, 7, 6, 3, 3]"), 0);

        TypeDescriptor type_array_char;
        type_array_char.type = LogicalType::TYPE_ARRAY;
        type_array_char.children.emplace_back(LogicalType::TYPE_VARCHAR);

        TypeDescriptor type_array_int;
        type_array_int.type = LogicalType::TYPE_ARRAY;
        type_array_int.children.emplace_back(LogicalType::TYPE_INT);

        TypeDescriptor type_struct_char_int;
        type_struct_char_int.type = LogicalType::TYPE_STRUCT;
        type_struct_char_int.children.emplace_back(type_array_char);
        type_struct_char_int.children.emplace_back(type_array_int);
        type_struct_char_int.field_names.emplace_back("vchar");
        type_struct_char_int.field_names.emplace_back("int");
        MutableColumnPtr res_struct_col = ColumnHelper::create_column(type_struct_char_int, true);
        array_agg_func->serialize_to_column(local_ctx.get(), state->state(), res_struct_col.get());
        ASSERT_EQ(strcmp(res_struct_col->debug_string().c_str(),
                         "[{vchar:['','bcd','cdrdfe','Datum()','esfg',NULL,NULL],int:[2,9,5,7,6,3,3]}]"),
                  0);

        state = ManagedAggrState::create(local_ctx.get(), array_agg_func);
        array_agg_func->merge_batch_single_state(local_ctx.get(), state->state(), res_struct_col.get(), 0,
                                                 res_struct_col->size());

        res_struct_col->resize(0);
        array_agg_func->convert_to_serialize_format(local_ctx.get(), columns, int_column->size(), res_struct_col);
        ASSERT_EQ(strcmp(res_struct_col->debug_string().c_str(), "[{vchar:[NULL],int:[3]}, {vchar:[NULL],int:[3]}]"),
                  0);

        MutableColumnPtr res_array_col = ColumnHelper::create_column(type_array_char, false);
        array_agg_func->finalize_to_column(local_ctx.get(), state->state(), res_array_col.get());
        ASSERT_EQ(strcmp(res_array_col->debug_string().c_str(), "[['bcd','Datum()','esfg','cdrdfe',NULL,NULL,'']]"), 0);
    }

    // only column + const column
    state = ManagedAggrState::create(local_ctx.get(), array_agg_func);
    {
        MutableColumnPtr char_column = ColumnHelper::create_const_null_column(2);
        auto int_column = ColumnHelper::create_const_column<TYPE_INT>(3, 2);

        std::vector<const Column*> raw_columns;
        Columns columns;
        columns.emplace_back(char_column);
        columns.emplace_back(int_column);
        raw_columns.resize(2);
        raw_columns[0] = char_column.get();
        raw_columns[1] = int_column.get();

        // test update
        array_agg_func->update_batch_single_state(local_ctx.get(), int_column->size(), raw_columns.data(),
                                                  state->state());
        auto agg_state = (ArrayAggAggregateStateV2*)(state->state());
        ASSERT_EQ(agg_state->data_columns.size(), 2);
        // data_columns in state are nullable
        ASSERT_EQ(strcmp((agg_state->data_columns)[0]->debug_string().c_str(), "[NULL, NULL]"), 0);
        ASSERT_EQ(strcmp((agg_state->data_columns)[1]->debug_string().c_str(), "[3, 3]"), 0);

        TypeDescriptor type_array_char;
        type_array_char.type = LogicalType::TYPE_ARRAY;
        type_array_char.children.emplace_back(LogicalType::TYPE_VARCHAR);

        TypeDescriptor type_array_int;
        type_array_int.type = LogicalType::TYPE_ARRAY;
        type_array_int.children.emplace_back(LogicalType::TYPE_INT);

        TypeDescriptor type_struct_char_int;
        type_struct_char_int.type = LogicalType::TYPE_STRUCT;
        type_struct_char_int.children.emplace_back(type_array_char);
        type_struct_char_int.children.emplace_back(type_array_int);
        type_struct_char_int.field_names.emplace_back("vchar");
        type_struct_char_int.field_names.emplace_back("int");
        MutableColumnPtr res_struct_col = ColumnHelper::create_column(type_struct_char_int, true);
        array_agg_func->serialize_to_column(local_ctx.get(), state->state(), res_struct_col.get());
        ASSERT_EQ(strcmp(res_struct_col->debug_string().c_str(), "[{vchar:[NULL,NULL],int:[3,3]}]"), 0);

        state = ManagedAggrState::create(local_ctx.get(), array_agg_func);
        array_agg_func->merge_batch_single_state(local_ctx.get(), state->state(), res_struct_col.get(), 0,
                                                 res_struct_col->size());

        res_struct_col->resize(0);
        array_agg_func->convert_to_serialize_format(local_ctx.get(), columns, int_column->size(), res_struct_col);
        ASSERT_EQ(strcmp(res_struct_col->debug_string().c_str(), "[{vchar:[NULL],int:[3]}, {vchar:[NULL],int:[3]}]"),
                  0);
    }

    // append nullable columns input
    {
        MutableColumnPtr char_column = ColumnHelper::create_column(TYPE_VARCHAR_DESC, true);
        char_column->append_datum(Datum());
        char_column->append_datum("bcd");
        char_column->append_datum("cdrdfe");
        char_column->append_datum(Datum());
        char_column->append_datum("esfg");

        auto int_type = TYPE_INT_DESC;
        MutableColumnPtr int_column = ColumnHelper::create_column(int_type, true);
        int_column->append_datum(Datum());
        int_column->append_datum(9);
        int_column->append_datum(Datum());
        int_column->append_datum(7);
        int_column->append_datum(6);

        std::vector<const Column*> raw_columns;
        Columns columns;
        columns.emplace_back(char_column);
        columns.emplace_back(int_column);
        raw_columns.resize(2);
        raw_columns[0] = char_column.get();
        raw_columns[1] = int_column.get();

        // test update
        array_agg_func->update_batch_single_state(local_ctx.get(), int_column->size(), raw_columns.data(),
                                                  state->state());
        auto agg_state = (ArrayAggAggregateStateV2*)(state->state());
        ASSERT_EQ(agg_state->data_columns.size(), 2);
        // data_columns in state are nullable
        ASSERT_EQ(strcmp((agg_state->data_columns)[0]->debug_string().c_str(),
                         "[NULL, NULL, NULL, 'bcd', 'cdrdfe', NULL, 'esfg']"),
                  0);
        ASSERT_EQ(strcmp((agg_state->data_columns)[1]->debug_string().c_str(), "[3, 3, NULL, 9, NULL, 7, 6]"), 0);

        TypeDescriptor type_array_char;
        type_array_char.type = LogicalType::TYPE_ARRAY;
        type_array_char.children.emplace_back(LogicalType::TYPE_VARCHAR);

        TypeDescriptor type_array_int;
        type_array_int.type = LogicalType::TYPE_ARRAY;
        type_array_int.children.emplace_back(LogicalType::TYPE_INT);

        TypeDescriptor type_struct_char_int;
        type_struct_char_int.type = LogicalType::TYPE_STRUCT;
        type_struct_char_int.children.emplace_back(type_array_char);
        type_struct_char_int.children.emplace_back(type_array_int);
        type_struct_char_int.field_names.emplace_back("vchar");
        type_struct_char_int.field_names.emplace_back("int");
        MutableColumnPtr res_struct_col = ColumnHelper::create_column(type_struct_char_int, true);
        array_agg_func->serialize_to_column(local_ctx.get(), state->state(), res_struct_col.get());
        ASSERT_EQ(strcmp(res_struct_col->debug_string().c_str(),
                         "[{vchar:[NULL,NULL,NULL,'bcd','cdrdfe',NULL,'esfg'],int:[3,3,NULL,9,NULL,7,6]}]"),
                  0);

        state = ManagedAggrState::create(local_ctx.get(), array_agg_func);
        array_agg_func->merge_batch_single_state(local_ctx.get(), state->state(), res_struct_col.get(), 0,
                                                 res_struct_col->size());

        res_struct_col->resize(0);
        array_agg_func->convert_to_serialize_format(local_ctx.get(), columns, int_column->size(), res_struct_col);
        ASSERT_EQ(strcmp(res_struct_col->debug_string().c_str(),
                         "[{vchar:[NULL],int:[NULL]}, {vchar:['bcd'],int:[9]}, {vchar:['cdrdfe'],int:[NULL]}, "
                         "{vchar:[NULL],int:[7]}, {vchar:['esfg'],int:[6]}]"),
                  0);

        MutableColumnPtr res_array_col = ColumnHelper::create_column(type_array_char, false);
        array_agg_func->finalize_to_column(local_ctx.get(), state->state(), res_array_col.get());
        // the order of the equal elements is not guaranteed(eg: int column is null), we need to check the order of the elements in the array.
        std::set<std::string> expected_strings = {"[['cdrdfe',NULL,'bcd',NULL,'esfg',NULL,NULL]]",
                                                  "[[NULL,'cdrdfe','bcd',NULL,'esfg',NULL,NULL]]"};
        ASSERT_TRUE(expected_strings.contains(res_array_col->debug_string().c_str()))
                << ", actual: " << res_array_col->debug_string().c_str();
    }
    // nullable columns input with cancelled
    {
        auto state = ManagedAggrState::create(local_ctx.get(), array_agg_func);
        MutableColumnPtr char_column = ColumnHelper::create_column(TYPE_VARCHAR_DESC, true);
        char_column->append_datum(Datum());
        char_column->append_datum("bcd");
        char_column->append_datum("cdrdfe");
        char_column->append_datum(Datum());
        char_column->append_datum("esfg");

        auto int_type = TYPE_INT_DESC;
        MutableColumnPtr int_column = ColumnHelper::create_column(int_type, true);
        int_column->append_datum(Datum());
        int_column->append_datum(9);
        int_column->append_datum(Datum());
        int_column->append_datum(7);
        int_column->append_datum(6);

        std::vector<const Column*> raw_columns;
        Columns columns;
        columns.emplace_back(char_column);
        columns.emplace_back(int_column);
        raw_columns.resize(2);
        raw_columns[0] = char_column.get();
        raw_columns[1] = int_column.get();

        // test update
        array_agg_func->update_batch_single_state(local_ctx.get(), int_column->size(), raw_columns.data(),
                                                  state->state());
        auto agg_state = (ArrayAggAggregateStateV2*)(state->state());
        ASSERT_EQ(agg_state->data_columns.size(), 2);
        // data_columns in state are nullable
        ASSERT_EQ((agg_state->data_columns)[0]->debug_string(), char_column->debug_string());
        ASSERT_EQ((agg_state->data_columns)[1]->debug_string(), int_column->debug_string());

        TypeDescriptor type_array_char;
        type_array_char.type = LogicalType::TYPE_ARRAY;
        type_array_char.children.emplace_back(LogicalType::TYPE_VARCHAR);

        TypeDescriptor type_array_int;
        type_array_int.type = LogicalType::TYPE_ARRAY;
        type_array_int.children.emplace_back(LogicalType::TYPE_INT);

        TypeDescriptor type_struct_char_int;
        type_struct_char_int.type = LogicalType::TYPE_STRUCT;
        type_struct_char_int.children.emplace_back(type_array_char);
        type_struct_char_int.children.emplace_back(type_array_int);
        type_struct_char_int.field_names.emplace_back("vchar");
        type_struct_char_int.field_names.emplace_back("int");
        MutableColumnPtr res_struct_col = ColumnHelper::create_column(type_struct_char_int, true);
        array_agg_func->serialize_to_column(local_ctx.get(), state->state(), res_struct_col.get());
        ASSERT_EQ(strcmp(res_struct_col->debug_string().c_str(),
                         "[{vchar:[NULL,'bcd','cdrdfe',NULL,'esfg'],int:[NULL,9,NULL,7,6]}]"),
                  0);

        state = ManagedAggrState::create(local_ctx.get(), array_agg_func);
        array_agg_func->merge_batch_single_state(local_ctx.get(), state->state(), res_struct_col.get(), 0,
                                                 res_struct_col->size());
        res_struct_col->resize(0);
        array_agg_func->convert_to_serialize_format(local_ctx.get(), columns, int_column->size(), res_struct_col);
        ASSERT_EQ(strcmp(res_struct_col->debug_string().c_str(),
                         "[{vchar:[NULL],int:[NULL]}, {vchar:['bcd'],int:[9]}, {vchar:['cdrdfe'],int:[NULL]}, "
                         "{vchar:[NULL],int:[7]}, {vchar:['esfg'],int:[6]}]"),
                  0);

        MutableColumnPtr res_array_col = ColumnHelper::create_column(type_array_char, false);
        local_ctx->state()->set_is_cancelled(true);
        array_agg_func->finalize_to_column(local_ctx.get(), state->state(), res_array_col.get());
        ASSERT_TRUE(local_ctx->has_error());
    }
}

// finalize_to_column() appends the null flag of the current row before the array row is built, so a
// cancellation detected while sorting the order-by columns used to leave the nullable result column
// with one more null flag than array rows, and the error placeholder appended on top of it.
TEST_F(ArrayAggTest, test_array_aggV2_cancelled_keeps_nullable_result_consistent) {
    std::vector<TypeDescriptor> arg_types = {TYPE_VARCHAR_DESC, TYPE_INT_DESC};
    auto return_type = TypeDescriptor::from_logical_type(TYPE_ARRAY);
    std::unique_ptr<RuntimeState> runtime_state = std::make_unique<RuntimeState>();
    std::unique_ptr<FunctionContext> local_ctx(FunctionContext::create_test_context(std::move(arg_types), return_type));
    std::vector<bool> is_asc_order{true};
    std::vector<bool> nulls_first{true};
    local_ctx->set_is_asc_order(is_asc_order);
    local_ctx->set_nulls_first(nulls_first);
    local_ctx->set_runtime_state(runtime_state.get());

    const AggregateFunction* array_agg_func = get_aggregate_function("array_agg2", TYPE_BIGINT, TYPE_ARRAY, false);
    auto state = ManagedAggrState::create(local_ctx.get(), array_agg_func);

    MutableColumnPtr char_column = ColumnHelper::create_column(TYPE_VARCHAR_DESC, true);
    char_column->append_datum("a");
    char_column->append_datum("b");

    MutableColumnPtr int_column = ColumnHelper::create_column(TYPE_INT_DESC, true);
    int_column->append_datum(2);
    int_column->append_datum(1);

    std::vector<const Column*> raw_columns{char_column.get(), int_column.get()};
    array_agg_func->update_batch_single_state(local_ctx.get(), int_column->size(), raw_columns.data(), state->state());

    TypeDescriptor type_array_char;
    type_array_char.type = LogicalType::TYPE_ARRAY;
    type_array_char.children.emplace_back(LogicalType::TYPE_VARCHAR);
    // a nullable result column is what the aggregator hands over when the output slot is nullable
    MutableColumnPtr res_array_col = ColumnHelper::create_column(type_array_char, true);
    res_array_col->append_datum(DatumArray{Slice("kept")});

    local_ctx->state()->set_is_cancelled(true);
    array_agg_func->finalize_to_column(local_ctx.get(), state->state(), res_array_col.get());
    ASSERT_TRUE(local_ctx->has_error());

    // exactly one placeholder row for this state, and null/data columns still agree
    auto* nullable_col = down_cast<NullableColumn*>(res_array_col.get());
    ASSERT_EQ(2, nullable_col->null_column()->size());
    ASSERT_EQ(2, nullable_col->data_column()->size());
    ASSERT_EQ(2, res_array_col->size());
    ASSERT_TRUE(res_array_col->is_null(1));
}

TEST_F(ArrayAggTest, test_array_aggV2_multi_order_by) {
    // Test array_agg2 with multiple ORDER BY columns
    // array_agg(varchar_col order by int_col, varchar_col2)
    std::vector<TypeDescriptor> arg_types = {TYPE_VARCHAR_DESC, TYPE_INT_DESC, TYPE_VARCHAR_DESC};
    auto return_type = TypeDescriptor::from_logical_type(TYPE_ARRAY);
    std::unique_ptr<RuntimeState> runtime_state = std::make_unique<RuntimeState>();
    std::unique_ptr<FunctionContext> local_ctx(FunctionContext::create_test_context(std::move(arg_types), return_type));

    // Set ORDER BY: first column DESC, second column ASC, both with NULLS FIRST
    std::vector<bool> is_asc_order{false, true};
    std::vector<bool> nulls_first{true, true};
    local_ctx->set_is_asc_order(is_asc_order);
    local_ctx->set_nulls_first(nulls_first);
    local_ctx->set_runtime_state(runtime_state.get());

    const AggregateFunction* array_agg_func = get_aggregate_function("array_agg2", TYPE_BIGINT, TYPE_ARRAY, false);
    auto state = ManagedAggrState::create(local_ctx.get(), array_agg_func);

    {
        // Test data:
        // varchar_col: ["a", "b", "c", "d", "e"]
        // int_col:     [10, 10, 20, 20, NULL]
        // varchar_col2:["x", "y", "x", "z", "w"]
        // Expected order after sorting by int_col DESC NULLS FIRST, varchar_col2 ASC NULLS FIRST:
        // - First by int_col DESC NULLS FIRST: [NULL, 20, 20, 10, 10]
        // - Then by varchar_col2 ASC (for same int_col):
        //   * NULL("w") -> 20("x") -> 20("z") -> 10("x") -> 10("y")
        // Result: ["e", "c", "d", "a", "b"]
        MutableColumnPtr char_column = ColumnHelper::create_column(TYPE_VARCHAR_DESC, true);
        char_column->append_datum("a");
        char_column->append_datum("b");
        char_column->append_datum("c");
        char_column->append_datum("d");
        char_column->append_datum("e");

        auto int_type = TYPE_INT_DESC;
        MutableColumnPtr int_column = ColumnHelper::create_column(int_type, true);
        int_column->append_datum(10);
        int_column->append_datum(10);
        int_column->append_datum(20);
        int_column->append_datum(20);
        int_column->append_datum(Datum());

        MutableColumnPtr char_column2 = ColumnHelper::create_column(TYPE_VARCHAR_DESC, false);
        char_column2->append_datum("x");
        char_column2->append_datum("y");
        char_column2->append_datum("x");
        char_column2->append_datum("z");
        char_column2->append_datum("w");

        std::vector<const Column*> raw_columns;
        std::vector<ColumnPtr> columns;
        columns.emplace_back(char_column);
        columns.emplace_back(int_column);
        columns.emplace_back(char_column2);
        raw_columns.resize(3);
        raw_columns[0] = char_column.get();
        raw_columns[1] = int_column.get();
        raw_columns[2] = char_column2.get();

        // test update
        array_agg_func->update_batch_single_state(local_ctx.get(), char_column->size(), raw_columns.data(),
                                                  state->state());
        auto agg_state = (ArrayAggAggregateStateV2*)(state->state());
        ASSERT_EQ(agg_state->data_columns.size(), 3);
        ASSERT_EQ((agg_state->data_columns)[0]->debug_string(), char_column->debug_string());
        ASSERT_EQ((agg_state->data_columns)[1]->debug_string(), int_column->debug_string());
        ASSERT_EQ((agg_state->data_columns)[2]->debug_string(), char_column2->debug_string());

        // Test serialize to STRUCT format
        TypeDescriptor type_array_varchar;
        type_array_varchar.type = LogicalType::TYPE_ARRAY;
        type_array_varchar.children.emplace_back(LogicalType::TYPE_VARCHAR);

        TypeDescriptor type_array_int;
        type_array_int.type = LogicalType::TYPE_ARRAY;
        type_array_int.children.emplace_back(LogicalType::TYPE_INT);

        TypeDescriptor type_struct;
        type_struct.type = LogicalType::TYPE_STRUCT;
        type_struct.children.emplace_back(type_array_varchar);
        type_struct.children.emplace_back(type_array_int);
        type_struct.children.emplace_back(type_array_varchar);
        type_struct.field_names.emplace_back("vchar");
        type_struct.field_names.emplace_back("int");
        type_struct.field_names.emplace_back("vchar2");

        MutableColumnPtr res_struct_col = ColumnHelper::create_column(type_struct, true);
        array_agg_func->serialize_to_column(local_ctx.get(), state->state(), res_struct_col.get());
        ASSERT_EQ(strcmp(res_struct_col->debug_string().c_str(),
                         "[{vchar:['a','b','c','d','e'],int:[10,10,20,20,NULL],vchar2:['x','y','x','z','w']}]"),
                  0);

        // Test merge
        state = ManagedAggrState::create(local_ctx.get(), array_agg_func);
        array_agg_func->merge_batch_single_state(local_ctx.get(), state->state(), res_struct_col.get(), 0,
                                                 res_struct_col->size());

        // Test convert_to_serialize_format
        res_struct_col->resize(0);
        array_agg_func->convert_to_serialize_format(local_ctx.get(), columns, char_column->size(), res_struct_col);
        ASSERT_EQ(strcmp(res_struct_col->debug_string().c_str(),
                         "[{vchar:['a'],int:[10],vchar2:['x']}, {vchar:['b'],int:[10],vchar2:['y']}, "
                         "{vchar:['c'],int:[20],vchar2:['x']}, {vchar:['d'],int:[20],vchar2:['z']}, "
                         "{vchar:['e'],int:[NULL],vchar2:['w']}]"),
                  0);

        // Test finalize - should be sorted by int_col DESC NULLS FIRST, then varchar_col2 ASC
        // Order: NULL("w") -> 20("x") -> 20("z") -> 10("x") -> 10("y")
        // Result: ["e", "c", "d", "a", "b"]
        MutableColumnPtr res_array_col = ColumnHelper::create_column(type_array_varchar, false);
        array_agg_func->finalize_to_column(local_ctx.get(), state->state(), res_array_col.get());
        ASSERT_EQ(strcmp(res_array_col->debug_string().c_str(), "[['e','c','d','a','b']]"), 0);
    }

    // Test with DISTINCT
    {
        // Expected behavior with DISTINCT:
        // - DISTINCT is applied on varchar_col (the first argument), regardless of ORDER BY values
        // - After sorting by int_col ASC NULLS LAST, then varchar_col2 DESC NULLS FIRST:
        //   Order: 10("zz") -> 10("zy") -> 10("xx") -> 15("zx") -> 15("xz") -> 20("yy") -> 20("xy")
        //   Values: ["b", "d", "a", "d", "c", "a", "b"]
        // - Then DISTINCT removes duplicates based on varchar_col value:
        //   "b" appears twice -> keep first one (at position with 10,"zz")
        //   "d" appears twice -> keep first one (at position with 10,"zy")
        //   "a" appears twice -> keep first one (at position with 10,"xx")
        // Result: ["b", "d", "a", "c"] (duplicates removed, keeping first occurrence after sorting)
        std::vector<bool> is_asc_order2{true, false};
        std::vector<bool> nulls_first2{false, true};
        local_ctx->set_is_asc_order(is_asc_order2);
        local_ctx->set_nulls_first(nulls_first2);
        local_ctx->set_is_distinct(true); // Enable DISTINCT
        local_ctx->set_runtime_state(runtime_state.get());

        state = ManagedAggrState::create(local_ctx.get(), array_agg_func);

        MutableColumnPtr char_column = ColumnHelper::create_column(TYPE_VARCHAR_DESC, false);
        // Add duplicate values: "a" appears twice, "b" appears twice, "c" appears once
        char_column->append_datum("a");
        char_column->append_datum("a"); // duplicate
        char_column->append_datum("b");
        char_column->append_datum("b"); // duplicate
        char_column->append_datum("c");
        char_column->append_datum("d");
        char_column->append_datum("d"); // duplicate

        auto int_type = TYPE_INT_DESC;
        MutableColumnPtr int_column = ColumnHelper::create_column(int_type, true);
        int_column->append_datum(10);
        int_column->append_datum(20); // different order by value for duplicate "a"
        int_column->append_datum(10);
        int_column->append_datum(20); // different order by value for duplicate "b"
        int_column->append_datum(15);
        int_column->append_datum(10);
        int_column->append_datum(15); // same order by value for duplicate "d"

        MutableColumnPtr char_column2 = ColumnHelper::create_column(TYPE_VARCHAR_DESC, false);
        char_column2->append_datum("xx");
        char_column2->append_datum("yy");
        char_column2->append_datum("zz");
        char_column2->append_datum("xy");
        char_column2->append_datum("xz");
        char_column2->append_datum("zy");
        char_column2->append_datum("zx");

        std::vector<const Column*> raw_columns;
        std::vector<ColumnPtr> columns;
        columns.emplace_back(char_column);
        columns.emplace_back(int_column);
        columns.emplace_back(char_column2);
        raw_columns.resize(3);
        raw_columns[0] = char_column.get();
        raw_columns[1] = int_column.get();
        raw_columns[2] = char_column2.get();

        // test update
        array_agg_func->update_batch_single_state(local_ctx.get(), char_column->size(), raw_columns.data(),
                                                  state->state());
        auto agg_state = (ArrayAggAggregateStateV2*)(state->state());
        ASSERT_EQ(agg_state->data_columns.size(), 3);
        ASSERT_EQ((agg_state->data_columns)[0]->debug_string(), char_column->debug_string());
        ASSERT_EQ((agg_state->data_columns)[1]->debug_string(), int_column->debug_string());
        ASSERT_EQ((agg_state->data_columns)[2]->debug_string(), char_column2->debug_string());

        // Test serialize to STRUCT format
        TypeDescriptor type_array_varchar;
        type_array_varchar.type = LogicalType::TYPE_ARRAY;
        type_array_varchar.children.emplace_back(LogicalType::TYPE_VARCHAR);

        TypeDescriptor type_array_int;
        type_array_int.type = LogicalType::TYPE_ARRAY;
        type_array_int.children.emplace_back(LogicalType::TYPE_INT);

        TypeDescriptor type_struct;
        type_struct.type = LogicalType::TYPE_STRUCT;
        type_struct.children.emplace_back(type_array_varchar);
        type_struct.children.emplace_back(type_array_int);
        type_struct.children.emplace_back(type_array_varchar);
        type_struct.field_names.emplace_back("vchar");
        type_struct.field_names.emplace_back("int");
        type_struct.field_names.emplace_back("vchar2");

        MutableColumnPtr res_struct_col = ColumnHelper::create_column(type_struct, true);
        array_agg_func->serialize_to_column(local_ctx.get(), state->state(), res_struct_col.get());
        ASSERT_EQ(strcmp(res_struct_col->debug_string().c_str(),
                         "[{vchar:['a','a','b','b','c','d','d'],int:[10,20,10,20,15,10,15],vchar2:['xx','yy','zz','xy',"
                         "'xz','zy','zx']}]"),
                  0);

        // Test merge
        state = ManagedAggrState::create(local_ctx.get(), array_agg_func);
        array_agg_func->merge_batch_single_state(local_ctx.get(), state->state(), res_struct_col.get(), 0,
                                                 res_struct_col->size());

        // Test convert_to_serialize_format
        res_struct_col->resize(0);
        array_agg_func->convert_to_serialize_format(local_ctx.get(), columns, char_column->size(), res_struct_col);
        ASSERT_EQ(strcmp(res_struct_col->debug_string().c_str(),
                         "[{vchar:['a'],int:[10],vchar2:['xx']}, {vchar:['a'],int:[20],vchar2:['yy']}, "
                         "{vchar:['b'],int:[10],vchar2:['zz']}, {vchar:['b'],int:[20],vchar2:['xy']}, "
                         "{vchar:['c'],int:[15],vchar2:['xz']}, {vchar:['d'],int:[10],vchar2:['zy']}, "
                         "{vchar:['d'],int:[15],vchar2:['zx']}]"),
                  0);

        MutableColumnPtr res_array_col = ColumnHelper::create_column(type_array_varchar, false);
        array_agg_func->finalize_to_column(local_ctx.get(), state->state(), res_array_col.get());
        ASSERT_EQ(strcmp(res_array_col->debug_string().c_str(), "[['b','d','a','c']]"), 0);
    }
}

TEST_F(ArrayAggTest, test_array_agg) {
    const AggregateFunction* agg_function = get_aggregate_function("array_agg", TYPE_VARCHAR, TYPE_ARRAY, false);
    auto state = ManagedAggrState::create(ctx, agg_function);

    auto data_column = BinaryColumn::create();

    for (int i = 0; i < 6; i++) {
        std::string val("starrocks");
        val.append(std::to_string(i));
        data_column->append(val);
    }
    const Column* row_column = data_column.get();

    // test update
    agg_function->update_batch_single_state(ctx, data_column->size(), &row_column, state->state());

    auto elem = BinaryColumn::create();
    auto offsets = UInt32Column::create(0);
    auto result_column = ArrayColumn::create(ColumnHelper::cast_to_nullable_column(elem), offsets);
    agg_function->finalize_to_column(ctx, state->state(), result_column->as_mutable_raw_ptr());

    // Access the element column through the ArrayColumn's elements, which is a NullableColumn
    const auto& elements_column = result_column->elements();
    const auto& nullable_elements = down_cast<const NullableColumn&>(elements_column);
    const auto& result_data_column = down_cast<const BinaryColumn&>(*nullable_elements.data_column());
    for (int i = 0; i < 6; i++) {
        std::string val("starrocks");
        val.append(std::to_string(i));
        ASSERT_EQ(val, result_data_column.get_slice(i).to_string());
    }
}

TEST_F(ArrayAggTest, test_array_agg_distinct) {
    const AggregateFunction* agg_function =
            get_aggregate_function("array_agg_distinct", TYPE_VARCHAR, TYPE_ARRAY, false);
    auto state = ManagedAggrState::create(ctx, agg_function);

    auto data_column = BinaryColumn::create();

    for (int i = 0; i < 6; i++) {
        std::string val("starrocks");
        val.append(std::to_string(i));
        data_column->append(val);
        data_column->append(val);
    }

    const Column* row_column = data_column.get();

    // test update
    agg_function->update_batch_single_state(ctx, data_column->size(), &row_column, state->state());

    auto elem = BinaryColumn::create();
    auto offsets = UInt32Column::create(0);
    auto result_column = ArrayColumn::create(ColumnHelper::cast_to_nullable_column(elem), offsets);
    agg_function->finalize_to_column(ctx, state->state(), result_column->as_mutable_raw_ptr());

    // Access the element column through the ArrayColumn's elements, which is a NullableColumn
    const auto& elements_column = result_column->elements();
    const auto& nullable_elements = down_cast<const NullableColumn&>(elements_column);
    const auto& result_data_column = down_cast<const BinaryColumn&>(*nullable_elements.data_column());

    ASSERT_EQ(6, result_data_column.size());
}

TEST_F(ArrayAggTest, test_array_agg_max_array_length) {
    const AggregateFunction* agg_function = get_aggregate_function("array_agg", TYPE_VARCHAR, TYPE_ARRAY, true);

    auto data_column = BinaryColumn::create();
    auto null_column = NullColumn::create();
    // 4 non-null values plus 2 nulls, so the group holds 6 elements only if nulls are counted.
    for (int i = 0; i < 6; i++) {
        std::string val("starrocks");
        val.append(std::to_string(i));
        data_column->append(val);
        null_column->append(i >= 4 ? 1 : 0);
    }
    auto column = NullableColumn::create(std::move(data_column), std::move(null_column));
    const Column* row_column = column.get();

    auto aggregate_and_finalize = [&](FunctionContext* fn_ctx) {
        auto state = ManagedAggrState::create(fn_ctx, agg_function);
        agg_function->update_batch_single_state(fn_ctx, column->size(), &row_column, state->state());
        auto elem = BinaryColumn::create();
        auto offsets = UInt32Column::create(0);
        auto result_column = ArrayColumn::create(ColumnHelper::cast_to_nullable_column(elem), offsets);
        agg_function->finalize_to_column(fn_ctx, state->state(), result_column->as_mutable_raw_ptr());
    };

    // unlimited by default
    {
        std::unique_ptr<FunctionContext> local_ctx(FunctionContext::create_test_context());
        aggregate_and_finalize(local_ctx.get());
        ASSERT_FALSE(local_ctx->has_error());
    }
    // a group whose size is exactly the limit is still accepted
    {
        std::unique_ptr<FunctionContext> local_ctx(FunctionContext::create_test_context());
        local_ctx->set_max_array_length(6);
        aggregate_and_finalize(local_ctx.get());
        ASSERT_FALSE(local_ctx->has_error());
    }
    // nulls count towards the limit, so the 4 non-null values alone do not keep the group under it
    {
        std::unique_ptr<FunctionContext> local_ctx(FunctionContext::create_test_context());
        local_ctx->set_max_array_length(5);
        aggregate_and_finalize(local_ctx.get());
        ASSERT_TRUE(local_ctx->has_error());
        ASSERT_NE(std::string(local_ctx->error_msg()).find("max_array_length"), std::string::npos);
    }
}

TEST_F(ArrayAggTest, test_array_agg_max_array_length_distinct) {
    const AggregateFunction* agg_function = get_aggregate_function("array_agg_distinct", TYPE_INT, TYPE_ARRAY, true);

    auto data_column = Int32Column::create();
    auto null_column = NullColumn::create();
    // 4 distinct values plus 2 nulls that collapse into one element, so DISTINCT yields 5 elements.
    for (int i = 0; i < 6; i++) {
        data_column->append(i % 4);
        null_column->append(i >= 4 ? 1 : 0);
    }
    auto column = NullableColumn::create(std::move(data_column), std::move(null_column));
    const Column* row_column = column.get();

    auto aggregate_and_finalize = [&](FunctionContext* fn_ctx) {
        auto state = ManagedAggrState::create(fn_ctx, agg_function);
        agg_function->update_batch_single_state(fn_ctx, column->size(), &row_column, state->state());
        auto elem = Int32Column::create();
        auto offsets = UInt32Column::create(0);
        auto result_column = ArrayColumn::create(ColumnHelper::cast_to_nullable_column(elem), offsets);
        agg_function->finalize_to_column(fn_ctx, state->state(), result_column->as_mutable_raw_ptr());
    };

    // duplicates removed by array_agg_distinct do not count towards the limit
    {
        std::unique_ptr<FunctionContext> local_ctx(FunctionContext::create_test_context());
        local_ctx->set_max_array_length(5);
        aggregate_and_finalize(local_ctx.get());
        ASSERT_FALSE(local_ctx->has_error());
    }
    {
        std::unique_ptr<FunctionContext> local_ctx(FunctionContext::create_test_context());
        local_ctx->set_max_array_length(4);
        aggregate_and_finalize(local_ctx.get());
        ASSERT_TRUE(local_ctx->has_error());
        ASSERT_NE(std::string(local_ctx->error_msg()).find("max_array_length"), std::string::npos);
    }
}

TEST_F(ArrayAggTest, test_array_aggV2_max_array_length) {
    auto build_context = [](RuntimeState* runtime_state, ssize_t max_size) {
        std::vector<TypeDescriptor> arg_types = {TYPE_VARCHAR_DESC, TYPE_INT_DESC};
        auto return_type = TypeDescriptor::from_logical_type(TYPE_ARRAY);
        std::unique_ptr<FunctionContext> fn_ctx(
                FunctionContext::create_test_context(std::move(arg_types), return_type));
        fn_ctx->set_is_asc_order(std::vector<bool>{false});
        fn_ctx->set_nulls_first(std::vector<bool>{true});
        fn_ctx->set_runtime_state(runtime_state);
        fn_ctx->set_max_array_length(max_size);
        return fn_ctx;
    };

    const AggregateFunction* array_agg_func = get_aggregate_function("array_agg2", TYPE_BIGINT, TYPE_ARRAY, false);

    MutableColumnPtr char_column = ColumnHelper::create_column(TYPE_VARCHAR_DESC, true);
    char_column->append_datum("bcd");
    char_column->append_datum(Datum());
    char_column->append_datum("esfg");
    char_column->append_datum("cdrdfe");

    auto int_type = TYPE_INT_DESC;
    MutableColumnPtr int_column = ColumnHelper::create_column(int_type, true);
    int_column->append_datum(9);
    int_column->append_datum(Datum());
    int_column->append_datum(7);
    int_column->append_datum(6);

    std::vector<const Column*> raw_columns{char_column.get(), int_column.get()};

    TypeDescriptor type_array_char;
    type_array_char.type = LogicalType::TYPE_ARRAY;
    type_array_char.children.emplace_back(LogicalType::TYPE_VARCHAR);

    auto aggregate_and_finalize = [&](FunctionContext* fn_ctx) {
        auto state = ManagedAggrState::create(fn_ctx, array_agg_func);
        array_agg_func->update_batch_single_state(fn_ctx, int_column->size(), raw_columns.data(), state->state());
        MutableColumnPtr res_array_col = ColumnHelper::create_column(type_array_char, false);
        array_agg_func->finalize_to_column(fn_ctx, state->state(), res_array_col.get());
    };

    std::unique_ptr<RuntimeState> runtime_state = std::make_unique<RuntimeState>();
    {
        auto fn_ctx = build_context(runtime_state.get(), 4);
        aggregate_and_finalize(fn_ctx.get());
        ASSERT_FALSE(fn_ctx->has_error());
    }
    {
        auto fn_ctx = build_context(runtime_state.get(), 3);
        aggregate_and_finalize(fn_ctx.get());
        ASSERT_TRUE(fn_ctx->has_error());
        ASSERT_NE(std::string(fn_ctx->error_msg()).find("max_array_length"), std::string::npos);
    }
}

TEST_F(ArrayAggTest, test_array_aggV2_max_array_length_distinct) {
    auto build_context = [](RuntimeState* runtime_state, ssize_t max_size) {
        std::vector<TypeDescriptor> arg_types = {TYPE_VARCHAR_DESC, TYPE_INT_DESC};
        auto return_type = TypeDescriptor::from_logical_type(TYPE_ARRAY);
        std::unique_ptr<FunctionContext> fn_ctx(
                FunctionContext::create_test_context(std::move(arg_types), return_type));
        fn_ctx->set_is_asc_order(std::vector<bool>{true});
        fn_ctx->set_nulls_first(std::vector<bool>{true});
        fn_ctx->set_is_distinct(true);
        fn_ctx->set_runtime_state(runtime_state);
        fn_ctx->set_max_array_length(max_size);
        return fn_ctx;
    };

    const AggregateFunction* array_agg_func = get_aggregate_function("array_agg2", TYPE_BIGINT, TYPE_ARRAY, false);

    MutableColumnPtr char_column = ColumnHelper::create_column(TYPE_VARCHAR_DESC, true);
    // 6 input rows, 2 distinct values after DISTINCT.
    char_column->append_datum("a");
    char_column->append_datum("a");
    char_column->append_datum("b");
    char_column->append_datum("a");
    char_column->append_datum("b");
    char_column->append_datum("b");

    auto int_type = TYPE_INT_DESC;
    MutableColumnPtr int_column = ColumnHelper::create_column(int_type, true);
    int_column->append_datum(1);
    int_column->append_datum(2);
    int_column->append_datum(3);
    int_column->append_datum(4);
    int_column->append_datum(5);
    int_column->append_datum(6);

    std::vector<const Column*> raw_columns{char_column.get(), int_column.get()};

    TypeDescriptor type_array_char;
    type_array_char.type = LogicalType::TYPE_ARRAY;
    type_array_char.children.emplace_back(LogicalType::TYPE_VARCHAR);

    auto aggregate_and_finalize = [&](FunctionContext* fn_ctx) {
        auto state = ManagedAggrState::create(fn_ctx, array_agg_func);
        array_agg_func->update_batch_single_state(fn_ctx, int_column->size(), raw_columns.data(), state->state());
        MutableColumnPtr res_array_col = ColumnHelper::create_column(type_array_char, false);
        array_agg_func->finalize_to_column(fn_ctx, state->state(), res_array_col.get());
    };

    std::unique_ptr<RuntimeState> runtime_state = std::make_unique<RuntimeState>();
    // Pre-dedup size is 6, but DISTINCT yields 2 elements, so a limit of 2 must pass.
    {
        auto fn_ctx = build_context(runtime_state.get(), 2);
        aggregate_and_finalize(fn_ctx.get());
        ASSERT_FALSE(fn_ctx->has_error());
    }
    {
        auto fn_ctx = build_context(runtime_state.get(), 1);
        aggregate_and_finalize(fn_ctx.get());
        ASSERT_TRUE(fn_ctx->has_error());
        ASSERT_NE(std::string(fn_ctx->error_msg()).find("max_array_length"), std::string::npos);
    }
}

TEST_F(ArrayAggTest, test_array_agg_nullable) {
    const AggregateFunction* func = get_aggregate_function("array_agg", TYPE_INT, TYPE_ARRAY, true);
    auto state = ManagedAggrState::create(ctx, func);

    auto data_column = Int32Column::create();
    auto null_column = NullColumn::create();

    for (int i = 0; i < 1024; i++) {
        data_column->append(i % 2 ? 0 : i);
        null_column->append(i % 2 ? 1 : 0);
    }

    auto column = NullableColumn::create(std::move(data_column), std::move(null_column));

    const Column* row_column = column.get();
    func->update_batch_single_state(ctx, column->size(), &row_column, state->state());
    auto elem = Int32Column::create();
    auto offsets = UInt32Column::create(0);
    auto result_column = ArrayColumn::create(ColumnHelper::cast_to_nullable_column(elem), offsets);
    func->finalize_to_column(ctx, state->state(), result_column->as_mutable_raw_ptr());

    ASSERT_EQ(1024, offsets->get_data().back());
}

TEST_F(ArrayAggTest, test_array_agg_nullable_distinct) {
    const AggregateFunction* func = get_aggregate_function("array_agg_distinct", TYPE_INT, TYPE_ARRAY, true);
    auto state = ManagedAggrState::create(ctx, func);

    auto data_column = Int32Column::create();
    auto null_column = NullColumn::create();

    for (int i = 0; i < 1024; i++) {
        data_column->append(i % 100);
        null_column->append(i % 4 ? 1 : 0);
    }

    auto column = NullableColumn::create(std::move(data_column), std::move(null_column));
    const Column* row_column = column.get();
    func->update_batch_single_state(ctx, column->size(), &row_column, state->state());

    auto elem = Int32Column::create();
    auto offsets = UInt32Column::create(0);
    auto result_column = ArrayColumn::create(ColumnHelper::cast_to_nullable_column(elem), offsets);

    func->finalize_to_column(ctx, state->state(), result_column->as_mutable_raw_ptr());

    ASSERT_EQ(26, offsets->get_data().back());
}

} // namespace starrocks
