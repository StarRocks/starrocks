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
#include <string>

#include "base/testutil/assert.h"
#include "column/chunk.h"
#include "column/column_builder.h"
#include "column/column_helper.h"
#include "column/column_viewer.h"
#include "column/const_column.h"
#include "column/global_dict/config.h"
#include "column/vectorized_fwd.h"
#include "common/object_pool.h"
#include "compute_env/global_dict/fragment_dict_state.h"
#include "exprs/column_ref.h"
#include "exprs/expr_context.h"
#include "exprs/placeholder_ref.h"
#include "exprs_ext/dict/dictmapping_expr.h"
#include "gen_cpp/Types_types.h"
#include "runtime/exception.h"
#include "runtime/mem_pool.h"
#include "runtime/runtime_state.h"
#include "types/logical_type.h"
#include "types/type_descriptor.h"

namespace starrocks {

template <class Provider>
class ProvideExpr final : public Expr {
public:
    ProvideExpr(Provider provider) : Expr(TYPE_VARCHAR_DESC, false), _provider(provider){};
    Expr* clone(ObjectPool* pool) const override { return pool->add(new ProvideExpr(*this)); }

    StatusOr<ColumnPtr> evaluate_checked(ExprContext* context, Chunk* ptr) override {
        return _provider(ptr->columns()[0]);
    }

private:
    Provider _provider;
};

class DictMappingTest : public ::testing::Test {
public:
    void SetUp() override {
        node.__set_node_type(TExprNodeType::DICT_EXPR);
        TTypeNode n_type;
        TScalarType scalar_type;
        scalar_type.__set_type(TPrimitiveType::VARCHAR);
        n_type.__set_scalar_type(scalar_type);
        node.type.types.emplace_back(n_type);

        std::vector<TGlobalDict> list;
        TGlobalDict dict;
        dict.__set_columnId(1);
        dict.__set_ids(std::vector<int32_t>{0, 1, 2, 3, 4});
        dict.__set_strings(std::vector<std::string>{"", "1", "2", "3", "4"});
        list.emplace_back(dict);
        state._obj_pool = std::make_shared<ObjectPool>();
        state._instance_mem_pool = std::make_unique<MemPool>();
        _fragment_dict_state = std::make_unique<FragmentDictState>();
        state.set_fragment_dict_state(_fragment_dict_state.get());
        ASSERT_OK(_fragment_dict_state->init_query_global_dict(&state, list));
    }

public:
    TExprNode node;
    ObjectPool pool;
    RuntimeState state;
    std::unique_ptr<FragmentDictState> _fragment_dict_state;
    Expr* dict_expr;
    Expr* origin;
    ExprContext* context;
};

TEST_F(DictMappingTest, test1) {
    auto origin = pool.add(new ProvideExpr([](const ColumnPtr& column) {
        ColumnViewer<TYPE_VARCHAR> viewer(column);
        ColumnBuilder<TYPE_VARCHAR> builder(5);
        size_t num_rows = column->size();
        for (size_t i = 0; i < num_rows; ++i) {
            auto value = viewer.value(i);
            if (viewer.is_null(i)) {
                builder.append("k");
            } else if (value == "4") {
                builder.append("k1");
            } else {
                builder.append("k" + value.to_string());
            }
        }

        return builder.build(false);
    }));
    auto pl = pool.add(new PlaceHolderRef(node));
    origin->add_child(pl);

    auto slot = pool.add(new ColumnRef(TYPE_INT_DESC, 1));
    dict_expr = pool.add(new DictMappingExpr(node));
    context = pool.add(new ExprContext(dict_expr));
    dict_expr->add_child(slot);
    dict_expr->add_child(origin);

    ASSERT_OK(context->prepare(&state));
    ASSERT_OK(context->open(&state));

    {
        auto chunk = std::make_unique<Chunk>();
        auto dict_column = Int32Column::create();
        dict_column->get_data().emplace_back(1);
        dict_column->get_data().emplace_back(2);
        dict_column->get_data().emplace_back(3);
        dict_column->get_data().emplace_back(4);
        auto nullable_column = NullableColumn::wrap_if_necessary(dict_column);
        auto c = down_cast<NullableColumn*>(nullable_column->as_mutable_raw_ptr());
        c->set_null(0);
        chunk->append_column(nullable_column, 1);

        auto column = context->evaluate(chunk.get());
        ASSERT_OK(column.status());
        EXPECT_EQ(column->get()->get(0).get_slice(), "k");
        EXPECT_EQ(column->get()->get(1).get_slice(), "k2");
        EXPECT_EQ(column->get()->get(2).get_slice(), "k3");
        EXPECT_EQ(column->get()->get(3).get_slice(), "k1");
    }
    {
        auto chunk = std::make_unique<Chunk>();
        auto dict_column = Int32Column::create();
        dict_column->get_data().emplace_back(1);

        chunk->append_column(ConstColumn::create(dict_column, 1), 1);

        auto column = context->evaluate(chunk.get());
        ASSERT_OK(column.status());
        EXPECT_EQ(column->get()->get(0).get_slice(), "k1");
    }
}
TEST_F(DictMappingTest, test_function_return_exception) {
    auto origin = pool.add(new ProvideExpr([](const ColumnPtr& column) {
        ColumnViewer<TYPE_VARCHAR> viewer(column);
        ColumnBuilder<TYPE_VARCHAR> builder(5);
        size_t num_rows = column->size();
        for (size_t i = 0; i < num_rows; ++i) {
            auto value = viewer.value(i);
            if (viewer.is_null(i)) {
                builder.append("k");
            } else if (value == "4") {
                throw RuntimeException("test return exception function");
            } else {
                builder.append("k" + value.to_string());
            }
        }

        return builder.build(false);
    }));
    auto pl = pool.add(new PlaceHolderRef(node));
    origin->add_child(pl);

    auto slot = pool.add(new ColumnRef(TYPE_INT_DESC, 1));
    dict_expr = pool.add(new DictMappingExpr(node));
    context = pool.add(new ExprContext(dict_expr));
    dict_expr->add_child(slot);
    dict_expr->add_child(origin);

    ASSERT_OK(context->prepare(&state));
    ASSERT_OK(context->open(&state));

    {
        auto chunk = std::make_unique<Chunk>();
        auto dict_column = Int32Column::create();
        dict_column->get_data().emplace_back(1);
        dict_column->get_data().emplace_back(2);
        dict_column->get_data().emplace_back(3);
        dict_column->get_data().emplace_back(4);
        dict_column->get_data().emplace_back(5);
        auto nullable_column = NullableColumn::wrap_if_necessary(dict_column);
        auto c = down_cast<NullableColumn*>(nullable_column->as_mutable_raw_ptr());
        c->set_null(0);
        chunk->append_column(nullable_column, 1);

        auto column = context->evaluate(chunk.get());
        ASSERT_ERROR(column.status());
    }
    {
        auto chunk = std::make_unique<Chunk>();
        auto dict_column = Int32Column::create();
        dict_column->get_data().emplace_back(1);

        chunk->append_column(ConstColumn::create(dict_column, 1), 1);

        auto column = context->evaluate(chunk.get());
        ASSERT_OK(column.status());
        EXPECT_EQ(column->get()->get(0).get_slice(), "k1");
    }
    {
        auto chunk = std::make_unique<Chunk>();
        auto dict_column = Int32Column::create();
        dict_column->get_data().emplace_back(4);

        chunk->append_column(ConstColumn::create(dict_column, 1), 1);

        auto column = context->evaluate(chunk.get());
        ASSERT_ERROR(column.status());
    }
}

TEST_F(DictMappingTest, test_expression_returns_null_for_some_values) {
    // Test case for expression that returns NULL for some dictionary values
    // and non-NULL strings for others. This simulates: if(v1 = '1', NULL, 'ok')
    auto origin = pool.add(new ProvideExpr([](const ColumnPtr& column) {
        ColumnViewer<TYPE_VARCHAR> viewer(column);
        ColumnBuilder<TYPE_VARCHAR> builder(column->size());
        size_t num_rows = column->size();
        for (size_t i = 0; i < num_rows; ++i) {
            if (viewer.is_null(i)) {
                builder.append("ok");
            } else if (viewer.value(i) == "1") {
                builder.append_null();
            } else {
                builder.append("ok");
            }
        }
        return builder.build(false);
    }));
    auto pl = pool.add(new PlaceHolderRef(node));
    origin->add_child(pl);

    auto slot = pool.add(new ColumnRef(TYPE_INT_DESC, 1));
    dict_expr = pool.add(new DictMappingExpr(node));
    context = pool.add(new ExprContext(dict_expr));
    dict_expr->add_child(slot);
    dict_expr->add_child(origin);

    ASSERT_OK(context->prepare(&state));
    ASSERT_OK(context->open(&state));

    auto chunk = std::make_unique<Chunk>();
    auto dict_column = Int32Column::create();
    dict_column->get_data().emplace_back(1); // '1' -> should be NULL
    dict_column->get_data().emplace_back(2); // '2' -> should be 'ok'
    dict_column->get_data().emplace_back(3); // '3' -> should be 'ok'
    dict_column->get_data().emplace_back(4); // '4' -> should be 'ok'
    auto nullable_column = NullableColumn::wrap_if_necessary(dict_column);
    chunk->append_column(nullable_column, 1);
    auto result = context->evaluate(chunk.get());
    ASSERT_OK(result.status());
    auto result_column = result.value();
    ASSERT_TRUE(result_column->is_nullable());
    auto* nullable_result = down_cast<const NullableColumn*>(result_column.get());
    EXPECT_TRUE(nullable_result->is_null(0)) << "Input '1' should produce NULL";
    EXPECT_FALSE(nullable_result->is_null(1)) << "Input '2' should produce 'ok'";
    EXPECT_EQ(nullable_result->get(1).get_slice(), "ok");
    EXPECT_FALSE(nullable_result->is_null(2)) << "Input '3' should produce 'ok'";
    EXPECT_EQ(nullable_result->get(2).get_slice(), "ok");
    EXPECT_FALSE(nullable_result->is_null(3)) << "Input '4' should produce 'ok'";
    EXPECT_EQ(nullable_result->get(3).get_slice(), "ok");
}

TEST_F(DictMappingTest, test_low_card_dict_output_reserves_overflow_padding) {
    auto origin = pool.add(new ProvideExpr([](const ColumnPtr& column) {
        ColumnViewer<TYPE_VARCHAR> viewer(column);
        ColumnBuilder<TYPE_VARCHAR> builder(column->size());
        size_t num_rows = column->size();
        for (size_t i = 0; i < num_rows; ++i) {
            if (viewer.is_null(i)) {
                builder.append_null();
            } else {
                builder.append("mapped_" + viewer.value(i).to_string());
            }
        }
        return builder.build(false);
    }));
    auto pl = pool.add(new PlaceHolderRef(node));
    origin->add_child(pl);

    TExprNode dict_node = node;
    dict_node.type.types[0].scalar_type.__set_type(TPrimitiveType::INT);

    auto slot = pool.add(new ColumnRef(TYPE_INT_DESC, 1));
    auto* mapping_expr = pool.add(new DictMappingExpr(dict_node));
    mapping_expr->set_output_id(2);
    dict_expr = mapping_expr;
    context = pool.add(new ExprContext(dict_expr));
    dict_expr->add_child(slot);
    dict_expr->add_child(origin);

    size_t allocated_before = state.instance_mem_pool()->total_allocated_bytes();
    ASSERT_OK(context->prepare(&state));
    ASSERT_OK(context->open(&state));
    size_t allocated_after = state.instance_mem_pool()->total_allocated_bytes();
    EXPECT_GT(allocated_after, allocated_before);

    const auto& dict_maps = _fragment_dict_state->query_global_dicts();
    auto map_it = dict_maps.find(2);
    ASSERT_NE(map_it, dict_maps.end());
    const auto& forward_map = map_it->second.first;
    const auto& reverse_map = map_it->second.second;
    // Dictionary values "1".."4" are mapped to "mapped_1".."mapped_4"; the NULL entry is not inserted.
    ASSERT_GE(forward_map.size(), 4);
    ASSERT_EQ(forward_map.size(), reverse_map.size());

    // Look up each key with an independently allocated buffer whose trailing padding differs from the pooled
    // key's tail, so lookups must only depend on the first `size` bytes.
    for (const auto& [dict_id, dict_slice] : reverse_map) {
        std::string padded_key = dict_slice.to_string() + std::string(SLICE_MEMEQUAL_OVERFLOW_PADDING, '\x5A');
        auto found = forward_map.find(Slice(padded_key.data(), dict_slice.size));
        ASSERT_NE(found, forward_map.end());
        EXPECT_EQ(found->second, dict_id);
    }

    std::string mapped_1 = "mapped_1" + std::string(SLICE_MEMEQUAL_OVERFLOW_PADDING, '\x5A');
    std::string mapped_3 = "mapped_3" + std::string(SLICE_MEMEQUAL_OVERFLOW_PADDING, '\x5A');
    auto it_1 = forward_map.find(Slice(mapped_1.data(), 8));
    auto it_3 = forward_map.find(Slice(mapped_3.data(), 8));
    ASSERT_NE(it_1, forward_map.end());
    ASSERT_NE(it_3, forward_map.end());

    auto chunk = std::make_unique<Chunk>();
    auto dict_column = Int32Column::create();
    dict_column->get_data().emplace_back(1);
    dict_column->get_data().emplace_back(3);
    chunk->append_column(dict_column, 1);

    auto result = context->evaluate(chunk.get());
    ASSERT_OK(result.status());
    EXPECT_EQ(result.value()->get(0).get_int32(), it_1->second);
    EXPECT_EQ(result.value()->get(1).get_int32(), it_3->second);
}

} // namespace starrocks
