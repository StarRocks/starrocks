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

#include "column/chunk.h"
#include "column/column_builder.h"
#include "column/column_helper.h"
#include "column/column_viewer.h"
#include "column/const_column.h"
#include "column/vectorized_fwd.h"
#include "common/object_pool.h"
#include "exprs/column_ref.h"
#include "exprs/dictmapping_expr.h"
#include "exprs/expr_context.h"
#include "exprs/placeholder_ref.h"
#include "gen_cpp/Types_types.h"
#include "runtime/exception.h"
#include "runtime/global_dict/config.h"
#include "runtime/mem_pool.h"
#include "runtime/runtime_state.h"
#include "runtime/types.h"
#include "testutil/assert.h"
#include "types/logical_type.h"

namespace starrocks {

template <class Provider>
class ProvideExpr final : public Expr {
public:
    ProvideExpr(Provider provider) : Expr(TypeDescriptor(TYPE_VARCHAR), false), _provider(provider){};
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
        ASSERT_OK(state.init_query_global_dict(list));
    }

public:
    TExprNode node;
    ObjectPool pool;
    RuntimeState state;
    Expr* dict_expr;
    Expr* origin;
    ExprContext* context;
};

TEST_F(DictMappingTest, test1) {
    auto origin = pool.add(new ProvideExpr([](ColumnPtr column) {
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

    auto slot = pool.add(new ColumnRef(TypeDescriptor(TYPE_INT), 1));
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
        auto c = down_cast<NullableColumn*>(nullable_column.get());
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
    auto origin = pool.add(new ProvideExpr([](ColumnPtr column) {
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

    auto slot = pool.add(new ColumnRef(TypeDescriptor(TYPE_INT), 1));
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
        auto c = down_cast<NullableColumn*>(nullable_column.get());
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

} // namespace starrocks