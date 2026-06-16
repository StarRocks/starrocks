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

#include "storage/primitive/predicate_tree/predicate_tree.hpp"

#include <gtest/gtest.h>

#include <memory>
#include <span>
#include <string>
#include <type_traits>
#include <vector>

#include "base/testutil/assert.h"
#include "column/chunk.h"
#include "column/chunk_factory.h"
#include "column/fixed_length_column.h"
#include "column/schema.h"
#include "types/type_info.h"

namespace starrocks {

namespace {

class IntComparisonPredicate final : public ColumnPredicate {
public:
    enum class Op { LT, GE };

    IntComparisonPredicate(ColumnId column_id, Op op, int32_t value, std::string label)
            : ColumnPredicate(get_type_info(TYPE_INT), column_id), _op(op), _value(value), _label(std::move(label)) {}

    Status evaluate(const Column* column, uint8_t* selection, uint16_t from, uint16_t to) const override {
        const auto& values = int_values(column);
        for (auto i = from; i < to; ++i) {
            selection[i] = match(values[i]);
        }
        return Status::OK();
    }

    Status evaluate_and(const Column* column, uint8_t* selection, uint16_t from, uint16_t to) const override {
        const auto& values = int_values(column);
        for (auto i = from; i < to; ++i) {
            selection[i] &= match(values[i]);
        }
        return Status::OK();
    }

    Status evaluate_or(const Column* column, uint8_t* selection, uint16_t from, uint16_t to) const override {
        const auto& values = int_values(column);
        for (auto i = from; i < to; ++i) {
            selection[i] |= match(values[i]);
        }
        return Status::OK();
    }

    bool can_vectorized() const override { return true; }

    PredicateType type() const override { return _op == Op::LT ? PredicateType::kLT : PredicateType::kGE; }

    Status convert_to(const ColumnPredicate** output, const TypeInfoPtr& target_type_info,
                      ObjectPool* obj_pool) const override {
        return Status::NotSupported("test predicate conversion");
    }

    std::string debug_string() const override { return _label; }

private:
    bool match(int32_t value) const {
        switch (_op) {
        case Op::LT:
            return value < _value;
        case Op::GE:
            return value >= _value;
        }
    }

    static std::span<const int32_t> int_values(const Column* column) {
        const auto* int_column = dynamic_cast<const Int32Column*>(column);
        CHECK(int_column != nullptr);
        return int_column->immutable_data();
    }

    Op _op;
    int32_t _value;
    std::string _label;
};

Schema int_schema() {
    FieldPtr field = std::make_shared<Field>(0, "c1", get_type_info(TYPE_INT), false);
    return Schema({field});
}

ChunkPtr make_int_chunk(const std::vector<int32_t>& values) {
    auto chunk = ChunkFactory::new_chunk(int_schema(), values.size());
    auto* column = chunk->get_column_raw_ptr_by_index(0);
    (void)column->append_numbers(values.data(), values.size() * sizeof(int32_t));
    return chunk;
}

std::vector<uint8_t> evaluate(PredicateTree* tree, const ChunkPtr& chunk) {
    std::vector<uint8_t> selection(chunk->num_rows(), 0);
    EXPECT_OK(tree->evaluate(chunk.get(), selection.data()));
    return selection;
}

} // namespace

TEST(PredicateTreeTest, EvaluateAndTreeAndMaps) {
    IntComparisonPredicate ge2(0, IntComparisonPredicate::Op::GE, 2, "ge2");
    IntComparisonPredicate lt4(0, IntComparisonPredicate::Op::LT, 4, "lt4");

    PredicateAndNode root;
    root.add_child(PredicateColumnNode(&ge2));
    root.add_child(PredicateColumnNode(&lt4));
    auto tree = PredicateTree::create(std::move(root));

    auto chunk = make_int_chunk({1, 2, 3, 4});
    EXPECT_EQ((std::vector<uint8_t>{0, 1, 1, 0}), evaluate(&tree, chunk));

    EXPECT_EQ(2, tree.size());
    EXPECT_EQ(1, tree.num_columns());
    EXPECT_TRUE(tree.contains_column(0));
    EXPECT_FALSE(tree.has_or_predicate());
    EXPECT_EQ(1, tree.get_immediate_column_predicate_map().size());
    EXPECT_EQ(2, tree.get_immediate_column_predicate_map().at(0).size());
    EXPECT_TRUE(tree.get_non_immediate_column_predicate_map().empty());
    EXPECT_EQ(2, tree.get_all_column_predicate_map().at(0).size());
}

TEST(PredicateTreeTest, EvaluateNestedOrTreeAndMaps) {
    IntComparisonPredicate lt2(0, IntComparisonPredicate::Op::LT, 2, "lt2");
    IntComparisonPredicate ge4(0, IntComparisonPredicate::Op::GE, 4, "ge4");

    PredicateOrNode disjunction;
    disjunction.add_child(PredicateColumnNode(&lt2));
    disjunction.add_child(PredicateColumnNode(&ge4));

    PredicateAndNode root;
    root.add_child(std::move(disjunction));
    auto tree = PredicateTree::create(std::move(root));

    auto chunk = make_int_chunk({1, 2, 3, 4});
    EXPECT_EQ((std::vector<uint8_t>{1, 0, 0, 1}), evaluate(&tree, chunk));

    EXPECT_EQ(2, tree.size());
    EXPECT_EQ(1, tree.num_columns());
    EXPECT_TRUE(tree.has_or_predicate());
    EXPECT_TRUE(tree.get_immediate_column_predicate_map().empty());
    EXPECT_EQ(2, tree.get_non_immediate_column_predicate_map().at(0).size());
    EXPECT_EQ(2, tree.get_all_column_predicate_map().at(0).size());

    const auto debug_string = tree.root().debug_string();
    EXPECT_NE(std::string::npos, debug_string.find("lt2"));
    EXPECT_NE(std::string::npos, debug_string.find("ge4"));
}

TEST(PredicateTreeTest, PartitionCopyAndMove) {
    IntComparisonPredicate c0_ge2(0, IntComparisonPredicate::Op::GE, 2, "c0_ge2");
    IntComparisonPredicate c1_lt4(1, IntComparisonPredicate::Op::LT, 4, "c1_lt4");

    auto column_zero = [](const auto& node_ptr) {
        return node_ptr.visit([](const auto& node) {
            using NodeType = std::decay_t<decltype(node)>;
            if constexpr (std::is_same_v<NodeType, PredicateColumnNode>) {
                return node.col_pred()->column_id() == 0;
            }
            return false;
        });
    };

    PredicateAndNode copy_source;
    copy_source.add_child(PredicateColumnNode(&c0_ge2));
    copy_source.add_child(PredicateColumnNode(&c1_lt4));

    PredicateAndNode copied_true;
    PredicateAndNode copied_false;
    copy_source.partition_copy(column_zero, &copied_true, &copied_false);
    EXPECT_EQ(1, copied_true.num_children());
    EXPECT_EQ(1, copied_false.num_children());
    EXPECT_EQ(2, copy_source.num_children());

    PredicateAndNode move_source;
    move_source.add_child(PredicateColumnNode(&c0_ge2));
    move_source.add_child(PredicateColumnNode(&c1_lt4));

    PredicateAndNode moved_true;
    PredicateAndNode moved_false;
    move_source.partition_move(column_zero, &moved_true, &moved_false);
    EXPECT_EQ(1, moved_true.num_children());
    EXPECT_EQ(1, moved_false.num_children());
}

} // namespace starrocks
