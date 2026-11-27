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

#include "storage_primitive/column_expr_predicate.h"

#include <gtest/gtest.h>

#include <functional>
#include <string>
#include <vector>

#include "base/testutil/assert.h"
#include "column/column_helper.h"
#include "common/object_pool.h"
#include "exprs/column_ref.h"
#include "exprs/compound_predicate.h"
#include "exprs/dictmapping_expr_interface.h"
#include "exprs/expr_context.h"
#include "exprs/literal.h"
#include "exprs/mock_vectorized_expr.h"
#include "exprs/placeholder_ref.h"
#include "runtime/descriptors.h"
#include "runtime/runtime_state.h"
#include "storage_primitive/zone_map_detail.h"
#include "types/type_info.h"

namespace starrocks {

TEST(ExtractLikePatternPrefixTest, prefix_rules) {
    struct Case {
        std::string pattern;
        std::string prefix;
        bool exact;
    };
    const std::vector<Case> cases = {
            {"abc%", "abc", false},                   // stops at the first '%'
            {"abc_%", "abc", false},                  // '_' is a wildcard too
            {"abc%def_", "abc", false},               // only the leading literal part counts
            {"a\\_c%", "a_c", false},                 // "\_" is a literal '_'
            {"a\\%c%", "a%c", false},                 // "\%" is a literal '%'
            {"a\\\\c%", "a\\c", false},               // "\\" is a literal '\'
            {"ab\\w%", "ab", false},                  // any other escape ends the prefix
            {"ab\\", "ab", false},                    // so does a trailing '\'
            {"star\\%", "star%", true},               // no wildcard: the pattern matches one value
            {"abc", "abc", true},                     // no wildcard
            {"\xea\xb0\x80%", "\xea\xb0\x80", false}, // multi-byte prefix
    };
    for (const auto& c : cases) {
        auto prefix = extract_like_pattern_prefix(c.pattern);
        ASSERT_TRUE(prefix.has_value()) << c.pattern;
        EXPECT_EQ(c.prefix, prefix->prefix) << c.pattern;
        EXPECT_EQ(c.exact, prefix->exact) << c.pattern;
    }

    for (const char* pattern : {"", "%abc", "_abc", "\\w%", "\\"}) {
        EXPECT_FALSE(extract_like_pattern_prefix(pattern).has_value()) << pattern;
    }
}

// Stands in for DictMappingExpr(column, string predicate); the rewrite reads only dict_mapping_origin_expr().
class MockDictMappingExpr final : public MockExpr, public DictMappingExprInterface {
public:
    MockDictMappingExpr(const TExprNode& node, ColumnPtr result) : MockExpr(node, std::move(result)) {}
    Expr* clone(ObjectPool* pool) const override { return pool->add(new MockDictMappingExpr(*this)); }
    SlotId dict_mapping_slot_id() const override { return 0; }
    Expr* dict_mapping_origin_expr() const override { return get_child(1); }
    Status rewrite_dict_mapping_expr(const std::function<StatusOr<Expr*>()>&) override { return Status::OK(); }
    void set_dict_mapping_output_id(SlotId) override {}
    void disable_dict_mapping_open_rewrite() override {}
};

// Builds `c LIKE '<pattern>'`, `starts_with(c, '<pattern>')` and similar predicates on a VARCHAR column and checks
// which zones the zone map rewrite keeps. The rewrite reads only the node type, the function name and the children of
// the call, and storage_primitive_test does not force-load the builtin functions, so a MockExpr stands in for the call.
class ColumnExprPredicateZoneMapRewriteTest : public ::testing::Test {
protected:
    Expr* string_literal(const std::string& value) {
        TExprNode node;
        node.node_type = TExprNodeType::STRING_LITERAL;
        node.type = _varchar_type.to_thrift();
        node.num_children = 0;
        TStringLiteral literal;
        literal.__set_value(value);
        node.__set_string_literal(literal);
        return _pool.add(new VectorizedLiteral(node));
    }

    Expr* column_ref() { return _pool.add(new ColumnRef(&_slot)); }

    // A global-dictionary predicate reads the column through a PlaceHolderRef with the slot id of the column.
    Expr* placeholder() {
        TPlaceHolder placeholder;
        placeholder.__set_slot_id(_slot.id());
        placeholder.__set_nullable(false);
        TExprNode node;
        node.node_type = TExprNodeType::PLACEHOLDER_EXPR;
        node.type = _varchar_type.to_thrift();
        node.num_children = 0;
        node.__set_vslot_ref(placeholder);
        return _pool.add(new PlaceHolderRef(node));
    }

    Expr* string_function(const std::string& name, const std::string& pattern) {
        return string_function(name, column_ref(), string_literal(pattern));
    }

    Expr* string_function(const std::string& name, Expr* column, Expr* pattern) {
        TFunctionName fn_name;
        fn_name.__set_function_name(name);
        TFunction fn;
        fn.__set_name(fn_name);

        TExprNode node;
        node.node_type = TExprNodeType::FUNCTION_CALL;
        node.type = TYPE_BOOLEAN_DESC.to_thrift();
        node.num_children = 2;
        node.__set_fn(fn);
        Expr* call = _pool.add(new MockExpr(node, ColumnHelper::create_const_column<TYPE_BOOLEAN>(true, 1)));
        call->add_child(column);
        call->add_child(pattern);
        return call;
    }

    Expr* negate(Expr* child) {
        TExprNode node;
        node.node_type = TExprNodeType::COMPOUND_PRED;
        node.type = TYPE_BOOLEAN_DESC.to_thrift();
        node.num_children = 1;
        node.__set_opcode(TExprOpcode::COMPOUND_NOT);
        Expr* expr = _pool.add(VectorizedCompoundPredicateFactory::from_thrift(node));
        expr->add_child(child);
        return expr;
    }

    // Returns the predicates that the zone map filter uses instead of `root`; empty when nothing is rewritten.
    std::vector<const ColumnExprPredicate*> rewrite(Expr* root, TypeInfoPtr type_info = get_type_info(TYPE_VARCHAR)) {
        std::vector<const ColumnExprPredicate*> output;
        auto* ctx = _pool.add(new ExprContext(root));
        Status st = ctx->prepare(&_state);
        if (st.ok()) {
            st = ctx->open(&_state);
        }
        EXPECT_OK(st);
        if (!st.ok()) {
            return output;
        }

        auto pred_or = ColumnExprPredicate::make_column_expr_predicate(std::move(type_info), 0, &_state, ctx, &_slot);
        EXPECT_TRUE(pred_or.ok());
        _original = _pool.add(pred_or.value());
        EXPECT_OK(_original->try_to_rewrite_for_zone_map_filter(&_pool, &output));
        return output;
    }

    // A zone is kept when every rewritten predicate keeps it (they form a conjunction).
    static bool keeps(const std::vector<const ColumnExprPredicate*>& preds, const std::string& min,
                      const std::string& max) {
        const ZoneMapDetail zone{Datum(Slice(min)), Datum(Slice(max))};
        for (const auto* pred : preds) {
            if (!pred->zone_map_filter(zone)) {
                return false;
            }
        }
        return true;
    }

    RuntimeState _state;
    ObjectPool _pool;
    TypeDescriptor _varchar_type = TypeDescriptor::create_varchar_type(32);
    SlotDescriptor _slot{0, "c", TypeDescriptor::create_varchar_type(32)};
    ColumnExprPredicate* _original = nullptr;
};

TEST_F(ColumnExprPredicateZoneMapRewriteTest, like_prefix) {
    auto preds = rewrite(string_function("like", "abc%"));
    ASSERT_EQ(2, preds.size());

    // The LIKE itself is not monotonic, so the zone map cannot use it.
    const ZoneMapDetail below{Datum(Slice("aaa")), Datum(Slice("abb"))};
    EXPECT_TRUE(_original->zone_map_filter(below));

    EXPECT_FALSE(keeps(preds, "aaa", "abb"));    // every value < 'abc'
    EXPECT_TRUE(keeps(preds, "abb", "abc"));     // max is 'abc'
    EXPECT_TRUE(keeps(preds, "abc_1", "abc_9")); // every value matches
    EXPECT_TRUE(keeps(preds, "aaa", "zzz"));     // range covers the prefix
    EXPECT_FALSE(keeps(preds, "abd", "azz"));    // every value >= 'abd'
}

TEST_F(ColumnExprPredicateZoneMapRewriteTest, like_with_underscore_and_escape) {
    auto preds = rewrite(string_function("LIKE", "ab\\_c_%"));
    ASSERT_EQ(2, preds.size());
    EXPECT_TRUE(keeps(preds, "ab_c1", "ab_c9"));
    EXPECT_FALSE(keeps(preds, "ab_d", "ab_z"));
    EXPECT_FALSE(keeps(preds, "ab", "ab_b"));
}

TEST_F(ColumnExprPredicateZoneMapRewriteTest, like_without_wildcard) {
    auto preds = rewrite(string_function("like", "star\\%"));
    ASSERT_EQ(2, preds.size());
    EXPECT_TRUE(keeps(preds, "star", "start"));
    EXPECT_FALSE(keeps(preds, "star%a", "stb"));
    EXPECT_FALSE(keeps(preds, "sta", "star"));
}

TEST_F(ColumnExprPredicateZoneMapRewriteTest, like_multi_byte_prefix) {
    // "\xea\xb0\x80\xeb\x82\x98" is two Hangul syllables; the upper bound increments the last byte.
    auto preds = rewrite(string_function("like", "\xea\xb0\x80\xeb\x82\x98%"));
    ASSERT_EQ(2, preds.size());
    EXPECT_TRUE(keeps(preds, "\xea\xb0\x80\xeb\x82\x98", "\xea\xb0\x80\xeb\x82\x98\xeb\x8b\xa4"));
    EXPECT_FALSE(keeps(preds, "\xea\xb0\x80\xeb\x8b\xa4", "\xea\xb0\x80\xeb\x9d\xbc"));

    // A prefix of 0xFF bytes has no upper bound, so only `col >= prefix` is used.
    preds = rewrite(string_function("like", "\xff\xff%"));
    ASSERT_EQ(1, preds.size());
    EXPECT_FALSE(keeps(preds, "a", "z"));
    EXPECT_TRUE(keeps(preds, "a", "\xff\xff\x01"));
}

TEST_F(ColumnExprPredicateZoneMapRewriteTest, starts_with) {
    auto preds = rewrite(string_function("starts_with", "abc"));
    ASSERT_EQ(2, preds.size());
    EXPECT_FALSE(keeps(preds, "aaa", "abb"));
    EXPECT_TRUE(keeps(preds, "abc", "abc"));
    EXPECT_FALSE(keeps(preds, "abd", "azz"));
}

TEST_F(ColumnExprPredicateZoneMapRewriteTest, dictionary_column) {
    // On a global-dictionary column the predicate arrives as DictMappingExpr(code column, LIKE(PlaceHolderRef, ...)).
    TExprNode node;
    node.node_type = TExprNodeType::DICT_EXPR;
    node.type = TYPE_BOOLEAN_DESC.to_thrift();
    node.num_children = 2;
    Expr* dict = _pool.add(new MockDictMappingExpr(node, ColumnHelper::create_const_column<TYPE_BOOLEAN>(true, 1)));
    dict->add_child(_pool.add(new ColumnRef(TYPE_INT_DESC, _slot.id())));
    dict->add_child(string_function("like", placeholder(), string_literal("abc%")));

    auto preds = rewrite(dict);
    ASSERT_EQ(2, preds.size());
    EXPECT_FALSE(keeps(preds, "aaa", "abb"));
    EXPECT_TRUE(keeps(preds, "abc_1", "abc_9"));
    EXPECT_FALSE(keeps(preds, "abd", "azz"));
}

TEST_F(ColumnExprPredicateZoneMapRewriteTest, null_only_zone) {
    auto preds = rewrite(string_function("like", "abc%"));
    ASSERT_EQ(2, preds.size());
    // LIKE on NULL is not true, and neither is the range.
    const ZoneMapDetail null_zone{Datum(), Datum()};
    for (const auto* pred : preds) {
        EXPECT_FALSE(pred->zone_map_filter(null_zone));
    }
}

TEST_F(ColumnExprPredicateZoneMapRewriteTest, not_rewritten) {
    EXPECT_TRUE(rewrite(string_function("like", "%abc")).empty());
    EXPECT_TRUE(rewrite(string_function("like", "_abc")).empty());
    EXPECT_TRUE(rewrite(string_function("starts_with", "")).empty());
    EXPECT_TRUE(rewrite(string_function("regexp", "^abc")).empty());
    EXPECT_TRUE(rewrite(negate(string_function("like", "abc%"))).empty());
    EXPECT_TRUE(rewrite(string_function("like", "abc%"), get_type_info(TYPE_CHAR)).empty());
    // The pattern must be a non-null VARCHAR literal.
    EXPECT_TRUE(rewrite(string_function("like", column_ref(), column_ref())).empty());
    EXPECT_TRUE(rewrite(string_function("like", column_ref(),
                                        _pool.add(new VectorizedLiteral(ColumnHelper::create_const_null_column(1),
                                                                        _varchar_type))))
                        .empty());
    // A predicate can carry a null type info: get_type_info(type, precision, scale) returns null for non-scalar types.
    EXPECT_TRUE(rewrite(string_function("like", "abc%"), nullptr).empty());
}

} // namespace starrocks
