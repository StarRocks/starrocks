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

#include "storage/column_predicate_inverted_index_fallback.h"

#include <gtest/gtest.h>

#include <memory>
#include <vector>

#include "base/testutil/assert.h"
#include "common/object_pool.h"
#include "roaring/roaring.hh"
#include "storage/column_expr_predicate.h"
#include "storage/column_predicate.h"
#include "storage/index/inverted/inverted_index_iterator.h"
#include "storage/olap_common.h"
#include "storage/predicate_tree/predicate_tree.h"
#include "storage/rowset/or_match_fallback_visitor.h"
#include "storage/types.h"
#include "types/logical_type.h"

namespace starrocks {

// Verifies that InvertedIndexFallbackPredicate, which is the wrapper introduced
// by the OR-MATCH fallback path, evaluates correctly against its segment-local
// rowid bitmap. The wrapped ColumnExprPredicate carries no real expression
// context here (passing nullptr expr_ctx is a documented no-op), so calls into
// the wrapped predicate that depend on the AST (is_negated_expr, is_match_expr)
// return the safe defaults. That is precisely the surface we want to exercise:
// the bitmap-vs-rowid loop and the [from, to) bounds contract.
class InvertedIndexFallbackPredicateTest : public ::testing::Test {
protected:
    static ColumnExprPredicate* make_wrapped(ColumnId cid = 0) {
        auto status_or =
                ColumnExprPredicate::make_column_expr_predicate(get_type_info(TYPE_VARCHAR), cid, /*state=*/nullptr,
                                                                /*expr_ctx=*/nullptr, /*slot_desc=*/nullptr);
        EXPECT_TRUE(status_or.ok());
        return status_or.value();
    }

    static std::string render(const std::vector<uint8_t>& v, size_t n) {
        std::string s;
        s.reserve(n * 2);
        for (size_t i = 0; i < n; i++) {
            if (i > 0) s.push_back(',');
            s.push_back(v[i] ? '1' : '0');
        }
        return s;
    }
};

TEST_F(InvertedIndexFallbackPredicateTest, evaluate_all_hits) {
    std::unique_ptr<ColumnExprPredicate> wrapped(make_wrapped());
    roaring::Roaring bitmap;
    bitmap.add(10);
    bitmap.add(20);
    bitmap.add(30);
    std::vector<rowid_t> rowids{10, 20, 30};

    InvertedIndexFallbackPredicate pred(wrapped.get(), std::move(bitmap), &rowids);
    std::vector<uint8_t> sel(3, 0);
    ASSERT_OK(pred.evaluate(/*column=*/nullptr, sel.data(), 0, 3));
    EXPECT_EQ("1,1,1", render(sel, 3));
}

TEST_F(InvertedIndexFallbackPredicateTest, evaluate_all_misses) {
    std::unique_ptr<ColumnExprPredicate> wrapped(make_wrapped());
    // empty bitmap simulates "no matches in this segment"
    roaring::Roaring bitmap;
    std::vector<rowid_t> rowids{1, 2, 3};

    InvertedIndexFallbackPredicate pred(wrapped.get(), std::move(bitmap), &rowids);
    std::vector<uint8_t> sel(3, 1);
    ASSERT_OK(pred.evaluate(/*column=*/nullptr, sel.data(), 0, 3));
    EXPECT_EQ("0,0,0", render(sel, 3));
}

TEST_F(InvertedIndexFallbackPredicateTest, evaluate_mixed) {
    std::unique_ptr<ColumnExprPredicate> wrapped(make_wrapped());
    roaring::Roaring bitmap;
    bitmap.add(10);
    bitmap.add(30);
    // rowids monotonic, matching how _read() appends from SparseRange
    std::vector<rowid_t> rowids{5, 10, 20, 30, 40};

    InvertedIndexFallbackPredicate pred(wrapped.get(), std::move(bitmap), &rowids);
    std::vector<uint8_t> sel(5, 0xAA); // pre-fill with sentinel
    ASSERT_OK(pred.evaluate(/*column=*/nullptr, sel.data(), 0, 5));
    EXPECT_EQ("0,1,0,1,0", render(sel, 5));
}

// Regression: the original implementation looped to _rowid_buffer->size()
// ignoring `to`, which could overwrite past the selection or read past the
// buffer if the two ever diverged. The refactor switched to honoring [from,
// to). This test wires the rowid buffer to be larger than `to` and asserts
// we only touch sel[0..to).
TEST_F(InvertedIndexFallbackPredicateTest, evaluate_honors_to_bound) {
    std::unique_ptr<ColumnExprPredicate> wrapped(make_wrapped());
    roaring::Roaring bitmap;
    bitmap.add(100);
    bitmap.add(200);
    bitmap.add(300);
    bitmap.add(400);
    std::vector<rowid_t> rowids{100, 200, 300, 400}; // 4 rowids captured

    InvertedIndexFallbackPredicate pred(wrapped.get(), std::move(bitmap), &rowids);
    // Evaluate only the first two rows; trailing two sentinel bytes must
    // survive untouched.
    std::vector<uint8_t> sel(4, 0x55);
    ASSERT_OK(pred.evaluate(/*column=*/nullptr, sel.data(), 0, 2));
    EXPECT_EQ(1, sel[0]);
    EXPECT_EQ(1, sel[1]);
    EXPECT_EQ(0x55, sel[2]);
    EXPECT_EQ(0x55, sel[3]);
}

TEST_F(InvertedIndexFallbackPredicateTest, evaluate_empty_range_is_noop) {
    std::unique_ptr<ColumnExprPredicate> wrapped(make_wrapped());
    roaring::Roaring bitmap;
    bitmap.add(7);
    std::vector<rowid_t> rowids{7};

    InvertedIndexFallbackPredicate pred(wrapped.get(), std::move(bitmap), &rowids);
    std::vector<uint8_t> sel(1, 0x42);
    ASSERT_OK(pred.evaluate(/*column=*/nullptr, sel.data(), 0, 0));
    EXPECT_EQ(0x42, sel[0]); // untouched
}

// evaluate_and clears (ANDs in zeros) where the bitmap misses.
TEST_F(InvertedIndexFallbackPredicateTest, evaluate_and_combines) {
    std::unique_ptr<ColumnExprPredicate> wrapped(make_wrapped());
    roaring::Roaring bitmap;
    bitmap.add(10);
    bitmap.add(30);
    std::vector<rowid_t> rowids{10, 20, 30, 40};

    InvertedIndexFallbackPredicate pred(wrapped.get(), std::move(bitmap), &rowids);
    std::vector<uint8_t> sel{1, 1, 1, 0}; // last row already filtered out
    ASSERT_OK(pred.evaluate_and(/*column=*/nullptr, sel.data(), 0, 4));
    // row 10 hits -> 1&1=1; row 20 misses -> 1&0=0; row 30 hits -> 1&1=1;
    // row 40 misses but selection already 0 -> 0&0=0
    EXPECT_EQ("1,0,1,0", render(sel, 4));
}

// evaluate_or sets bits where the bitmap hits; pre-set selection bits stay set.
TEST_F(InvertedIndexFallbackPredicateTest, evaluate_or_combines) {
    std::unique_ptr<ColumnExprPredicate> wrapped(make_wrapped());
    roaring::Roaring bitmap;
    bitmap.add(20);
    bitmap.add(40);
    std::vector<rowid_t> rowids{10, 20, 30, 40};

    InvertedIndexFallbackPredicate pred(wrapped.get(), std::move(bitmap), &rowids);
    std::vector<uint8_t> sel{1, 0, 0, 0}; // first row already matched by sibling OR-arm
    ASSERT_OK(pred.evaluate_or(/*column=*/nullptr, sel.data(), 0, 4));
    EXPECT_EQ("1,1,0,1", render(sel, 4));
}

// evaluate_and / evaluate_or must respect [from, to) too: the temp buffer they
// allocate is sized to (to - from). If evaluate ignored `to` and instead used
// _rowid_buffer->size(), this case would write past the temp buffer.
TEST_F(InvertedIndexFallbackPredicateTest, evaluate_and_honors_to_bound) {
    std::unique_ptr<ColumnExprPredicate> wrapped(make_wrapped());
    roaring::Roaring bitmap;
    bitmap.add(1);
    bitmap.add(2);
    bitmap.add(3);
    std::vector<rowid_t> rowids{1, 2, 3}; // 3 rowids captured

    InvertedIndexFallbackPredicate pred(wrapped.get(), std::move(bitmap), &rowids);
    std::vector<uint8_t> sel{1, 1, 0xCC}; // pre-existing 0xCC must survive
    ASSERT_OK(pred.evaluate_and(/*column=*/nullptr, sel.data(), 0, 2));
    EXPECT_EQ(1, sel[0]);
    EXPECT_EQ(1, sel[1]);
    EXPECT_EQ(0xCC, sel[2]); // untouched
}

// Construction must inherit index_filter_only / is_expr_predicate from the
// wrapped predicate so that the rest of the storage pipeline treats the
// wrapper consistently.
TEST_F(InvertedIndexFallbackPredicateTest, inherits_wrapped_attributes) {
    std::unique_ptr<ColumnExprPredicate> wrapped(make_wrapped(/*cid=*/7));
    wrapped->set_index_filter_only(true);

    roaring::Roaring bitmap;
    std::vector<rowid_t> rowids;
    InvertedIndexFallbackPredicate pred(wrapped.get(), std::move(bitmap), &rowids);

    EXPECT_TRUE(pred.is_index_filter_only());
    EXPECT_TRUE(pred.is_expr_predicate());
    EXPECT_EQ(PredicateType::kGinFallback, pred.type());
    EXPECT_EQ(7u, pred.column_id());
}

// Calling seek_inverted_index on a fallback wrapper is a programming error:
// fallback predicates already carry their pre-loaded segment bitmap.
TEST_F(InvertedIndexFallbackPredicateTest, seek_inverted_index_is_rejected) {
    std::unique_ptr<ColumnExprPredicate> wrapped(make_wrapped());
    roaring::Roaring bitmap;
    std::vector<rowid_t> rowids;
    InvertedIndexFallbackPredicate pred(wrapped.get(), std::move(bitmap), &rowids);

    roaring::Roaring out;
    auto st = pred.seek_inverted_index("c0", /*iterator=*/nullptr, &out);
    EXPECT_FALSE(st.ok());
    EXPECT_TRUE(st.is_internal_error());
}

// is_negated_expr() forwards to the wrapped predicate; with an empty expr ctx
// chain the wrapped predicate reports false, so hits stay 1 and misses stay 0.
// This is the same expectation as evaluate_mixed, just exercised through the
// is_negated_expr() forwarding code path explicitly.
TEST_F(InvertedIndexFallbackPredicateTest, non_negated_uses_hit_one) {
    std::unique_ptr<ColumnExprPredicate> wrapped(make_wrapped());
    EXPECT_FALSE(wrapped->is_negated_expr());

    roaring::Roaring bitmap;
    bitmap.add(50);
    std::vector<rowid_t> rowids{50, 60};

    InvertedIndexFallbackPredicate pred(wrapped.get(), std::move(bitmap), &rowids);
    EXPECT_FALSE(pred.is_negated_expr());

    std::vector<uint8_t> sel(2, 0);
    ASSERT_OK(pred.evaluate(/*column=*/nullptr, sel.data(), 0, 2));
    EXPECT_EQ("1,0", render(sel, 2));
}

// ---------------------------------------------------------------------------
// OrMatchFallbackVisitor tests
// ---------------------------------------------------------------------------
//
// Coverage scope: the visitor's reachable branches when wrapping doesn't fire
// (non-Expr predicate, non-MATCH Expr predicate) and the compound-node
// template overload that recurses through OR-subtrees.
//
// The "MATCH wrap" path (load bitmap, allocate InvertedIndexFallbackPredicate,
// set_col_pred) is reachable only with a real MATCH expression whose AST
// satisfies ColumnExprPredicate::read_inverted_index's structural checks
// (MATCH_EXPR root with slot_ref + string_literal children). Standing up that
// expression machinery at the BE unit-test level is significantly heavier than
// the surface it would cover. End-to-end coverage of that path, plus the
// SegmentIterator::do_get_next() branches that flip on
// _inverted_index_ctx->has_fallback_predicates, comes from the SQL-tester
// integration suite under test/sql/test_inverted_index.

class OrMatchFallbackVisitorTest : public ::testing::Test {
protected:
    bool has_fallback = false;
    std::vector<InvertedIndexIterator*> iterators{16, nullptr};
    ObjectPool pool;
    std::vector<rowid_t> rowid_buffer;

    OrMatchFallbackVisitor make_visitor() {
        return OrMatchFallbackVisitor{iterators, pool, &rowid_buffer, &has_fallback};
    }
};

// A predicate column node whose underlying predicate is NOT a ColumnExprPredicate
// must short-circuit at the kExpr type check; the visitor is a no-op.
TEST_F(OrMatchFallbackVisitorTest, skips_non_expr_predicate) {
    std::unique_ptr<ColumnPredicate> eq(new_column_eq_predicate(get_type_info(TYPE_INT), /*cid=*/0, "1"));
    PredicateColumnNode node(eq.get());
    const auto* original = node.col_pred();

    auto visitor = make_visitor();
    ASSERT_OK(visitor(node));

    EXPECT_FALSE(has_fallback);
    EXPECT_EQ(original, node.col_pred()); // not swapped for a wrapper
}

// An Expr predicate that is NOT a MATCH predicate (here: empty expr-ctx chain,
// is_match_expr() returns false) must also short-circuit, after the down_cast
// runs. Covers the `pred->type() == kExpr` -> `!is_match_expr()` branch.
TEST_F(OrMatchFallbackVisitorTest, skips_non_match_expr_predicate) {
    auto status_or = ColumnExprPredicate::make_column_expr_predicate(get_type_info(TYPE_VARCHAR), /*cid=*/0,
                                                                     /*state=*/nullptr,
                                                                     /*expr_ctx=*/nullptr, /*slot_desc=*/nullptr);
    ASSERT_TRUE(status_or.ok());
    std::unique_ptr<ColumnExprPredicate> expr_pred(status_or.value());
    ASSERT_EQ(PredicateType::kExpr, expr_pred->type());
    ASSERT_FALSE(expr_pred->is_match_expr());

    PredicateColumnNode node(expr_pred.get());
    auto visitor = make_visitor();
    ASSERT_OK(visitor(node));

    EXPECT_FALSE(has_fallback);
    EXPECT_EQ(expr_pred.get(), node.col_pred());
}

// The compound-node template overload must invoke the column-node operator on
// every leaf reachable through children(). Build an OR node holding two
// non-Expr column children; the visitor walks both without wrapping anything.
TEST_F(OrMatchFallbackVisitorTest, recurses_into_compound_children) {
    std::unique_ptr<ColumnPredicate> eq0(new_column_eq_predicate(get_type_info(TYPE_INT), /*cid=*/0, "1"));
    std::unique_ptr<ColumnPredicate> eq1(new_column_eq_predicate(get_type_info(TYPE_INT), /*cid=*/1, "2"));

    PredicateOrNode or_node;
    or_node.add_child(PredicateColumnNode(eq0.get()));
    or_node.add_child(PredicateColumnNode(eq1.get()));
    ASSERT_EQ(2u, or_node.num_children());

    auto visitor = make_visitor();
    ASSERT_OK(visitor(or_node));

    EXPECT_FALSE(has_fallback);
}

// Nested compound traversal: OR( AND( col, col ), col ). The OR-overload
// recurses into the AND-overload, which in turn dispatches to the
// column-node overload for each leaf.
TEST_F(OrMatchFallbackVisitorTest, recurses_through_nested_compound) {
    std::unique_ptr<ColumnPredicate> eq0(new_column_eq_predicate(get_type_info(TYPE_INT), /*cid=*/0, "1"));
    std::unique_ptr<ColumnPredicate> eq1(new_column_eq_predicate(get_type_info(TYPE_INT), /*cid=*/1, "2"));
    std::unique_ptr<ColumnPredicate> eq2(new_column_eq_predicate(get_type_info(TYPE_INT), /*cid=*/2, "3"));

    PredicateAndNode and_node;
    and_node.add_child(PredicateColumnNode(eq0.get()));
    and_node.add_child(PredicateColumnNode(eq1.get()));

    PredicateOrNode or_node;
    or_node.add_child(std::move(and_node));
    or_node.add_child(PredicateColumnNode(eq2.get()));

    auto visitor = make_visitor();
    ASSERT_OK(visitor(or_node));

    EXPECT_FALSE(has_fallback);
}

} // namespace starrocks
