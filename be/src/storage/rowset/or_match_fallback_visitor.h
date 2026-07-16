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

#pragma once

#include <vector>

#include "common/object_pool.h"
#include "common/status.h"
#include "gutil/casts.h"
#include "runtime/descriptors.h" // SlotDescriptor::col_name() is called below
#include "storage/column_predicate_inverted_index_fallback.h"
#include "storage/index/inverted/inverted_index_iterator.h"
#include "storage/olap_common.h"
#include "storage_primitive/column_expr_predicate.h"
#include "storage_primitive/column_predicate_factory.h"
#include "storage_primitive/predicate_tree/predicate_tree.h"

namespace starrocks {

// Walks an OR subtree of a predicate tree and replaces every MATCH-style
// ColumnExprPredicate it finds with an InvertedIndexFallbackPredicate that
// carries the segment-local rowid bitmap pre-loaded from the inverted index.
//
// The bitmap is built through the same ColumnExprPredicate::seek_inverted_index
// path the normal AND filtering uses, so it already is the set of rows for
// which the predicate evaluates TRUE. In particular the negated case
// (NOT (col MATCH x)) has both the matching rows and the NULL rows removed,
// matching SQL three-valued logic (NOT (NULL MATCH x) = NULL, not TRUE). The
// fallback predicate can therefore select rows by plain bitmap membership
// without re-applying the negation.
//
// Lives in a header (rather than as a function-local class) for two reasons:
//   1. C++ forbids member function templates inside function-local classes,
//      and the compound-node overload here is a template on CompoundNodeType.
//   2. Unit tests can construct it directly to exercise the early-return
//      branches without standing up a full SegmentIterator.
//
// The visitor's lifetime is bounded by a single
// SegmentIterator::_wrap_or_match_predicates_for_fallback() call; references
// it holds (iterators, pool, rowid_buffer, has_fallback_predicates) must
// outlive that call.
struct OrMatchFallbackVisitor {
    const std::vector<InvertedIndexIterator*>& iterators;
    ObjectPool& pool;
    const std::vector<rowid_t>* rowid_buffer;
    bool* has_fallback_predicates;
    uint32_t num_rows; // segment row count; the universe seek_inverted_index filters down from

    Status operator()(PredicateColumnNode& node) const {
        const auto* pred = node.col_pred();
        if (pred->type() != PredicateType::kExpr) {
            return Status::OK();
        }
        const auto* expr_pred = down_cast<const ColumnExprPredicate*>(pred);
        if (!expr_pred->is_match_expr()) {
            return Status::OK();
        }
        InvertedIndexIterator* iter = iterators[pred->column_id()];
        if (iter == nullptr) {
            return Status::OK();
        }
        // Seed with the whole segment and let seek_inverted_index narrow it to the
        // rows where the predicate is TRUE (intersecting matches for a plain MATCH,
        // subtracting both matches and NULLs for a negated MATCH).
        roaring::Roaring bitmap;
        bitmap.addRange(0, num_rows);
        std::string col_name(expr_pred->slot_desc()->col_name());
        RETURN_IF_ERROR(expr_pred->seek_inverted_index(col_name, iter, &bitmap));
        auto* wrapper = pool.add(new InvertedIndexFallbackPredicate(expr_pred, std::move(bitmap), rowid_buffer));
        node.set_col_pred(wrapper);
        *has_fallback_predicates = true;
        return Status::OK();
    }

    template <CompoundNodeType Type>
    Status operator()(PredicateCompoundNode<Type>& node) const {
        for (auto child : node.children()) {
            RETURN_IF_ERROR(child.visit(*this));
        }
        return Status::OK();
    }
};

} // namespace starrocks
