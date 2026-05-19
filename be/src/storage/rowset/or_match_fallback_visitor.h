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
#include "storage/column_expr_predicate.h"
#include "storage/column_predicate.h"
#include "storage/column_predicate_inverted_index_fallback.h"
#include "storage/index/inverted/inverted_index_iterator.h"
#include "storage/olap_common.h"
#include "storage/predicate_tree/predicate_tree.h"

namespace starrocks {

// Walks an OR subtree of a predicate tree and replaces every MATCH-style
// ColumnExprPredicate it finds with an InvertedIndexFallbackPredicate that
// carries the segment-local rowid bitmap pre-loaded from the inverted index.
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
        ASSIGN_OR_RETURN(auto bitmap_opt, expr_pred->read_inverted_index(expr_pred->slot_desc()->col_name(), iter));
        if (!bitmap_opt.has_value()) {
            bitmap_opt.emplace(); // empty bitmap => no matches in this segment
        }
        auto* wrapper = pool.add(new InvertedIndexFallbackPredicate(expr_pred, std::move(*bitmap_opt), rowid_buffer));
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
