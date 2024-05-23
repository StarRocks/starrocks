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

#include "bitmap_index_reader.h"
#include "storage/olap_common.h"
#include "storage/predicate_tree/predicate_tree_fwd.h"
#include "storage/range.h"

namespace starrocks {

struct BitmapContext {
    struct ColumnContext {
        SparseRange<> bitmap_ranges;
        size_t cardinality;
        std::vector<const PredicateColumnNode*> nodes;
        bool has_is_null_pred = false;
    };

    struct CompoundNodeContext {
        std::unordered_map<ColumnId, ColumnContext> col_contexts;
        bool used = false;
    };

    /// Note that the address of a predicate node is used as the identity,
    /// and therefore do not modify the related PredicateTree, which may cause the address to change.

    std::unordered_map<const PredicateBaseNode*, bool> is_node_support_bitmap;
    // It ensures that `compound_node_to_context[node]` can be accessed after `is_node_support_bitmap[node]` is true.
    std::unordered_map<const PredicateBaseNode*, CompoundNodeContext> compound_node_to_context;

    std::unordered_set<const PredicateBaseNode*> nodes_to_erase;
};

class BitmapIndexEvaluator {
public:
    using BitmapIndexIteratorSupplier = std::function<StatusOr<BitmapIndexIterator*>(ColumnId)>;

    BitmapIndexEvaluator(const Schema& schema, const PredicateTree& pred_tree)
            : _schema(schema), _pred_tree(pred_tree) {}
    ~BitmapIndexEvaluator() { close(); }

    Status init(BitmapIndexIteratorSupplier&& supplier);

    Status evaluate(SparseRange<>& dst_scan_range, PredicateTree& dst_pred_tree);

    void close();

    bool has_bitmap_index() const { return _has_bitmap_index; }

private:
    friend struct BitmapIndexInitializer;
    friend struct BitmapIndexSeeker;
    friend struct BitmapIndexRetriver;
    friend struct BitmapIndexPredicateEraser;

    const Schema& _schema;
    const PredicateTree& _pred_tree;

    BitmapContext _ctx;
    std::vector<BitmapIndexIterator*> _bitmap_index_iterators;
    bool _has_bitmap_index = false;

    bool _closed = false;
};

} // namespace starrocks
