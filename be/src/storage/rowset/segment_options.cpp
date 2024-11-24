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

#include "segment_options.h"

#include "storage/predicate_tree/predicate_tree.hpp"

namespace starrocks {

struct PredicateTreeConverter {
    template <CompoundNodeType ParentType>
    Status operator()(const PredicateColumnNode& node, PredicateCompoundNode<ParentType>& parent) {
        const auto* col_pred = node.col_pred();
        const auto cid = col_pred->column_id();
        const ColumnPredicate* new_col_pred;
        RETURN_IF_ERROR(col_pred->convert_to(&new_col_pred, get_type_info(new_types[cid]), obj_pool));
        parent.add_child(PredicateColumnNode{new_col_pred});
        return Status::OK();
    }

    template <CompoundNodeType Type, CompoundNodeType ParentType>
    Status operator()(const PredicateCompoundNode<Type>& node, PredicateCompoundNode<ParentType>& parent) {
        PredicateCompoundNode<Type> new_node;
        for (const auto& child : node.children()) {
            RETURN_IF_ERROR(child.visit(*this, new_node));
        }
        parent.add_child(std::move(new_node));
        return Status::OK();
    }

    const std::vector<LogicalType>& new_types;
    ObjectPool* obj_pool;
};

Status SegmentReadOptions::convert_to(SegmentReadOptions* dst, const std::vector<LogicalType>& new_types,
                                      ObjectPool* obj_pool) const {
    // ranges
    int num_ranges = ranges.size();
    dst->ranges.resize(num_ranges);
    for (int i = 0; i < num_ranges; ++i) {
        ranges[i].convert_to(&dst->ranges[i], new_types);
    }

    // predicates
    PredicateAndNode new_pred_root;
    RETURN_IF_ERROR(pred_tree.visit(PredicateTreeConverter{new_types, obj_pool}, new_pred_root));
    dst->pred_tree = PredicateTree::create(std::move(new_pred_root));

    // delete predicates
    RETURN_IF_ERROR(delete_predicates.convert_to(&dst->delete_predicates, new_types, obj_pool));

    dst->fs = fs;
    dst->stats = stats;
    dst->use_page_cache = use_page_cache;
    dst->temporary_data = temporary_data;
    dst->profile = profile;
    dst->global_dictmaps = global_dictmaps;
    dst->rowid_range_option = rowid_range_option;
    dst->short_key_ranges = short_key_ranges;
    dst->is_first_split_of_segment = is_first_split_of_segment;

    return Status::OK();
}

std::string SegmentReadOptions::debug_string() const {
    std::stringstream ss;
    ss << "ranges=[";
    for (int i = 0; i < ranges.size(); ++i) {
        if (i != 0) {
            ss << ", ";
        }
        ss << ranges[i].debug_string();
    }
    ss << "],predicates=[";
    ss << pred_tree.root().debug_string();
    ss << "],delete_predicates={";
    ss << "},unsafe_tablet_schema_ref={";
    ss << "},use_page_cache=" << use_page_cache;
    return ss.str();
}

} // namespace starrocks
