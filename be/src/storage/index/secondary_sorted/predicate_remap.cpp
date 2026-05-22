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

#include "storage/index/secondary_sorted/predicate_remap.h"

#include "common/logging.h"
#include "common/object_pool.h"
#include "storage/predicate_type.h"

namespace starrocks::secondary_sorted {

const ColumnPredicate* clone_predicate_with_remapped_col_id(const ColumnPredicate* src, ColumnId new_col_id,
                                                            ObjectPool* obj_pool) {
    if (src == nullptr) return nullptr;
    auto type_info = src->type_info_ptr();

    switch (src->type()) {
    case PredicateType::kEQ:
        return obj_pool->add(new_column_eq_predicate_from_datum(type_info, new_col_id, src->value()));
    case PredicateType::kNE:
        return obj_pool->add(new_column_ne_predicate_from_datum(type_info, new_col_id, src->value()));
    case PredicateType::kLT:
        return obj_pool->add(new_column_lt_predicate_from_datum(type_info, new_col_id, src->value()));
    case PredicateType::kLE:
        return obj_pool->add(new_column_le_predicate_from_datum(type_info, new_col_id, src->value()));
    case PredicateType::kGT:
        return obj_pool->add(new_column_gt_predicate_from_datum(type_info, new_col_id, src->value()));
    case PredicateType::kGE:
        return obj_pool->add(new_column_ge_predicate_from_datum(type_info, new_col_id, src->value()));
    case PredicateType::kInList:
        return obj_pool->add(new_column_in_predicate_from_datum(type_info, new_col_id, src->values()));
    case PredicateType::kNotInList:
        return obj_pool->add(new_column_not_in_predicate_from_datum(type_info, new_col_id, src->values()));
    case PredicateType::kIsNull:
        return obj_pool->add(new_column_null_predicate(type_info, new_col_id, /*is_null=*/true));
    case PredicateType::kNotNull:
        return obj_pool->add(new_column_null_predicate(type_info, new_col_id, /*is_null=*/false));
    default:
        // Unsupported predicate type for the secondary-index path -- skip it
        // and let the downstream segment scan filter the remaining rows.
        return nullptr;
    }
}

PredicateTree build_remapped_predicate_tree(const PredicateTree& source_tree,
                                            const std::vector<uint32_t>& source_index_col_ids, ObjectPool* obj_pool) {
    PredicateAndNode synthetic_root;
    if (source_tree.empty() || source_index_col_ids.empty() || obj_pool == nullptr) {
        return PredicateTree::create(std::move(synthetic_root));
    }

    // Map source schema column id -> synthetic column id (the declared order
    // of the index column in this index, 0..K-1).
    std::unordered_map<ColumnId, ColumnId> src_to_syn;
    src_to_syn.reserve(source_index_col_ids.size());
    for (size_t i = 0; i < source_index_col_ids.size(); ++i) {
        src_to_syn[source_index_col_ids[i]] = static_cast<ColumnId>(i);
    }

    const auto& immediate_map = source_tree.get_immediate_column_predicate_map();
    size_t remapped = 0;
    size_t skipped = 0;
    for (auto& [src_cid, preds] : immediate_map) {
        auto it = src_to_syn.find(src_cid);
        if (it == src_to_syn.end()) continue;
        const ColumnId syn_cid = it->second;
        for (const ColumnPredicate* src_pred : preds) {
            const ColumnPredicate* cloned = clone_predicate_with_remapped_col_id(src_pred, syn_cid, obj_pool);
            if (cloned == nullptr) {
                ++skipped;
                continue;
            }
            synthetic_root.add_child(PredicateColumnNode{cloned});
            ++remapped;
        }
    }

    VLOG(2) << "secondary_sorted::build_remapped_predicate_tree: remapped=" << remapped << " skipped=" << skipped;
    return PredicateTree::create(std::move(synthetic_root));
}

} // namespace starrocks::secondary_sorted
