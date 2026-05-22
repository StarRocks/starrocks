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

#include "storage/column_predicate.h"
#include "storage/olap_common.h"
#include "storage/predicate_tree/predicate_tree.h"

namespace starrocks {
class ObjectPool;
} // namespace starrocks

namespace starrocks::secondary_sorted {

// Reconstructs |src| with a new column id by reading back its Datum operands
// via ColumnPredicate::value() / values() and dispatching to the matching
// new_column_*_predicate_from_datum factory. Returns nullptr (and leaves
// |obj_pool| untouched) when the predicate's PredicateType is not in the
// set we can rebuild for the secondary index path: kEQ / kNE / kLT / kLE /
// kGT / kGE / kInList / kNotInList / kIsNull / kNotNull.
//
// Returned predicate lifetime is owned by |obj_pool|.
const ColumnPredicate* clone_predicate_with_remapped_col_id(const ColumnPredicate* src, ColumnId new_col_id,
                                                            ObjectPool* obj_pool);

// Build a PredicateTree against the secondary-index file's synthetic schema
// (column ids 0..K-1 in declared order, K = source_index_col_ids.size()) by
// extracting predicates from |source_tree.get_immediate_column_predicate_map()|
// that touch any of the source-schema column ids in |source_index_col_ids|,
// cloning them with the remapped column id, and AND-ing them under a fresh
// root.
//
// Predicates that fall under an OR subtree of |source_tree| are intentionally
// skipped -- the immediate-children map suffices for the AND case that
// drives 95%+ of the index-eligible queries in practice. Unsupported
// PredicateType variants (kExpr, kMap, ...) are silently dropped; the worst
// case is that the index returns a superset of the candidate rows and the
// downstream segment scan filters the rest.
PredicateTree build_remapped_predicate_tree(const PredicateTree& source_tree,
                                            const std::vector<uint32_t>& source_index_col_ids, ObjectPool* obj_pool);

} // namespace starrocks::secondary_sorted
