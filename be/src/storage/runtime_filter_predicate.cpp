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

#include "storage/runtime_filter_predicate.h"

#include <cstring>

#include "base/simd/simd.h"
#include "common/config_rowset_fwd.h"
#include "gutil/strings/fastmem.h"
#include "runtime/mem_tracker.h"
#include "storage/rowset/column_iterator.h"

namespace starrocks {

Status RuntimeFilterPredicatesRewriter::rewrite(ObjectPool* obj_pool, RuntimeFilterPredicates& preds,
                                                const std::vector<std::unique_ptr<ColumnIterator>>& column_iterators,
                                                const Schema& schema) {
    auto& predicates = preds._rf_predicates;
    for (size_t i = 0; i < predicates.size(); i++) {
        auto* pred = predicates[i];
        ColumnId column_id = pred->get_column_id();
        if (!column_iterators[column_id]->all_page_dict_encoded()) {
            continue;
        }
        auto* rf_desc = pred->get_rf_desc();
        // For dictionary-encoded columns, we can directly calculate the runtime filter based on the dictionary words
        // instead of decoding it first.
        std::vector<Slice> all_words;
        RETURN_IF_ERROR(column_iterators[column_id]->fetch_all_dict_words(&all_words));
        std::vector<std::string> dict_words;
        dict_words.reserve(all_words.size());
        for (const auto& word : all_words) {
            dict_words.emplace_back(word.get_data(), word.get_size());
        }
        predicates[i] = obj_pool->add(new DictColumnRuntimeFilterPredicate(rf_desc, column_id, std::move(dict_words)));
    }
    return Status::OK();
}
} // namespace starrocks