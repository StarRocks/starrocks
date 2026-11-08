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
#include <mutex>
#include <unordered_map>

#include "base/simd/simd.h"
#include "column/schema.h"
#include "common/config_rowset_fwd.h"
#include "common/object_pool.h"
#include "gutil/strings/fastmem.h"
#include "runtime/mem_tracker.h"
#include "storage/rowset/column_iterator.h"

namespace starrocks {

namespace {
struct PoolStaticDictMaskEntry {
    Filter mask;
    bool is_nullable = true;
};

using PoolStaticDictMasks = std::unordered_map<ColumnId, PoolStaticDictMaskEntry>;

std::mutex g_pool_static_dict_masks_mutex;
std::unordered_map<const ObjectPool*, PoolStaticDictMasks> g_pool_static_dict_masks;

struct PoolStaticDictMaskCleanup {
    explicit PoolStaticDictMaskCleanup(const ObjectPool* pool) : pool(pool) {}
    ~PoolStaticDictMaskCleanup() {
        std::lock_guard<std::mutex> lock(g_pool_static_dict_masks_mutex);
        g_pool_static_dict_masks.erase(pool);
    }
    const ObjectPool* pool;
};
} // namespace

void RuntimeFilterPredicatesRewriter::set_static_dict_mask(ObjectPool* obj_pool, ColumnId column_id, const Filter& mask,
                                                           bool is_nullable) {
    if (obj_pool == nullptr) {
        return;
    }
    bool need_register_cleanup = false;
    {
        std::lock_guard<std::mutex> lock(g_pool_static_dict_masks_mutex);
        auto [it, inserted] = g_pool_static_dict_masks.try_emplace(obj_pool);
        need_register_cleanup = inserted;
        it->second[column_id] = PoolStaticDictMaskEntry{mask, is_nullable};
    }
    if (need_register_cleanup) {
        obj_pool->add(new PoolStaticDictMaskCleanup(obj_pool));
    }
}

Status RuntimeFilterPredicatesRewriter::rewrite(ObjectPool* obj_pool, RuntimeFilterPredicates& preds,
                                                const std::vector<std::unique_ptr<ColumnIterator>>& column_iterators,
                                                const Schema& schema) {
    if (obj_pool != nullptr) {
        PoolStaticDictMasks pool_masks;
        {
            std::lock_guard<std::mutex> lock(g_pool_static_dict_masks_mutex);
            if (auto it = g_pool_static_dict_masks.find(obj_pool); it != g_pool_static_dict_masks.end()) {
                pool_masks = it->second;
            }
        }
        for (const auto& [cid, info] : pool_masks) {
            preds.set_static_dict_mask(cid, info.mask, info.is_nullable);
        }
    }

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
        bool is_nullable = true;
        for (const auto& field : schema.fields()) {
            if (field != nullptr && field->id() == column_id) {
                is_nullable = field->is_nullable();
                break;
            }
        }
        auto* dict_pred = obj_pool->add(
                new DictColumnRuntimeFilterPredicate(rf_desc, column_id, std::move(dict_words), is_nullable));
        if (pred->has_static_dict_mask()) {
            dict_pred->set_static_dict_mask(pred->static_dict_mask(), pred->is_nullable());
        }
        predicates[i] = dict_pred;
    }
    return Status::OK();
}
} // namespace starrocks