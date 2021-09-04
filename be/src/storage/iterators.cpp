// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/be/src/olap/iterators.cpp

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "storage/iterators.h"

#include "common/object_pool.h"
#include "storage/column_predicate.h"
#include "storage/olap_cond.h"
#include "storage/row_cursor.h"
#include "storage/tablet_schema.h"

namespace starrocks {

Status StorageReadOptions::convert_to(const StorageReadOptions** output, const TabletSchema& tablet_schema,
                                      const std::vector<FieldType>& new_types, ObjectPool* obj_pool) const {
    StorageReadOptions* new_opt = obj_pool->add(new StorageReadOptions());

    // key_ranges
    int num_key_ranges = key_ranges.size();
    new_opt->key_ranges.resize(num_key_ranges);
    for (int i = 0; i < num_key_ranges; ++i) {
        auto& key_range = key_ranges[i];

        // lower_key
        if (key_range.lower_key != nullptr) {
            RETURN_IF_ERROR(key_range.lower_key->convert_to(&new_opt->key_ranges[i].lower_key, new_types, obj_pool));
        } else {
            new_opt->key_ranges[i].lower_key = nullptr;
        }
        new_opt->key_ranges[i].include_lower = key_range.include_lower;
        // upper_key
        if (key_range.upper_key != nullptr) {
            RETURN_IF_ERROR(key_range.upper_key->convert_to(&new_opt->key_ranges[i].upper_key, new_types, obj_pool));
        } else {
            new_opt->key_ranges[i].upper_key = nullptr;
        }
        new_opt->key_ranges[i].include_upper = key_range.include_lower;
    }
    // conditions
    if (conditions != nullptr) {
        RETURN_IF_ERROR(conditions->convert_to(&new_opt->conditions, new_types, obj_pool));
    } else {
        new_opt->conditions = nullptr;
    }
    // delete_conditions
    size_t num_delete_conds = delete_conditions.size();
    new_opt->delete_conditions.resize(num_delete_conds, nullptr);
    for (size_t i = 0; i < num_delete_conds; ++i) {
        RETURN_IF_ERROR(delete_conditions[i]->convert_to(&new_opt->delete_conditions[i], new_types, obj_pool));
    }
    // column_predicates
    if (column_predicates != nullptr) {
        size_t num_preds = column_predicates->size();
        std::vector<const ColumnPredicate*>* new_preds =
                obj_pool->add(new std::vector<const ColumnPredicate*>(num_preds, nullptr));
        for (size_t i = 0; i < num_preds; ++i) {
            auto cid = (*column_predicates)[i]->column_id();
            FieldType from_type = tablet_schema.column(cid).type();
            FieldType to_type = new_types[cid];
            RETURN_IF_ERROR((*column_predicates)[i]->convert_to(&(*new_preds)[i], from_type, to_type, obj_pool));
        }
        new_opt->column_predicates = new_preds;
    } else {
        new_opt->column_predicates = nullptr;
    }

    new_opt->block_mgr = block_mgr;
    new_opt->stats = stats;
    new_opt->use_page_cache = use_page_cache;

    *output = new_opt;
    return Status::OK();
}

} // namespace starrocks
