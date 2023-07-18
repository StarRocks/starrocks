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

#include "storage/column_or_predicate.h"

#include "common/object_pool.h"

namespace starrocks {

Status ColumnOrPredicate::evaluate(const Column* column, uint8_t* selection, uint16_t from, uint16_t to) const {
    return _evaluate(column, selection, from, to);
}

Status ColumnOrPredicate::evaluate_and(const Column* column, uint8_t* selection, uint16_t from, uint16_t to) const {
    _buff.resize(column->size());
    RETURN_IF_ERROR(_evaluate(column, _buff.data(), from, to));
    const uint8_t* p = _buff.data();
    for (uint16_t i = from; i < to; i++) {
        DCHECK((bool)(selection[i] & p[i]) == (selection[i] && p[i]));
        selection[i] &= p[i];
    }
    return Status::OK();
}

Status ColumnOrPredicate::evaluate_or(const Column* column, uint8_t* selection, uint16_t from, uint16_t to) const {
    for (const ColumnPredicate* child : _child) {
        RETURN_IF_ERROR(child->evaluate_or(column, selection, from, to));
    }
    return Status::OK();
}

bool ColumnOrPredicate::zone_map_filter(const ZoneMapDetail& detail) const {
    for (const ColumnPredicate* child : _child) {
        RETURN_IF(child->zone_map_filter(detail), true);
    }
    return _child.empty();
}

Status ColumnOrPredicate::_evaluate(const Column* column, uint8_t* selection, uint16_t from, uint16_t to) const {
    RETURN_IF_ERROR(_child[0]->evaluate(column, selection, from, to));
    for (size_t i = 1; i < _child.size(); i++) {
        RETURN_IF_ERROR(_child[i]->evaluate_or(column, selection, from, to));
    }
    return Status::OK();
}

Status ColumnOrPredicate::convert_to(const ColumnPredicate** output, const TypeInfoPtr& target_type_ptr,
                                     ObjectPool* obj_pool) const {
    ColumnOrPredicate* new_pred =
            obj_pool->add(new ColumnOrPredicate(get_type_info(target_type_ptr.get()), _column_id));
    for (auto pred : _child) {
        const ColumnPredicate* new_child = nullptr;
        RETURN_IF_ERROR(pred->convert_to(&new_child, get_type_info(target_type_ptr.get()), obj_pool));
        new_pred->_child.emplace_back(new_child);
    }
    *output = new_pred;
    return Status::OK();
}

} // namespace starrocks
