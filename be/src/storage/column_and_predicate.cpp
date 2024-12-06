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

#include "storage/column_and_predicate.h"

namespace starrocks {
Status ColumnAndPredicate::evaluate(const Column* column, uint8_t* selection, uint16_t from, uint16_t to) const {
    return _evaluate(column, selection, from, to);
}

Status ColumnAndPredicate::evaluate_and(const Column* column, uint8_t* selection, uint16_t from, uint16_t to) const {
    for (const ColumnPredicate* child : _child) {
        RETURN_IF_ERROR(child->evaluate_and(column, selection, from, to));
    }
    return Status::OK();
}

Status ColumnAndPredicate::evaluate_or(const Column* column, uint8_t* selection, uint16_t from, uint16_t to) const {
    _buff.resize(column->size());
    RETURN_IF_ERROR(_evaluate(column, _buff.data(), from, to));
    const uint8_t* p = _buff.data();
    for (uint16_t i = from; i < to; i++) {
        selection[i] |= p[i];
    }
    return Status::OK();
}

std::string ColumnAndPredicate::debug_string() const {
    std::stringstream ss;
    ss << "AND(";
    for (size_t i = 0; i < _child.size(); i++) {
        if (i != 0) {
            ss << ", ";
        }
        ss << i << ":" << _child[i]->debug_string();
    }
    ss << ")";
    return ss.str();
}

Status ColumnAndPredicate::_evaluate(const Column* column, uint8_t* selection, uint16_t from, uint16_t to) const {
    RETURN_IF_ERROR(_child[0]->evaluate(column, selection, from, to));
    for (size_t i = 1; i < _child.size(); i++) {
        RETURN_IF_ERROR(_child[i]->evaluate_and(column, selection, from, to));
    }
    return Status::OK();
}

// return false if page not satisfied
bool ColumnAndPredicate::zone_map_filter(const ZoneMapDetail& detail) const {
    for (const ColumnPredicate* child : _child) {
        RETURN_IF(!child->zone_map_filter(detail), false);
    }
    return true;
}

Status ColumnAndPredicate::convert_to(const ColumnPredicate** output, const TypeInfoPtr& target_type_ptr,
                                      ObjectPool* obj_pool) const {
    ColumnAndPredicate* new_pred =
            obj_pool->add(new ColumnAndPredicate(get_type_info(target_type_ptr.get()), _column_id));
    for (auto pred : _child) {
        const ColumnPredicate* new_child = nullptr;
        RETURN_IF_ERROR(pred->convert_to(&new_child, get_type_info(target_type_ptr.get()), obj_pool));
        new_pred->_child.emplace_back(new_child);
    }
    *output = new_pred;
    return Status::OK();
}
} // namespace starrocks