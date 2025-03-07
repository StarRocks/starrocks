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

#include <cstring>

#include "column/column.h"
#include "column/column_helper.h"
#include "common/compiler_util.h"
#include "common/statusor.h"
#include "storage/column_predicate.h"

namespace starrocks {

class ColumnPlaceholderPredicate final : public ColumnPredicate {
public:
    explicit ColumnPlaceholderPredicate(const TypeInfoPtr& type_info, ColumnId id) : ColumnPredicate(type_info, id) {}

    Status evaluate(const Column* column, uint8_t* selection, uint16_t from, uint16_t to) const override {
        return Status::OK();
    }
    Status evaluate_and(const Column* column, uint8_t* selection, uint16_t from, uint16_t to) const override {
        return Status::OK();
    }
    Status evaluate_or(const Column* column, uint8_t* selection, uint16_t from, uint16_t to) const override {
        return Status::OK();
    }
    StatusOr<uint16_t> evaluate_branchless(const Column* column, uint16_t* sel, uint16_t sel_size) const override {
        return sel_size;
    }
    bool can_vectorized() const override { return false; }
    PredicateType type() const override { return PredicateType::kPlaceHolder; }
    Status convert_to(const ColumnPredicate** output, const TypeInfoPtr& target_type_info,
                      ObjectPool* obj_pool) const override {
        return Status::NotSupported("not implemented");
    }
    std::string debug_string() const override {
        std::stringstream ss;
        ss << "placeholder " << ColumnPredicate::debug_string();
        return ss.str();
    }
};

ColumnPredicate* new_column_placeholder_predicate(const TypeInfoPtr& type_info, ColumnId id) {
    return new ColumnPlaceholderPredicate(type_info, id);
}
} // namespace starrocks