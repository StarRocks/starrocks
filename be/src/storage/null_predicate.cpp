// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/be/src/olap/null_predicate.cpp

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

#include "storage/null_predicate.h"

#include "runtime/string_value.hpp"
#include "storage/field.h"

namespace starrocks {

NullPredicate::NullPredicate(uint32_t column_id, bool is_null) : ColumnPredicate(column_id), _is_null(is_null) {}

NullPredicate::~NullPredicate() = default;

void NullPredicate::evaluate(ColumnBlock* block, uint16_t* sel, uint16_t* size) const {
    uint16_t new_size = 0;
    if (!block->is_nullable() && _is_null) {
        *size = 0;
        return;
    }
    for (uint16_t i = 0; i < *size; ++i) {
        uint16_t idx = sel[i];
        sel[new_size] = idx;
        new_size += (block->cell(idx).is_null() == _is_null);
    }
    *size = new_size;
}

Status NullPredicate::evaluate(const Schema& schema, const std::vector<BitmapIndexIterator*>& iterators,
                               uint32_t num_rows, Roaring* roaring) const {
    if (iterators[_column_id] != nullptr) {
        Roaring null_bitmap;
        RETURN_IF_ERROR(iterators[_column_id]->read_null_bitmap(&null_bitmap));
        if (_is_null) {
            *roaring &= null_bitmap;
        } else {
            *roaring -= null_bitmap;
        }
    }
    return Status::OK();
}

} //namespace starrocks
