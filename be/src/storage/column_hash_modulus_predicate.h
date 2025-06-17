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

#include <string>
#include <vector>

#include "common/status.h"
#include "storage/tablet_schema.h"


namespace starrocks {
/*
 * ColumnHashModulusPredicate is used to evaluate a chunk of data as following:
 * 1. Compute the hash value of the specified columns.
 * 2. Check if the hash value modulo a given modulus equals a specified remainder.
*/
class ColumnHashModulusPredicate {
public:
    static StatusOr<ColumnHashModulusPredicate> create(
            const ColumnHashModulusPredicatePB& column_hash_modulus_predicate_pb);
    static Status get_column_ids(const ColumnHashModulusPredicate& pred, const TabletSchemaCSPtr& tablet_schema,
                                 std::set<ColumnId>* column_ids);
    static Status get_column_ids(const ColumnHashModulusPredicate& pred, const Schema& schema,
                                 std::set<ColumnId>* column_ids);

    ColumnHashModulusPredicate();
    ~ColumnHashModulusPredicate() = default;

    ColumnHashModulusPredicate(ColumnHashModulusPredicate&& other) = default;
    ColumnHashModulusPredicate& operator=(ColumnHashModulusPredicate&& other) = default;
    ColumnHashModulusPredicate(const ColumnHashModulusPredicate& other) = default;
    ColumnHashModulusPredicate& operator=(const ColumnHashModulusPredicate& other) = default;

    Status evaluate(Chunk* chunk, uint8_t* selection) const;
    Status evaluate(Chunk* chunk, uint8_t* selection, uint16_t from, uint16_t to) const;

    Status init(const ColumnHashModulusPredicatePB& column_hash_modulus_predicate_pb);
    Status contains_hash_columns(const Schema& schema) const;

    bool empty() const { return _hash_column_names.empty(); }
    const std::vector<std::string>& hash_column_names() const { return _hash_column_names; }

private:
    Status _check_valid_pb(const ColumnHashModulusPredicatePB& column_hash_modulus_predicate_pb) const;

    ColumnHashModulusPredicatePB::ColumnHashTypePB _type;
    int64_t _modulus;
    int64_t _remainder;
    std::vector<std::string> _hash_column_names;

    bool _inited;
};

} // namespace starrocks
