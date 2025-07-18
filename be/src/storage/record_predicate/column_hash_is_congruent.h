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

#include "storage/record_predicate/record_predicate.h"

namespace starrocks {
/*
 * ColumnHashIsCongruentPB is used to evaluate a chunk of data as following:
 * 1. Compute the hash value of the specified columns.
 * 2. Check if the hash value modulo a given modulus equals a specified remainder.
*/
class ColumnHashIsCongruent final : public RecordPredicate {
public:
    using ColumnHashIsCongruentMeta = RecordPredicatePB::ColumnHashIsCongruentPB;

    ColumnHashIsCongruent(const RecordPredicatePB& record_predicate_pb);
    ~ColumnHashIsCongruent() override = default;

    Status evaluate(Chunk* chunk, uint8_t* selection) const override;

    Status evaluate(Chunk* chunk, uint8_t* selection, uint16_t from, uint16_t to) const override;

    Status init(const RecordPredicatePB& record_predicate_pb) override;

    const std::vector<std::string>* column_names() const override { return &_column_names; }

    bool equals(const RecordPredicate& other) const override {
        if (typeid(*this) != typeid(other)) {
            return false;
        }
        const auto& other_predicate = static_cast<const ColumnHashIsCongruent&>(other);
        return _modulus == other_predicate._modulus && _remainder == other_predicate._remainder &&
               _column_names == other_predicate._column_names;
    }

private:
    Status _check_valid_pb(const ColumnHashIsCongruentMeta& column_hash_is_congruent_meta) const;

    int64_t _modulus;
    int64_t _remainder;
    std::vector<std::string> _column_names;
};

} // namespace starrocks
