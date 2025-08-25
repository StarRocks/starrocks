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

#include "storage/record_predicate/column_hash_is_congruent.h"

#include "column/chunk.h"
#include "storage/record_predicate/record_predicate_helper.h"

namespace starrocks {

ColumnHashIsCongruent::ColumnHashIsCongruent(const RecordPredicatePB& record_predicate_pb)
        : RecordPredicate(record_predicate_pb), _modulus(0), _remainder(0) {}

Status ColumnHashIsCongruent::evaluate(Chunk* chunk, uint8_t* selection) const {
    return evaluate(chunk, selection, 0, chunk->num_rows());
}

Status ColumnHashIsCongruent::evaluate(Chunk* chunk, uint8_t* selection, uint16_t from, uint16_t to) const {
    RETURN_IF_ERROR(RecordPredicateHelper::check_valid_schema(*this, *chunk->schema()));
    size_t size = to - from;
    std::vector<uint32_t> hashes(size, 0);
    for (const auto& col_name : _column_names) {
        chunk->get_column_by_name(col_name)->crc32_hash(&(hashes)[0], from, size);
    }

    const uint32_t* hashes_data = hashes.data();
    for (size_t i = from; i < to; ++i) {
        selection[i] = (hashes_data[i - from] % _modulus == _remainder);
    }
    return Status::OK();
}

Status ColumnHashIsCongruent::init(const RecordPredicatePB& record_predicate_pb) {
    const auto& column_hash_is_congruent_pb = record_predicate_pb.column_hash_is_congruent();
    RETURN_IF_ERROR(_check_valid_pb(column_hash_is_congruent_pb));
    _modulus = column_hash_is_congruent_pb.modulus();
    _remainder = column_hash_is_congruent_pb.remainder();
    _column_names.insert(_column_names.end(), column_hash_is_congruent_pb.column_names().begin(),
                         column_hash_is_congruent_pb.column_names().end());
    return Status::OK();
}

Status ColumnHashIsCongruent::_check_valid_pb(const ColumnHashIsCongruentMeta& column_hash_is_congruent_meta) const {
    if (column_hash_is_congruent_meta.column_names().empty()) {
        return Status::InternalError("column hash congruent predicate has no hash columns defined");
    }
    for (const auto& column_name : column_hash_is_congruent_meta.column_names()) {
        if (column_name.empty()) {
            return Status::InternalError("column hash congruent predicate has empty hash column name");
        }
    }

    int64_t modulus = column_hash_is_congruent_meta.modulus();
    int64_t remainder = column_hash_is_congruent_meta.remainder();
    if (modulus <= 0 || remainder < 0 || remainder >= modulus) {
        return Status::InternalError("Invalid modulus or remainder in column hash congruent predicate");
    }
    return Status::OK();
}

} // namespace starrocks
