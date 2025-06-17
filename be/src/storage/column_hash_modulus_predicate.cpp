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

#include "storage/column_hash_modulus_predicate.h"
#include "storage/chunk_helper.h"

namespace starrocks {

StatusOr<ColumnHashModulusPredicate> ColumnHashModulusPredicate::create(
        const ColumnHashModulusPredicatePB& column_hash_modulus_predicate_pb) {
    ColumnHashModulusPredicate predicate;
    RETURN_IF_ERROR(predicate.init(column_hash_modulus_predicate_pb));
    return predicate;
}

Status ColumnHashModulusPredicate::get_column_ids(const ColumnHashModulusPredicate& pred, const TabletSchemaCSPtr& tablet_schema,
                                                  std::set<ColumnId>* column_ids) {
    std::vector<ColumnId> schema_cids(tablet_schema->columns().size());
    std::iota(schema_cids.begin(), schema_cids.end(), 0);
    Schema schema = ChunkHelper::convert_schema(tablet_schema, schema_cids);
    return ColumnHashModulusPredicate::get_column_ids(pred, schema, column_ids);
}

Status ColumnHashModulusPredicate::get_column_ids(const ColumnHashModulusPredicate& pred, const Schema& schema,
                                                  std::set<ColumnId>* column_ids) {
    std::set<ColumnId> tmp_column_cids;
    for (const auto& col_name : pred.hash_column_names()) {
        auto f = schema.get_field_by_name(col_name);
        if (f != nullptr) {
            tmp_column_cids.insert(f->id());
        } else {
            return Status::NotFound("Column not found in hash filter: " + col_name);
        }
    }

    for (auto const& col_id : tmp_column_cids) {
        column_ids->insert(col_id);
    }
    return Status::OK();
}

ColumnHashModulusPredicate::ColumnHashModulusPredicate()
        : _type(ColumnHashModulusPredicatePB::UNKNOWN),
          _modulus(0), _remainder(0), _hash_column_names(), _inited(false) {}

Status ColumnHashModulusPredicate::evaluate(Chunk* chunk, uint8_t* selection) const {
    return evaluate(chunk, selection, 0, chunk->num_rows());
}

Status ColumnHashModulusPredicate::evaluate(Chunk* chunk, uint8_t* selection, uint16_t from, uint16_t to) const {
    if (!_inited) {
        return Status::InternalError("column hash modulus predicate is not initialized");
    }
    RETURN_IF_ERROR(contains_hash_columns(*chunk->schema()));

    size_t size = to - from;
    std::vector<uint32_t> hashes(size, 0);
    for (const auto& col_name : _hash_column_names) {
        chunk->get_column_by_name(col_name)->crc32_hash(&(hashes)[0], from, to);
    }

    const uint32_t* hashes_data = hashes.data();
    for (size_t i = from; i < to; ++i) {
        selection[i] = (hashes_data[i] % _modulus == _remainder);
    }
    return Status::OK();
}

Status ColumnHashModulusPredicate::init(const ColumnHashModulusPredicatePB& column_hash_modulus_predicate_pb) {
    RETURN_IF_ERROR(_check_valid_pb(column_hash_modulus_predicate_pb));

    _type = column_hash_modulus_predicate_pb.type();
    _modulus = column_hash_modulus_predicate_pb.modulus();
    _remainder = column_hash_modulus_predicate_pb.remainder();
    _hash_column_names.assign(column_hash_modulus_predicate_pb.hash_column_names().begin(),
                              column_hash_modulus_predicate_pb.hash_column_names().end());

    _inited = true;
    return Status::OK();
}

Status ColumnHashModulusPredicate::_check_valid_pb(const ColumnHashModulusPredicatePB& column_hash_modulus_predicate_pb) const {
    if (column_hash_modulus_predicate_pb.type() != ColumnHashModulusPredicatePB::CRC32) {
        return Status::InternalError("column hash modulus predicate type is not CRC32");
    }
    if (column_hash_modulus_predicate_pb.hash_column_names().empty()) {
        return Status::InternalError("column hash modulus predicate has no hash columns defined");
    }
    for (const auto& column_name : column_hash_modulus_predicate_pb.hash_column_names()) {
        if (column_name.empty()) {
            return Status::InternalError("column hash modulus predicate has empty hash column name");
        }
    }
    int64_t modulus = column_hash_modulus_predicate_pb.modulus();
    int64_t remainder = column_hash_modulus_predicate_pb.remainder();
    if (modulus <= 0 || remainder < 0 || remainder >= modulus) {
        return Status::InternalError("Invalid modulus or remainder in column hash modulus predicate");

    }
    return Status::OK();
}

Status ColumnHashModulusPredicate::contains_hash_columns(const Schema& schema) const {
    for (const auto& column_name : _hash_column_names) {
        if (schema.get_field_index_by_name(column_name) == -1) {
            return Status::NotFound("Column not found in hash filter: " + column_name);
        }
    }
    return Status::OK();
}

} // namespace starrocks
