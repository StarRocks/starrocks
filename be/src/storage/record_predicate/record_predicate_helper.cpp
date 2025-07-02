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

#include "storage/record_predicate/record_predicate_helper.h"

#include <fmt/format.h>

#include "storage/chunk_helper.h"
#include "storage/record_predicate/column_hash_is_congruent.h"
#include "storage/tablet_schema.h"

namespace starrocks {

StatusOr<RecordPredicateUPtr> RecordPredicateHelper::create(const RecordPredicatePB& record_predicate_pb) {
    RETURN_IF_ERROR(RecordPredicateHelper::_check_valid_pb(record_predicate_pb));
    RecordPredicateUPtr predicate;
    switch (record_predicate_pb.type()) {
    case RecordPredicatePB::COLUMN_HASH_IS_CONGRUENT: {
        predicate = std::make_unique<ColumnHashIsCongruent>(record_predicate_pb);
        RETURN_IF_ERROR(predicate->init(record_predicate_pb));
        break;
    }
    default:
        return Status::InternalError(
                fmt::format("Unknown record predicate type: {}",
                            RecordPredicateHelper::from_type_to_string(record_predicate_pb.type())));
    }
    return predicate;
}

Status RecordPredicateHelper::check_valid_schema(const RecordPredicate& predicate, const Schema& chunk_schema) {
    auto column_names = predicate.column_names();
    for (const auto& column_name : *column_names) {
        if (chunk_schema.get_field_index_by_name(column_name) == -1) {
            return Status::InternalError(fmt::format(
                    "chunk schema does not contains all columns which record predicate need, {}", column_name));
        }
    }
    return Status::OK();
}

Status RecordPredicateHelper::get_column_ids(const RecordPredicate& predicate,
                                             const std::shared_ptr<const TabletSchema>& tablet_schema,
                                             std::set<ColumnId>* column_ids) {
    std::vector<ColumnId> schema_cids(tablet_schema->columns().size());
    std::iota(schema_cids.begin(), schema_cids.end(), 0);
    Schema schema = ChunkHelper::convert_schema(tablet_schema, schema_cids);
    return RecordPredicateHelper::get_column_ids(predicate, schema, column_ids);
}

Status RecordPredicateHelper::get_column_ids(const RecordPredicate& predicate, const Schema& schema,
                                             std::set<ColumnId>* column_ids) {
    RETURN_IF_ERROR(RecordPredicateHelper::check_valid_schema(predicate, schema));
    auto column_names = predicate.column_names();
    for (const auto& col_name : *column_names) {
        auto f = schema.get_field_by_name(col_name);
        if (f != nullptr) {
            column_ids->insert(f->id());
        }
    }
    return Status::OK();
}

std::string RecordPredicateHelper::from_type_to_string(RecordPredicatePB::RecordPredicateTypePB pred_type) {
    switch (pred_type) {
    case RecordPredicatePB::COLUMN_HASH_IS_CONGRUENT:
        return "COLUMN_HASH_IS_CONGRUENT";
    default:
        break;
    }
    return "UNKNOWN";
}

Status RecordPredicateHelper::_check_valid_pb(const RecordPredicatePB& record_predicate_pb) {
    switch (record_predicate_pb.type()) {
    case RecordPredicatePB::COLUMN_HASH_IS_CONGRUENT: {
        if (!record_predicate_pb.has_column_hash_is_congruent()) {
            return Status::InternalError(
                    "record predicate type is COLUMN_HASH_IS_CONGRUENT but no corresponding PredicatePB was defined");
        }
        break;
    }
    case RecordPredicatePB::UNKNOWN:
    default:
        return Status::InternalError(
                fmt::format("Unknown record predicate type: {}",
                            RecordPredicateHelper::from_type_to_string(record_predicate_pb.type())));
    }
    return Status::OK();
}

} // namespace starrocks