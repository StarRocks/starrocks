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

#include "storage/lake/replication_schema_utils.h"

#include <fmt/format.h>
#include <google/protobuf/util/message_differencer.h>

#include <climits>
#include <map>
#include <set>
#include <unordered_set>
#include <vector>

namespace starrocks::lake {
namespace {
using google::protobuf::util::MessageDifferencer;
using Columns = std::map<int32_t, const ColumnPB*>;

Status unsupported(std::string_view reason) {
    return Status::NotSupported(fmt::format("LAKE_REPLICATION_UNSUPPORTED_LIGHT_SCHEMA_CHANGE: {}", reason));
}

bool has_unknown_fields(const google::protobuf::Message& message) {
    const auto* reflection = message.GetReflection();
    if (reflection->GetUnknownFields(message).field_count() != 0) return true;
    std::vector<const google::protobuf::FieldDescriptor*> fields;
    reflection->ListFields(message, &fields);
    for (const auto* field : fields) {
        if (field->cpp_type() != google::protobuf::FieldDescriptor::CPPTYPE_MESSAGE) continue;
        if (field->is_repeated()) {
            for (int i = 0; i < reflection->FieldSize(message, field); ++i) {
                if (has_unknown_fields(reflection->GetRepeatedMessage(message, field, i))) return true;
            }
        } else if (has_unknown_fields(reflection->GetMessage(message, field))) {
            return true;
        }
    }
    return false;
}

Status collect_columns(const TabletSchemaPB& schema, Columns* columns) {
    if (!schema.has_id() || !schema.IsInitialized() || schema.column_size() == 0) {
        return Status::Corruption("Invalid light schema change tablet schema");
    }
    if (has_unknown_fields(schema)) return unsupported("unknown schema fields");
    if (!schema.dropped_table_indices().empty()) return unsupported("dropped index definitions");
    std::unordered_set<std::string> names;
    for (const auto& column : schema.column()) {
        if (column.unique_id() < 0 || column.unique_id() == INT_MAX || column.name().empty() || column.type().empty() ||
            !columns->emplace(column.unique_id(), &column).second || !names.insert(column.name()).second) {
            return Status::Corruption(fmt::format("Invalid/duplicate column identity: schema_id={} column_uid={}",
                                                  schema.id(), column.unique_id()));
        }
    }
    return Status::OK();
}

bool equivalent_column(const ColumnPB& a, const ColumnPB& b) {
    auto lhs = a;
    auto rhs = b;
    lhs.clear_unique_id();
    rhs.clear_unique_id();
    // Child UIDs deliberately remain significant: nested UID conversion is not supported.
    return MessageDifferencer::Equivalent(lhs, rhs);
}

bool ordinary_value_column(const ColumnPB& column) {
    static const std::unordered_set<std::string> scalar_types = {
            "BOOL",   "BOOLEAN", "TINYINT",   "SMALLINT",  "INT",        "BIGINT",     "LARGEINT", "FLOAT",
            "DOUBLE", "DECIMAL", "DECIMALV2", "DECIMAL32", "DECIMAL64",  "DECIMAL128", "CHAR",     "VARCHAR",
            "DATE",   "DATE_V2", "DATETIME",  "TIMESTAMP", "DECIMAL_V2", "VARBINARY"};
    return !column.is_key() && !column.is_auto_increment() && !column.has_referenced_column_id() &&
           !column.has_referenced_column() && !column.has_agg_state_desc() && column.children_columns().empty() &&
           scalar_types.count(column.type()) != 0;
}

Status sort_columns(const TabletSchemaPB& schema, const Columns& columns, std::vector<int32_t>* uids) {
    std::vector<int32_t> by_position;
    for (auto index : schema.sort_key_idxes()) {
        if (index >= schema.column_size()) return Status::Corruption("Invalid sort key index");
        by_position.push_back(schema.column(index).unique_id());
    }
    for (auto uid : schema.sort_key_unique_ids()) {
        if (columns.count(uid) == 0) return Status::Corruption("Invalid sort key UID");
        uids->push_back(uid);
    }
    if (!uids->empty() && !by_position.empty() && *uids != by_position) {
        return Status::Corruption("Inconsistent sort key indexes and UIDs");
    }
    if (uids->empty()) *uids = std::move(by_position);
    if (uids->empty()) {
        for (const auto& column : schema.column()) {
            if (column.is_key()) uids->push_back(column.unique_id());
        }
    }
    return Status::OK();
}

// Embedded indexes are addressed by their column. External index files require a separate conversion.
Status comparable_indexes(const TabletSchemaPB& schema, const Columns& columns,
                          const std::set<std::string>& common_names, std::map<std::string, std::string>* indexes) {
    for (const auto& index : schema.table_indices()) {
        if (index.index_type() != BITMAP && index.index_type() != NGRAMBF) {
            return unsupported("external or unknown tablet index");
        }
        auto canonical = index;
        canonical.clear_index_id();
        canonical.clear_col_unique_id();
        std::string key = index.index_name();
        bool common = true;
        if (index.col_unique_id().empty()) return Status::Corruption("Index without columns");
        for (auto uid : index.col_unique_id()) {
            auto it = columns.find(uid);
            if (it == columns.end()) return Status::Corruption("Index references missing column");
            const auto& name = it->second->name();
            common &= common_names.count(name) > 0;
            key += fmt::format("/{}:{}", name.size(), name);
        }
        if (common && !indexes->emplace(key, canonical.SerializeAsString()).second) {
            return Status::Corruption("Duplicate tablet index definition");
        }
    }
    return Status::OK();
}

Status compatible_schema_properties(const TabletSchemaPB& a, const Columns& ac, const TabletSchemaPB& b,
                                    const Columns& bc) {
    auto properties = [](TabletSchemaPB schema) {
        schema.clear_id();
        schema.clear_deprecated_id();
        schema.clear_schema_version();
        schema.clear_next_column_unique_id();
        schema.clear_column();
        schema.clear_sort_key_idxes();
        schema.clear_sort_key_unique_ids();
        schema.clear_table_indices();
        // Physical write options are not a different read schema.
        schema.clear_compression_type();
        schema.clear_compression_level();
        schema.clear_num_rows_per_row_block();
        schema.clear_deprecated_is_in_memory();
        return schema;
    };
    if (!MessageDifferencer::Equivalent(properties(a), properties(b))) {
        return unsupported("table/key/short-key properties changed");
    }
    std::vector<int32_t> au, bu;
    RETURN_IF_ERROR(sort_columns(a, ac, &au));
    RETURN_IF_ERROR(sort_columns(b, bc, &bu));
    std::vector<std::string> an, bn, ak, bk;
    for (auto uid : au) an.push_back(ac.at(uid)->name());
    for (auto uid : bu) bn.push_back(bc.at(uid)->name());
    for (const auto& c : a.column())
        if (c.is_key()) ak.push_back(c.name());
    for (const auto& c : b.column())
        if (c.is_key()) bk.push_back(c.name());
    if (an != bn || ak != bk) return unsupported("key or sort column order changed");
    std::set<std::string> common_names;
    std::unordered_set<std::string_view> other_names;
    for (const auto& [_, column] : bc) other_names.insert(column->name());
    for (const auto& [_, column] : ac) {
        if (other_names.count(column->name())) common_names.insert(column->name());
    }
    std::map<std::string, std::string> ai, bi;
    RETURN_IF_ERROR(comparable_indexes(a, ac, common_names, &ai));
    RETURN_IF_ERROR(comparable_indexes(b, bc, common_names, &bi));
    if (ai != bi) return unsupported("embedded index definition changed");
    return Status::OK();
}
} // namespace

Status build_light_schema_change_uid_map(const TabletMetadataPB& source, const TabletMetadataPB& target,
                                         std::unordered_map<uint32_t, uint32_t>* uid_map,
                                         std::unordered_set<uint32_t>* dropped_column_uids) {
    uid_map->clear();
    dropped_column_uids->clear();
    // Shared/range files may already have been converted by a sibling tablet using a
    // different UID map. UUID-based reuse cannot prove that their footers match ours.
    if (source.has_range() || target.has_range()) return unsupported("range tablets");
    if (!source.idg_meta().idgs().empty()) return unsupported("external index delta group files");
    Columns current, destination;
    RETURN_IF_ERROR(collect_columns(source.schema(), &current));
    RETURN_IF_ERROR(collect_columns(target.schema(), &destination));
    RETURN_IF_ERROR(compatible_schema_properties(source.schema(), current, target.schema(), destination));
    if (current.size() != destination.size()) return unsupported("source/target current column count differs");
    std::unordered_map<std::string, const ColumnPB*> target_by_name;
    for (const auto& [uid, column] : destination) {
        target_by_name.emplace(column->name(), column);
    }
    for (const auto& [uid, column] : current) {
        auto it = target_by_name.find(column->name());
        if (it == target_by_name.end() || !equivalent_column(*column, *it->second)) {
            return unsupported(fmt::format("source/target column mismatch: column_uid={}", uid));
        }
        uid_map->emplace(uid, it->second->unique_id());
    }

    std::map<int64_t, const TabletSchemaPB*> referenced;
    std::unordered_set<uint32_t> rowset_ids;
    for (const auto& rowset : source.rowsets()) {
        if (!rowset.has_id() || !rowset_ids.insert(rowset.id()).second) {
            return Status::Corruption("Missing/duplicate live rowset ID");
        }
        if (rowset.has_range()) return unsupported("range rowsets");
        for (const auto& segment : rowset.segment_metas()) {
            if (segment.has_bundle_file_offset()) return unsupported("bundled segment files");
            if (segment.shared()) return unsupported("shared segment files");
            if (!segment.vector_index_ids().empty()) return unsupported("external segment index files");
        }
        auto it = source.rowset_to_schema().find(rowset.id());
        const auto schema_id = it == source.rowset_to_schema().end() ? source.schema().id() : it->second;
        const auto history = source.historical_schemas().find(schema_id);
        if (schema_id == source.schema().id()) {
            if (history != source.historical_schemas().end() &&
                !MessageDifferencer::Equivalent(history->second, source.schema())) {
                return Status::Corruption("Current schema conflicts with historical schema entry");
            }
            continue;
        }
        if (history == source.historical_schemas().end() || history->second.id() != schema_id) {
            return Status::Corruption(fmt::format("Missing/mismatched historical schema: rowset_id={} schema_id={}",
                                                  rowset.id(), schema_id));
        }
        referenced.emplace(schema_id, &history->second);
    }

    Columns all_columns = current;
    std::set<std::string> stable_names;
    for (const auto& [_, c] : current) stable_names.insert(c->name());
    for (const auto& [schema_id, schema] : referenced) {
        Columns historical;
        RETURN_IF_ERROR(collect_columns(*schema, &historical));
        RETURN_IF_ERROR(compatible_schema_properties(*schema, historical, source.schema(), current));
        std::vector<int32_t> sort_uids;
        RETURN_IF_ERROR(sort_columns(*schema, historical, &sort_uids));
        std::unordered_set<int32_t> sort_set(sort_uids.begin(), sort_uids.end());
        for (const auto& [uid, column] : historical) {
            auto old = all_columns.find(uid);
            if (old != all_columns.end() && !equivalent_column(*column, *old->second)) {
                return unsupported(
                        fmt::format("column identity/definition changed: schema_id={} column_uid={}", schema_id, uid));
            }
            all_columns.emplace(uid, column);
            if (current.count(uid) == 0 && (!ordinary_value_column(*column) || sort_set.count(uid))) {
                return unsupported(fmt::format("non-scalar/key/sort/generated column dropped: column_uid={}", uid));
            }
        }
        for (const auto& [uid, column] : current) {
            if (historical.count(uid)) continue;
            stable_names.erase(column->name());
            if (!ordinary_value_column(*column) || (!column->is_nullable() && !column->has_default_value())) {
                return unsupported(fmt::format("added column has no compatible default/NULL: column_uid={}", uid));
            }
        }
    }
    // Predicates are name-addressed. Never reinterpret a predicate on a dropped/re-added column.
    for (const auto& rowset : source.rowsets()) {
        if (!rowset.has_delete_predicate()) continue;
        const auto& predicate = rowset.delete_predicate();
        if (!predicate.IsInitialized() || has_unknown_fields(predicate) || !predicate.sub_predicates().empty()) {
            return unsupported("unrecognized delete predicate");
        }
        auto stable = [&](const auto& predicates) {
            for (const auto& p : predicates)
                if (!stable_names.count(p.column_name())) return false;
            return true;
        };
        if (!stable(predicate.binary_predicates()) || !stable(predicate.in_predicates()) ||
            !stable(predicate.is_null_predicates()))
            return unsupported("delete predicate references changed column");
    }
    // These columns were explicitly validated as dropped ordinary value columns.
    // Remove their footer metadata, not just their schema history: keeping an
    // invented retired UID would make a subsequent light-schema replication unable
    // to prove the identity of that physical column after normalization.
    for (const auto& [uid, _] : all_columns) {
        if (!uid_map->contains(uid)) dropped_column_uids->insert(uid);
    }
    return Status::OK();
}

void normalize_replication_rowset_schemas(TabletMetadataPB* metadata) {
    metadata->clear_rowset_to_schema();
    metadata->clear_historical_schemas();
    if (metadata->rowsets().empty()) return;
    const auto schema_id = metadata->schema().id();
    (*metadata->mutable_historical_schemas())[schema_id].CopyFrom(metadata->schema());
    for (const auto& rowset : metadata->rowsets()) (*metadata->mutable_rowset_to_schema())[rowset.id()] = schema_id;
}

Status validate_normalized_replication_schema(const TxnLogPB::OpReplication& op, const TabletMetadataPB& target) {
    if (!op.normalize_rowset_schema()) return Status::OK();
    if (!op.has_tablet_metadata() || op.txn_meta().incremental_snapshot() || !op.tablet_metadata().has_schema()) {
        return Status::Corruption("Invalid normalized lake replication log");
    }
    const auto& copied = op.tablet_metadata();
    if (!MessageDifferencer::Equals(copied.schema(), target.schema())) {
        return unsupported("target schema changed before publish; do not publish this snapshot");
    }
    if (copied.rowset_to_schema_size() != copied.rowsets_size() ||
        copied.historical_schemas_size() != (copied.rowsets().empty() ? 0 : 1)) {
        return Status::Corruption("Invalid normalized lake replication schema tracking");
    }
    for (const auto& rowset : copied.rowsets()) {
        auto mapping = copied.rowset_to_schema().find(rowset.id());
        auto schema = copied.historical_schemas().find(copied.schema().id());
        if (mapping == copied.rowset_to_schema().end() || mapping->second != copied.schema().id() ||
            schema == copied.historical_schemas().end() ||
            !MessageDifferencer::Equals(schema->second, copied.schema())) {
            return Status::Corruption("Dangling/conflicting normalized lake replication schema");
        }
    }
    return Status::OK();
}
} // namespace starrocks::lake
