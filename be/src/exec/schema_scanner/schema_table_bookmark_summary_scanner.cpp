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

#include "exec/schema_scanner/schema_table_bookmark_summary_scanner.h"

#include <fmt/format.h>

#include "common/logging.h"
#include "exec/schema_scanner/schema_helper.h"
#include "runtime/runtime_state.h"
#include "types/datum.h"
#include "types/logical_type.h"
#include "types/timestamp_value.h"

namespace starrocks {

TypeDescriptor SchemaTableBookmarkSummaryScanner::_latest_changed_physical_partitions_type =
        TypeDescriptor::create_array_type(TypeDescriptor::create_struct_type(
                {"id", "version", "time"},
                {TypeDescriptor::from_logical_type(TYPE_BIGINT), TypeDescriptor::from_logical_type(TYPE_BIGINT),
                 TypeDescriptor::from_logical_type(TYPE_DATETIME)}));

TypeDescriptor SchemaTableBookmarkSummaryScanner::_reference_summary_type = TypeDescriptor::create_struct_type(
        {"id", "time", "ttl_ms"},
        {TypeDescriptor::create_varchar_type(sizeof(Slice)), TypeDescriptor::from_logical_type(TYPE_DATETIME),
         TypeDescriptor::from_logical_type(TYPE_BIGINT)});

SchemaScanner::ColumnDesc SchemaTableBookmarkSummaryScanner::_s_columns_desc[] = {
        //   name,                                  type,                                                   size,                   is_null
        {"DB_ID", TypeDescriptor::from_logical_type(TYPE_BIGINT), sizeof(int64_t), false},
        {"TABLE_ID", TypeDescriptor::from_logical_type(TYPE_BIGINT), sizeof(int64_t), false},
        {"BOOKMARK_ID", TypeDescriptor::from_logical_type(TYPE_BIGINT), sizeof(int64_t), false},
        {"CREATE_TIME", TypeDescriptor::from_logical_type(TYPE_DATETIME), sizeof(DateTimeValue), false},
        {"LOGICAL_PARTITION_COUNT", TypeDescriptor::from_logical_type(TYPE_BIGINT), sizeof(int64_t), false},
        {"PHYSICAL_PARTITION_COUNT", TypeDescriptor::from_logical_type(TYPE_BIGINT), sizeof(int64_t), false},
        {"REFERENCE_COUNT", TypeDescriptor::from_logical_type(TYPE_BIGINT), sizeof(int64_t), false},
        // ARRAY<STRUCT<id BIGINT, version BIGINT, time DATETIME>>
        {"LATEST_CHANGED_PHYSICAL_PARTITIONS", _latest_changed_physical_partitions_type, 16, true},
        // STRUCT<id VARCHAR, time DATETIME, ttl_ms BIGINT>
        {"OLDEST_REFERENCE", _reference_summary_type, 16, true},
        {"NEWEST_REFERENCE", _reference_summary_type, 16, true},
};

SchemaTableBookmarkSummaryScanner::SchemaTableBookmarkSummaryScanner()
        : SchemaScanner(_s_columns_desc, sizeof(_s_columns_desc) / sizeof(SchemaScanner::ColumnDesc)) {}

SchemaTableBookmarkSummaryScanner::~SchemaTableBookmarkSummaryScanner() = default;

Status SchemaTableBookmarkSummaryScanner::start(RuntimeState* state) {
    if (!_is_init) {
        return Status::InternalError("used before initialized.");
    }
    RETURN_IF_ERROR(SchemaScanner::init_schema_scanner_state(state));
    _ctz = state->timezone_obj();
    return _fetch_all();
}

Status SchemaTableBookmarkSummaryScanner::_fetch_all() {
    TGetTableBookmarkSummaryRequest req;
    req.__set_auth_info(build_auth_info());

    // Predicate pushdown: forward db_id / table_id / bookmark_id from SchemaScannerParam
    // into the FE request so FE skips rows the query would discard.
    if (_param->db_id >= 0) {
        req.__set_db_id(_param->db_id);
    }
    if (_param->table_id >= 0) {
        req.__set_table_id(_param->table_id);
    }
    if (_param->bookmark_id >= 0) {
        req.__set_bookmark_id(_param->bookmark_id);
    }

    // Projection: build selected_columns from dest_slot_descs.
    if (_param->dest_slot_descs != nullptr) {
        std::vector<std::string> selected;
        selected.reserve(_param->dest_slot_descs->size());
        for (SlotDescriptor* slot : *_param->dest_slot_descs) {
            selected.emplace_back(slot->col_name());
        }
        req.__set_selected_columns(selected);
    }

    TGetTableBookmarkSummaryResponse resp;
    RETURN_IF_ERROR(SchemaHelper::get_table_bookmark_summary(_ss_state, req, &resp));

    _rows = resp.table_bookmark_summary_infos;
    return Status::OK();
}

Status SchemaTableBookmarkSummaryScanner::get_next(ChunkPtr* chunk, bool* eos) {
    if (!_is_init) {
        return Status::InternalError("Used before initialized.");
    }
    if (nullptr == chunk || nullptr == eos) {
        return Status::InternalError("input pointer is nullptr.");
    }
    if (_row_idx >= _rows.size()) {
        *eos = true;
        return Status::OK();
    }
    *eos = false;
    return _fill_chunk(chunk);
}

namespace {

// Build a struct-shaped Datum (or null) for a TBookmarkReferenceSummary, converting
// the unix-millisecond time into a DATETIME bound to the session timezone.
// FE sends epoch-millis on the wire (Reference.getAcquiredAtMs); DATETIME has
// second resolution, so divide by 1000 before from_unixtime which expects seconds.
Datum reference_summary_to_datum(const TBookmarkReferenceSummary& ref, const cctz::time_zone& ctz) {
    if (!ref.__isset.id && !ref.__isset.time) {
        return kNullDatum;
    }
    DatumStruct fields;
    fields.reserve(3);
    if (ref.__isset.id) {
        fields.emplace_back(Slice(ref.id));
    } else {
        fields.emplace_back(kNullDatum);
    }
    if (ref.__isset.time && ref.time > 0) {
        fields.emplace_back(TimestampValue::create_from_unixtime(ref.time / 1000, ctz));
    } else {
        fields.emplace_back(kNullDatum);
    }
    // TTL: raw value (<= 0 means disabled). Default -1 for version skew.
    fields.emplace_back(ref.__isset.ttl ? ref.ttl : static_cast<int64_t>(-1));
    return Datum(fields);
}

} // namespace

Status SchemaTableBookmarkSummaryScanner::_fill_chunk(ChunkPtr* chunk) {
    const TTableBookmarkSummaryInfo& info = _rows[_row_idx];
    const auto& slot_id_to_index_map = (*chunk)->get_slot_id_to_index_map();
    for (const auto& [slot_id, index] : slot_id_to_index_map) {
        if (slot_id < 1 || slot_id > _column_num) {
            return Status::InternalError(fmt::format("invalid slot id:{}", slot_id));
        }
        auto* column = (*chunk)->get_column_raw_ptr_by_slot_id(slot_id);

        switch (slot_id) {
        case 1: {
            // DB_ID
            fill_column_with_slot<TYPE_BIGINT>(column, (void*)&info.db_id);
            break;
        }
        case 2: {
            // TABLE_ID
            fill_column_with_slot<TYPE_BIGINT>(column, (void*)&info.table_id);
            break;
        }
        case 3: {
            // BOOKMARK_ID
            fill_column_with_slot<TYPE_BIGINT>(column, (void*)&info.bookmark_id);
            break;
        }
        case 4: {
            // CREATE_TIME
            // FE sends epoch-millis (Bookmark.getBookmarkTimeMs); DATETIME stores
            // only seconds, so divide by 1000 before from_unixtime.
            if (info.create_time > 0) {
                DateTimeValue ts;
                ts.from_unixtime(info.create_time / 1000, _ctz);
                fill_column_with_slot<TYPE_DATETIME>(column, (void*)&ts);
            } else {
                fill_data_column_with_null(column);
            }
            break;
        }
        case 5: {
            // LOGICAL_PARTITION_COUNT
            fill_column_with_slot<TYPE_BIGINT>(column, (void*)&info.logical_partition_count);
            break;
        }
        case 6: {
            // PHYSICAL_PARTITION_COUNT
            fill_column_with_slot<TYPE_BIGINT>(column, (void*)&info.physical_partition_count);
            break;
        }
        case 7: {
            // REFERENCE_COUNT
            fill_column_with_slot<TYPE_BIGINT>(column, (void*)&info.reference_count);
            break;
        }
        case 8: {
            // LATEST_CHANGED_PHYSICAL_PARTITIONS: ARRAY<STRUCT<id, version, time>>
            DatumArray entries;
            entries.reserve(info.latest_changed_physical_partitions.size());
            for (const auto& entry : info.latest_changed_physical_partitions) {
                DatumStruct fields;
                fields.reserve(3);
                fields.emplace_back(entry.id);
                fields.emplace_back(entry.version);
                // FE sends epoch-millis (PhysicalPartitionMeta.getVisibleVersionTimeMs);
                // divide by 1000 since create_from_unixtime expects seconds.
                if (entry.__isset.time && entry.time > 0) {
                    fields.emplace_back(TimestampValue::create_from_unixtime(entry.time / 1000, _ctz));
                } else {
                    fields.emplace_back(kNullDatum);
                }
                entries.emplace_back(Datum(fields));
            }
            column->append_datum(Datum(entries));
            break;
        }
        case 9: {
            // OLDEST_REFERENCE: STRUCT<id, time>
            if (info.__isset.oldest_reference) {
                column->append_datum(reference_summary_to_datum(info.oldest_reference, _ctz));
            } else {
                column->append_datum(kNullDatum);
            }
            break;
        }
        case 10: {
            // NEWEST_REFERENCE: STRUCT<id, time>
            if (info.__isset.newest_reference) {
                column->append_datum(reference_summary_to_datum(info.newest_reference, _ctz));
            } else {
                column->append_datum(kNullDatum);
            }
            break;
        }
        default:
            break;
        }
    }
    _row_idx++;
    return Status::OK();
}

} // namespace starrocks
