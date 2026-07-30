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

#include "schema_scanner/schema_compression_dict_stats_scanner.h"

#include "column/nullable_column.h"
#include "common/system/master_info.h"
#include "fs/fs.h"
#include "gen_cpp/olap_file.pb.h"
#include "gen_cpp/segment.pb.h"
#include "gen_cpp/types.pb.h"
#include "gutil/casts.h"
#include "gutil/strings/substitute.h"
#include "schema_scanner/schema_helper.h"
#include "storage/lake/tablet_manager.h"
#include "storage/rowset/rowset.h"
#include "storage/rowset/segment.h"
#include "storage/storage_engine.h"
#include "storage/storage_env.h"
#include "storage/tablet.h"
#include "storage/tablet_manager.h"
#include "storage/tablet_schema.h"
#include "storage_primitive/tablet_basic_info.h"

namespace starrocks {

// clang-format off
SchemaScanner::ColumnDesc SchemaCompressionDictStatsScanner::_s_columns[] = {
        //   name,               type,                                              size,           is_null
        {"TABLE_SCHEMA",      TypeDescriptor::create_varchar_type(sizeof(Slice)),   sizeof(Slice),   false},
        {"TABLE_NAME",        TypeDescriptor::create_varchar_type(sizeof(Slice)),   sizeof(Slice),   false},
        {"PARTITION_ID",      TypeDescriptor::from_logical_type(TYPE_BIGINT),       sizeof(int64_t), false},
        {"TABLET_ID",         TypeDescriptor::from_logical_type(TYPE_BIGINT),       sizeof(int64_t), false},
        {"SEGMENT_ID",        TypeDescriptor::from_logical_type(TYPE_BIGINT),       sizeof(int64_t), true},
        {"COLUMN_NAME",       TypeDescriptor::create_varchar_type(sizeof(Slice)),   sizeof(Slice),   false},
        {"ENCODING",          TypeDescriptor::create_varchar_type(sizeof(Slice)),   sizeof(Slice),   true},
        {"COMPRESSION",       TypeDescriptor::create_varchar_type(sizeof(Slice)),   sizeof(Slice),   true},
        {"USE_COMPRESSION_DICT",   TypeDescriptor::from_logical_type(TYPE_BOOLEAN),      sizeof(bool),    false},
        {"HAS_COMPRESSION_DICT",   TypeDescriptor::from_logical_type(TYPE_BOOLEAN),      sizeof(bool),    true},
        {"COMPRESSION_DICT_SIZE",  TypeDescriptor::from_logical_type(TYPE_BIGINT),       sizeof(int64_t), true},
        {"DATA_SIZE",         TypeDescriptor::from_logical_type(TYPE_BIGINT),       sizeof(int64_t), true},
        {"UNCOMPRESSED_SIZE", TypeDescriptor::from_logical_type(TYPE_BIGINT),       sizeof(int64_t), true},
        {"COMPRESSION_RATIO", TypeDescriptor::from_logical_type(TYPE_DOUBLE),       sizeof(double),  true},
};
// clang-format on

SchemaCompressionDictStatsScanner::SchemaCompressionDictStatsScanner()
        : SchemaScanner(_s_columns, sizeof(_s_columns) / sizeof(SchemaScanner::ColumnDesc)) {}

SchemaCompressionDictStatsScanner::~SchemaCompressionDictStatsScanner() = default;

std::string SchemaCompressionDictStatsScanner::_table_schema_of(int64_t table_id) const {
    auto it = _table_id_to_schema.find(table_id);
    return it != _table_id_to_schema.end() ? it->second : std::string();
}

std::string SchemaCompressionDictStatsScanner::_table_name_of(int64_t table_id) const {
    auto it = _table_id_to_name.find(table_id);
    return it != _table_id_to_name.end() ? it->second : std::string();
}

std::set<int64_t> SchemaCompressionDictStatsScanner::_resolve_requested_table_ids() {
    std::set<int64_t> table_ids;
    // No TABLE_NAME predicate was pushed down: nothing to resolve by name.
    if (_param->table == nullptr || _param->table->empty()) {
        return table_ids;
    }
    const std::string& want_name = *_param->table;
    const bool has_db = (_param->db != nullptr && !_param->db->empty());
    for (const auto& info : _tables_config_response.tables_config_infos) {
        if (!info.__isset.table_id || !info.__isset.table_name) {
            continue;
        }
        if (info.table_name != want_name) {
            continue;
        }
        if (has_db && info.__isset.table_schema && info.table_schema != *_param->db) {
            continue;
        }
        table_ids.insert(info.table_id);
    }
    return table_ids;
}

Status SchemaCompressionDictStatsScanner::start(RuntimeState* state) {
    if (!_is_init) {
        return Status::InternalError("used before initialized.");
    }

    TAuthInfo auth_info;
    if (nullptr != _param->db) {
        auth_info.__set_pattern(*(_param->db));
    }
    if (nullptr != _param->current_user_ident) {
        auth_info.__set_current_user_ident(*(_param->current_user_ident));
    } else {
        if (nullptr != _param->user) {
            auth_info.__set_user(*(_param->user));
        }
        if (nullptr != _param->user_ip) {
            auth_info.__set_user_ip(*(_param->user_ip));
        }
    }
    TGetTablesConfigRequest tables_config_req;
    tables_config_req.__set_auth_info(auth_info);

    RETURN_IF_ERROR(SchemaScanner::init_schema_scanner_state(state));
    RETURN_IF_ERROR(SchemaHelper::get_tables_config(_ss_state, tables_config_req, &_tables_config_response));

    // Build the authorization set and the table_id -> (schema, name) maps that
    // back the TABLE_SCHEMA / TABLE_NAME output columns. Only tables the user is
    // authorized on appear in the tables-config response.
    std::set<int64_t> authorized_table_ids;
    for (const auto& info : _tables_config_response.tables_config_infos) {
        if (!info.__isset.table_id) {
            continue;
        }
        authorized_table_ids.insert(info.table_id);
        if (info.__isset.table_schema) {
            _table_id_to_schema.emplace(info.table_id, info.table_schema);
        }
        if (info.__isset.table_name) {
            _table_id_to_name.emplace(info.table_id, info.table_name);
        }
    }

    const bool has_tablet_predicate = _param->tablet_id > 0;
    const bool has_table_predicate = (_param->table != nullptr && !_param->table->empty()) || _param->table_id > 0;

    // Cost guard: opening one footer per segment is expensive, so a
    // full-database scan is rejected. Require a TABLE_NAME (optionally with
    // TABLE_SCHEMA) or a TABLET_ID equality predicate.
    if (!has_tablet_predicate && !has_table_predicate) {
        return Status::NotSupported(
                "information_schema.compression_dict_stats requires a `tablet_id = <id>` or "
                "`table_name = '<name>'` (optionally with `table_schema = '<db>'`) equality predicate; "
                "a full-database scan is not allowed because it opens a segment footer per tablet.");
    }

    _rows.clear();
    _cur_idx = 0;

    // Resolve the requested table ids from the TABLE_NAME predicate (restricted
    // to the authorized set). When only a raw table_id was pushed down (there is
    // no TABLE_ID column on this table, but keep it for forward-compat), honor it
    // too.
    std::set<int64_t> requested_table_ids = _resolve_requested_table_ids();
    if (_param->table_id > 0 && authorized_table_ids.count(_param->table_id) > 0) {
        requested_table_ids.insert(_param->table_id);
    }

    auto run_mode = get_master_run_mode();
    const bool shared_nothing = !run_mode.has_value() || run_mode.value() == TRunMode::SHARED_NOTHING;
    const bool shared_data = !run_mode.has_value() || run_mode.value() == TRunMode::SHARED_DATA;

    // Enumerate the in-scope, authorized tablets by reusing the exact scoping /
    // authorization path that information_schema.be_tablets uses, then expand
    // each tablet into per-(segment, column) rows.
    std::vector<TabletBasicInfo> infos;
    if (shared_nothing) {
        auto manager = StorageEngine::instance()->tablet_manager();
        if (manager != nullptr) {
            if (has_tablet_predicate) {
                manager->get_tablets_basic_infos(-1, _param->partition_id, _param->tablet_id, infos,
                                                 &authorized_table_ids);
            } else {
                for (int64_t tid : requested_table_ids) {
                    manager->get_tablets_basic_infos(tid, _param->partition_id, -1, infos, &authorized_table_ids);
                }
            }
        }
        for (const auto& info : infos) {
            _expand_local_tablet(info.table_id, info.partition_id, info.tablet_id);
        }
    }

#ifndef __APPLE__
    if (shared_data) {
        auto lake_manager = StorageEnv::GetInstance()->lake_tablet_manager();
        if (lake_manager != nullptr) {
            std::unordered_map<int64_t, int64_t> partition_versions;
            int64_t table_id_offset = 0;
            while (true) {
                TGetPartitionsMetaRequest partitions_meta_req;
                partitions_meta_req.__set_auth_info(auth_info);
                partitions_meta_req.__set_start_table_id_offset(table_id_offset);
                TGetPartitionsMetaResponse partitions_meta_response;
                RETURN_IF_ERROR(
                        SchemaHelper::get_partitions_meta(_ss_state, partitions_meta_req, &partitions_meta_response));
                for (const auto& info : partitions_meta_response.partitions_meta_infos) {
                    partition_versions.emplace(info.partition_id, info.visible_version);
                }
                table_id_offset = partitions_meta_response.next_table_id_offset;
                if (!table_id_offset) {
                    break;
                }
            }

            std::vector<TabletBasicInfo> lake_infos;
            if (has_tablet_predicate) {
                lake_manager->get_tablets_basic_info(-1, _param->partition_id, _param->tablet_id, authorized_table_ids,
                                                     partition_versions, lake_infos);
            } else {
                for (int64_t tid : requested_table_ids) {
                    lake_manager->get_tablets_basic_info(tid, _param->partition_id, -1, authorized_table_ids,
                                                         partition_versions, lake_infos);
                }
            }
            for (const auto& info : lake_infos) {
                _expand_lake_tablet(info.table_id, info.partition_id, info.tablet_id);
            }
        }
    }
#endif // __APPLE__

    LOG(INFO) << strings::Substitute(
            "compression_dict_stats scan table_id:$0 partition:$1 tablet:$2 table_name:$3 #rows:$4", _param->table_id,
            _param->partition_id, _param->tablet_id, _param->table != nullptr ? *_param->table : std::string(),
            _rows.size());
    return Status::OK();
}

void SchemaCompressionDictStatsScanner::_expand_local_tablet(int64_t table_id, int64_t partition_id,
                                                             int64_t tablet_id) {
    auto manager = StorageEngine::instance()->tablet_manager();
    if (manager == nullptr) {
        return;
    }
    TabletSharedPtr tablet = manager->get_tablet(tablet_id, true, nullptr);
    if (tablet == nullptr) {
        return;
    }
    // For primary-key tablets the applied rowsets are not exposed through the
    // version tracker, so a footer read would risk attributing wrong segments.
    // Fall back to schema-derived rows (documented behaviour).
    auto schema = tablet->tablet_schema();
    bool footer_read = false;
    if (schema != nullptr && schema->keys_type() != KeysType::PRIMARY_KEYS) {
        footer_read = _try_expand_local_footers(tablet, table_id, partition_id, tablet_id);
    }
    if (!footer_read && schema != nullptr) {
        _emit_schema_fallback_rows(*schema, table_id, partition_id, tablet_id);
    }
}

bool SchemaCompressionDictStatsScanner::_try_expand_local_footers(const TabletSharedPtr& tablet, int64_t table_id,
                                                                  int64_t partition_id, int64_t tablet_id) {
    auto schema = tablet->tablet_schema();
    if (schema == nullptr) {
        return false;
    }

    // unique_id -> (column name, use_compression_dict) for top-level columns.
    std::unordered_map<uint32_t, std::pair<std::string, bool>> uid_to_col;
    for (const auto& col : schema->columns()) {
        uid_to_col.emplace(col.unique_id(), std::make_pair(std::string(col.name()), col.use_compression_dict()));
    }

    std::vector<RowsetSharedPtr> rowsets;
    if (Status st = tablet->capture_consistent_rowsets(tablet->max_version(), &rowsets); !st.ok()) {
        return false;
    }

    auto* fs = FileSystem::Default();
    bool any_segment = false;
    for (const auto& rs : rowsets) {
        if (rs == nullptr) {
            continue;
        }
        const int64_t num_segments = rs->num_segments();
        for (int64_t seg_id = 0; seg_id < num_segments; ++seg_id) {
            std::string seg_path = Rowset::segment_file_path(rs->rowset_path(), rs->rowset_id(), seg_id);
            auto file_or = fs->new_random_access_file(seg_path);
            if (!file_or.ok()) {
                // Encrypted / bundled / missing segment: skip. Never fabricate.
                continue;
            }
            SegmentFooterPB footer;
            if (auto footer_or = Segment::parse_segment_footer(file_or.value().get(), &footer, nullptr, nullptr);
                !footer_or.ok()) {
                continue;
            }
            any_segment = true;
            for (const auto& col_meta : footer.columns()) {
                std::string node_name;
                bool node_use_compression_dict = false;
                auto it = uid_to_col.find(col_meta.unique_id());
                if (it != uid_to_col.end()) {
                    node_name = it->second.first;
                    node_use_compression_dict = it->second.second;
                } else if (col_meta.has_name() && !col_meta.name().empty()) {
                    node_name = col_meta.name();
                } else {
                    node_name = strings::Substitute("__uid_$0", col_meta.unique_id());
                }
                _expand_footer_column(col_meta, seg_id, node_name, node_use_compression_dict, table_id, partition_id,
                                      tablet_id);
            }
        }
    }
    return any_segment;
}

void SchemaCompressionDictStatsScanner::_expand_footer_column(const ColumnMetaPB& meta, int64_t segment_id,
                                                              const std::string& node_name,
                                                              bool node_use_compression_dict, int64_t table_id,
                                                              int64_t partition_id, int64_t tablet_id) {
    if (meta.children_columns_size() > 0) {
        // Container column (e.g. a flat-JSON column). Emit one row per leaf
        // sub-column; do not emit the container itself. Flat-JSON sub-columns
        // carry their own name in ColumnMetaPB.name (field 33) and inherit the
        // parent column's use_compression_dict flag.
        for (int i = 0; i < meta.children_columns_size(); ++i) {
            const ColumnMetaPB& child = meta.children_columns(i);
            std::string child_name;
            if (child.has_name() && !child.name().empty()) {
                child_name = node_name.empty() ? child.name() : node_name + "." + child.name();
            } else {
                child_name = strings::Substitute("$0.$1", node_name, i);
            }
            _expand_footer_column(child, segment_id, child_name, node_use_compression_dict, table_id, partition_id,
                                  tablet_id);
        }
        return;
    }
    _append_footer_leaf_row(meta, segment_id, node_name, node_use_compression_dict, table_id, partition_id, tablet_id);
}

void SchemaCompressionDictStatsScanner::_append_footer_leaf_row(const ColumnMetaPB& meta, int64_t segment_id,
                                                                const std::string& column_name,
                                                                bool use_compression_dict, int64_t table_id,
                                                                int64_t partition_id, int64_t tablet_id) {
    DictStatsRow row;
    row.table_schema = _table_schema_of(table_id);
    row.table_name = _table_name_of(table_id);
    row.partition_id = partition_id;
    row.tablet_id = tablet_id;
    row.segment_id = segment_id;
    row.column_name = column_name;
    row.use_compression_dict = use_compression_dict;
    row.encoding = EncodingTypePB_Name(meta.encoding());
    row.compression = CompressionTypePB_Name(meta.compression());
    row.has_compression_dict = meta.has_compression_dict_page();
    row.compression_dict_size =
            meta.has_compression_dict_page() ? static_cast<int64_t>(meta.compression_dict_page().size()) : 0;

    // Size mapping (documented).
    // UNCOMPRESSED_SIZE = total_mem_footprint (field 31): the in-memory /
    // uncompressed footprint recorded for vertical-compaction chunk sizing.
    // DATA_SIZE is NULL: ColumnMetaPB carries NO per-column on-disk compressed
    // size (data_footprint is deprecated and segment-level). Emitting
    // total_mem_footprint here too would look like a real compressed size and
    // silently read as "compression ratio 1.0". NULL says "not available".
    // Consequently COMPRESSION_RATIO is NULL as well.
    // TODO(compression dict): to make DATA_SIZE/COMPRESSION_RATIO real, accumulate the
    // per-column compressed page bytes at write time into ColumnMetaPB (a new
    // field), or sum the column's data-page pointer sizes via its ordinal index.
    row.uncompressed_size = static_cast<int64_t>(meta.total_mem_footprint());
    row.data_size = std::nullopt;
    row.compression_ratio = std::nullopt;

    _rows.push_back(std::move(row));
}

void SchemaCompressionDictStatsScanner::_emit_schema_fallback_rows(const TabletSchema& schema, int64_t table_id,
                                                                   int64_t partition_id, int64_t tablet_id) {
    // No footer available: emit one row per top-level schema column with the
    // footer-derived columns as NULL so nothing is fabricated.
    for (const auto& col : schema.columns()) {
        DictStatsRow row;
        row.table_schema = _table_schema_of(table_id);
        row.table_name = _table_name_of(table_id);
        row.partition_id = partition_id;
        row.tablet_id = tablet_id;
        row.segment_id = std::nullopt;
        row.column_name = std::string(col.name());
        row.use_compression_dict = col.use_compression_dict();
        // encoding / compression / has_compression_dict / *_size / ratio stay NULL.
        _rows.push_back(std::move(row));
    }
}

void SchemaCompressionDictStatsScanner::_expand_lake_tablet(int64_t table_id, int64_t partition_id, int64_t tablet_id) {
#ifndef __APPLE__
    auto lake_manager = StorageEnv::GetInstance()->lake_tablet_manager();
    if (lake_manager == nullptr) {
        return;
    }
    // TODO(compression dict): read shared-data segment footers to fill ENCODING / COMPRESSION /
    // HAS_COMPRESSION_DICT / COMPRESSION_DICT_SIZE / *_SIZE for lake tablets. This needs
    // bundle-file-offset aware segment loading through the lake segment loader
    // (see reference: reading a lake segment outside the standard loader needs
    // bundle_file_offset), which cannot be validated here. Until then, lake
    // tablets emit schema-derived rows with the footer columns as NULL.
    auto schema_or = lake_manager->get_tablet_schema(tablet_id);
    if (!schema_or.ok() || schema_or.value() == nullptr) {
        return;
    }
    _emit_schema_fallback_rows(*schema_or.value(), table_id, partition_id, tablet_id);
#endif // __APPLE__
}

Status SchemaCompressionDictStatsScanner::fill_chunk(ChunkPtr* chunk) {
    const auto& slot_id_to_index_map = (*chunk)->get_slot_id_to_index_map();
    const DictStatsRow& row = _rows[_cur_idx];
    for (const auto& [slot_id, index] : slot_id_to_index_map) {
        if (slot_id < 1 || slot_id > 14) {
            return Status::InternalError(strings::Substitute("invalid slot id:$0", slot_id));
        }
        auto* column = (*chunk)->get_column_raw_ptr_by_slot_id(slot_id);
        switch (slot_id) {
        case 1: {
            Slice v = Slice(row.table_schema);
            fill_column_with_slot<TYPE_VARCHAR>(column, (void*)&v);
            break;
        }
        case 2: {
            Slice v = Slice(row.table_name);
            fill_column_with_slot<TYPE_VARCHAR>(column, (void*)&v);
            break;
        }
        case 3: {
            fill_column_with_slot<TYPE_BIGINT>(column, (void*)&row.partition_id);
            break;
        }
        case 4: {
            fill_column_with_slot<TYPE_BIGINT>(column, (void*)&row.tablet_id);
            break;
        }
        case 5: {
            if (row.segment_id.has_value()) {
                int64_t v = row.segment_id.value();
                fill_column_with_slot<TYPE_BIGINT>(column, (void*)&v);
            } else {
                down_cast<NullableColumn*>(column)->append_nulls(1);
            }
            break;
        }
        case 6: {
            Slice v = Slice(row.column_name);
            fill_column_with_slot<TYPE_VARCHAR>(column, (void*)&v);
            break;
        }
        case 7: {
            if (row.encoding.has_value()) {
                Slice v = Slice(row.encoding.value());
                fill_column_with_slot<TYPE_VARCHAR>(column, (void*)&v);
            } else {
                down_cast<NullableColumn*>(column)->append_nulls(1);
            }
            break;
        }
        case 8: {
            if (row.compression.has_value()) {
                Slice v = Slice(row.compression.value());
                fill_column_with_slot<TYPE_VARCHAR>(column, (void*)&v);
            } else {
                down_cast<NullableColumn*>(column)->append_nulls(1);
            }
            break;
        }
        case 9: {
            bool v = row.use_compression_dict;
            fill_column_with_slot<TYPE_BOOLEAN>(column, (void*)&v);
            break;
        }
        case 10: {
            if (row.has_compression_dict.has_value()) {
                bool v = row.has_compression_dict.value();
                fill_column_with_slot<TYPE_BOOLEAN>(column, (void*)&v);
            } else {
                down_cast<NullableColumn*>(column)->append_nulls(1);
            }
            break;
        }
        case 11: {
            if (row.compression_dict_size.has_value()) {
                int64_t v = row.compression_dict_size.value();
                fill_column_with_slot<TYPE_BIGINT>(column, (void*)&v);
            } else {
                down_cast<NullableColumn*>(column)->append_nulls(1);
            }
            break;
        }
        case 12: {
            if (row.data_size.has_value()) {
                int64_t v = row.data_size.value();
                fill_column_with_slot<TYPE_BIGINT>(column, (void*)&v);
            } else {
                down_cast<NullableColumn*>(column)->append_nulls(1);
            }
            break;
        }
        case 13: {
            if (row.uncompressed_size.has_value()) {
                int64_t v = row.uncompressed_size.value();
                fill_column_with_slot<TYPE_BIGINT>(column, (void*)&v);
            } else {
                down_cast<NullableColumn*>(column)->append_nulls(1);
            }
            break;
        }
        case 14: {
            if (row.compression_ratio.has_value()) {
                double v = row.compression_ratio.value();
                fill_column_with_slot<TYPE_DOUBLE>(column, (void*)&v);
            } else {
                down_cast<NullableColumn*>(column)->append_nulls(1);
            }
            break;
        }
        default:
            break;
        }
    }
    return Status::OK();
}

Status SchemaCompressionDictStatsScanner::get_next(ChunkPtr* chunk, bool* eos) {
    if (!_is_init) {
        return Status::InternalError("call this before initial.");
    }
    if (nullptr == chunk || nullptr == eos) {
        return Status::InternalError("invalid parameter.");
    }
    if (_cur_idx >= _rows.size()) {
        *eos = true;
        return Status::OK();
    }
    *eos = false;
    RETURN_IF_ERROR(fill_chunk(chunk));
    _cur_idx++;
    return Status::OK();
}

} // namespace starrocks
