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

#include "storage/lake_meta_reader.h"

#include <vector>

#include "column/chunk.h"
#include "column/column_helper.h"
#include "common/status.h"
#include "exec/pipeline/fragment_context.h"
#include "runtime/exec_env.h"
#include "runtime/global_dict/config.h"
#include "storage/lake/column_mode_partial_update_handler.h"
#include "storage/lake/rowset.h"
#include "storage/lake/table_schema_service.h"
#include "storage/rowset/column_iterator.h"
#include "storage/rowset/rowset.h"

namespace starrocks {

LakeMetaReader::LakeMetaReader() : MetaReader() {}

LakeMetaReader::~LakeMetaReader() = default;

Status LakeMetaReader::init(const LakeMetaReaderParams& read_params) {
    _params = read_params;

    lake::TabletManager* tablet_manager = ExecEnv::GetInstance()->lake_tablet_manager();
    ASSIGN_OR_RETURN(auto tablet, tablet_manager->get_tablet(read_params.tablet_id, read_params.version.second));

    TabletSchemaCSPtr base_schema;
    if (read_params.schema_key.has_value()) {
        auto runtime_state = read_params.runtime_state;
        ASSIGN_OR_RETURN(base_schema, tablet_manager->table_schema_service()->get_schema_for_scan(
                                              *read_params.schema_key, read_params.tablet_id, runtime_state->query_id(),
                                              runtime_state->fragment_ctx()->fe_addr(), tablet.metadata()));
    } else {
        // no schema key indicates FE has not been upgraded to use fast schema evolution v2,
        // so fallback to the old way to get schema from tablet metadata
        base_schema = tablet.get_schema();
    }

    // Build and possibly extend tablet schema using access paths
    TabletSchemaCSPtr tablet_schema = base_schema;
    if (read_params.column_access_paths != nullptr && !read_params.column_access_paths->empty()) {
        TabletSchemaSPtr tmp_schema = TabletSchema::copy(*base_schema);
        int field_number = read_params.next_uniq_id;
        for (auto& path : *read_params.column_access_paths) {
            int root_column_index = tmp_schema->field_index(path->path());
            RETURN_IF(root_column_index < 0, Status::RuntimeError("unknown access path: " + path->path()));

            TabletColumn column;
            column.set_name(path->linear_path());
            column.set_unique_id(++field_number);
            column.set_type(path->value_type().type);
            column.set_length(path->value_type().len);
            column.set_is_nullable(true);
            int32_t root_uid = tmp_schema->column(static_cast<size_t>(root_column_index)).unique_id();
            column.set_extended_info(std::make_unique<ExtendedColumnInfo>(path.get(), root_uid));

            tmp_schema->append_column(column);
            VLOG(2) << "extend the tablet-schema: " << column.debug_string();
        }
        tablet_schema = tmp_schema;
    }

    RETURN_IF_ERROR(_build_collect_context(tablet_schema, read_params));
    RETURN_IF_ERROR(_init_seg_meta_collecters(tablet, read_params));

    _collect_context.cursor_idx = 0;
    _is_init = true;
    _has_more = true;
    return Status::OK();
}

Status LakeMetaReader::_build_collect_context(const TabletSchemaCSPtr& tablet_schema,
                                              const LakeMetaReaderParams& read_params) {
    for (const auto& it : *(read_params.id_to_names)) {
        std::string col_name = "";
        std::string collect_field = "";
        RETURN_IF_ERROR(SegmentMetaCollecter::parse_field_and_colname(it.second, &collect_field, &col_name));

        int32_t index = tablet_schema->field_index(col_name);
        if (index < 0) {
            std::stringstream ss;
            ss << "invalid column name: " << it.second;
            LOG(WARNING) << ss.str();
            return Status::InternalError(ss.str());
        }

        // get column type
        LogicalType type = tablet_schema->column(index).type();
        _collect_context.seg_collecter_params.field_type.emplace_back(type);

        // get collect field
        _collect_context.seg_collecter_params.fields.emplace_back(collect_field);

        // get column id
        _collect_context.seg_collecter_params.cids.emplace_back(index);

        // low cardinality threshold
        _collect_context.seg_collecter_params.low_cardinality_threshold = read_params.low_card_threshold;

        // get result slot id
        _collect_context.result_slot_ids.emplace_back(it.first);

        // only collect the field of dict need read data page
        // others just depend on footer
        if (collect_field == META_DICT_MERGE || collect_field == META_COUNT_COL ||
            collect_field == META_COLUMN_COMPRESSED_SIZE) {
            _collect_context.seg_collecter_params.read_page.emplace_back(true);
        } else {
            _collect_context.seg_collecter_params.read_page.emplace_back(false);
        }
        _has_count_agg |= (collect_field == META_COUNT_ROWS);
        _has_count_agg |= (collect_field == META_COUNT_COL);
    }
    _collect_context.seg_collecter_params.tablet_schema = tablet_schema;
    return Status::OK();
}

Status LakeMetaReader::_init_seg_meta_collecters(const lake::VersionedTablet& tablet,
                                                 const LakeMetaReaderParams& params) {
    std::vector<SegmentSharedPtr> segments;
    std::vector<SegmentMetaCollectOptions> options_list;
    RETURN_IF_ERROR(_get_segments(tablet, &segments, &options_list));
    for (int i = 0; i < segments.size(); ++i) {
        auto& segment = segments[i];
        auto& options = options_list[i];
        auto seg_collecter = std::make_unique<SegmentMetaCollecter>(segment);

        RETURN_IF_ERROR(seg_collecter->init(&_collect_context.seg_collecter_params, options));
        _collect_context.seg_collecters.emplace_back(std::move(seg_collecter));
    }

    return Status::OK();
}

Status LakeMetaReader::_get_segments(const lake::VersionedTablet& tablet, std::vector<SegmentSharedPtr>* segments,
                                     std::vector<SegmentMetaCollectOptions>* options_list) {
    auto rowsets = tablet.get_rowsets();
    for (const auto& rowset : rowsets) {
        ASSIGN_OR_RETURN(auto rowset_segs, rowset->segments(true));
        for (int seg_id = 0; seg_id < rowset_segs.size(); ++seg_id) {
            SegmentMetaCollectOptions options;
            options.is_primary_keys = tablet.get_schema()->keys_type() == KeysType::PRIMARY_KEYS;
            // In shared-data arch, only primary key table support delta column group for now.
            if (options.is_primary_keys) {
                options.tablet_id = tablet.metadata()->id();
                options.version = tablet.version();
                options.segment_id = seg_id;
                options.pk_rowsetid = rowset->id();
                options.dcg_loader = std::make_shared<lake::LakeDeltaColumnGroupLoader>(tablet.metadata());
            }
            options_list->emplace_back(std::move(options));
        }
        segments->insert(segments->end(), rowset_segs.begin(), rowset_segs.end());
    }
    return Status::OK();
}

Status LakeMetaReader::do_get_next(ChunkPtr* result) {
    const uint32_t chunk_capacity = _params.chunk_size;
    uint16_t chunk_start = 0;

    *result = std::make_shared<Chunk>();
    if (nullptr == result->get()) {
        return Status::InternalError("Failed to allocate new chunk.");
    }

    RETURN_IF_ERROR(_fill_result_chunk(result->get()));

    while ((chunk_start < chunk_capacity) && _has_more) {
        RETURN_IF_ERROR(_read((*result).get(), chunk_capacity - chunk_start));
        (*result)->check_or_die();
        size_t next_start = (*result)->num_rows();
        chunk_start = next_start;
    }

    return Status::OK();
}
} // namespace starrocks
