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

#include "column/array_column.h"
#include "column/chunk.h"
#include "column/column_helper.h"
#include "column/datum_convert.h"
#include "common/status.h"
#include "runtime/global_dict/config.h"
#include "storage/lake/rowset.h"
#include "storage/rowset/column_iterator.h"
#include "storage/rowset/column_reader.h"
#include "storage/rowset/rowset.h"

namespace starrocks {

LakeMetaReader::LakeMetaReader() : MetaReader() {}

LakeMetaReader::~LakeMetaReader() = default;

Status LakeMetaReader::init(const LakeMetaReaderParams& read_params) {
    _params = read_params;

    ASSIGN_OR_RETURN(auto tablet, ExecEnv::GetInstance()->lake_tablet_manager()->get_tablet(
                                          read_params.tablet_id, read_params.version.second));

    RETURN_IF_ERROR(_build_collect_context(tablet, read_params));
    RETURN_IF_ERROR(_init_seg_meta_collecters(tablet, read_params));

    _collect_context.cursor_idx = 0;
    _is_init = true;
    _has_more = true;
    return Status::OK();
}

Status LakeMetaReader::_build_collect_context(const lake::VersionedTablet& tablet,
                                              const LakeMetaReaderParams& read_params) {
    auto tablet_schema = tablet.get_schema();
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

        // get result slot id
        _collect_context.result_slot_ids.emplace_back(it.first);

        // only collect the field of dict need read data page
        // others just depend on footer
        if (collect_field == "dict_merge") {
            _collect_context.seg_collecter_params.read_page.emplace_back(true);
        } else {
            _collect_context.seg_collecter_params.read_page.emplace_back(false);
        }
        _has_count_agg |= (collect_field == "count");
    }
    _collect_context.seg_collecter_params.tablet_schema = tablet_schema;
    return Status::OK();
}

Status LakeMetaReader::_init_seg_meta_collecters(const lake::VersionedTablet& tablet,
                                                 const LakeMetaReaderParams& params) {
    std::vector<SegmentSharedPtr> segments;
    RETURN_IF_ERROR(_get_segments(tablet, &segments));
    for (auto& segment : segments) {
        auto seg_collecter = std::make_unique<SegmentMetaCollecter>(segment);

        RETURN_IF_ERROR(seg_collecter->init(&_collect_context.seg_collecter_params));
        _collect_context.seg_collecters.emplace_back(std::move(seg_collecter));
    }

    return Status::OK();
}

Status LakeMetaReader::_get_segments(const lake::VersionedTablet& tablet, std::vector<SegmentSharedPtr>* segments) {
    ASSIGN_OR_RETURN(auto rowsets, tablet.get_rowsets());
    for (const auto& rowset : rowsets) {
        ASSIGN_OR_RETURN(auto rowset_segs, rowset->segments(false));
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
