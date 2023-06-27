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

#include "storage/olap_meta_reader.h"

#include <vector>

#include "column/array_column.h"
#include "column/chunk.h"
#include "column/column_helper.h"
#include "column/datum_convert.h"
#include "common/status.h"
#include "runtime/global_dict/config.h"
#include "storage/rowset/column_iterator.h"
#include "storage/rowset/column_reader.h"
#include "storage/rowset/rowset.h"
#include "storage/tablet.h"

namespace starrocks {

OlapMetaReader::OlapMetaReader() : MetaReader() {}

Status OlapMetaReader::_init_params(const OlapMetaReaderParams& read_params) {
    read_params.check_validation();
    _tablet = read_params.tablet;
    _version = read_params.version;
    _chunk_size = read_params.chunk_size;
    _params = read_params;

    return Status::OK();
}

OlapMetaReader::~OlapMetaReader() {
    Rowset::release_readers(_rowsets);
}

Status OlapMetaReader::init(const OlapMetaReaderParams& read_params) {
    RETURN_IF_ERROR(_init_params(read_params));
    RETURN_IF_ERROR(_build_collect_context(read_params));
    RETURN_IF_ERROR(_init_seg_meta_collecters(read_params));

    _collect_context.cursor_idx = 0;
    _is_init = true;
    _has_more = true;
    return Status::OK();
}

Status OlapMetaReader::_build_collect_context(const OlapMetaReaderParams& read_params) {
    _collect_context.seg_collecter_params.max_cid = 0;
    for (const auto& it : *(read_params.id_to_names)) {
        std::string col_name = "";
        std::string collect_field = "";
        RETURN_IF_ERROR(SegmentMetaCollecter::parse_field_and_colname(it.second, &collect_field, &col_name));

        int32_t index = _tablet->field_index(col_name);
        if (index < 0) {
            std::stringstream ss;
            ss << "invalid column name: " << it.second;
            LOG(WARNING) << ss.str();
            return Status::InternalError(ss.str());
        }

        // get column type
        LogicalType type = _tablet->tablet_schema().column(index).type();
        _collect_context.seg_collecter_params.field_type.emplace_back(type);

        // get collect field
        _collect_context.seg_collecter_params.fields.emplace_back(collect_field);

        // get column id
        _collect_context.seg_collecter_params.cids.emplace_back(index);
        _collect_context.seg_collecter_params.max_cid = std::max(_collect_context.seg_collecter_params.max_cid, index);

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
    return Status::OK();
}

Status OlapMetaReader::_init_seg_meta_collecters(const OlapMetaReaderParams& params) {
    std::vector<SegmentSharedPtr> segments;
    RETURN_IF_ERROR(_get_segments(params.tablet, params.version, &segments));

    for (auto& segment : segments) {
        auto seg_collecter = std::make_unique<SegmentMetaCollecter>(segment);

        RETURN_IF_ERROR(seg_collecter->init(&_collect_context.seg_collecter_params));
        _collect_context.seg_collecters.emplace_back(std::move(seg_collecter));
    }

    return Status::OK();
}

Status OlapMetaReader::_get_segments(const TabletSharedPtr& tablet, const Version& version,
                                     std::vector<SegmentSharedPtr>* segments) {
    if (tablet->updates() != nullptr) {
        LOG(INFO) << "Skipped Update tablet";
        return Status::OK();
    }

    Status acquire_rowset_st;
    {
        std::shared_lock l(tablet->get_header_lock());
        acquire_rowset_st = tablet->capture_consistent_rowsets(_version, &_rowsets);
    }

    if (!acquire_rowset_st.ok()) {
        _rowsets.clear();
        std::stringstream ss;
        ss << "fail to init reader. tablet=" << tablet->full_name() << "res=" << acquire_rowset_st;
        LOG(WARNING) << ss.str();
        return Status::InternalError(ss.str().c_str());
    }
    Rowset::acquire_readers(_rowsets);

    for (auto& rowset : _rowsets) {
        RETURN_IF_ERROR(rowset->load());
        for (const auto& seg : rowset->segments()) {
            segments->emplace_back(seg);
        }
    }

    return Status::OK();
}

Status OlapMetaReader::do_get_next(ChunkPtr* result) {
    const uint32_t chunk_capacity = _chunk_size;
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
