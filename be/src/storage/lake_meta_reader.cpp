// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#include "storage/lake_meta_reader.h"


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

namespace starrocks::vectorized {

LakeMetaReader::LakeMetaReader() : _is_init(false), _has_more(false) {}

LakeMetaReader::~LakeMetaReader() {
    // Rowset::release_readers(_rowsets);
}


Status LakeMetaReader::init(const LakeMetaReaderParams& read_params) {
    // for debug
    LOG(INFO) << "enter LakeMetaReader::init";

    RETURN_IF_ERROR(_init_params(read_params));
    RETURN_IF_ERROR(_build_collect_context(read_params));
    RETURN_IF_ERROR(_init_seg_meta_collecters(read_params));

    if (_collect_context.seg_collecters.size() == 0) {
        _has_more = false;
        return Status::OK();
    }

    _collect_context.cursor_idx = 0;
    _is_init = true;
    _has_more = true;
    return Status::OK();
}

Status LakeMetaReader::_init_params(const LakeMetaReaderParams& read_params) {
    // for debug
    LOG(INFO) << "enter LakeMetaReader::_init_params";
    read_params.check_validation();
    _tablet = read_params.tablet;
    _tablet_schema = read_params.tablet_schema;
    _version = read_params.version;
    _chunk_size = read_params.chunk_size;
    _params = read_params;

    return Status::OK();
}

Status LakeMetaReader::_build_collect_context(const LakeMetaReaderParams& read_params) {
    // for debug
    LOG(INFO) << "enter LakeMetaReader::_build_collect_context";
    LOG(INFO) << "read_params.id_to_names size is " << read_params.id_to_names->size();
    _collect_context.seg_collecter_params.max_cid = 0;
    for (auto it : *(read_params.id_to_names)) {
        std::string col_name = "";
        std::string collect_field = "";
        RETURN_IF_ERROR(SegmentMetaCollecter::parse_field_and_colname(it.second, &collect_field, &col_name));
        // for debug
        LOG(INFO) << "after SegmentMetaCollecter::parse_field_and_colname";

        int32_t index = _tablet_schema->field_index(col_name);
        if (index < 0) {
            std::stringstream ss;
            ss << "invalid column name: " << it.second;
            LOG(WARNING) << ss.str();
            return Status::InternalError(ss.str());
        }

        // get column type
        FieldType type = _tablet_schema->column(index).type();
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
    }
    return Status::OK();
}

Status LakeMetaReader::_init_seg_meta_collecters(const LakeMetaReaderParams& params) {
    // for debug
    LOG(INFO) << "enter LakeMetaReader::_init_seg_meta_collecters";
    std::vector<SegmentSharedPtr> segments;
    RETURN_IF_ERROR(_get_segments(params.tablet.value(), params.version, &segments));

    for (auto& segment : segments) {
        auto seg_collecter = std::make_unique<SegmentMetaCollecter>(segment);

        RETURN_IF_ERROR(seg_collecter->init(&_collect_context.seg_collecter_params));
        _collect_context.seg_collecters.emplace_back(std::move(seg_collecter));
    }

    return Status::OK();
}

Status LakeMetaReader::_get_segments(lake::Tablet tablet, const Version& version,
                                 std::vector<SegmentSharedPtr>* segments) {
    // for debug
    LOG(INFO) << "enter LakeMetaReader::_get_segments";

    size_t footer_size_hint = 16 * 1024;
    uint32_t seg_id = 0;                                
    ASSIGN_OR_RETURN(auto rowsets, tablet.get_rowsets(version.second));
    for (const auto& rowset : rowsets) {
        auto rowset_metadata = rowset->metadata();
        segments->reserve(rowset_metadata.segments().size());
        for (const auto& seg_name : rowset_metadata.segments()) {
            ASSIGN_OR_RETURN(auto segment, _tablet->load_segment(seg_name, seg_id++, &footer_size_hint, false));
            segments->emplace_back(std::move(segment));  
        }
    }

    return Status::OK();
}

Status LakeMetaReader::_fill_result_chunk(Chunk* chunk) {
    for (size_t i = 0; i < _collect_context.result_slot_ids.size(); i++) {
        auto s_id = _collect_context.result_slot_ids[i];
        auto slot = _params.desc_tbl->get_slot_descriptor(s_id);
        if (_collect_context.seg_collecter_params.fields[i] == "dict_merge") {
            TypeDescriptor item_desc;
            item_desc = slot->type();
            TypeDescriptor desc;
            desc.type = TYPE_ARRAY;
            desc.children.emplace_back(item_desc);
            vectorized::ColumnPtr column = vectorized::ColumnHelper::create_column(desc, false);
            chunk->append_column(std::move(column), slot->id());
        } else {
            vectorized::ColumnPtr column = vectorized::ColumnHelper::create_column(slot->type(), false);
            chunk->append_column(std::move(column), slot->id());
        }
    }
    return Status::OK();
}

Status LakeMetaReader::do_get_next(ChunkPtr* result) {
    const uint32_t chunk_capacity = _chunk_size;
    uint16_t chunk_start = 0;

    *result = std::make_shared<vectorized::Chunk>();
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

Status LakeMetaReader::open() {
    return Status::OK();
}

Status LakeMetaReader::_read(Chunk* chunk, size_t n) {
    std::vector<vectorized::Column*> columns;
    for (size_t i = 0; i < _collect_context.seg_collecter_params.fields.size(); ++i) {
        const ColumnPtr& col = chunk->get_column_by_index(i);
        columns.emplace_back(col.get());
    }

    size_t remaining = n;
    while (remaining > 0) {
        if (_collect_context.cursor_idx >= _collect_context.seg_collecters.size()) {
            _has_more = false;
            return Status::OK();
        }
        RETURN_IF_ERROR(_collect_context.seg_collecters[_collect_context.cursor_idx]->open());
        RETURN_IF_ERROR(_collect_context.seg_collecters[_collect_context.cursor_idx]->collect(&columns));
        _collect_context.seg_collecters[_collect_context.cursor_idx].reset();
        remaining--;
        _collect_context.cursor_idx++;
    }

    return Status::OK();
}

bool LakeMetaReader::has_more() {
    return _has_more;
}




} // namespace starrocks::vectorized
