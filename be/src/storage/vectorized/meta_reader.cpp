// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#include "storage/vectorized/meta_reader.h"

#include <vector>

#include "column/datum_convert.h"
#include "common/status.h"
#include "storage/rowset/beta_rowset.h"
#include "storage/rowset/column_iterator.h"
#include "storage/rowset/column_reader.h"
#include "storage/tablet.h"
#include "storage/vectorized/chunk_helper.h"

namespace starrocks::vectorized {

std::vector<std::string> SegmentMetaCollecter::support_collect_fields = {"dict_merge", "max", "min"};

Status SegmentMetaCollecter::parse_field_and_colname(const std::string& item, std::string* field,
                                                     std::string* col_name) {
    for (size_t i = 0; i < support_collect_fields.size(); i++) {
        if (item.size() <= support_collect_fields[i].size()) {
            continue;
        }

        if (item.find(support_collect_fields[i]) != std::string::npos &&
            item.substr(0, support_collect_fields[i].size()) == support_collect_fields[i]) {
            *field = support_collect_fields[i];
            *col_name = item.substr(support_collect_fields[i].size() + 1);
            return Status::OK();
        }
    }
    return Status::InvalidArgument(item);
}

MetaReader::MetaReader() : _is_init(false), _has_more(false) {}

MetaReader::~MetaReader() {}

Status MetaReader::init(const MetaReaderParams& read_params) {
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

Status MetaReader::_init_params(const MetaReaderParams& read_params) {
    read_params.check_validation();
    _tablet = read_params.tablet;
    _version = read_params.version;
    _chunk_size = read_params.chunk_size;
    _params = read_params;

    return Status::OK();
}

Status MetaReader::_build_collect_context(const MetaReaderParams& read_params) {
    _collect_context.seg_collecter_params.max_cid = 0;
    for (auto it : *(read_params.id_to_names)) {
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
        FieldType type = _tablet->tablet_schema().column(index).type();
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

Status MetaReader::_init_seg_meta_collecters(const MetaReaderParams& params) {
    std::vector<SegmentSharedPtr> segments;
    RETURN_IF_ERROR(_get_segments(params.tablet, params.version, &segments));

    for (auto& segment : segments) {
        auto seg_collecter = std::make_unique<SegmentMetaCollecter>(segment);

        RETURN_IF_ERROR(seg_collecter->init(&_collect_context.seg_collecter_params));
        _collect_context.seg_collecters.emplace_back(std::move(seg_collecter));
    }

    return Status::OK();
}

Status MetaReader::_get_segments(const TabletSharedPtr& tablet, const Version& version,
                                 std::vector<SegmentSharedPtr>* segments) {
    if (tablet->updates() != nullptr) {
        LOG(INFO) << "Skipped Update tablet";
        return Status::OK();
    }

    std::vector<RowsetSharedPtr> rowsets;
    Status acquire_rowset_st;
    {
        std::shared_lock l(tablet->get_header_lock());
        acquire_rowset_st = tablet->capture_consistent_rowsets(_version, &rowsets);
    }

    if (!acquire_rowset_st.ok()) {
        std::stringstream ss;
        ss << "fail to init reader. tablet=" << tablet->full_name() << "res=" << acquire_rowset_st;
        LOG(WARNING) << ss.str();
        return Status::InternalError(ss.str().c_str());
    }

    for (auto& rs : rowsets) {
        RETURN_IF_ERROR(rs->load());
        auto beta_rowset = down_cast<BetaRowset*>(rs.get());
        for (auto seg : beta_rowset->segments()) {
            segments->emplace_back(seg);
        }
    }

    return Status::OK();
}

Status MetaReader::_fill_result_chunk(Chunk* chunk) {
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

Status MetaReader::do_get_next(ChunkPtr* result) {
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

Status MetaReader::open() {
    return Status::OK();
}

Status MetaReader::_read(Chunk* chunk, size_t n) {
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

bool MetaReader::has_more() {
    return _has_more;
}

SegmentMetaCollecter::SegmentMetaCollecter(SegmentSharedPtr segment) : _segment(segment) {}

SegmentMetaCollecter::~SegmentMetaCollecter() {}

Status SegmentMetaCollecter::init(const SegmentMetaCollecterParams* params) {
    _params = params;
    return Status::OK();
}

Status SegmentMetaCollecter::open() {
    RETURN_IF_ERROR(_init_return_column_iterators());
    return Status::OK();
}

Status SegmentMetaCollecter::_init_return_column_iterators() {
    DCHECK_EQ(_params->fields.size(), _params->cids.size());
    DCHECK_EQ(_params->fields.size(), _params->read_page.size());

    fs::BlockManager* block_mgr = fs::fs_util::block_manager();
    RETURN_IF_ERROR(block_mgr->open_block(_segment->file_name(), &_rblock));

    _column_iterators.resize(_params->max_cid + 1, nullptr);
    for (int i = 0; i < _params->fields.size(); i++) {
        if (_params->read_page[i]) {
            auto cid = _params->cids[i];
            if (_column_iterators[cid] == nullptr) {
                RETURN_IF_ERROR(_segment->new_column_iterator(cid, &_column_iterators[cid]));
                _obj_pool.add(_column_iterators[cid]);

                ColumnIteratorOptions iter_opts;
                iter_opts.check_dict_encoding = true;
                iter_opts.rblock = _rblock.get();
                iter_opts.stats = &_stats;
                RETURN_IF_ERROR(_column_iterators[cid]->init(iter_opts));
            }
        }
    }
    return Status::OK();
}

Status SegmentMetaCollecter::collect(std::vector<vectorized::Column*>* dsts) {
    DCHECK_EQ(dsts->size(), _params->fields.size());

    for (size_t i = 0; i < _params->fields.size(); i++) {
        RETURN_IF_ERROR(_collect(_params->fields[i], _params->cids[i], (*dsts)[i], _params->field_type[i]));
    }
    return Status::OK();
}

Status SegmentMetaCollecter::_collect(const std::string& name, ColumnId cid, vectorized::Column* column,
                                      FieldType type) {
    if (name == "dict_merge") {
        return _collect_dict(cid, column, type);
    } else if (name == "max") {
        return _collect_max(cid, column, type);
    } else if (name == "min") {
        return _collect_min(cid, column, type);
    }
    return Status::NotSupported("Not Support Collect Meta: " + name);
}

// collect dict
Status SegmentMetaCollecter::_collect_dict(ColumnId cid, vectorized::Column* column, FieldType type) {
    if (!_column_iterators[cid]) {
        return Status::InvalidArgument("Invalid Collect Params.");
    }

    std::vector<Slice> words;
    if (!_column_iterators[cid]->all_page_dict_encoded()) {
        return Status::GlobalDictError("no global dict");
    } else {
        RETURN_IF_ERROR(_column_iterators[cid]->fetch_all_dict_words(&words));
    }

    vectorized::ArrayColumn* array_column = nullptr;
    array_column = down_cast<vectorized::ArrayColumn*>(column);

    auto* offsets = array_column->offsets_column().get();
    auto& data = offsets->get_data();
    size_t end_offset = data.back();
    end_offset += words.size();
    offsets->append(end_offset);

    // add elements
    auto dst = array_column->elements_column().get();
    dst->append_strings(words);

    return Status::OK();
}

Status SegmentMetaCollecter::_collect_max(ColumnId cid, vectorized::Column* column, FieldType type) {
    return __collect_max_or_min<true>(cid, column, type);
}

Status SegmentMetaCollecter::_collect_min(ColumnId cid, vectorized::Column* column, FieldType type) {
    return __collect_max_or_min<false>(cid, column, type);
}

template <bool is_max>
Status SegmentMetaCollecter::__collect_max_or_min(ColumnId cid, vectorized::Column* column, FieldType type) {
    if (cid >= _segment->num_columns()) {
        return Status::NotFound("");
    }
    const ColumnReader* col_reader = _segment->column(cid);
    if (col_reader == nullptr || col_reader->segment_zone_map() == nullptr) {
        return Status::NotFound("");
    }
    if (col_reader->column_type() != type) {
        return Status::InternalError("column type mismatch");
    }
    const ZoneMapPB* segment_zone_map_pb = col_reader->segment_zone_map();
    TypeInfoPtr type_info = get_type_info(delegate_type(type));
    if constexpr (!is_max) {
        vectorized::Datum min;
        if (!segment_zone_map_pb->has_null()) {
            RETURN_IF_ERROR(vectorized::datum_from_string(type_info.get(), &min, segment_zone_map_pb->min(), nullptr));
            column->append_datum(min);
        }
    } else if constexpr (is_max) {
        vectorized::Datum max;
        if (segment_zone_map_pb->has_not_null()) {
            RETURN_IF_ERROR(vectorized::datum_from_string(type_info.get(), &max, segment_zone_map_pb->max(), nullptr));
            column->append_datum(max);
        }
    }
    return Status::OK();
}

} // namespace starrocks::vectorized
