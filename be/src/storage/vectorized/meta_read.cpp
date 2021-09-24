// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#include "storage/vectorized/meta_read.h"

#include <vector>

#include "storage/tablet.h"
#include "storage/vectorized/chunk_helper.h"
#include "storage/rowset/beta_rowset.h"
#include "storage/rowset/segment_v2/column_reader.h"

// just for debug
#include <iostream>

namespace starrocks {
namespace vectorized { 

MetaReader::MetaReader() 
    : _is_init(false),
      _has_more(false) {}

MetaReader::~MetaReader() {}

Status MetaReader::init(const MetaReaderParams& read_params) {
    RETURN_IF_ERROR(_init_params(read_params));
    RETURN_IF_ERROR(_init_seg_meta_collecters(read_params));
    
    if (_seg_collecters.size() == 0) {
        _has_more = false;
        return Status::OK();
    }

    _collecter_cursor = _seg_collecters.front();
    _end_collecter = _seg_collecters.back();
    _is_init = true;
    _has_more = true;
    return Status::OK();
}

// 检查参数，判断是否需要打开 Column 迭代器（会发生 io）
Status MetaReader::_init_params(const MetaReaderParams& read_params) {
    read_params.check_validation();
    // Note: 初始化各种参数
    _tablet = read_params.tablet;
    _version = read_params.version;
    _chunk_size = read_params.chunk_size;
    _params = read_params;

    for (auto& slot : *(read_params.slots)) {
        if (!slot->is_materialized()) {
            continue;
        }
        // 可能需要拆分 col name, 才能得出正确的 col_name
        int32_t index = _tablet->field_index(slot->col_name());

        // just for debug
        std::cout << "slot name" << slot->col_name() << std::endl;

        if (index < 0) {
            std::stringstream ss;
            ss << "invalid field name: " << slot->col_name();
            LOG(WARNING) << ss.str();
            return Status::InternalError(ss.str());
        }

        _seg_collecter_params.fields.emplace_back(slot->col_name());
        _seg_collecter_params.cids.emplace_back(index);
        _seg_collecter_params.read_page.emplace_back(true);
    }

    return Status::OK();
}

Status MetaReader::_init_seg_meta_collecters(const MetaReaderParams& params) {
    std::vector<segment_v2::SegmentSharedPtr> segments;
    RETURN_IF_ERROR(_get_segments(params.tablet, params.version, &segments));

    //get segments
    for (auto& segment : segments) {
        SegmentMetaCollecter* seg_collecter = new SegmentMetaCollecter(segment);
        _obj_pool.add(seg_collecter);

        RETURN_IF_ERROR(seg_collecter->init(&_seg_collecter_params));
        _seg_collecters.emplace_back(seg_collecter);
    }

    return Status::OK();
}

Status MetaReader::_get_segments(const TabletSharedPtr& tablet, const Version& version,
                                 std::vector<segment_v2::SegmentSharedPtr>* segments) {
    if (tablet->updates() != nullptr) {
        LOG(INFO) << "Skipped Update tablet";
        return Status::OK();
    }
                               
    std::vector<RowsetReaderSharedPtr> rs_reader;
    tablet->obtain_header_rdlock();
    OLAPStatus acquire_reader_st = tablet->capture_rs_readers(_version, &rs_reader);
    if (acquire_reader_st != OLAP_SUCCESS) {      
        std::stringstream ss;
        ss << "fail to init reader. tablet=" << tablet->full_name() 
           << "res=" << acquire_reader_st;
        LOG(WARNING) << ss.str();
        return Status::InternalError(ss.str().c_str());
    } 

    for (auto& rs : rs_reader) {
        auto beta_rowset = down_cast<BetaRowset*>(rs->rowset().get());
        for (auto seg : beta_rowset->segments()) {
            segments->emplace_back(seg);
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
    
    for (auto& slot : *(_params.slots)) {
        vectorized::ColumnPtr column = vectorized::ColumnHelper::create_column(slot->type(), false);
        (*result)->append_column(std::move(column), slot->id());
    }

    while ((chunk_start < chunk_capacity) & _has_more) {
        RETURN_IF_ERROR(_read((*result).get(), chunk_capacity - chunk_start));
        (*result)->check_or_die();
        size_t next_start = (*result)->num_rows();
        chunk_start = next_start;
    }

   return Status::OK();
}

Status MetaReader::_read(Chunk* chunk, size_t n) {
    // prepare return coloumns;
    std::vector<vectorized::Column*> columns;
    for (size_t i = 0; i < _params.slots->size(); ++i) {
        const ColumnPtr& col = chunk->get_column_by_index(i);
        columns.emplace_back(col.get());
    }
    
    size_t remaining  = n;
    while (remaining > 0) {
        if (!_collecter_cursor) {
            _has_more = false;
            return Status::OK();
        }
        RETURN_IF_ERROR(_collecter_cursor->collect(&columns));
        remaining--;
        _next_cursor();
    }

    return Status::OK();
}

void MetaReader::_next_cursor() {
    _collecter_cursor++;
    if (_collecter_cursor > _end_collecter) { _collecter_cursor = nullptr; }
}

bool MetaReader::has_more() {
    return _has_more;
}

SegmentMetaCollecter::SegmentMetaCollecter(segment_v2::SegmentSharedPtr segment) 
    : _segment(segment) {}

SegmentMetaCollecter::~SegmentMetaCollecter() {}

Status SegmentMetaCollecter::init(const SegmentMetaCollecterParams* params) {
    _params = params;
    RETURN_IF_ERROR(_init_return_column_iterators());
    
    return Status::OK();
}

Status SegmentMetaCollecter::_init_return_column_iterators() {
    DCHECK_EQ(_params->fields.size(), _params->cids.size());
    DCHECK_EQ(_params->fields.size(), _params->read_page.size());

    _column_iterators.resize(_params->fields.size(), nullptr);
    for (int i = 0; i < _params->fields.size(); i++) {
        if (_params->read_page[i]) {
            auto cid = _params->cids[i];
            if (_column_iterators[cid] == nullptr) {
                RETURN_IF_ERROR(_segment->new_column_iterator(cid, &_column_iterators[cid]));
                _obj_pool.add(_column_iterators[cid]);

                ColumnIteratorOptions iter_opts;
                RETURN_IF_ERROR(_column_iterators[i]->init(iter_opts));
            }
        }
    }
    return Status::OK();
}

Status SegmentMetaCollecter::collect(std::vector<vectorized::Column*>* dsts) {
    DCHECK_EQ(dsts->size(), _params->fields.size());

    for (size_t i = 0; i < _params->fields.size(); i++) {
        RETURN_IF_ERROR(_collect(_params->fields[i], _params->cids[i], (*dsts)[i]));
    }
    return Status::OK();
}

Status SegmentMetaCollecter::_collect(const std::string& name, ColumnId cid, vectorized::Column*column) {
    // 目前比较 trick 的写法
    if (name == "dict") {
        return _collect_dict(cid, column);
    } else if (name == "max") {
        return _collect_max(cid, column);
    } else if (name == "min") {
        return _collect_min(cid, column);
    }
    return Status::NotSupported("");
}

// collect dict
Status SegmentMetaCollecter::_collect_dict(ColumnId cid, vectorized::Column* column) {
    if (!_column_iterators[cid]) { 
        return Status::InvalidArgument("Invalid Collet Params.");
    }

    if (!_column_iterators[cid]->all_page_dict_encoded()) { 
        return Status::NotSupported("no all page dict encoded."); 
    }

    std::vector<Slice> words;
    RETURN_IF_ERROR(_column_iterators[cid]->fetch_all_dict_words(&words));     

    vectorized::ArrayColumn* array_column = nullptr;
    array_column = down_cast<vectorized::ArrayColumn*>(column);

    auto* offsets = array_column->offsets_column().get();
    auto& data = offsets->get_data();
    size_t end_offset = data.back();
    end_offset += words.size();
    offsets->append(end_offset);

    // read elements
    auto dst = array_column->elements_column().get();
    dst->append_strings(words);

    return Status::OK();
}

Status SegmentMetaCollecter::_collect_max(ColumnId cid, vectorized::Column* column) {
    return __collect_max_or_min<false>(cid, column);
}

Status SegmentMetaCollecter::_collect_min(ColumnId cid, vectorized::Column* column) {
    return __collect_max_or_min<true>(cid, column);
}

template<bool flag>
Status SegmentMetaCollecter::__collect_max_or_min(ColumnId cid, vectorized::Column* column) {
    auto footer_pb = _segment->footer();
    BinaryColumn* binary_column;
    binary_column = down_cast<BinaryColumn*>(column);
    for (size_t i = 0; i < footer_pb.columns_size(); i++) {
        if (footer_pb.columns(i).column_id() == cid) {
            const ColumnIndexMetaPB& c_meta_pb = footer_pb.columns(i).indexes(0);
            const ZoneMapIndexPB& zone_map_index_pb = c_meta_pb.zone_map_index();
            const ZoneMapPB& segment_zone_map_pb = zone_map_index_pb.segment_zone_map();

            std::string value;
            if (flag) {
                value = segment_zone_map_pb.max();
            } else {
                value = segment_zone_map_pb.min();
            }

            binary_column->append_string(value);
            return Status::OK();
        }
    }
    return Status::NotFound("");
}

} // namespace vectorized

} // namespace starrocks




