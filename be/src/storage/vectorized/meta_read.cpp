// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#include "storage/vectorized/meta_read.h"

#include <vector>

#include "storage/tablet.h"
#include "storage/vectorized/chunk_helper.h"
#include "storage/rowset/beta_rowset.h"
#include "storage/rowset/segment_v2/column_reader.h"
#include "column/datum_convert.h"

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

    std::cout << "_seg_collecters size " << _seg_collecters.size() << std::endl;
    
    if (_seg_collecters.size() == 0) {
        _has_more = false;
        return Status::OK();
    }

    _collecter_cursor = _seg_collecters[0];
    _cursor_idx = 0;
    _is_init = true;
    _has_more = true;
    return Status::OK();
}

static std::vector<std::string> split_string(const std::string &str, const char pattern) {
    std::vector<std::string> res;
    std::stringstream input(str);   //读取str到字符串流中
    std::string temp;
    //使用getline函数从字符串流中读取,遇到分隔符时停止,和从cin中读取类似
    //注意,getline默认是可以读取空格的
    while(std::getline(input, temp, pattern))
    {
        res.push_back(temp);
    }
    return res;
}
static std::vector<std::string> func_names = {
    "max",
    "min",
    "dict_merge"
};

static int get_func_name_and_col_name(const std::string& in, std::string* func_name, std::string* col_name) {
    for (size_t i = 0; i < func_names.size(); i++) {
        if (in.size() <= func_names[i].size()) { continue; }
        if (in.find(func_names[i]) != std::string::npos 
            && in.substr(0, func_names[i].size()) == func_names[i]) {
                *func_name = func_names[i];
                *col_name = in.substr(func_names[i].size() + 1);
                return 0;
        }
    }
    return -1;
}

// 检查参数，判断是否需要打开 Column 迭代器（会发生 io）
Status MetaReader::_init_params(const MetaReaderParams& read_params) {
    read_params.check_validation();
    // Note: 初始化各种参数
    _tablet = read_params.tablet;
    _version = read_params.version;
    _chunk_size = read_params.chunk_size;
    _params = read_params;

    _seg_collecter_params.max_cid = 0;

    // 拆分需要收集的项目
    for (auto it : *(read_params.id_to_names)) {
        // just for debug
        //std::cout << "name: " << it.second << " id " << it.first << std::endl;
        // 拆分 col_name 和 collect_name, 目前用比较 trick 的写法了

        /*
            auto sub_strings = split_string(it.second , '_');  
            auto& collect_name = sub_strings[0];
            std::string col_name;
            for (size_t i = 1; i < sub_strings.size(); i++) {
                col_name += sub_strings[i];
                if (i != sub_strings.size() - 1) {
                    col_name += "_";
                }
            }
        */
        std::string col_name = "";
        std::string collect_name = "";
        get_func_name_and_col_name(it.second, &collect_name, &col_name);
     
        
        // just for debug
        //std::cout << "max: " << collect_name << " col_name " << col_name << std::endl;
        
        int32_t index = _tablet->field_index(col_name);

        if (index < 0) {
            std::stringstream ss;
            ss << "invalid field name: " << it.second;
            LOG(WARNING) << ss.str();
            return Status::InternalError(ss.str());
        }

        // get field type
        FieldType type = _tablet->tablet_schema().column(index).type();
        // 类型
        _seg_collecter_params.field_type.emplace_back(type);

        // 采集项
        _seg_collecter_params.fields.emplace_back(collect_name);
        _collect_names.emplace_back(collect_name);
        // 对应的列 id
        _seg_collecter_params.cids.emplace_back(index);
        _collect_col_ids.emplace_back(index);

        _seg_collecter_params.max_cid = std::max(_seg_collecter_params.max_cid, index);

        // 对应的 slot id
        _return_slot_ids.emplace_back(it.first);
        // 是否需要打开 column 迭代器
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
                               
    //std::vector<RowsetReaderSharedPtr> rs_reader;
    std::vector<RowsetSharedPtr> rowsets;
    tablet->obtain_header_rdlock();
    /*
    OLAPStatus acquire_reader_st = tablet->capture_rs_readers(_version, &rs_reader);
    if (acquire_reader_st != OLAP_SUCCESS) {      
        std::stringstream ss;
        ss << "fail to init reader. tablet=" << tablet->full_name() 
           << "res=" << acquire_reader_st;
        LOG(WARNING) << ss.str();
        return Status::InternalError(ss.str().c_str());
    } 

    std::cout << "rs_readers: " << rs_reader.size() << std::endl;
    for (auto& rs : rs_reader) {
        auto beta_rowset = down_cast<BetaRowset*>(rs->rowset().get());
        std::cout << "segments: " << beta_rowset->segments().size() << std::endl;
        for (auto seg : beta_rowset->segments()) {
            segments->emplace_back(seg);
        }
        std::cout << "segments: " << segments->size() << std::endl;
    }
    */
    OLAPStatus acquire_rowset_st = tablet->capture_consistent_rowsets(_version, &rowsets);
    tablet->release_header_lock();
     if (acquire_rowset_st != OLAP_SUCCESS) {      
        std::stringstream ss;
        ss << "fail to init reader. tablet=" << tablet->full_name() 
           << "res=" << acquire_rowset_st;
        LOG(WARNING) << ss.str();
        return Status::InternalError(ss.str().c_str());
    } 

    std::cout << "rowset_set: " << rowsets.size() << " version: " << _version << std::endl;

    for (auto& rs : rowsets) {
        RETURN_IF_ERROR(rs->load());
        auto beta_rowset = down_cast<BetaRowset*>(rs.get());
        std::cout << "segments: " << beta_rowset->segments().size() << std::endl;
        for (auto seg : beta_rowset->segments()) {
            segments->emplace_back(seg);
        }
        std::cout << "segments: " << segments->size() << std::endl;
    }
   
    return Status::OK();
}

Status MetaReader::do_get_next(ChunkPtr* result) {
    const uint32_t chunk_capacity = _chunk_size;
    uint16_t chunk_start = 0;

    //std::cout << "do_get_next: " << std::endl;

    *result = std::make_shared<vectorized::Chunk>();
    if (nullptr == result->get()) {
        return Status::InternalError("Failed to allocate new chunk.");
    } 
    
    /*
    for (auto& slot : *(_params.slots)) {
        vectorized::ColumnPtr column = vectorized::ColumnHelper::create_column(slot->type(), false);
        (*result)->append_column(std::move(column), slot->id());
    }
    */

    for (auto& s_id : _return_slot_ids) {
        auto slot = _params.desc_tbl->get_slot_descriptor(s_id);
        vectorized::ColumnPtr column = vectorized::ColumnHelper::create_column(slot->type(), false);
        (*result)->append_column(std::move(column), slot->id());
    }

    //std::cout << "chunk_start: " << chunk_start << std::endl;
    //std::cout << "chunk_capacity: " << chunk_capacity << std::endl;
    //std::cout << "has_more: " << _has_more << std::endl;

    while ((chunk_start < chunk_capacity) && _has_more) {
        std::cout << "read one chunk" << std::endl;
        RETURN_IF_ERROR(_read((*result).get(), chunk_capacity - chunk_start));
        (*result)->check_or_die();
        size_t next_start = (*result)->num_rows();
        chunk_start = next_start;
        std::cout << "chunk_start: " << chunk_start << std::endl;

    }

   return Status::OK();
}

Status MetaReader::_read(Chunk* chunk, size_t n) {
    // prepare return coloumns;
    std::vector<vectorized::Column*> columns;
    for (size_t i = 0; i < _collect_names.size(); ++i) {
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
    _cursor_idx++;
    if (_cursor_idx >= _seg_collecters.size()) {
        _collecter_cursor = nullptr;
    } else {
        _collecter_cursor = _seg_collecters[_cursor_idx];
    }
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
    std::cout << "collect one row "<< std::endl;

    for (size_t i = 0; i < _params->fields.size(); i++) {
        RETURN_IF_ERROR(_collect(_params->fields[i], _params->cids[i], (*dsts)[i], _params->field_type[i]));
    }
    return Status::OK();
}

Status SegmentMetaCollecter::_collect(const std::string& name, ColumnId cid, vectorized::Column*column, FieldType type) {
    //std::cout << "collect name: " << name << std::endl;
    // 目前比较 trick 的写法
    if (name == "dict") {
        return _collect_dict(cid, column, type);
    } else if (name == "max") {
        return _collect_max(cid, column, type);
    } else if (name == "min") {
        return _collect_min(cid, column, type);
    }
    return Status::NotSupported("");
}

// collect dict
Status SegmentMetaCollecter::_collect_dict(ColumnId cid, vectorized::Column* column, FieldType type) {
    // just for debug
    std::cout << "_collect_dict: " << cid <<  std::endl;
    if (!_column_iterators[cid]) { 
        return Status::InvalidArgument("Invalid Collet Params.");
    }

    if (!_column_iterators[cid]->all_page_dict_encoded()) { 
        return Status::NotSupported("no all page dict encoded."); 
    }

    std::vector<Slice> words;
    RETURN_IF_ERROR(_column_iterators[cid]->fetch_all_dict_words(&words));    

    std::cout << "Collect words size: " << words.size() << std::endl;
    for (size_t i = 0; i <  words.size(); i++) {
        std::cout << "word: " << words[i] << std::endl;
    } 

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

Status SegmentMetaCollecter::_collect_max(ColumnId cid, vectorized::Column* column, FieldType type) {
    return __collect_max_or_min<false>(cid, column, type);
}

Status SegmentMetaCollecter::_collect_min(ColumnId cid, vectorized::Column* column, FieldType type) {
    return __collect_max_or_min<true>(cid, column, type);
}

template<bool flag>
Status SegmentMetaCollecter::__collect_max_or_min(ColumnId cid, vectorized::Column* column, FieldType type) {
    std::cout << "collect max or min "<< std::endl;

    auto footer_pb = _segment->footer();
    //BinaryColumn* binary_column;
    //binary_column = down_cast<BinaryColumn*>(column);
    for (size_t i = 0; i < footer_pb.columns_size(); i++) {
        if (footer_pb.columns(i).column_id() == cid) {
            for (size_t j = 0; j < footer_pb.columns(i).indexes_size(); j++) {
                if (footer_pb.columns(i).indexes(j).type() != ZONE_MAP_INDEX) {
                    continue;
                }
                const ColumnIndexMetaPB& c_meta_pb = footer_pb.columns(i).indexes(j);
                const ZoneMapIndexPB& zone_map_index_pb = c_meta_pb.zone_map_index();
                const ZoneMapPB& segment_zone_map_pb = zone_map_index_pb.segment_zone_map();

                TypeInfoPtr type_info = get_type_info(delegate_type(type));
                vectorized::Datum min;
                vectorized::Datum max;
                 if (!segment_zone_map_pb.has_null()) {
                    RETURN_IF_ERROR(vectorized::datum_from_string(type_info.get(), &min, segment_zone_map_pb.min(), nullptr));
                }
                if (segment_zone_map_pb.has_not_null()) {
                    RETURN_IF_ERROR(vectorized::datum_from_string(type_info.get(), &max, segment_zone_map_pb.max(), nullptr));

                }
                if (flag) {
                    column->append_datum(min);
                } else {
                    column->append_datum(max);
                }

                return Status::OK();

            }
            break;
        }
    }
    return Status::NotFound("");
}

} // namespace vectorized

} // namespace starrocks




