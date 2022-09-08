// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/be/src/olap/rowset/segment_v2/column_reader.cpp

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "storage/rowset/column_reader.h"

#include <fmt/format.h>

#include <memory>
#include <utility>

#include "column/column.h"
#include "column/column_helper.h"
#include "column/datum_convert.h"
#include "common/logging.h"
#include "storage/rowset/array_column_iterator.h"
#include "storage/rowset/binary_dict_page.h"
#include "storage/rowset/bitmap_index_reader.h"
#include "storage/rowset/bloom_filter.h"
#include "storage/rowset/bloom_filter_index_reader.h"
#include "storage/rowset/encoding_info.h"
#include "storage/rowset/page_handle.h"
#include "storage/rowset/page_io.h"
#include "storage/rowset/page_pointer.h"
#include "storage/rowset/scalar_column_iterator.h"
#include "storage/rowset/zone_map_index.h"
#include "storage/types.h"
#include "storage/vectorized_column_predicate.h"
#include "storage/wrapper_field.h"
#include "util/compression/block_compression.h"
#include "util/rle_encoding.h"

namespace starrocks {

StatusOr<std::unique_ptr<ColumnReader>> ColumnReader::create(ColumnMetaPB* meta, const Segment* segment) {
    auto r = std::make_unique<ColumnReader>(private_type(0), segment);
    RETURN_IF_ERROR(r->_init(meta));
    return std::move(r);
}

ColumnReader::ColumnReader(const private_type&, const Segment* segment) : _segment(segment) {
    MEM_TRACKER_SAFE_CONSUME(ExecEnv::GetInstance()->column_metadata_mem_tracker(), sizeof(ColumnReader));
}

ColumnReader::~ColumnReader() {
    if (_segment_zone_map != nullptr) {
        MEM_TRACKER_SAFE_RELEASE(ExecEnv::GetInstance()->segment_zonemap_mem_tracker(),
                                 _segment_zone_map->SpaceUsedLong());
        _segment_zone_map.reset(nullptr);
    }
    if (_ordinal_index_meta != nullptr) {
        MEM_TRACKER_SAFE_RELEASE(ExecEnv::GetInstance()->ordinal_index_mem_tracker(),
                                 _ordinal_index_meta->SpaceUsedLong());
        _ordinal_index_meta.reset(nullptr);
    }
    if (_zonemap_index_meta != nullptr) {
        MEM_TRACKER_SAFE_RELEASE(ExecEnv::GetInstance()->column_zonemap_index_mem_tracker(),
                                 _zonemap_index_meta->SpaceUsedLong());
        _zonemap_index_meta.reset(nullptr);
    }
    if (_bitmap_index_meta != nullptr) {
        MEM_TRACKER_SAFE_RELEASE(ExecEnv::GetInstance()->bitmap_index_mem_tracker(),
                                 _bitmap_index_meta->SpaceUsedLong());
        _bitmap_index_meta.reset(nullptr);
    }
    if (_bloom_filter_index_meta != nullptr) {
        MEM_TRACKER_SAFE_RELEASE(ExecEnv::GetInstance()->bloom_filter_index_mem_tracker(),
                                 _bloom_filter_index_meta->SpaceUsedLong());
        _bloom_filter_index_meta.reset(nullptr);
    }
    MEM_TRACKER_SAFE_RELEASE(ExecEnv::GetInstance()->column_metadata_mem_tracker(), sizeof(ColumnReader));
}

Status ColumnReader::_init(ColumnMetaPB* meta) {
    _column_type = static_cast<FieldType>(meta->type());
    _dict_page_pointer = PagePointer(meta->dict_page());
    _total_mem_footprint = meta->total_mem_footprint();

    if (meta->is_nullable()) _flags |= kIsNullableMask;
    if (meta->has_all_dict_encoded()) _flags |= kHasAllDictEncodedMask;
    if (meta->all_dict_encoded()) _flags |= kAllDictEncodedMask;

    if (_column_type == OLAP_FIELD_TYPE_JSON && meta->has_json_meta()) {
        // TODO(mofei) store format_version in ColumnReader
        const JsonMetaPB& json_meta = meta->json_meta();
        CHECK_EQ(kJsonMetaDefaultFormatVersion, json_meta.format_version()) << "Only format_version=1 is supported";
    }
    if (is_scalar_field_type(delegate_type(_column_type))) {
        RETURN_IF_ERROR(EncodingInfo::get(delegate_type(_column_type), meta->encoding(), &_encoding_info));
        RETURN_IF_ERROR(get_block_compression_codec(meta->compression(), &_compress_codec));

        for (int i = 0; i < meta->indexes_size(); i++) {
            auto* index_meta = meta->mutable_indexes(i);
            switch (index_meta->type()) {
            case ORDINAL_INDEX:
                _ordinal_index_meta.reset(index_meta->release_ordinal_index());
                MEM_TRACKER_SAFE_CONSUME(ExecEnv::GetInstance()->ordinal_index_mem_tracker(),
                                         _ordinal_index_meta->SpaceUsedLong());
                _ordinal_index = std::make_unique<OrdinalIndexReader>();
                break;
            case ZONE_MAP_INDEX:
                _zonemap_index_meta.reset(index_meta->release_zone_map_index());
                MEM_TRACKER_SAFE_CONSUME(ExecEnv::GetInstance()->column_zonemap_index_mem_tracker(),
                                         _zonemap_index_meta->SpaceUsedLong());
                _zonemap_index = std::make_unique<ZoneMapIndexReader>();
                _segment_zone_map.reset(_zonemap_index_meta->release_segment_zone_map());
                MEM_TRACKER_SAFE_CONSUME(ExecEnv::GetInstance()->segment_zonemap_mem_tracker(),
                                         _segment_zone_map->SpaceUsedLong());
                break;
            case BITMAP_INDEX:
                _bitmap_index_meta.reset(index_meta->release_bitmap_index());
                MEM_TRACKER_SAFE_CONSUME(ExecEnv::GetInstance()->bitmap_index_mem_tracker(),
                                         _bitmap_index_meta->SpaceUsedLong());
                _bitmap_index = std::make_unique<BitmapIndexReader>();
                break;
            case BLOOM_FILTER_INDEX:
                _bloom_filter_index_meta.reset(index_meta->release_bloom_filter_index());
                MEM_TRACKER_SAFE_CONSUME(ExecEnv::GetInstance()->bloom_filter_index_mem_tracker(),
                                         _bloom_filter_index_meta->SpaceUsedLong());
                _bloom_filter_index = std::make_unique<BloomFilterIndexReader>();
                break;
            case UNKNOWN_INDEX_TYPE:
                return Status::Corruption(fmt::format("Bad file {}: unknown index type", file_name()));
            }
        }
        if (_ordinal_index == nullptr) {
            return Status::Corruption(
                    fmt::format("Bad file {}: missing ordinal index for column {}", file_name(), meta->column_id()));
        }
        return Status::OK();
    } else if (_column_type == FieldType::OLAP_FIELD_TYPE_ARRAY) {
        _sub_readers = std::make_unique<SubReaderList>();
        if (meta->is_nullable()) {
            if (meta->children_columns_size() != 3) {
                return Status::InvalidArgument("nullable array should have 3 children columns");
            }
            _sub_readers->reserve(3);

            // elements
            auto res = ColumnReader::create(meta->mutable_children_columns(0), _segment);
            RETURN_IF_ERROR(res);
            _sub_readers->emplace_back(std::move(res).value());

            // null flags
            res = ColumnReader::create(meta->mutable_children_columns(1), _segment);
            RETURN_IF_ERROR(res);
            _sub_readers->emplace_back(std::move(res).value());

            // offsets
            res = ColumnReader::create(meta->mutable_children_columns(2), _segment);
            RETURN_IF_ERROR(res);
            _sub_readers->emplace_back(std::move(res).value());
        } else {
            if (meta->children_columns_size() != 2) {
                return Status::InvalidArgument("non-nullable array should have 2 children columns");
            }
            _sub_readers->reserve(2);

            // elements
            auto res = ColumnReader::create(meta->mutable_children_columns(0), _segment);
            RETURN_IF_ERROR(res);
            _sub_readers->emplace_back(std::move(res).value());

            // offsets
            res = ColumnReader::create(meta->mutable_children_columns(1), _segment);
            RETURN_IF_ERROR(res);
            _sub_readers->emplace_back(std::move(res).value());
        }
        return Status::OK();
    } else {
        return Status::NotSupported(fmt::format("unsupported field type {}", (int)_column_type));
    }
}

Status ColumnReader::new_bitmap_index_iterator(BitmapIndexIterator** iterator) {
    RETURN_IF_ERROR(_load_bitmap_index());
    RETURN_IF_ERROR(_bitmap_index->new_iterator(iterator));
    return Status::OK();
}

Status ColumnReader::read_page(const ColumnIteratorOptions& iter_opts, const PagePointer& pp, PageHandle* handle,
                               Slice* page_body, PageFooterPB* footer) {
    iter_opts.sanity_check();
    PageReadOptions opts;
    opts.read_file = iter_opts.read_file;
    opts.page_pointer = pp;
    opts.codec = _compress_codec;
    opts.stats = iter_opts.stats;
    opts.verify_checksum = true;
    opts.use_page_cache = iter_opts.use_page_cache;
    opts.encoding_type = _encoding_info->encoding();
    opts.kept_in_memory = keep_in_memory();

    return PageIO::read_and_decompress_page(opts, handle, page_body, footer);
}

Status ColumnReader::_calculate_row_ranges(const std::vector<uint32_t>& page_indexes,
                                           vectorized::SparseRange* row_ranges) {
    for (auto i : page_indexes) {
        ordinal_t page_first_id = _ordinal_index->get_first_ordinal(i);
        ordinal_t page_last_id = _ordinal_index->get_last_ordinal(i);
        row_ranges->add({static_cast<rowid_t>(page_first_id), static_cast<rowid_t>(page_last_id + 1)});
    }
    return Status::OK();
}

Status ColumnReader::_parse_zone_map(const ZoneMapPB& zm, vectorized::ZoneMapDetail* detail) const {
    // DECIMAL32/DECIMAL64/DECIMAL128 stored as INT32/INT64/INT128
    // The DECIMAL type will be delegated to INT type.
    TypeInfoPtr type_info = get_type_info(delegate_type(_column_type));
    detail->set_has_null(zm.has_null());

    if (zm.has_not_null()) {
        RETURN_IF_ERROR(vectorized::datum_from_string(type_info.get(), &(detail->min_value()), zm.min(), nullptr));
        RETURN_IF_ERROR(vectorized::datum_from_string(type_info.get(), &(detail->max_value()), zm.max(), nullptr));
    }
    detail->set_num_rows(static_cast<size_t>(num_rows()));
    return Status::OK();
}

// prerequisite: at least one predicate in |predicates| support bloom filter.
Status ColumnReader::bloom_filter(const std::vector<const vectorized::ColumnPredicate*>& predicates,
                                  vectorized::SparseRange* row_ranges) {
    RETURN_IF_ERROR(_load_bloom_filter_index());
    vectorized::SparseRange bf_row_ranges;
    std::unique_ptr<BloomFilterIndexIterator> bf_iter;
    RETURN_IF_ERROR(_bloom_filter_index->new_iterator(&bf_iter));
    size_t range_size = row_ranges->size();
    // get covered page ids
    std::set<int32_t> page_ids;
    for (int i = 0; i < range_size; ++i) {
        vectorized::Range r = (*row_ranges)[i];
        int64_t idx = r.begin();
        auto iter = _ordinal_index->seek_at_or_before(r.begin());
        while (idx < r.end()) {
            page_ids.insert(iter.page_index());
            idx = static_cast<int>(iter.last_ordinal() + 1);
            iter.next();
        }
    }
    for (const auto& pid : page_ids) {
        std::unique_ptr<BloomFilter> bf;
        RETURN_IF_ERROR(bf_iter->read_bloom_filter(pid, &bf));
        for (const auto* pred : predicates) {
            if (pred->support_bloom_filter() && pred->bloom_filter(bf.get())) {
                bf_row_ranges.add(vectorized::Range(_ordinal_index->get_first_ordinal(pid),
                                                    _ordinal_index->get_last_ordinal(pid) + 1));
            }
        }
    }
    *row_ranges = row_ranges->intersection(bf_row_ranges);
    return Status::OK();
}

Status ColumnReader::load_ordinal_index() {
    return _load_ordinal_index();
}

Status ColumnReader::_load_ordinal_index() {
    if (_ordinal_index == nullptr || _ordinal_index->loaded()) return Status::OK();
    SCOPED_THREAD_LOCAL_CHECK_MEM_LIMIT_SETTER(false);
    auto fs = file_system();
    auto meta = _ordinal_index_meta.get();
    auto use_page_cache = !config::disable_storage_page_cache;
    auto kept_in_memory = keep_in_memory();
    ASSIGN_OR_RETURN(auto first_load,
                     _ordinal_index->load(fs, file_name(), *meta, num_rows(), use_page_cache, kept_in_memory));
    if (UNLIKELY(first_load)) {
        MEM_TRACKER_SAFE_RELEASE(ExecEnv::GetInstance()->ordinal_index_mem_tracker(),
                                 _ordinal_index_meta->SpaceUsedLong());
        _ordinal_index_meta.reset();
    }
    return Status::OK();
}

Status ColumnReader::_load_zonemap_index() {
    if (_zonemap_index == nullptr || _zonemap_index->loaded()) return Status::OK();
    SCOPED_THREAD_LOCAL_CHECK_MEM_LIMIT_SETTER(false);
    auto fs = file_system();
    auto meta = _zonemap_index_meta.get();
    auto use_page_cache = !config::disable_storage_page_cache;
    auto kept_in_memory = keep_in_memory();
    ASSIGN_OR_RETURN(auto first_load, _zonemap_index->load(fs, file_name(), *meta, use_page_cache, kept_in_memory));
    if (UNLIKELY(first_load)) {
        MEM_TRACKER_SAFE_RELEASE(ExecEnv::GetInstance()->column_zonemap_index_mem_tracker(),
                                 _zonemap_index_meta->SpaceUsedLong());
        _zonemap_index_meta.reset();
    }
    return Status::OK();
}

Status ColumnReader::_load_bitmap_index() {
    if (_bitmap_index == nullptr || _bitmap_index->loaded()) return Status::OK();
    SCOPED_THREAD_LOCAL_CHECK_MEM_LIMIT_SETTER(false);
    auto fs = file_system();
    auto meta = _bitmap_index_meta.get();
    auto use_page_cache = !config::disable_storage_page_cache;
    auto kept_in_memory = keep_in_memory();
    ASSIGN_OR_RETURN(auto first_load, _bitmap_index->load(fs, file_name(), *meta, use_page_cache, kept_in_memory));
    if (UNLIKELY(first_load)) {
        MEM_TRACKER_SAFE_RELEASE(ExecEnv::GetInstance()->bitmap_index_mem_tracker(),
                                 _bitmap_index_meta->SpaceUsedLong());
        _bitmap_index_meta.reset();
    }
    return Status::OK();
}

Status ColumnReader::_load_bloom_filter_index() {
    if (_bloom_filter_index == nullptr || _bloom_filter_index->loaded()) return Status::OK();
    SCOPED_THREAD_LOCAL_CHECK_MEM_LIMIT_SETTER(false);
    auto fs = file_system();
    auto meta = _bloom_filter_index_meta.get();
    auto use_page_cache = !config::disable_storage_page_cache;
    auto kept_in_memory = keep_in_memory();
    ASSIGN_OR_RETURN(auto first_load,
                     _bloom_filter_index->load(fs, file_name(), *meta, use_page_cache, kept_in_memory));
    if (UNLIKELY(first_load)) {
        MEM_TRACKER_SAFE_RELEASE(ExecEnv::GetInstance()->bloom_filter_index_mem_tracker(),
                                 _bloom_filter_index_meta->SpaceUsedLong());
        _bloom_filter_index_meta.reset();
    }
    return Status::OK();
}

Status ColumnReader::seek_to_first(OrdinalPageIndexIterator* iter) {
    *iter = _ordinal_index->begin();
    if (!iter->valid()) {
        return Status::NotFound("Failed to seek to first rowid");
    }
    return Status::OK();
}

Status ColumnReader::seek_at_or_before(ordinal_t ordinal, OrdinalPageIndexIterator* iter) {
    *iter = _ordinal_index->seek_at_or_before(ordinal);
    if (!iter->valid()) {
        return Status::NotFound(fmt::format("Failed to seek to ordinal {}", ordinal));
    }
    return Status::OK();
}

Status ColumnReader::zone_map_filter(const std::vector<const vectorized::ColumnPredicate*>& predicates,
                                     const vectorized::ColumnPredicate* del_predicate,
                                     std::unordered_set<uint32_t>* del_partial_filtered_pages,
                                     vectorized::SparseRange* row_ranges) {
    RETURN_IF_ERROR(_load_zonemap_index());
    std::vector<uint32_t> page_indexes;
    RETURN_IF_ERROR(_zone_map_filter(predicates, del_predicate, del_partial_filtered_pages, &page_indexes));
    RETURN_IF_ERROR(_calculate_row_ranges(page_indexes, row_ranges));
    return Status::OK();
}

Status ColumnReader::_zone_map_filter(const std::vector<const vectorized::ColumnPredicate*>& predicates,
                                      const vectorized::ColumnPredicate* del_predicate,
                                      std::unordered_set<uint32_t>* del_partial_filtered_pages,
                                      std::vector<uint32_t>* pages) {
    const std::vector<ZoneMapPB>& zone_maps = _zonemap_index->page_zone_maps();
    int32_t page_size = _zonemap_index->num_pages();
    for (int32_t i = 0; i < page_size; ++i) {
        const ZoneMapPB& zm = zone_maps[i];
        vectorized::ZoneMapDetail detail;
        _parse_zone_map(zm, &detail);
        bool matched = true;
        for (const auto* predicate : predicates) {
            if (!predicate->zone_map_filter(detail)) {
                matched = false;
                break;
            }
        }
        if (!matched) {
            continue;
        }
        pages->emplace_back(i);

        if (del_predicate && del_predicate->zone_map_filter(detail)) {
            del_partial_filtered_pages->emplace(i);
        }
    }
    return Status::OK();
}

bool ColumnReader::segment_zone_map_filter(const std::vector<const vectorized::ColumnPredicate*>& predicates) const {
    if (_segment_zone_map == nullptr) {
        return true;
    }
    vectorized::ZoneMapDetail detail;
    _parse_zone_map(*_segment_zone_map, &detail);
    auto filter = [&](const vectorized::ColumnPredicate* pred) { return pred->zone_map_filter(detail); };
    return std::all_of(predicates.begin(), predicates.end(), filter);
}

Status ColumnReader::new_iterator(ColumnIterator** iterator) {
    if (is_scalar_field_type(delegate_type(_column_type))) {
        *iterator = new ScalarColumnIterator(this);
        return Status::OK();
    } else if (_column_type == FieldType::OLAP_FIELD_TYPE_ARRAY) {
        size_t col = 0;
        ColumnIterator* element_iterator;
        RETURN_IF_ERROR((*_sub_readers)[col++]->new_iterator(&element_iterator));

        ColumnIterator* null_iterator = nullptr;
        if (is_nullable()) {
            RETURN_IF_ERROR((*_sub_readers)[col++]->new_iterator(&null_iterator));
        }

        ColumnIterator* array_size_iterator;
        RETURN_IF_ERROR((*_sub_readers)[col]->new_iterator(&array_size_iterator));

        *iterator = new ArrayColumnIterator(null_iterator, array_size_iterator, element_iterator);
        return Status::OK();
    } else {
        return Status::NotSupported("unsupported type to create iterator: " + std::to_string(_column_type));
    }
}

} // namespace starrocks
