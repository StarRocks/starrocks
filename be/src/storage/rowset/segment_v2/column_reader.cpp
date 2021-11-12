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

#include "storage/rowset/segment_v2/column_reader.h"

#include <fmt/format.h>

#include <memory>
#include <utility>

#include "column/column.h"
#include "column/column_helper.h"
#include "column/datum_convert.h"
#include "common/logging.h"
#include "storage/olap_cond.h"
#include "storage/rowset/segment_v2/array_column_iterator.h"
#include "storage/rowset/segment_v2/binary_dict_page.h" // for BinaryDictPageDecoder
#include "storage/rowset/segment_v2/bitmap_index_reader.h"
#include "storage/rowset/segment_v2/bloom_filter_index_reader.h"
#include "storage/rowset/segment_v2/encoding_info.h"
#include "storage/rowset/segment_v2/page_handle.h" // for PageHandle
#include "storage/rowset/segment_v2/page_io.h"
#include "storage/rowset/segment_v2/page_pointer.h" // for PagePointer
#include "storage/rowset/segment_v2/scalar_column_iterator.h"
#include "storage/rowset/segment_v2/zone_map_index.h"
#include "storage/types.h" // for TypeInfo
#include "storage/vectorized/column_predicate.h"
#include "util/block_compression.h"
#include "util/rle_encoding.h" // for RleDecoder

namespace starrocks::segment_v2 {

StatusOr<std::unique_ptr<ColumnReader>> ColumnReader::create(const ColumnReaderOptions& opts, ColumnMetaPB* meta,
                                                             const std::string& file_name) {
    auto r = std::make_unique<ColumnReader>(private_type(0), opts, file_name);
    RETURN_IF_ERROR(r->_init(meta));
    return std::move(r);
}

ColumnReader::ColumnReader(const private_type&, const ColumnReaderOptions& opts, const std::string& file_name)
        : _opts(opts),
          _file_name(file_name),
          _zone_map_index(),
          _ordinal_index(),
          _bitmap_index(),
          _bloom_filter_index() {}

ColumnReader::~ColumnReader() {
    delete (_flags[kHasOrdinalIndexMetaPos] ? _ordinal_index.meta : nullptr);
    delete (_flags[kHasOrdinalIndexReaderPos] ? _ordinal_index.reader : nullptr);

    delete (_flags[kHasZoneMapIndexMetaPos] ? _zone_map_index.meta : nullptr);
    delete (_flags[kHasZoneMapIndexReaderPos] ? _zone_map_index.reader : nullptr);

    delete (_flags[kHasBitmapIndexMetaPos] ? _bitmap_index.meta : nullptr);
    delete (_flags[kHasBitmapIndexReaderPos] ? _bitmap_index.reader : nullptr);

    delete (_flags[kHasBloomFilterIndexMetaPos] ? _bloom_filter_index.meta : nullptr);
    delete (_flags[kHasBloomFilterIndexReaderPos] ? _bloom_filter_index.reader : nullptr);
}

Status ColumnReader::_init(ColumnMetaPB* meta) {
    _column_length = meta->length();
    _column_type = static_cast<FieldType>(meta->type());
    _dict_page_pointer = PagePointer(meta->dict_page());
    _num_rows = meta->num_rows();
    _flags.set(kHasAllDictEncodedPos, meta->has_all_dict_encoded());
    _flags.set(kAllDictEncodedPos, meta->all_dict_encoded());
    _flags.set(kIsNullablePos, meta->is_nullable());

    if (is_scalar_field_type(delegate_type(_column_type))) {
        RETURN_IF_ERROR(EncodingInfo::get(delegate_type(_column_type), meta->encoding(), &_encoding_info));
        RETURN_IF_ERROR(get_block_compression_codec(meta->compression(), &_compress_codec));

        for (int i = 0; i < meta->indexes_size(); i++) {
            auto* index_meta = meta->mutable_indexes(i);
            switch (index_meta->type()) {
            case ORDINAL_INDEX:
                _ordinal_index.meta = index_meta->release_ordinal_index();
                _flags.set(kHasOrdinalIndexMetaPos, true);
                break;
            case ZONE_MAP_INDEX:
                _zone_map_index.meta = index_meta->release_zone_map_index();
                _segment_zone_map.reset(_zone_map_index.meta->release_segment_zone_map());
                _flags.set(kHasZoneMapIndexMetaPos, true);
                break;
            case BITMAP_INDEX:
                _bitmap_index.meta = index_meta->release_bitmap_index();
                _flags.set(kHasBitmapIndexMetaPos, true);
                break;
            case BLOOM_FILTER_INDEX:
                _bloom_filter_index.meta = index_meta->release_bloom_filter_index();
                _flags.set(kHasBloomFilterIndexMetaPos, true);
                break;
            case UNKNOWN_INDEX_TYPE:
                return Status::Corruption(fmt::format("Bad file {}: unknown index type", _file_name));
            }
        }
        if (!_flags[kHasOrdinalIndexMetaPos]) {
            return Status::Corruption(
                    fmt::format("Bad file {}: missing ordinal index for column {}", _file_name, meta->column_id()));
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
            auto res = ColumnReader::create(_opts, meta->mutable_children_columns(0), _file_name);
            if (!res.ok()) {
                return res.status();
            }
            _sub_readers->emplace_back(std::move(res).value());

            // null flags
            res = ColumnReader::create(_opts, meta->mutable_children_columns(1), _file_name);
            if (!res.ok()) {
                return res.status();
            }
            _sub_readers->emplace_back(std::move(res).value());

            // offsets
            res = ColumnReader::create(_opts, meta->mutable_children_columns(2), _file_name);
            if (!res.ok()) {
                return res.status();
            }
            _sub_readers->emplace_back(std::move(res).value());
        } else {
            if (meta->children_columns_size() != 2) {
                return Status::InvalidArgument("non-nullable array should have 2 children columns");
            }
            _sub_readers->reserve(2);

            // elements
            auto res = ColumnReader::create(_opts, meta->mutable_children_columns(0), _file_name);
            if (!res.ok()) {
                return res.status();
            }
            _sub_readers->emplace_back(std::move(res).value());

            // offsets
            res = ColumnReader::create(_opts, meta->mutable_children_columns(1), _file_name);
            if (!res.ok()) {
                return res.status();
            }
            _sub_readers->emplace_back(std::move(res).value());
        }
        return Status::OK();
    } else {
        return Status::NotSupported(fmt::format("unsupported field type {}", (int)_column_type));
    }
}

Status ColumnReader::new_bitmap_index_iterator(BitmapIndexIterator** iterator) {
    RETURN_IF_ERROR(_bitmap_index.reader->new_iterator(iterator));
    return Status::OK();
}

Status ColumnReader::read_page(const ColumnIteratorOptions& iter_opts, const PagePointer& pp, PageHandle* handle,
                               Slice* page_body, PageFooterPB* footer) {
    iter_opts.sanity_check();
    PageReadOptions opts;
    opts.rblock = iter_opts.rblock;
    opts.page_pointer = pp;
    opts.codec = _compress_codec;
    opts.stats = iter_opts.stats;
    opts.verify_checksum = _opts.verify_checksum;
    opts.use_page_cache = iter_opts.use_page_cache;
    opts.kept_in_memory = _opts.kept_in_memory;

    return PageIO::read_and_decompress_page(opts, handle, page_body, footer);
}

Status ColumnReader::get_row_ranges_by_zone_map(CondColumn* cond_column, CondColumn* delete_condition,
                                                std::unordered_set<uint32_t>* delete_partial_filtered_pages,
                                                RowRanges* row_ranges) {
    std::vector<uint32_t> page_indexes;
    RETURN_IF_ERROR(_get_filtered_pages(cond_column, delete_condition, delete_partial_filtered_pages, &page_indexes));
    RETURN_IF_ERROR(_calculate_row_ranges(page_indexes, row_ranges));
    return Status::OK();
}

bool ColumnReader::match_condition(CondColumn* cond) const {
    if (_segment_zone_map == nullptr || cond == nullptr) {
        return true;
    }
    std::unique_ptr<WrapperField> min_value(WrapperField::create_by_type(delegate_type(_column_type), _column_length));
    std::unique_ptr<WrapperField> max_value(WrapperField::create_by_type(delegate_type(_column_type), _column_length));
    _parse_zone_map(*_segment_zone_map, min_value.get(), max_value.get());
    return _zone_map_match_condition(*_segment_zone_map, min_value.get(), max_value.get(), cond);
}

void ColumnReader::_parse_zone_map(const ZoneMapPB& zone_map, WrapperField* min_value_container,
                                   WrapperField* max_value_container) {
    // min value and max value are valid if has_not_null is true
    if (zone_map.has_not_null()) {
        min_value_container->from_string(zone_map.min());
        max_value_container->from_string(zone_map.max());
    }
    // for compatible original Cond eval logic
    // TODO(hkp): optimize OlapCond
    if (zone_map.has_null()) {
        // for compatible, if exist null, original logic treat null as min
        min_value_container->set_null();
        if (!zone_map.has_not_null()) {
            // for compatible OlapCond's 'is not null'
            max_value_container->set_null();
        }
    }
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
    detail->num_rows = num_rows();
    return Status::OK();
}

bool ColumnReader::_zone_map_match_condition(const ZoneMapPB& zone_map, WrapperField* min_value_container,
                                             WrapperField* max_value_container, CondColumn* cond) {
    if (!zone_map.has_not_null() && !zone_map.has_null()) {
        return false; // no data in this zone
    }

    if (cond == nullptr) {
        return true;
    }

    return cond->eval({min_value_container, max_value_container});
}

Status ColumnReader::_get_filtered_pages(CondColumn* cond_column, CondColumn* delete_condition,
                                         std::unordered_set<uint32_t>* delete_partial_filtered_pages,
                                         std::vector<uint32_t>* page_indexes) {
    const std::vector<ZoneMapPB>& zone_maps = _zone_map_index.reader->page_zone_maps();
    int32_t page_size = _zone_map_index.reader->num_pages();
    std::unique_ptr<WrapperField> min_value(WrapperField::create_by_type(delegate_type(_column_type), _column_length));
    std::unique_ptr<WrapperField> max_value(WrapperField::create_by_type(delegate_type(_column_type), _column_length));
    for (int32_t i = 0; i < page_size; ++i) {
        _parse_zone_map(zone_maps[i], min_value.get(), max_value.get());
        if (_zone_map_match_condition(zone_maps[i], min_value.get(), max_value.get(), cond_column)) {
            bool should_read = true;
            if (delete_condition != nullptr) {
                int state = delete_condition->del_eval({min_value.get(), max_value.get()});
                if (state == DEL_SATISFIED) {
                    should_read = false;
                } else if (state == DEL_PARTIAL_SATISFIED) {
                    delete_partial_filtered_pages->insert(i);
                }
            }
            if (should_read) {
                page_indexes->push_back(i);
            }
        }
    }
    return Status::OK();
}

Status ColumnReader::_calculate_row_ranges(const std::vector<uint32_t>& page_indexes, RowRanges* row_ranges) {
    row_ranges->clear();
    for (auto i : page_indexes) {
        ordinal_t page_first_id = _ordinal_index.reader->get_first_ordinal(i);
        ordinal_t page_last_id = _ordinal_index.reader->get_last_ordinal(i);
        RowRanges page_row_ranges(
                RowRanges::create_single(static_cast<int64_t>(page_first_id), static_cast<int64_t>(page_last_id + 1)));
        RowRanges::ranges_union(*row_ranges, page_row_ranges, row_ranges);
    }
    return Status::OK();
}

Status ColumnReader::_calculate_row_ranges(const std::vector<uint32_t>& page_indexes,
                                           vectorized::SparseRange* row_ranges) {
    for (auto i : page_indexes) {
        ordinal_t page_first_id = _ordinal_index.reader->get_first_ordinal(i);
        ordinal_t page_last_id = _ordinal_index.reader->get_last_ordinal(i);
        row_ranges->add({static_cast<rowid_t>(page_first_id), static_cast<rowid_t>(page_last_id + 1)});
    }
    return Status::OK();
}

Status ColumnReader::get_row_ranges_by_bloom_filter(CondColumn* cond_column, RowRanges* row_ranges) {
    RowRanges bf_row_ranges;
    std::unique_ptr<BloomFilterIndexIterator> bf_iter;
    RETURN_IF_ERROR(_bloom_filter_index.reader->new_iterator(&bf_iter));
    size_t range_size = row_ranges->range_size();
    // get covered page ids
    std::set<int32_t> page_ids;
    for (int i = 0; i < range_size; ++i) {
        int64_t from = row_ranges->get_range_from(i);
        int64_t idx = from;
        int64_t to = row_ranges->get_range_to(i);
        auto iter = _ordinal_index.reader->seek_at_or_before(from);
        while (idx < to) {
            page_ids.insert(iter.page_index());
            idx = static_cast<int>(iter.last_ordinal() + 1);
            iter.next();
        }
    }
    for (const auto& pid : page_ids) {
        std::unique_ptr<BloomFilter> bf;
        RETURN_IF_ERROR(bf_iter->read_bloom_filter(pid, &bf));
        if (cond_column->eval(bf.get())) {
            bf_row_ranges.add(RowRange(static_cast<int64_t>(_ordinal_index.reader->get_first_ordinal(pid)),
                                       static_cast<int64_t>(_ordinal_index.reader->get_last_ordinal(pid) + 1)));
        }
    }
    RowRanges::ranges_intersection(*row_ranges, bf_row_ranges, row_ranges);
    return Status::OK();
}

// prerequisite: at least one predicate in |predicates| support bloom filter.
Status ColumnReader::bloom_filter(const std::vector<const vectorized::ColumnPredicate*>& predicates,
                                  vectorized::SparseRange* row_ranges) {
    vectorized::SparseRange bf_row_ranges;
    std::unique_ptr<BloomFilterIndexIterator> bf_iter;
    RETURN_IF_ERROR(_bloom_filter_index.reader->new_iterator(&bf_iter));
    size_t range_size = row_ranges->size();
    // get covered page ids
    std::set<int32_t> page_ids;
    for (int i = 0; i < range_size; ++i) {
        vectorized::Range r = (*row_ranges)[i];
        int64_t idx = r.begin();
        auto iter = _ordinal_index.reader->seek_at_or_before(r.begin());
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
                bf_row_ranges.add(vectorized::Range(_ordinal_index.reader->get_first_ordinal(pid),
                                                    _ordinal_index.reader->get_last_ordinal(pid) + 1));
            }
        }
    }
    *row_ranges = row_ranges->intersection(bf_row_ranges);
    return Status::OK();
}

Status ColumnReader::_load_ordinal_index(bool use_page_cache, bool kept_in_memory) {
    Status st;
    if (_flags[kHasOrdinalIndexMetaPos]) {
        std::unique_ptr<OrdinalIndexPB> index_meta(_ordinal_index.meta);
        _flags.set(kHasOrdinalIndexMetaPos, false);
        _ordinal_index.reader = new OrdinalIndexReader();
        _flags.set(kHasOrdinalIndexReaderPos, true);
        st = _ordinal_index.reader->load(_opts.block_mgr, _file_name, index_meta.get(), _num_rows, use_page_cache,
                                         kept_in_memory);
    }
    return st;
}

Status ColumnReader::_load_zone_map_index(bool use_page_cache, bool kept_in_memory) {
    Status st;
    if (_flags[kHasZoneMapIndexMetaPos]) {
        std::unique_ptr<ZoneMapIndexPB> index_meta(_zone_map_index.meta);
        _flags.set(kHasZoneMapIndexMetaPos, false);
        _zone_map_index.reader = new ZoneMapIndexReader();
        _flags.set(kHasZoneMapIndexReaderPos, true);
        st = _zone_map_index.reader->load(_opts.block_mgr, _file_name, index_meta.get(), use_page_cache,
                                          kept_in_memory);
    }
    return st;
}

Status ColumnReader::_load_bitmap_index(bool use_page_cache, bool kept_in_memory) {
    Status st;
    if (_flags[kHasBitmapIndexMetaPos]) {
        std::unique_ptr<BitmapIndexPB> index_meta(_bitmap_index.meta);
        _flags.set(kHasBitmapIndexMetaPos, false);
        _bitmap_index.reader = new BitmapIndexReader();
        _flags.set(kHasBitmapIndexReaderPos, true);
        st = _bitmap_index.reader->load(_opts.block_mgr, _file_name, index_meta.get(), use_page_cache, kept_in_memory);
    }
    return st;
}

Status ColumnReader::_load_bloom_filter_index(bool use_page_cache, bool kept_in_memory) {
    Status st;
    if (_flags[kHasBloomFilterIndexMetaPos]) {
        std::unique_ptr<BloomFilterIndexPB> index_meta(_bloom_filter_index.meta);
        _flags.set(kHasBloomFilterIndexMetaPos, false);
        _bloom_filter_index.reader = new BloomFilterIndexReader();
        _flags.set(kHasBloomFilterIndexReaderPos, true);
        st = _bloom_filter_index.reader->load(_opts.block_mgr, _file_name, index_meta.get(), use_page_cache,
                                              kept_in_memory);
    }
    return st;
}

Status ColumnReader::seek_to_first(OrdinalPageIndexIterator* iter) {
    *iter = _ordinal_index.reader->begin();
    if (!iter->valid()) {
        return Status::NotFound("Failed to seek to first rowid");
    }
    return Status::OK();
}

Status ColumnReader::seek_at_or_before(ordinal_t ordinal, OrdinalPageIndexIterator* iter) {
    *iter = _ordinal_index.reader->seek_at_or_before(ordinal);
    if (!iter->valid()) {
        return Status::NotFound(fmt::format("Failed to seek to ordinal {}", ordinal));
    }
    return Status::OK();
}

Status ColumnReader::zone_map_filter(const std::vector<const vectorized::ColumnPredicate*>& predicates,
                                     const vectorized::ColumnPredicate* del_predicate,
                                     std::unordered_set<uint32_t>* del_partial_filtered_pages,
                                     vectorized::SparseRange* row_ranges) {
    std::vector<uint32_t> page_indexes;
    RETURN_IF_ERROR(_zone_map_filter(predicates, del_predicate, del_partial_filtered_pages, &page_indexes));
    RETURN_IF_ERROR(_calculate_row_ranges(page_indexes, row_ranges));
    return Status::OK();
}

Status ColumnReader::_zone_map_filter(const std::vector<const vectorized::ColumnPredicate*>& predicates,
                                      const vectorized::ColumnPredicate* del_predicate,
                                      std::unordered_set<uint32_t>* del_partial_filtered_pages,
                                      std::vector<uint32_t>* pages) {
    const std::vector<ZoneMapPB>& zone_maps = _zone_map_index.reader->page_zone_maps();
    int32_t page_size = _zone_map_index.reader->num_pages();
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

Status ColumnReader::ensure_index_loaded(ReaderType reader_type) {
    Status status = _load_ordinal_index_once.call([this] {
        bool use_page_cache = !config::disable_storage_page_cache;
        RETURN_IF_ERROR(_load_ordinal_index(use_page_cache, _opts.kept_in_memory));
        return Status::OK();
    });
    RETURN_IF_ERROR(status);

    if (is_query(reader_type)) {
        status = _load_indices_once.call([this] {
            // ZoneMap, Bitmap, BloomFilter is only necessary for query.
            bool use_page_cache = !config::disable_storage_page_cache;
            RETURN_IF_ERROR(_load_zone_map_index(use_page_cache, _opts.kept_in_memory));
            RETURN_IF_ERROR(_load_bitmap_index(use_page_cache, _opts.kept_in_memory));
            RETURN_IF_ERROR(_load_bloom_filter_index(use_page_cache, _opts.kept_in_memory));
            return Status::OK();
        });
    }
    return status;
}

} // namespace starrocks::segment_v2
