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

#include <column/datum_convert.h>

#include <memory>
#include <utility>

#include "column/array_column.h"
#include "column/column.h"
#include "column/column_helper.h"
#include "column/datum.h"
#include "column/nullable_column.h"
#include "common/logging.h"
#include "gutil/casts.h"
#include "gutil/strings/substitute.h" // for Substitute
#include "storage/column_block.h"     // for ColumnBlockView
#include "storage/olap_cond.h"
#include "storage/rowset/segment_v2/binary_dict_page.h" // for BinaryDictPageDecoder
#include "storage/rowset/segment_v2/bitmap_index_reader.h"
#include "storage/rowset/segment_v2/bloom_filter_index_reader.h"
#include "storage/rowset/segment_v2/encoding_info.h" // for EncodingInfo
#include "storage/rowset/segment_v2/page_handle.h"   // for PageHandle
#include "storage/rowset/segment_v2/page_io.h"
#include "storage/rowset/segment_v2/page_pointer.h" // for PagePointer
#include "storage/rowset/segment_v2/zone_map_index.h"
#include "storage/types.h" // for TypeInfo
#include "storage/vectorized/column_predicate.h"
#include "util/block_compression.h"
#include "util/rle_encoding.h" // for RleDecoder

namespace starrocks::segment_v2 {

using strings::Substitute;

Status ColumnReader::create(MemTracker* mem_tracker, const ColumnReaderOptions& opts, const ColumnMetaPB& meta,
                            uint64_t num_rows, const std::string& file_name, std::unique_ptr<ColumnReader>* reader) {
    auto type = static_cast<FieldType>(meta.type());
    if (is_scalar_type(delegate_type(type))) {
        std::unique_ptr<ColumnReader> reader_local(new ColumnReader(mem_tracker, opts, meta, num_rows, file_name));
        RETURN_IF_ERROR(reader_local->init(meta));
        *reader = std::move(reader_local);
        return Status::OK();
    }

    if (type == FieldType::OLAP_FIELD_TYPE_ARRAY) {
        std::unique_ptr<ColumnReader> array_reader(new ColumnReader(mem_tracker, opts, meta, num_rows, file_name));

        size_t col = 0;
        std::unique_ptr<ColumnReader> element_reader;
        RETURN_IF_ERROR(ColumnReader::create(mem_tracker, opts, meta.children_columns(col),
                                             meta.children_columns(col).num_rows(), file_name, &element_reader));
        RETURN_IF_ERROR(element_reader->init(meta.children_columns(col)));
        col++;
        array_reader->_sub_readers.emplace_back(std::move(element_reader));

        if (array_reader->is_nullable()) {
            std::unique_ptr<ColumnReader> null_reader;
            RETURN_IF_ERROR(ColumnReader::create(mem_tracker, opts, meta.children_columns(col),
                                                 meta.children_columns(col).num_rows(), file_name, &null_reader));
            RETURN_IF_ERROR(null_reader->init(meta.children_columns(col)));
            col++;
            array_reader->_sub_readers.emplace_back(std::move(null_reader));
        }

        std::unique_ptr<ColumnReader> array_size_reader;
        RETURN_IF_ERROR(ColumnReader::create(mem_tracker, opts, meta.children_columns(col),
                                             meta.children_columns(col).num_rows(), file_name, &array_size_reader));
        RETURN_IF_ERROR(array_size_reader->init(meta.children_columns(col)));
        array_reader->_sub_readers.emplace_back(std::move(array_size_reader));

        RETURN_IF_ERROR(array_reader->init(meta));
        *reader = std::move(array_reader);
        return Status::OK();
    }

    return Status::NotSupported("unsupported type for ColumnReader: " + std::to_string(type));
}

ColumnReader::ColumnReader(MemTracker* mem_tracker, const ColumnReaderOptions& opts, const ColumnMetaPB& meta,
                           uint64_t num_rows, const std::string& file_name)
        : _mem_tracker(mem_tracker),
          _column_length(meta.length()),
          _column_type(static_cast<FieldType>(meta.type())),
          _is_nullable(meta.is_nullable()),
          _dict_page_pointer(meta.dict_page()),
          _has_all_dict_encoded(meta.has_all_dict_encoded()),
          _all_dict_encoded(meta.all_dict_encoded()),
          _opts(opts),
          _num_rows(num_rows),
          _file_name(file_name) {
    _mem_tracker->consume(sizeof(ColumnReader));
}

Status ColumnReader::init(const ColumnMetaPB& meta) {
    if (_column_type == OLAP_FIELD_TYPE_ARRAY) {
        return Status::OK();
    }

    RETURN_IF_ERROR(EncodingInfo::get(delegate_type(_column_type), meta.encoding(), &_encoding_info));
    RETURN_IF_ERROR(get_block_compression_codec(meta.compression(), &_compress_codec));

    for (int i = 0; i < meta.indexes_size(); i++) {
        const auto& index_meta = meta.indexes(i);
        switch (index_meta.type()) {
        case ORDINAL_INDEX:
            _ordinal_index_meta = &index_meta.ordinal_index();
            break;
        case ZONE_MAP_INDEX:
            _zone_map_index_meta = &index_meta.zone_map_index();
            break;
        case BITMAP_INDEX:
            _bitmap_index_meta = &index_meta.bitmap_index();
            break;
        case BLOOM_FILTER_INDEX:
            _bf_index_meta = &index_meta.bloom_filter_index();
            break;
        default:
            return Status::Corruption(
                    strings::Substitute("Bad file $0: invalid column index type $1", _file_name, index_meta.type()));
        }
    }
    if (_ordinal_index_meta == nullptr) {
        return Status::Corruption(
                strings::Substitute("Bad file $0: missing ordinal index for column $1", _file_name, meta.column_id()));
    }
    return Status::OK();
}

Status ColumnReader::new_bitmap_index_iterator(BitmapIndexIterator** iterator) {
    RETURN_IF_ERROR(_bitmap_index->new_iterator(iterator));
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
    if (_zone_map_index_meta == nullptr || cond == nullptr) {
        return true;
    }
    std::unique_ptr<WrapperField> min_value(WrapperField::create_by_type(delegate_type(_column_type), _column_length));
    std::unique_ptr<WrapperField> max_value(WrapperField::create_by_type(delegate_type(_column_type), _column_length));
    _parse_zone_map(_zone_map_index_meta->segment_zone_map(), min_value.get(), max_value.get());
    return _zone_map_match_condition(_zone_map_index_meta->segment_zone_map(), min_value.get(), max_value.get(), cond);
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

Status ColumnReader::_parse_zone_map(const ZoneMapPB& zm, vectorized::Datum* min, vectorized::Datum* max) const {
    // DECIMAL32/DECIMAL64/DECIMAL128 stored as INT32/INT64/INT128
    // The DECIMAL type will be delegated to INT type.
    TypeInfoPtr type_info = get_type_info(delegate_type(_column_type));
    if (!zm.has_null()) {
        RETURN_IF_ERROR(vectorized::datum_from_string(type_info.get(), min, zm.min(), nullptr));
    }
    if (zm.has_not_null()) {
        RETURN_IF_ERROR(vectorized::datum_from_string(type_info.get(), max, zm.max(), nullptr));
    }
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
    const std::vector<ZoneMapPB>& zone_maps = _zone_map_index->page_zone_maps();
    int32_t page_size = _zone_map_index->num_pages();
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
        ordinal_t page_first_id = _ordinal_index->get_first_ordinal(i);
        ordinal_t page_last_id = _ordinal_index->get_last_ordinal(i);
        RowRanges page_row_ranges(RowRanges::create_single(page_first_id, page_last_id + 1));
        RowRanges::ranges_union(*row_ranges, page_row_ranges, row_ranges);
    }
    return Status::OK();
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

Status ColumnReader::get_row_ranges_by_bloom_filter(CondColumn* cond_column, RowRanges* row_ranges) {
    RowRanges bf_row_ranges;
    std::unique_ptr<BloomFilterIndexIterator> bf_iter;
    RETURN_IF_ERROR(_bloom_filter_index->new_iterator(&bf_iter));
    size_t range_size = row_ranges->range_size();
    // get covered page ids
    std::set<int32_t> page_ids;
    for (int i = 0; i < range_size; ++i) {
        int64_t from = row_ranges->get_range_from(i);
        int64_t idx = from;
        int64_t to = row_ranges->get_range_to(i);
        auto iter = _ordinal_index->seek_at_or_before(from);
        while (idx < to) {
            page_ids.insert(iter.page_index());
            idx = iter.last_ordinal() + 1;
            iter.next();
        }
    }
    for (const auto& pid : page_ids) {
        std::unique_ptr<BloomFilter> bf;
        RETURN_IF_ERROR(bf_iter->read_bloom_filter(pid, &bf));
        if (cond_column->eval(bf.get())) {
            bf_row_ranges.add(
                    RowRange(_ordinal_index->get_first_ordinal(pid), _ordinal_index->get_last_ordinal(pid) + 1));
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
            idx = iter.last_ordinal() + 1;
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

Status ColumnReader::_load_ordinal_index(bool use_page_cache, bool kept_in_memory) {
    DCHECK(_ordinal_index_meta != nullptr);
    _ordinal_index = std::make_unique<OrdinalIndexReader>();
    Status status = _ordinal_index->load(_opts.block_mgr, _file_name, _ordinal_index_meta, _num_rows, use_page_cache,
                                         kept_in_memory);
    _mem_tracker->consume(_ordinal_index->mem_usage());
    return Status::OK();
}

Status ColumnReader::_load_zone_map_index(bool use_page_cache, bool kept_in_memory) {
    if (_zone_map_index_meta != nullptr) {
        _zone_map_index = std::make_unique<ZoneMapIndexReader>();
        Status status = _zone_map_index->load(_opts.block_mgr, _file_name, _zone_map_index_meta, use_page_cache,
                                              kept_in_memory);
        _mem_tracker->consume(_zone_map_index->mem_usage());
        return status;
    }
    return Status::OK();
}

Status ColumnReader::_load_bitmap_index(bool use_page_cache, bool kept_in_memory) {
    if (_bitmap_index_meta != nullptr) {
        _bitmap_index = std::make_unique<BitmapIndexReader>();
        Status status =
                _bitmap_index->load(_opts.block_mgr, _file_name, _bitmap_index_meta, use_page_cache, kept_in_memory);
        _mem_tracker->consume(_bitmap_index->mem_usage());
        return status;
    }
    return Status::OK();
}

Status ColumnReader::_load_bloom_filter_index(bool use_page_cache, bool kept_in_memory) {
    if (_bf_index_meta != nullptr) {
        _bloom_filter_index = std::make_unique<BloomFilterIndexReader>();
        Status status =
                _bloom_filter_index->load(_opts.block_mgr, _file_name, _bf_index_meta, use_page_cache, kept_in_memory);
        _mem_tracker->consume(_bloom_filter_index->mem_usage());
        return status;
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
        return Status::NotFound(strings::Substitute("Failed to seek to ordinal $0, ", ordinal));
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
    const std::vector<ZoneMapPB>& zone_maps = _zone_map_index->page_zone_maps();
    int32_t page_size = _zone_map_index->num_pages();
    for (int32_t i = 0; i < page_size; ++i) {
        vectorized::Datum min;
        vectorized::Datum max;
        const ZoneMapPB& zm = zone_maps[i];
        _parse_zone_map(zm, &min, &max);
        bool matched = true;
        for (const auto* predicate : predicates) {
            if (!predicate->zone_map_filter(min, max)) {
                matched = false;
                break;
            }
        }
        if (!matched) {
            continue;
        }
        pages->emplace_back(i);

        if (del_predicate && del_predicate->zone_map_filter(min, max)) {
            del_partial_filtered_pages->emplace(i);
        }
    }
    return Status::OK();
}

bool ColumnReader::segment_zone_map_filter(const std::vector<const vectorized::ColumnPredicate*>& predicates) const {
    if (_zone_map_index_meta == nullptr) {
        return true;
    }
    const ZoneMapPB& zm = _zone_map_index_meta->segment_zone_map();
    vectorized::Datum min;
    vectorized::Datum max;
    _parse_zone_map(zm, &min, &max);
    auto filter = [&](const vectorized::ColumnPredicate* pred) { return pred->zone_map_filter(min, max); };
    return std::all_of(predicates.begin(), predicates.end(), filter);
}

Status ColumnReader::new_iterator(ColumnIterator** iterator) {
    if (is_scalar_type(delegate_type(_column_type))) {
        *iterator = new FileColumnIterator(this);
        return Status::OK();
    } else {
        switch (_column_type) {
        case FieldType::OLAP_FIELD_TYPE_ARRAY: {
            size_t col = 0;
            ColumnIterator* element_iterator;
            RETURN_IF_ERROR(_sub_readers[col++]->new_iterator(&element_iterator));

            ColumnIterator* null_iterator = nullptr;
            if (is_nullable()) {
                RETURN_IF_ERROR(_sub_readers[col++]->new_iterator(&null_iterator));
            }

            ColumnIterator* array_size_iterator;
            RETURN_IF_ERROR(_sub_readers[col]->new_iterator(&array_size_iterator));

            *iterator = new ArrayFileColumnIterator(null_iterator, array_size_iterator, element_iterator);
            return Status::OK();
        }
        default:
            return Status::NotSupported("unsupported type to create iterator: " + std::to_string(_column_type));
        }
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

////////////////////////////////////////////////////////////////////////////////

ArrayFileColumnIterator::ArrayFileColumnIterator(ColumnIterator* null_iterator, ColumnIterator* array_size_iterator,
                                                 ColumnIterator* element_iterator) {
    if (null_iterator != nullptr) {
        _null_iterator.reset(down_cast<FileColumnIterator*>(null_iterator));
    }
    _array_size_iterator.reset(down_cast<FileColumnIterator*>(array_size_iterator));
    _element_iterator.reset(element_iterator);
}

Status ArrayFileColumnIterator::init(const ColumnIteratorOptions& opts) {
    if (_null_iterator != nullptr) {
        RETURN_IF_ERROR(_null_iterator->init(opts));
    }
    RETURN_IF_ERROR(_array_size_iterator->init(opts));
    RETURN_IF_ERROR(_element_iterator->init(opts));

    const TypeInfoPtr& null_type = get_type_info(FieldType::OLAP_FIELD_TYPE_TINYINT);
    RETURN_IF_ERROR(ColumnVectorBatch::create(opts.chunk_size, true, null_type, nullptr, &_null_batch));

    const TypeInfoPtr& array_size_type = get_type_info(FieldType::OLAP_FIELD_TYPE_INT);
    RETURN_IF_ERROR(ColumnVectorBatch::create(opts.chunk_size, false, array_size_type, nullptr, &_array_size_batch));
    return Status::OK();
}

// every time invoke this method, _array_size_batch will be modified, so this method is not thread safe.
Status ArrayFileColumnIterator::next_batch(size_t* n, ColumnBlockView* dst, bool* has_null) {
    ColumnBlock* array_block = dst->column_block();
    auto* array_batch = reinterpret_cast<ArrayColumnVectorBatch*>(array_block->vector_batch());

    // 1. Read null column
    if (_null_iterator != nullptr) {
        _null_batch->resize(*n);
        ColumnBlock null_block(_null_batch.get(), nullptr);
        ColumnBlockView null_view(&null_block);
        RETURN_IF_ERROR(_null_iterator->next_batch(n, &null_view, has_null));
        uint8_t* null_signs = array_batch->null_signs();
        memcpy(null_signs, _null_batch->data(), sizeof(uint8_t) * *n);
    }

    // 2. read offsets into _array_size_batch
    _array_size_batch->resize(*n);
    ColumnBlock ordinal_block(_array_size_batch.get(), nullptr);
    ColumnBlockView ordinal_view(&ordinal_block);
    bool array_size_null = false;
    RETURN_IF_ERROR(_array_size_iterator->next_batch(n, &ordinal_view, &array_size_null));

    auto* offsets = array_batch->offsets();

    size_t prev_array_size = dst->current_offset();
    size_t end_offset = (*offsets)[prev_array_size];
    size_t num_to_read = end_offset;

    auto* array_size = reinterpret_cast<uint32_t*>(_array_size_batch->data());
    for (size_t i = 0; i < *n; ++i) {
        end_offset += array_size[i];
        (*offsets)[prev_array_size + i + 1] = static_cast<uint32_t>(end_offset);
    }
    num_to_read = end_offset - num_to_read;

    // 3. Read elements
    ColumnVectorBatch* element_vector_batch = array_batch->elements();
    element_vector_batch->resize(num_to_read);
    ColumnBlock element_block = ColumnBlock(element_vector_batch, dst->pool());
    ColumnBlockView element_view(&element_block);
    bool element_null = false;
    RETURN_IF_ERROR(_element_iterator->next_batch(&num_to_read, &element_view, &element_null));

    array_batch->prepare_for_read(prev_array_size, prev_array_size + *n);

    return Status::OK();
}

Status ArrayFileColumnIterator::next_batch(size_t* n, vectorized::Column* dst) {
    vectorized::ArrayColumn* array_column = nullptr;
    vectorized::NullColumn* null_column = nullptr;
    if (dst->is_nullable()) {
        auto* nullable_column = down_cast<vectorized::NullableColumn*>(dst);

        array_column = down_cast<vectorized::ArrayColumn*>(nullable_column->data_column().get());
        null_column = down_cast<vectorized::NullColumn*>(nullable_column->null_column().get());
    } else {
        array_column = down_cast<vectorized::ArrayColumn*>(dst);
    }

    // 1. Read null column
    if (_null_iterator != nullptr) {
        RETURN_IF_ERROR(_null_iterator->next_batch(n, null_column));
        down_cast<vectorized::NullableColumn*>(dst)->update_has_null();
    }

    // 2. Read offset column
    // [1, 2, 3], [4, 5, 6]
    // In memory, it will be transformed to actual offset(0, 3, 6)
    // On disk, offset is stored as length array(3, 3)
    auto* offsets = array_column->offsets_column().get();
    auto& data = offsets->get_data();
    size_t end_offset = data.back();

    size_t prev_array_size = offsets->size();
    RETURN_IF_ERROR(_array_size_iterator->next_batch(n, offsets));
    size_t curr_array_size = offsets->size();

    size_t num_to_read = end_offset;
    for (size_t i = prev_array_size; i < curr_array_size; ++i) {
        end_offset += data[i];
        data[i] = end_offset;
    }
    num_to_read = end_offset - num_to_read;

    // 3. Read elements
    RETURN_IF_ERROR(_element_iterator->next_batch(&num_to_read, array_column->elements_column().get()));

    return Status::OK();
}

Status ArrayFileColumnIterator::fetch_values_by_rowid(const rowid_t* rowids, size_t size, vectorized::Column* values) {
    vectorized::ArrayColumn* array_column = nullptr;
    vectorized::NullColumn* null_column = nullptr;
    // 1. Read null column
    if (_null_iterator != nullptr) {
        auto* nullable_column = down_cast<vectorized::NullableColumn*>(values);
        array_column = down_cast<vectorized::ArrayColumn*>(nullable_column->data_column().get());
        null_column = down_cast<vectorized::NullColumn*>(nullable_column->null_column().get());
        RETURN_IF_ERROR(_null_iterator->fetch_values_by_rowid(rowids, size, null_column));
        nullable_column->update_has_null();
    } else {
        array_column = down_cast<vectorized::ArrayColumn*>(values);
    }

    // 2. Read offset column
    vectorized::UInt32Column array_size;
    array_size.reserve(size);
    RETURN_IF_ERROR(_array_size_iterator->fetch_values_by_rowid(rowids, size, &array_size));

    // [1, 2, 3], [4, 5, 6]
    // In memory, it will be transformed to actual offset(0, 3, 6)
    // On disk, offset is stored as length array(3, 3)
    auto* offsets = array_column->offsets_column().get();
    offsets->reserve(offsets->size() + array_size.size());
    size_t offset = offsets->get_data().back();
    for (size_t i = 0; i < array_size.size(); ++i) {
        offset += array_size.get_data()[i];
        offsets->append(offset);
    }

    // 3. Read elements
    for (size_t i = 0; i < size; ++i) {
        RETURN_IF_ERROR(_array_size_iterator->seek_to_ordinal_and_calc_element_ordinal(rowids[i]));
        size_t element_ordinal = _array_size_iterator->element_ordinal();
        RETURN_IF_ERROR(_element_iterator->seek_to_ordinal(element_ordinal));
        size_t size_to_read = array_size.get_data()[i];
        RETURN_IF_ERROR(_element_iterator->next_batch(&size_to_read, array_column->elements_column().get()));
    }
    return Status::OK();
}

////////////////////////////////////////////////////////////////////////////////

FileColumnIterator::FileColumnIterator(ColumnReader* reader) : _reader(reader) {}

FileColumnIterator::~FileColumnIterator() = default;

Status FileColumnIterator::init(const ColumnIteratorOptions& opts) {
    _opts = opts;
    RETURN_IF_ERROR(_reader->ensure_index_loaded(_opts.reader_type));

    if (_reader->encoding_info()->encoding() != DICT_ENCODING) {
        return Status::OK();
    }

    if (_reader->column_type() == OLAP_FIELD_TYPE_CHAR) {
        _init_dict_decoder_func = &FileColumnIterator::_do_init_dict_decoder<OLAP_FIELD_TYPE_CHAR>;
    } else if (_reader->column_type() == OLAP_FIELD_TYPE_VARCHAR) {
        _init_dict_decoder_func = &FileColumnIterator::_do_init_dict_decoder<OLAP_FIELD_TYPE_VARCHAR>;
    } else {
        return Status::NotSupported("dict encoding with unsupported field type");
    }

    if (opts.check_dict_encoding) {
        if (_reader->has_all_dict_encoded()) {
            _all_dict_encoded = _reader->all_dict_encoded();
            // if _all_dict_encoded is true, load dictionary page into memory for `dict_lookup`.
            RETURN_IF(!_all_dict_encoded, Status::OK());
            RETURN_IF_ERROR(_load_dict_page());
        } else if (_reader->num_rows() > 0) {
            // old version segment file dost not have `all_dict_encoded`, in order to check
            // whether all data pages are using dict encoding, must load the last data page
            // and check its encoding.
            ordinal_t last_row = _reader->num_rows() - 1;
            RETURN_IF_ERROR(seek_to_ordinal(last_row));
            _all_dict_encoded = _page->encoding_type() == DICT_ENCODING;
        }
    }

    if (_all_dict_encoded && _reader->column_type() == OLAP_FIELD_TYPE_CHAR) {
        _decode_dict_codes_func = &FileColumnIterator::_do_decode_dict_codes<OLAP_FIELD_TYPE_CHAR>;
        _dict_lookup_func = &FileColumnIterator::_do_dict_lookup<OLAP_FIELD_TYPE_CHAR>;
        _next_dict_codes_func = &FileColumnIterator::_do_next_dict_codes<OLAP_FIELD_TYPE_CHAR>;
        _fetch_all_dict_words_func = &FileColumnIterator::_fetch_all_dict_words<OLAP_FIELD_TYPE_CHAR>;
    } else if (_all_dict_encoded && _reader->column_type() == OLAP_FIELD_TYPE_VARCHAR) {
        _decode_dict_codes_func = &FileColumnIterator::_do_decode_dict_codes<OLAP_FIELD_TYPE_VARCHAR>;
        _dict_lookup_func = &FileColumnIterator::_do_dict_lookup<OLAP_FIELD_TYPE_VARCHAR>;
        _next_dict_codes_func = &FileColumnIterator::_do_next_dict_codes<OLAP_FIELD_TYPE_VARCHAR>;
        _fetch_all_dict_words_func = &FileColumnIterator::_fetch_all_dict_words<OLAP_FIELD_TYPE_VARCHAR>;
    }
    return Status::OK();
}

Status FileColumnIterator::seek_to_first() {
    RETURN_IF_ERROR(_reader->seek_to_first(&_page_iter));
    RETURN_IF_ERROR(_read_data_page(_page_iter));

    _seek_to_pos_in_page(_page.get(), 0);
    _current_ordinal = 0;
    return Status::OK();
}

Status FileColumnIterator::seek_to_ordinal(ordinal_t ord) {
    // if current page contains this row, we don't need to seek
    if (_page == nullptr || !_page->contains(ord)) {
        RETURN_IF_ERROR(_reader->seek_at_or_before(ord, &_page_iter));
        RETURN_IF_ERROR(_read_data_page(_page_iter));
    }
    _seek_to_pos_in_page(_page.get(), ord - _page->first_ordinal());
    _current_ordinal = ord;
    return Status::OK();
}

Status FileColumnIterator::seek_to_ordinal_and_calc_element_ordinal(ordinal_t ord) {
    // if current page contains this row, we don't need to seek
    if (_page == nullptr || !_page->contains(ord)) {
        RETURN_IF_ERROR(_reader->seek_at_or_before(ord, &_page_iter));
        RETURN_IF_ERROR(_read_data_page(_page_iter));
    }
    _array_size.resize(0);
    _element_ordinal = _page->corresponding_element_ordinal();
    _current_ordinal = _page->first_ordinal();
    _seek_to_pos_in_page(_page.get(), 0);
    size_t size_to_read = ord - _current_ordinal;
    RETURN_IF_ERROR(_page->read(&_array_size, &size_to_read));
    _current_ordinal += size_to_read;
    CHECK_EQ(ord, _current_ordinal);
    for (auto e : _array_size.get_data()) {
        _element_ordinal += e;
    }
    return Status::OK();
}

void FileColumnIterator::_seek_to_pos_in_page(ParsedPage* page, ordinal_t offset_in_page) {
    if (page->offset() == offset_in_page) {
        // fast path, do nothing
        return;
    }
    page->seek(offset_in_page);
}

Status FileColumnIterator::next_batch(size_t* n, ColumnBlockView* dst, bool* has_null) {
    size_t remaining = *n;
    bool contain_deleted_row = false;
    while (remaining > 0) {
        if (_page->remaining() == 0) {
            bool eos = false;
            RETURN_IF_ERROR(_load_next_page(&eos));
            if (eos) {
                break;
            }
        }

        contain_deleted_row |= _delete_partial_satisfied_pages.count(_page->page_index());
        // number of rows to be read from this page
        size_t nread = remaining;
        RETURN_IF_ERROR(_page->read(dst, &nread));
        _current_ordinal += nread;
        remaining -= nread;
    }
    dst->column_block()->set_delete_state(contain_deleted_row ? DEL_PARTIAL_SATISFIED : DEL_NOT_SATISFIED);

    *n -= remaining;
    // TODO(hkp): for string type, the bytes_read should be passed to page decoder
    // bytes_read = data size + null bitmap size
    _opts.stats->bytes_read += *n * dst->type_info()->size() + BitmapSize(*n);
    return Status::OK();
}

Status FileColumnIterator::next_batch(size_t* n, vectorized::Column* dst) {
    size_t remaining = *n;
    size_t prev_bytes = dst->byte_size();
    bool contain_deleted_row = (dst->delete_state() != DEL_NOT_SATISFIED);
    while (remaining > 0) {
        if (_page->remaining() == 0) {
            bool eos = false;
            RETURN_IF_ERROR(_load_next_page(&eos));
            if (eos) {
                break;
            }
        }

        contain_deleted_row = contain_deleted_row || _contains_deleted_row(_page->page_index());
        // number of rows to be read from this page
        size_t nread = remaining;
        RETURN_IF_ERROR(_page->read(dst, &nread));
        _current_ordinal += nread;
        remaining -= nread;
    }
    dst->set_delete_state(contain_deleted_row ? DEL_PARTIAL_SATISFIED : DEL_NOT_SATISFIED);
    *n -= remaining;
    _opts.stats->bytes_read += (dst->byte_size() - prev_bytes);
    return Status::OK();
}

Status FileColumnIterator::_load_next_page(bool* eos) {
    _page_iter.next();
    if (!_page_iter.valid()) {
        *eos = true;
        return Status::OK();
    }

    RETURN_IF_ERROR(_read_data_page(_page_iter));
    _seek_to_pos_in_page(_page.get(), 0);
    *eos = false;
    return Status::OK();
}

Status FileColumnIterator::_load_dict_page() {
    DCHECK(_dict_decoder == nullptr);
    // read dictionary page
    Slice dict_data;
    PageFooterPB dict_footer;
    RETURN_IF_ERROR(
            _reader->read_page(_opts, _reader->get_dict_page_pointer(), &_dict_page_handle, &dict_data, &dict_footer));
    // ignore dict_footer.dict_page_footer().encoding() due to only
    // PLAIN_ENCODING is supported for dict page right now
    if (_reader->column_type() == OLAP_FIELD_TYPE_CHAR) {
        _dict_decoder = std::make_unique<BinaryPlainPageDecoder<OLAP_FIELD_TYPE_CHAR>>(dict_data);
    } else {
        _dict_decoder = std::make_unique<BinaryPlainPageDecoder<OLAP_FIELD_TYPE_VARCHAR>>(dict_data);
    }
    return _dict_decoder->init();
}

template <FieldType Type>
Status FileColumnIterator::_do_init_dict_decoder() {
    static_assert(Type == OLAP_FIELD_TYPE_CHAR || Type == OLAP_FIELD_TYPE_VARCHAR);
    auto dict_page_decoder = down_cast<BinaryDictPageDecoder<Type>*>(_page->data_decoder());
    if (dict_page_decoder->encoding_type() == DICT_ENCODING) {
        if (_dict_decoder == nullptr) {
            RETURN_IF_ERROR(_load_dict_page());
        }
        dict_page_decoder->set_dict_decoder(_dict_decoder.get());
    }
    return Status::OK();
}

Status FileColumnIterator::_read_data_page(const OrdinalPageIndexIterator& iter) {
    PageHandle handle;
    Slice page_body;
    PageFooterPB footer;
    RETURN_IF_ERROR(_reader->read_page(_opts, iter.page(), &handle, &page_body, &footer));
    RETURN_IF_ERROR(parse_page(&_page, std::move(handle), page_body, footer.data_page_footer(),
                               _reader->encoding_info(), iter.page(), iter.page_index()));

    // dictionary page is read when the first data page that uses it is read,
    // this is to optimize the memory usage: when there is no query on one column, we could
    // release the memory of dictionary page.
    // note that concurrent iterators for the same column won't repeatedly read dictionary page
    // because of page cache.
    if (_init_dict_decoder_func != nullptr) {
        RETURN_IF_ERROR((this->*_init_dict_decoder_func)());
    }
    return Status::OK();
}

Status FileColumnIterator::get_row_ranges_by_zone_map(CondColumn* cond_column, CondColumn* delete_condition,
                                                      RowRanges* row_ranges) {
    if (_reader->has_zone_map()) {
        RETURN_IF_ERROR(_reader->get_row_ranges_by_zone_map(cond_column, delete_condition,
                                                            &_delete_partial_satisfied_pages, row_ranges));
    }
    return Status::OK();
}

Status FileColumnIterator::get_row_ranges_by_zone_map(const std::vector<const vectorized::ColumnPredicate*>& predicates,
                                                      const vectorized::ColumnPredicate* del_predicate,
                                                      vectorized::SparseRange* row_ranges) {
    DCHECK(row_ranges->empty());
    if (_reader->has_zone_map()) {
        RETURN_IF_ERROR(
                _reader->zone_map_filter(predicates, del_predicate, &_delete_partial_satisfied_pages, row_ranges));
    } else {
        row_ranges->add({0, static_cast<rowid_t>(_reader->num_rows())});
    }
    return Status::OK();
}

Status FileColumnIterator::get_row_ranges_by_bloom_filter(CondColumn* cond_column, RowRanges* row_ranges) {
    if (cond_column != nullptr && cond_column->can_do_bloom_filter() && _reader->has_bloom_filter_index()) {
        RETURN_IF_ERROR(_reader->get_row_ranges_by_bloom_filter(cond_column, row_ranges));
    }
    return Status::OK();
}

Status FileColumnIterator::get_row_ranges_by_bloom_filter(
        const std::vector<const vectorized::ColumnPredicate*>& predicates, vectorized::SparseRange* row_ranges) {
    RETURN_IF(!_reader->has_bloom_filter_index(), Status::OK());
    bool support = false;
    for (const auto* pred : predicates) {
        support = support | pred->support_bloom_filter();
    }
    RETURN_IF(!support, Status::OK());
    RETURN_IF_ERROR(_reader->bloom_filter(predicates, row_ranges));
    return Status::OK();
}

int FileColumnIterator::dict_lookup(const Slice& word) {
    DCHECK(all_page_dict_encoded());
    return (this->*_dict_lookup_func)(word);
}

Status FileColumnIterator::next_dict_codes(size_t* n, vectorized::Column* dst) {
    DCHECK(all_page_dict_encoded());
    return (this->*_next_dict_codes_func)(n, dst);
}

Status FileColumnIterator::decode_dict_codes(const int32_t* codes, size_t size, vectorized::Column* words) {
    DCHECK(all_page_dict_encoded());
    return (this->*_decode_dict_codes_func)(codes, size, words);
}

Status FileColumnIterator::fetch_all_dict_words(std::vector<Slice>* words) const {
    DCHECK(all_page_dict_encoded());
    return (this->*_fetch_all_dict_words_func)(words);
}

template <FieldType Type>
Status FileColumnIterator::_fetch_all_dict_words(std::vector<Slice>* words) const {
    auto dict = down_cast<BinaryPlainPageDecoder<Type>*>(_dict_decoder.get());
    size_t words_count = dict->count();
    words->reserve(words_count);
    for (size_t i = 0; i < words_count; i++) {
        if constexpr (Type != OLAP_FIELD_TYPE_CHAR) {
            words->emplace_back(dict->string_at_index(i));
        } else {
            Slice s = dict->string_at_index(i);
            s.size = strnlen(s.data, s.size);
            words->emplace_back(s);
        }
    }
    return Status::OK();
}

template <FieldType Type>
int FileColumnIterator::_do_dict_lookup(const Slice& word) {
    auto dict = down_cast<BinaryPlainPageDecoder<Type>*>(_dict_decoder.get());
    return dict->find(word);
}

template <FieldType Type>
Status FileColumnIterator::_do_next_dict_codes(size_t* n, vectorized::Column* dst) {
    size_t remaining = *n;
    bool contain_delted_row = false;
    while (remaining > 0) {
        if (_page->remaining() == 0) {
            bool eos = false;
            RETURN_IF_ERROR(_load_next_page(&eos));
            if (eos) {
                break;
            }
        }
        DCHECK(_page->encoding_type() == DICT_ENCODING);

        contain_delted_row = contain_delted_row || _contains_deleted_row(_page->page_index());
        // number of rows to be read from this page
        size_t nread = remaining;
        RETURN_IF_ERROR(_page->read_dict_codes(dst, &nread));
        _current_ordinal += nread;
        remaining -= nread;
        _opts.stats->bytes_read += nread * sizeof(int32_t);
    }
    dst->set_delete_state(contain_delted_row ? DEL_PARTIAL_SATISFIED : DEL_NOT_SATISFIED);
    *n -= remaining;
    return Status::OK();
}

template <FieldType Type>
Status FileColumnIterator::_do_decode_dict_codes(const int32_t* codes, size_t size, vectorized::Column* words) {
    auto dict = down_cast<BinaryPlainPageDecoder<Type>*>(_dict_decoder.get());
    std::vector<Slice> slices;
    slices.reserve(size);
    for (size_t i = 0; i < size; i++) {
        if (codes[i] >= 0) {
            if constexpr (Type != OLAP_FIELD_TYPE_CHAR) {
                slices.emplace_back(dict->string_at_index(codes[i]));
            } else {
                Slice s = dict->string_at_index(codes[i]);
                s.size = strnlen(s.data, s.size);
                slices.emplace_back(s);
            }
        } else {
            slices.emplace_back("");
        }
    }
    [[maybe_unused]] bool ok = words->append_strings(slices);
    DCHECK(ok);
    _opts.stats->bytes_read += words->byte_size() + BitmapSize(slices.size());
    return Status::OK();
}

Status FileColumnIterator::fetch_values_by_rowid(const rowid_t* rowids, size_t size, vectorized::Column* values) {
    DCHECK(std::is_sorted(rowids, rowids + size));
    RETURN_IF(size == 0, Status::OK());
    size_t prev_bytes = values->byte_size();
    const rowid_t* const end = rowids + size;
    bool contain_deleted_row = (values->delete_state() != DEL_NOT_SATISFIED);
    do {
        RETURN_IF_ERROR(seek_to_ordinal(*rowids));
        contain_deleted_row = contain_deleted_row || _contains_deleted_row(_page->page_index());
        auto last_rowid = implicit_cast<rowid_t>(_page->first_ordinal() + _page->num_rows());
        const rowid_t* next_page_rowid = std::lower_bound(rowids, end, last_rowid);
        while (rowids != next_page_rowid) {
            DCHECK_EQ(_current_ordinal, _page->first_ordinal() + _page->offset());
            rowid_t curr = *rowids;
            _current_ordinal = implicit_cast<ordinal_t>(curr);
            _page->seek(curr - _page->first_ordinal());
            const rowid_t* p = rowids + 1;
            while ((next_page_rowid != p) && (*p == curr + 1)) {
                curr = *p++;
            }
            size_t nread = p - rowids;
            RETURN_IF_ERROR(_page->read(values, &nread));
            _current_ordinal += nread;
            rowids = p;
        }
        DCHECK_EQ(_current_ordinal, _page->first_ordinal() + _page->offset());
    } while (rowids != end);
    values->set_delete_state(contain_deleted_row ? DEL_PARTIAL_SATISFIED : DEL_NOT_SATISFIED);
    _opts.stats->bytes_read += values->byte_size() - prev_bytes;
    DCHECK_EQ(_current_ordinal, _page->first_ordinal() + _page->offset());
    return Status::OK();
}

int FileColumnIterator::dict_size() {
    if (_reader->column_type() == OLAP_FIELD_TYPE_CHAR) {
        auto dict = down_cast<BinaryPlainPageDecoder<OLAP_FIELD_TYPE_CHAR>*>(_dict_decoder.get());
        return dict->dict_size();
    } else if (_reader->column_type() == OLAP_FIELD_TYPE_VARCHAR) {
        auto dict = down_cast<BinaryPlainPageDecoder<OLAP_FIELD_TYPE_VARCHAR>*>(_dict_decoder.get());
        return dict->dict_size();
    }
    __builtin_unreachable();
    return 0;
}

Status DefaultValueColumnIterator::init(const ColumnIteratorOptions& opts) {
    _opts = opts;
    // be consistent with segment v1
    // if _has_default_value, we should create default column iterator for this column, and
    // "NULL" is a special default value which means the default value is null.
    if (_has_default_value) {
        if (_default_value == "NULL") {
            DCHECK(_is_nullable);
            _is_default_value_null = true;
        } else {
            _type_size = _type_info->size();
            _mem_value = reinterpret_cast<void*>(_pool->allocate(_type_size));
            if (UNLIKELY(_mem_value == nullptr)) {
                return Status::InternalError("Mem usage has exceed the limit of BE");
            }
            OLAPStatus s = OLAP_SUCCESS;
            if (_type_info->type() == OLAP_FIELD_TYPE_CHAR) {
                int32_t length = _schema_length;
                char* string_buffer = reinterpret_cast<char*>(_pool->allocate(length));
                if (UNLIKELY(string_buffer == nullptr)) {
                    return Status::InternalError("Mem usage has exceed the limit of BE");
                }
                memset(string_buffer, 0, length);
                memory_copy(string_buffer, _default_value.c_str(), _default_value.length());
                (static_cast<Slice*>(_mem_value))->size = length;
                (static_cast<Slice*>(_mem_value))->data = string_buffer;
            } else if (_type_info->type() == OLAP_FIELD_TYPE_VARCHAR || _type_info->type() == OLAP_FIELD_TYPE_HLL ||
                       _type_info->type() == OLAP_FIELD_TYPE_OBJECT ||
                       _type_info->type() == OLAP_FIELD_TYPE_PERCENTILE) {
                int32_t length = _default_value.length();
                char* string_buffer = reinterpret_cast<char*>(_pool->allocate(length));
                if (UNLIKELY(string_buffer == nullptr)) {
                    return Status::InternalError("Mem usage has exceed the limit of BE");
                }
                memory_copy(string_buffer, _default_value.c_str(), length);
                (static_cast<Slice*>(_mem_value))->size = length;
                (static_cast<Slice*>(_mem_value))->data = string_buffer;
            } else if (_type_info->type() == OLAP_FIELD_TYPE_ARRAY) {
                // TODO llj for Array default value
                return Status::NotSupported("Array default type is unsupported");
            } else {
                s = _type_info->from_string(_mem_value, _default_value);
            }
            if (s != OLAP_SUCCESS) {
                return Status::InternalError(
                        strings::Substitute("get value of type from default value failed. status:$0", s));
            }
        }
    } else if (_is_nullable) {
        // if _has_default_value is false but _is_nullable is true, we should return null as default value.
        _is_default_value_null = true;
    } else {
        return Status::InternalError("invalid default value column for no default value and not nullable");
    }
    return Status::OK();
}

Status DefaultValueColumnIterator::next_batch(size_t* n, ColumnBlockView* dst, bool* has_null) {
    if (dst->is_nullable()) {
        dst->set_null_bits(*n, _is_default_value_null);
    }

    if (_is_default_value_null) {
        *has_null = true;
        dst->advance(*n);
    } else {
        *has_null = false;
        for (int i = 0; i < *n; ++i) {
            memcpy(dst->data(), _mem_value, _type_size);
            dst->advance(1);
        }
    }
    _current_rowid += *n;
    return Status::OK();
}

Status DefaultValueColumnIterator::next_batch(size_t* n, vectorized::Column* dst) {
    if (_is_default_value_null) {
        [[maybe_unused]] bool ok = dst->append_nulls(*n);
        _current_rowid += *n;
        DCHECK(ok) << "cannot append null to non-nullable column";
    } else {
        if (_type_info->type() == OLAP_FIELD_TYPE_OBJECT || _type_info->type() == OLAP_FIELD_TYPE_HLL ||
            _type_info->type() == OLAP_FIELD_TYPE_PERCENTILE) {
            std::vector<Slice> slices;
            slices.reserve(*n);
            for (size_t i = 0; i < *n; i++) {
                slices.emplace_back(*reinterpret_cast<const Slice*>(_mem_value));
            }
            dst->append_strings(slices);
        } else {
            dst->append_value_multiple_times(_mem_value, *n);
        }
        _current_rowid += *n;
    }
    return Status::OK();
}

Status DefaultValueColumnIterator::fetch_values_by_rowid(const rowid_t* rowids, size_t size,
                                                         vectorized::Column* values) {
    return next_batch(&size, values);
}

Status DefaultValueColumnIterator::get_row_ranges_by_zone_map(
        const std::vector<const vectorized::ColumnPredicate*>& predicates,
        const vectorized::ColumnPredicate* del_predicate, vectorized::SparseRange* row_ranges) {
    DCHECK(row_ranges->empty());
    // TODO
    row_ranges->add({0, static_cast<rowid_t>(_num_rows)});
    return Status::OK();
}

Status ColumnIterator::decode_dict_codes(const vectorized::Column& codes, vectorized::Column* words) {
    if (codes.is_nullable()) {
        const vectorized::ColumnPtr& data_column = down_cast<const vectorized::NullableColumn&>(codes).data_column();
        const vectorized::Buffer<int32_t>& v =
                std::static_pointer_cast<vectorized::Int32Column>(data_column)->get_data();
        return this->decode_dict_codes(v.data(), v.size(), words);
    } else {
        const vectorized::Buffer<int32_t>& v = down_cast<const vectorized::Int32Column&>(codes).get_data();
        return this->decode_dict_codes(v.data(), v.size(), words);
    }
}

Status ColumnIterator::fetch_values_by_rowid(const vectorized::Column& rowids, vectorized::Column* values) {
    static_assert(std::is_same_v<uint32_t, rowid_t>);
    const auto& numeric_col = down_cast<const vectorized::FixedLengthColumn<rowid_t>&>(rowids);
    const auto* p = reinterpret_cast<const rowid_t*>(numeric_col.get_data().data());
    return fetch_values_by_rowid(p, rowids.size(), values);
}

} // namespace starrocks::segment_v2
