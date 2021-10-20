// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/be/src/olap/rowset/segment_v2/column_reader.h

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

#pragma once

#include <algorithm>
#include <cstddef> // for size_t
#include <cstdint> // for uint32_t
#include <memory>  // for unique_ptr
#include <utility>

#include "column/datum.h"
#include "column/fixed_length_column.h"
#include "column/vectorized_fwd.h"
#include "common/status.h"         // for Status
#include "gen_cpp/segment_v2.pb.h" // for ColumnMetaPB
#include "runtime/mem_pool.h"
#include "runtime/mem_tracker.h"
#include "storage/fs/fs_util.h"
#include "storage/rowset/segment_v2/bitmap_index_reader.h"
#include "storage/rowset/segment_v2/bloom_filter_index_reader.h"
#include "storage/rowset/segment_v2/common.h"
#include "storage/rowset/segment_v2/ordinal_page_index.h" // for OrdinalPageIndexIterator
#include "storage/rowset/segment_v2/page_handle.h"
#include "storage/rowset/segment_v2/zone_map_index.h"
#include "storage/vectorized/range.h"
#include "util/once.h"

namespace starrocks {

class BlockCompressionCodec;
class ColumnBlock;
class ColumnBlockView;
class ColumnVectorBatch;
class CondColumn;
class TypeInfo;
using TypeInfoPtr = std::shared_ptr<TypeInfo>;
class WrapperField;

namespace fs {
class ReadableBlock;
}

namespace vectorized {
class ColumnPredicate;
class Column;
} // namespace vectorized

namespace segment_v2 {

class BitmapIndexIterator;
class BitmapIndexReader;
class ColumnIterator;
class EncodingInfo;
class PageDecoder;
class PagePointer;
class ParsedPage;
class RowRanges;
class ZoneMapIndexPB;
class ZoneMapPB;

struct ColumnReaderOptions {
    fs::BlockManager* block_mgr = fs::fs_util::block_manager();
    // version in SegmentFooterPB
    uint32_t storage_format_version = 1;
    // whether verify checksum when read page
    bool verify_checksum = true;
    // for in memory olap table, use DURABLE CachePriority in page cache
    bool kept_in_memory = false;
};

struct ColumnIteratorOptions {
    fs::ReadableBlock* rblock = nullptr;
    // reader statistics
    OlapReaderStatistics* stats = nullptr;
    bool use_page_cache = false;

    // check whether column pages are all dictionary encoding.
    bool check_dict_encoding = false;

    void sanity_check() const {
        CHECK_NOTNULL(rblock);
        CHECK_NOTNULL(stats);
    }

    // used to indicate that if this column is nullable, this flag can help ColumnIterator do
    // some optimization.
    bool is_nullable = true;

    ReaderType reader_type = READER_QUERY;
    int chunk_size = DEFAULT_CHUNK_SIZE;
};

// There will be concurrent users to read the same column. So
// we should do our best to reduce resource usage through share
// same information, such as OrdinalPageIndex and Page data.
// This will cache data shared by all reader
class ColumnReader {
public:
    // Create an initialized ColumnReader in *reader.
    // This should be a lightweight operation without I/O.
    static Status create(MemTracker* mem_tracker, const ColumnReaderOptions& opts, const ColumnMetaPB& meta,
                         uint64_t num_rows, const std::string& file_name, std::unique_ptr<ColumnReader>* reader);

    ~ColumnReader() = default;

    // create a new column iterator. Client should delete returned iterator
    Status new_iterator(ColumnIterator** iterator);
    // Client should delete returned iterator
    Status new_bitmap_index_iterator(BitmapIndexIterator** iterator);

    // Seek to the first entry in the column.
    Status seek_to_first(OrdinalPageIndexIterator* iter);
    Status seek_at_or_before(ordinal_t ordinal, OrdinalPageIndexIterator* iter);

    // read a page from file into a page handle
    Status read_page(const ColumnIteratorOptions& iter_opts, const PagePointer& pp, PageHandle* handle,
                     Slice* page_body, PageFooterPB* footer);

    bool is_nullable() const { return _is_nullable; }

    const EncodingInfo* encoding_info() const { return _encoding_info; }

    bool has_zone_map() const { return _zone_map_index_meta != nullptr; }
    bool has_bitmap_index() const { return _bitmap_index_meta != nullptr; }
    bool has_bloom_filter_index() const { return _bf_index_meta != nullptr; }

    // Check if this column could match `cond' using segment zone map.
    // Since segment zone map is stored in metadata, this function is fast without I/O.
    // Return true if segment zone map is absent or `cond' could be satisfied, false otherwise.
    bool match_condition(CondColumn* cond) const;

    // get row ranges with zone map
    // - cond_column is user's query predicate
    // - delete_condition is a delete predicate of one version
    Status get_row_ranges_by_zone_map(CondColumn* cond_column, CondColumn* delete_condition,
                                      std::unordered_set<uint32_t>* delete_partial_filtered_pages,
                                      RowRanges* row_ranges);

    // get row ranges with bloom filter index
    Status get_row_ranges_by_bloom_filter(CondColumn* cond_column, RowRanges* row_ranges);

    PagePointer get_dict_page_pointer() const { return _dict_page_pointer; }
    FieldType column_type() const { return _column_type; }
    bool has_all_dict_encoded() const { return _has_all_dict_encoded; }
    bool all_dict_encoded() const { return _all_dict_encoded; }

    size_t num_rows() const { return _num_rows; }

    ///-----------------------------------
    /// vectorized APIs
    ///-----------------------------------

    // page-level zone map filter.
    Status zone_map_filter(const std::vector<const ::starrocks::vectorized::ColumnPredicate*>& p,
                           const ::starrocks::vectorized::ColumnPredicate* del_predicate,
                           std::unordered_set<uint32_t>* del_partial_filtered_pages,
                           vectorized::SparseRange* row_ranges);

    // segment-level zone map filter.
    // Return false to filter out this segment.
    // same as `match_condition`, used by vector engine.
    bool segment_zone_map_filter(const std::vector<const ::starrocks::vectorized::ColumnPredicate*>& predicates) const;

    // prerequisite: at least one predicate in |predicates| support bloom filter.
    Status bloom_filter(const std::vector<const ::starrocks::vectorized::ColumnPredicate*>& p,
                        vectorized::SparseRange* ranges);

    uint32_t version() const { return _opts.storage_format_version; }

    // Read and load necessary column indexes into memory if it hasn't been loaded.
    // May be called multiple times, subsequent calls will no op.
    Status ensure_index_loaded(ReaderType reader_type);

private:
    ColumnReader(MemTracker* mem_tracker, const ColumnReaderOptions& opts, const ColumnMetaPB& meta, uint64_t num_rows,
                 const std::string& file_name);

    Status init(const ColumnMetaPB& meta);

    Status _load_zone_map_index(bool use_page_cache, bool kept_in_memory);
    Status _load_ordinal_index(bool use_page_cache, bool kept_in_memory);
    Status _load_bitmap_index(bool use_page_cache, bool kept_in_memory);
    Status _load_bloom_filter_index(bool use_page_cache, bool kept_in_memory);

    static bool _zone_map_match_condition(const ZoneMapPB& zone_map, WrapperField* min_value_container,
                                          WrapperField* max_value_container, CondColumn* cond);

    static void _parse_zone_map(const ZoneMapPB& zone_map, WrapperField* min_value_container,
                                WrapperField* max_value_container);

    Status _parse_zone_map(const ZoneMapPB& zm, vectorized::Datum* min, vectorized::Datum* max) const;

    Status _get_filtered_pages(CondColumn* cond_column, CondColumn* delete_conditions,
                               std::unordered_set<uint32_t>* delete_partial_filtered_pages,
                               std::vector<uint32_t>* page_indexes);

    Status _calculate_row_ranges(const std::vector<uint32_t>& page_indexes, RowRanges* row_ranges);

    Status _calculate_row_ranges(const std::vector<uint32_t>& page_indexes, vectorized::SparseRange* row_ranges);

    Status _zone_map_filter(const std::vector<const vectorized::ColumnPredicate*>& predicates,
                            const vectorized::ColumnPredicate* del_predicate,
                            std::unordered_set<uint32_t>* del_partial_filtered_pages, std::vector<uint32_t>* pages);

    MemTracker* _mem_tracker = nullptr;

    // ColumnReader will be resident in memory. When there are many columns in the table,
    // the meta in ColumnReader takes up a lot of memory,
    // and now the content that is not needed in Meta is not saved to ColumnReader
    int32_t _column_length = 0;
    FieldType _column_type = OLAP_FIELD_TYPE_UNKNOWN;
    bool _is_nullable = false;
    PagePointer _dict_page_pointer;
    bool _has_all_dict_encoded = false;
    bool _all_dict_encoded = false;
    ColumnReaderOptions _opts;
    uint64_t _num_rows;
    const std::string& _file_name;

    // initialized in init(), used for create PageDecoder
    const EncodingInfo* _encoding_info = nullptr;
    const BlockCompressionCodec* _compress_codec = nullptr; // initialized in init()

    // meta for various column indexes (null if the index is absent)
    const ZoneMapIndexPB* _zone_map_index_meta = nullptr;
    const OrdinalIndexPB* _ordinal_index_meta = nullptr;
    const BitmapIndexPB* _bitmap_index_meta = nullptr;
    const BloomFilterIndexPB* _bf_index_meta = nullptr;

    // The read operation comprise of compaction, query, checksum and so on.
    // The ordinal index must be loaded before read operation.
    // zonemap, bitmap, bloomfilter is only necessary for query.
    // the other operations can not load these indices.
    StarRocksCallOnce<Status> _load_ordinal_index_once;
    StarRocksCallOnce<Status> _load_indices_once;

    std::unique_ptr<ZoneMapIndexReader> _zone_map_index;
    std::unique_ptr<OrdinalIndexReader> _ordinal_index;
    std::unique_ptr<BitmapIndexReader> _bitmap_index;
    std::unique_ptr<BloomFilterIndexReader> _bloom_filter_index;

    std::vector<std::unique_ptr<ColumnReader>> _sub_readers;
};

// Base iterator to read one column data
class ColumnIterator {
public:
    ColumnIterator() = default;
    virtual ~ColumnIterator() = default;

    virtual Status init(const ColumnIteratorOptions& opts) {
        _opts = opts;
        return Status::OK();
    }

    // Seek to the first entry in the column.
    virtual Status seek_to_first() = 0;

    // Seek to the given ordinal entry in the column.
    // Entry 0 is the first entry written to the column.
    // If provided seek point is past the end of the file,
    // then returns false.
    virtual Status seek_to_ordinal(ordinal_t ord) = 0;

    Status next_batch(size_t* n, ColumnBlockView* dst) {
        bool has_null;
        return next_batch(n, dst, &has_null);
    }

    // After one seek, we can call this function many times to read data
    // into ColumnBlockView. when read string type data, memory will allocated
    // from MemPool
    virtual Status next_batch(size_t* n, ColumnBlockView* dst, bool* has_null) = 0;

    virtual Status next_batch(size_t* n, vectorized::Column* dst) = 0;

    virtual ordinal_t get_current_ordinal() const = 0;

    virtual Status get_row_ranges_by_zone_map(CondColumn* cond_column, CondColumn* delete_condition,
                                              RowRanges* row_ranges) {
        return Status::OK();
    }

    virtual Status get_row_ranges_by_bloom_filter(CondColumn* cond_column, RowRanges* row_ranges) {
        return Status::OK();
    }

    /// for vectorized engine
    virtual Status get_row_ranges_by_zone_map(const std::vector<const vectorized::ColumnPredicate*>& predicates,
                                              const vectorized::ColumnPredicate* del_predicate,
                                              vectorized::SparseRange* row_ranges) = 0;

    virtual Status get_row_ranges_by_bloom_filter(const std::vector<const vectorized::ColumnPredicate*>& predicates,
                                                  vectorized::SparseRange* row_ranges) {
        return Status::OK();
    }

    // return true iff all data pages of this column are encoded as dictionary encoding.
    // NOTE: the ColumnIterator must have been initialized with `check_dict_encoding`,
    // otherwise this method will always return false.
    virtual bool all_page_dict_encoded() const { return false; }

    // if all data page of this colum are encoded as dictionary encoding.
    // return all dictionary words that store in dict page
    virtual Status fetch_all_dict_words(std::vector<Slice>* words) const {
        return Status::NotSupported("Not Support dict.");
    }

    // return a non-negative dictionary code of |word| if it exist in this segment file,
    // otherwise -1 is returned.
    // NOTE: this method can be invoked only if `all_page_dict_encoded` returns true.
    virtual int dict_lookup(const Slice& word) { return -1; }

    // like `next_batch` but instead of return a batch of column values, this method returns a
    // batch of dictionary codes for dictionary encoded values.
    // this method can be invoked only if `all_page_dict_encoded` returns true.
    // type of |dst| must be `FixedLengthColumn<int32_t>` or `NullableColumn(FixedLengthColumn<int32_t>)`.
    virtual Status next_dict_codes(size_t* n, vectorized::Column* dst) { return Status::NotSupported(""); }

    // given a list of dictionary codes, fill |dst| column with the decoded values.
    // |codes| pointer to the array of dictionary codes.
    // |size| size of dictionary code array.
    // |words| column used to save the columns values, by append into it.
    virtual Status decode_dict_codes(const int32_t* codes, size_t size, vectorized::Column* words) {
        return Status::NotSupported("");
    }

    // same as `decode_dict_codes(const int32_t*, size_t, vectorized::Column*)` but extract
    // dictionary codes from the column |codes|.
    // |codes| must be of type `FixedLengthColumn<int32_t>` or `NullableColumn<FixedLengthColumn<int32_t>`
    // and assume no `null` value in |codes|.
    Status decode_dict_codes(const vectorized::Column& codes, vectorized::Column* words);

    // given a list of ordinals, fetch corresponding values.
    // |ordinals| must be ascending sorted.
    virtual Status fetch_values_by_rowid(const rowid_t* rowids, size_t size, vectorized::Column* values) {
        return Status::NotSupported("");
    }

    Status fetch_values_by_rowid(const vectorized::Column& rowids, vectorized::Column* values);

protected:
    ColumnIteratorOptions _opts;
};

// for scalar type
class FileColumnIterator final : public ColumnIterator {
public:
    explicit FileColumnIterator(ColumnReader* reader);
    ~FileColumnIterator() override;

    Status init(const ColumnIteratorOptions& opts) override;

    Status seek_to_first() override;

    Status seek_to_ordinal(ordinal_t ord) override;

    // This function used for array column.
    // Array column is made of offset and element.
    // This function seek to specified ordinal for offset column.
    // As well, calculate the element ordinal for element column.
    Status seek_to_ordinal_and_calc_element_ordinal(ordinal_t ord);

    Status next_batch(size_t* n, ColumnBlockView* dst, bool* has_null) override;

    Status next_batch(size_t* n, vectorized::Column* dst) override;

    ordinal_t get_current_ordinal() const override { return _current_ordinal; }

    // get row ranges by zone map
    // - cond_column is user's query predicate
    // - delete_condition is delete predicate of one version
    Status get_row_ranges_by_zone_map(CondColumn* cond_column, CondColumn* delete_condition,
                                      RowRanges* row_ranges) override;

    Status get_row_ranges_by_bloom_filter(CondColumn* cond_column, RowRanges* row_ranges) override;

    Status get_row_ranges_by_zone_map(const std::vector<const vectorized::ColumnPredicate*>& predicate,
                                      const vectorized::ColumnPredicate* del_predicate,
                                      vectorized::SparseRange* range) override;

    Status get_row_ranges_by_bloom_filter(const std::vector<const vectorized::ColumnPredicate*>& predicates,
                                          vectorized::SparseRange* range) override;

    bool all_page_dict_encoded() const override { return _all_dict_encoded; }

    Status fetch_all_dict_words(std::vector<Slice>* words) const override;

    int dict_lookup(const Slice& word) override;

    Status next_dict_codes(size_t* n, vectorized::Column* dst) override;

    Status decode_dict_codes(const int32_t* codes, size_t size, vectorized::Column* words) override;

    Status fetch_values_by_rowid(const rowid_t* rowids, size_t size, vectorized::Column* values) override;

    ParsedPage* get_current_page() { return _page.get(); }

    bool is_nullable() { return _reader->is_nullable(); }

    int64_t element_ordinal() const { return _element_ordinal; }

    // only work when all_page_dict_encoded was true.
    // used to acquire load local dict
    int dict_size();

private:
    static void _seek_to_pos_in_page(ParsedPage* page, ordinal_t offset_in_page);
    Status _load_next_page(bool* eos);
    Status _read_data_page(const OrdinalPageIndexIterator& iter);

    template <FieldType Type>
    int _do_dict_lookup(const Slice& word);

    template <FieldType Type>
    Status _do_next_dict_codes(size_t* n, vectorized::Column* dst);

    template <FieldType Type>
    Status _do_decode_dict_codes(const int32_t* codes, size_t size, vectorized::Column* words);

    template <FieldType Type>
    Status _do_init_dict_decoder();

    template <FieldType Type>
    Status _fetch_all_dict_words(std::vector<Slice>* words) const;

    Status _load_dict_page();

    bool _contains_deleted_row(uint32_t page_index) const {
        if (_reader->has_zone_map()) {
            return _delete_partial_satisfied_pages.count(page_index) > 0;
        }
        // if there is no zone map should be treated as DEL_PARTIAL_SATISFIED
        return true;
    }

    ColumnReader* _reader;

    // 1. The _page represents current page.
    // 2. We define an operation is one seek and following read,
    //    If new seek is issued, the _page will be reset.
    // 3. When _page is null, it means that this reader can not be read.
    std::unique_ptr<ParsedPage> _page;

    // keep dict page decoder
    std::unique_ptr<PageDecoder> _dict_decoder;

    // keep dict page handle to avoid released
    PageHandle _dict_page_handle;

    // page iterator used to get next page when current page is finished.
    // This value will be reset when a new seek is issued
    OrdinalPageIndexIterator _page_iter;

    // current value ordinal
    ordinal_t _current_ordinal = 0;

    // page indexes those are DEL_PARTIAL_SATISFIED
    std::unordered_set<uint32_t> _delete_partial_satisfied_pages;

    int (FileColumnIterator::*_dict_lookup_func)(const Slice&) = nullptr;
    Status (FileColumnIterator::*_next_dict_codes_func)(size_t* n, vectorized::Column* dst) = nullptr;
    Status (FileColumnIterator::*_decode_dict_codes_func)(const int32_t* codes, size_t size,
                                                          vectorized::Column* words) = nullptr;
    Status (FileColumnIterator::*_init_dict_decoder_func)() = nullptr;

    Status (FileColumnIterator::*_fetch_all_dict_words_func)(std::vector<Slice>* words) const = nullptr;

    // whether all data pages are dict-encoded.
    bool _all_dict_encoded = false;

    // variable used for array column(offset, element)
    // It's used to get element ordinal for specfied offset value.
    int64_t _element_ordinal = 0;

    vectorized::UInt32Column _array_size;
};

class ArrayFileColumnIterator final : public ColumnIterator {
public:
    explicit ArrayFileColumnIterator(ColumnIterator* null_iterator, ColumnIterator* array_size_iterator,
                                     ColumnIterator* item_iterator);

    ~ArrayFileColumnIterator() override = default;

    Status init(const ColumnIteratorOptions& opts) override;

    Status next_batch(size_t* n, ColumnBlockView* dst, bool* has_null) override;

    Status next_batch(size_t* n, vectorized::Column* dst) override;

    Status seek_to_first() override {
        if (_null_iterator != nullptr) {
            RETURN_IF_ERROR(_null_iterator->seek_to_first());
        }
        RETURN_IF_ERROR(_array_size_iterator->seek_to_first());
        RETURN_IF_ERROR(_element_iterator->seek_to_first());
        return Status::OK();
    }

    Status seek_to_ordinal(ordinal_t ord) override {
        if (_null_iterator != nullptr) {
            RETURN_IF_ERROR(_null_iterator->seek_to_ordinal(ord));
        }
        RETURN_IF_ERROR(_array_size_iterator->seek_to_ordinal_and_calc_element_ordinal(ord));
        size_t element_ordinal = _array_size_iterator->element_ordinal();
        RETURN_IF_ERROR(_element_iterator->seek_to_ordinal(element_ordinal));
        return Status::OK();
    }

    ordinal_t get_current_ordinal() const override { return _array_size_iterator->get_current_ordinal(); }

    /// for vectorized engine
    Status get_row_ranges_by_zone_map(const std::vector<const vectorized::ColumnPredicate*>& predicates,
                                      const vectorized::ColumnPredicate* del_predicate,
                                      vectorized::SparseRange* row_ranges) override {
        CHECK(false) << "array column does not has zone map index";
        return Status::OK();
    }

    Status fetch_values_by_rowid(const rowid_t* rowids, size_t size, vectorized::Column* values) override;

private:
    std::unique_ptr<FileColumnIterator> _null_iterator;
    std::unique_ptr<FileColumnIterator> _array_size_iterator;
    std::unique_ptr<ColumnIterator> _element_iterator;

    std::unique_ptr<ColumnVectorBatch> _null_batch;
    std::unique_ptr<ColumnVectorBatch> _array_size_batch;
};

// This iterator is used to read default value column
class DefaultValueColumnIterator final : public ColumnIterator {
public:
    DefaultValueColumnIterator(bool has_default_value, std::string default_value, bool is_nullable,
                               TypeInfoPtr type_info, size_t schema_length, ordinal_t num_rows)
            : _has_default_value(has_default_value),
              _default_value(std::move(default_value)),
              _is_nullable(is_nullable),
              _type_info(std::move(type_info)),
              _schema_length(schema_length),
              _is_default_value_null(false),
              _type_size(0),
              _pool(new MemPool(&_tracker)),
              _num_rows(num_rows) {}

    Status init(const ColumnIteratorOptions& opts) override;

    Status seek_to_first() override {
        _current_rowid = 0;
        return Status::OK();
    }

    Status seek_to_ordinal(ordinal_t ord_idx) override {
        _current_rowid = ord_idx;
        return Status::OK();
    }

    Status next_batch(size_t* n, ColumnBlockView* dst, bool* has_null) override;

    Status next_batch(size_t* n, vectorized::Column* dst) override;

    ordinal_t get_current_ordinal() const override { return _current_rowid; }

    Status get_row_ranges_by_zone_map(const std::vector<const vectorized::ColumnPredicate*>& predicates,
                                      const vectorized::ColumnPredicate* del_predicate,
                                      vectorized::SparseRange* row_ranges) override;

    bool all_page_dict_encoded() const override { return false; }

    int dict_lookup(const Slice& word) override { return -1; }

    Status next_dict_codes(size_t* n, vectorized::Column* dst) override {
        return Status::NotSupported("DefaultValueColumnIterator does not support");
    }

    Status decode_dict_codes(const int32_t* codes, size_t size, vectorized::Column* words) override {
        return Status::NotSupported("DefaultValueColumnIterator does not support");
    }

    Status fetch_values_by_rowid(const rowid_t* rowids, size_t size, vectorized::Column* values) override;

private:
    bool _has_default_value;
    std::string _default_value;
    bool _is_nullable;
    TypeInfoPtr _type_info;
    size_t _schema_length;
    bool _is_default_value_null;
    size_t _type_size;
    void* _mem_value = nullptr;
    MemTracker _tracker;
    std::unique_ptr<MemPool> _pool;

    // current rowid
    ordinal_t _current_rowid = 0;
    ordinal_t _num_rows = 0;
};

// To handle DictDecode
class ColumnDecoder {
public:
    ColumnDecoder() : _iter(nullptr) {}
    ColumnDecoder(ColumnIterator* iter) : _iter(iter) {}
    void set_iterator(ColumnIterator* iter) { _iter = iter; }
    Status decode_dict_codes(const vectorized::Column& codes, vectorized::Column* words) {
        DCHECK(_iter != nullptr);
        return _iter->decode_dict_codes(codes, words);
    }

private:
    ColumnIterator* _iter;
};

} // namespace segment_v2
} // namespace starrocks
