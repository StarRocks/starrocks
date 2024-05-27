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
#include <bitset>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <utility>

#include "column/datum.h"
#include "column/fixed_length_column.h"
#include "column/vectorized_fwd.h"
#include "common/statusor.h"
#include "gen_cpp/segment.pb.h"
#include "runtime/mem_pool.h"
#include "storage/inverted/inverted_index_iterator.h"
#include "storage/predicate_tree/predicate_tree_fwd.h"
#include "storage/range.h"
#include "storage/rowset/bitmap_index_reader.h"
#include "storage/rowset/bloom_filter_index_reader.h"
#include "storage/rowset/common.h"
#include "storage/rowset/ordinal_page_index.h"
#include "storage/rowset/page_handle.h"
#include "storage/rowset/segment.h"
#include "storage/rowset/zone_map_index.h"
#include "util/once.h"

namespace starrocks {

class BlockCompressionCodec;
class MemTracker;

class ColumnPredicate;
class Column;
class ZoneMapDetail;

class BitmapIndexIterator;
class BitmapIndexReader;
class ColumnIterator;
struct ColumnIteratorOptions;
class EncodingInfo;
class PageDecoder;
class PagePointer;
class ParsedPage;
class ZoneMapIndexPB;
class ZoneMapPB;
class Segment;
struct NgramBloomFilterReaderOptions;

// There will be concurrent users to read the same column. So
// we should do our best to reduce resource usage through share
// same information, such as OrdinalPageIndex and Page data.
// This will cache data shared by all reader
class ColumnReader {
    struct private_type;

public:
    // Create and initialize a ColumnReader.
    // This method will not take the ownership of |meta|.
    // Note that |meta| is mutable, this method may change its internal state.
    //
    // To developers: keep this method lightweight, should not incur any I/O.
    static StatusOr<std::unique_ptr<ColumnReader>> create(ColumnMetaPB* meta, Segment* segment);

    ColumnReader(const private_type&, Segment* segment);
    ~ColumnReader();

    ColumnReader(const ColumnReader&) = delete;
    void operator=(const ColumnReader&) = delete;
    ColumnReader(ColumnReader&&) = delete;
    void operator=(ColumnReader&&) = delete;

    // create a new column iterator.
    StatusOr<std::unique_ptr<ColumnIterator>> new_iterator(ColumnAccessPath* path = nullptr);

    // Caller should free returned iterator after unused.
    // TODO: StatusOr<std::unique_ptr<ColumnIterator>> new_bitmap_index_iterator()
    Status new_bitmap_index_iterator(const IndexReadOptions& opts, BitmapIndexIterator** iterator);

    // Seek to the first entry in the column.
    Status seek_to_first(OrdinalPageIndexIterator* iter);
    Status seek_at_or_before(ordinal_t ordinal, OrdinalPageIndexIterator* iter);
    Status seek_by_page_index(int page_index, OrdinalPageIndexIterator* iter);

    // read a page from file into a page handle
    Status read_page(const ColumnIteratorOptions& iter_opts, const PagePointer& pp, PageHandle* handle,
                     Slice* page_body, PageFooterPB* footer);

    bool is_nullable() const { return _flags & kIsNullableMask; }

    const EncodingInfo* encoding_info() const { return _encoding_info; }

    bool has_zone_map() const { return _zonemap_index != nullptr; }
    bool has_bitmap_index() const { return _bitmap_index != nullptr; }
    bool has_bloom_filter_index() const { return _bloom_filter_index != nullptr; }
    bool has_original_bloom_filter_index() const {
        return _bloom_filter_index != nullptr && (!_segment->tablet_schema().has_index(_column_unique_id, NGRAMBF));
    }
    bool has_ngram_bloom_filter_index() const {
        return _bloom_filter_index != nullptr && _segment->tablet_schema().has_index(_column_unique_id, NGRAMBF);
    }

    ZoneMapPB* segment_zone_map() const { return _segment_zone_map.get(); }

    PagePointer get_dict_page_pointer() const { return _dict_page_pointer; }
    LogicalType column_type() const { return _column_type; }
    bool has_all_dict_encoded() const { return _flags & kHasAllDictEncodedMask; }
    bool all_dict_encoded() const { return _flags & kAllDictEncodedMask; }

    uint64_t total_mem_footprint() const { return _total_mem_footprint; }

    int32_t num_data_pages() { return _ordinal_index ? _ordinal_index->num_data_pages() : 0; }

    // page-level zone map filter.

    Status zone_map_filter(const std::vector<const ::starrocks::ColumnPredicate*>& p,
                           const ::starrocks::ColumnPredicate* del_predicate,
                           std::unordered_set<uint32_t>* del_partial_filtered_pages, SparseRange<>* row_ranges,
                           const IndexReadOptions& opts, CompoundNodeType pred_relation);

    // segment-level zone map filter.
    // Return false to filter out this segment.
    // same as `match_condition`, used by vector engine.
    bool segment_zone_map_filter(const std::vector<const ::starrocks::ColumnPredicate*>& predicates) const;

    /// Treat the relationship between |predicates| as `(s_pred_1 OR s_pred_2 OR ... OR s_pred_n) AND (ns_pred_1 AND ns_pred_2 AND ... AND ns_pred_n)`,
    /// where s_pred_i denotes a predicate which supports bloom filter, and ns_pred_i denotes a predicate which does not support bloom filter.
    /// That is,
    /// - only keep the rows in |row_ranges| which satisfy any predicate that supports bloom filter in |predicates|.
    ///
    /// prerequisite:
    /// - if the original relationship between |predicates| is OR, all of them need to support bloom filter.
    /// - if the original relationship between |predicates| is AND, at least one of them need to support bloom filter.
    Status original_bloom_filter(const std::vector<const ::starrocks::ColumnPredicate*>& p, SparseRange<>* ranges,
                                 const IndexReadOptions& opts);

    Status ngram_bloom_filter(const std::vector<const ::starrocks::ColumnPredicate*>& p, SparseRange<>* ranges,
                              const IndexReadOptions& opts);

    Status load_ordinal_index(const IndexReadOptions& opts);

    Status new_inverted_index_iterator(const std::shared_ptr<TabletIndex>& index_meta, InvertedIndexIterator** iterator,
                                       const SegmentReadOptions& opts);

    uint32_t num_rows() const { return _segment->num_rows(); }

    void print_debug_info() { _ordinal_index->print_debug_info(); }

    size_t mem_usage() const;

    const std::string& name() const { return _name; }

    const std::vector<std::unique_ptr<ColumnReader>>* sub_readers() const { return _sub_readers.get(); }

private:
    const std::string& file_name() const { return _segment->file_name(); }
    template <bool is_original_bf>
    Status bloom_filter(const std::vector<const ColumnPredicate*>& predicates, SparseRange<>* row_ranges,
                        const IndexReadOptions& opts);
    struct private_type {
        explicit private_type(int) {}
    };

    constexpr static uint8_t kIsNullableMask = 1;
    constexpr static uint8_t kHasAllDictEncodedMask = 2;
    constexpr static uint8_t kAllDictEncodedMask = 4;

    Status _init(ColumnMetaPB* meta);

    Status _load_zonemap_index(const IndexReadOptions& opts);
    Status _load_bitmap_index(const IndexReadOptions& opts);
    Status _load_bloom_filter_index(const IndexReadOptions& opts);

    Status _parse_zone_map(LogicalType type, const ZoneMapPB& zm, ZoneMapDetail* detail) const;

    Status _calculate_row_ranges(const std::vector<uint32_t>& page_indexes, SparseRange<>* row_ranges);

    template <CompoundNodeType PredRelation>
    Status _zone_map_filter(const std::vector<const ColumnPredicate*>& predicates, const ColumnPredicate* del_predicate,
                            std::unordered_set<uint32_t>* del_partial_filtered_pages, std::vector<uint32_t>* pages);

    Status _load_inverted_index(const std::shared_ptr<TabletIndex>& index_meta, const SegmentReadOptions& opts);

    NgramBloomFilterReaderOptions _get_reader_options_for_ngram() const;

    bool _inverted_index_loaded() const { return invoked(_inverted_index_load_once); }

    // ColumnReader will be resident in memory. When there are many columns in the table,
    // the meta in ColumnReader takes up a lot of memory,
    // and now the content that is not needed in Meta is not saved to ColumnReader
    LogicalType _column_type = TYPE_UNKNOWN;
    LogicalType _column_child_type = TYPE_UNKNOWN;
    PagePointer _dict_page_pointer;
    uint64_t _total_mem_footprint = 0;
    uint32 _column_unique_id = std::numeric_limits<uint32_t>::max();

    // initialized in init(), used for create PageDecoder
    const EncodingInfo* _encoding_info = nullptr;
    const BlockCompressionCodec* _compress_codec = nullptr; // initialized in init()

    std::unique_ptr<ZoneMapIndexPB> _zonemap_index_meta;
    std::unique_ptr<OrdinalIndexPB> _ordinal_index_meta;
    std::unique_ptr<BitmapIndexPB> _bitmap_index_meta;
    std::unique_ptr<BloomFilterIndexPB> _bloom_filter_index_meta;

    std::unique_ptr<ZoneMapIndexReader> _zonemap_index;
    std::unique_ptr<OrdinalIndexReader> _ordinal_index;
    std::unique_ptr<BitmapIndexReader> _bitmap_index;
    std::unique_ptr<BloomFilterIndexReader> _bloom_filter_index;
    std::unique_ptr<InvertedReader> _inverted_index;

    std::unique_ptr<ZoneMapPB> _segment_zone_map;

    using SubReaderList = std::vector<std::unique_ptr<ColumnReader>>;
    std::unique_ptr<SubReaderList> _sub_readers;

    // Pointer to its father segment, as the column reader
    // is never released before the end of the parent's life cycle,
    // so here we just use a normal pointer
    Segment* _segment = nullptr;

    uint8_t _flags = 0;
    // counter to record the reader's mem usage, sub readers excluded.
    std::atomic<size_t> _meta_mem_usage = 0;

    // only for json flat column
    std::string _name;

    // only used for inverted index load
    OnceFlag _inverted_index_load_once;
};

} // namespace starrocks
