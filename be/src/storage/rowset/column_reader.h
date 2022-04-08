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
#include <bitset>
#include <cstddef> // for size_t
#include <cstdint> // for uint32_t
#include <memory>  // for unique_ptr
#include <utility>

#include "column/datum.h"
#include "column/fixed_length_column.h"
#include "column/vectorized_fwd.h"
#include "common/statusor.h"    // for Status
#include "gen_cpp/segment.pb.h" // for ColumnMetaPB
#include "runtime/mem_pool.h"
#include "storage/fs/fs_util.h"
#include "storage/rowset/bitmap_index_reader.h"
#include "storage/rowset/bloom_filter_index_reader.h"
#include "storage/rowset/common.h"
#include "storage/rowset/ordinal_page_index.h" // for OrdinalPageIndexIterator
#include "storage/rowset/page_handle.h"
#include "storage/rowset/segment.h"
#include "storage/rowset/zone_map_index.h"
#include "storage/vectorized/range.h"
#include "util/once.h"

namespace starrocks {

class BlockCompressionCodec;
class ColumnBlock;
class ColumnBlockView;
class ColumnVectorBatch;
class MemTracker;
class WrapperField;

namespace vectorized {
class ColumnPredicate;
class Column;
class ZoneMapDetail;
} // namespace vectorized

class BitmapIndexIterator;
class BitmapIndexReader;
class ColumnIterator;
class ColumnIteratorOptions;
class EncodingInfo;
class PageDecoder;
class PagePointer;
class ParsedPage;
class ZoneMapIndexPB;
class ZoneMapPB;
class Segment;

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
    static StatusOr<std::unique_ptr<ColumnReader>> create(ColumnMetaPB* meta, const Segment* segment);

    ColumnReader(const private_type&, const Segment* segment);

    ~ColumnReader();

    // create a new column iterator. Caller should free the returned iterator after unused.
    // TODO: StatusOr<std::unique_ptr<ColumnIterator>> new_iterator()
    Status new_iterator(ColumnIterator** iterator);

    // Caller should free returned iterator after unused.
    // TODO: StatusOr<std::unique_ptr<ColumnIterator>> new_bitmap_index_iterator()
    Status new_bitmap_index_iterator(BitmapIndexIterator** iterator);

    // Seek to the first entry in the column.
    Status seek_to_first(OrdinalPageIndexIterator* iter);
    Status seek_at_or_before(ordinal_t ordinal, OrdinalPageIndexIterator* iter);

    // read a page from file into a page handle
    Status read_page(const ColumnIteratorOptions& iter_opts, const PagePointer& pp, PageHandle* handle,
                     Slice* page_body, PageFooterPB* footer);

    bool is_nullable() const { return _flags[kIsNullablePos]; }

    const EncodingInfo* encoding_info() const { return _encoding_info; }

    bool has_zone_map() const { return _flags[kHasZoneMapIndexMetaPos] || _flags[kHasZoneMapIndexReaderPos]; }
    bool has_bitmap_index() const { return _flags[kHasBitmapIndexMetaPos] || _flags[kHasBitmapIndexReaderPos]; }
    bool has_bloom_filter_index() const {
        return _flags[kHasBloomFilterIndexMetaPos] || _flags[kHasBloomFilterIndexReaderPos];
    }

    ZoneMapPB* segment_zone_map() const { return _segment_zone_map.get(); }

    PagePointer get_dict_page_pointer() const { return _dict_page_pointer; }
    FieldType column_type() const { return _column_type; }
    bool has_all_dict_encoded() const { return _flags[kHasAllDictEncodedPos]; }
    bool all_dict_encoded() const { return _flags[kAllDictEncodedPos]; }

    uint64_t total_mem_footprint() const { return _total_mem_footprint; }

    int32_t num_data_pages() { return _ordinal_index.reader ? _ordinal_index.reader->num_data_pages() : 0; }

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

    Status load_ordinal_index_once();

    uint32_t num_rows() const { return _segment->num_rows(); }

    // this function is just used for unit test
    uint32_t num_rows_from_meta_pb(const ColumnMetaPB* meta) const { return meta->num_rows(); }

private:
    const std::string& file_name() const { return _segment->file_name(); }

    MemTracker* mem_tracker() const { return _segment->mem_tracker(); }

    fs::BlockManager* block_manager() const { return _segment->block_manager(); }

    bool keep_in_memory() const { return _segment->keep_in_memory(); }

    struct private_type {
        private_type(int) {}
    };

    template <typename Meta, typename Reader>
    union ColumnIndex {
        Meta* meta;
        Reader* reader;
    };

    constexpr static size_t kHasZoneMapIndexMetaPos = 0;
    constexpr static size_t kHasZoneMapIndexReaderPos = 1;
    constexpr static size_t kHasOrdinalIndexMetaPos = 2;
    constexpr static size_t kHasOrdinalIndexReaderPos = 3;
    constexpr static size_t kHasBitmapIndexMetaPos = 4;
    constexpr static size_t kHasBitmapIndexReaderPos = 5;
    constexpr static size_t kHasBloomFilterIndexMetaPos = 6;
    constexpr static size_t kHasBloomFilterIndexReaderPos = 7;
    constexpr static size_t kIsNullablePos = 8;
    constexpr static size_t kHasAllDictEncodedPos = 9;
    constexpr static size_t kAllDictEncodedPos = 10;

    // Disable copy and assignment
    ColumnReader(const ColumnReader&) = delete;
    void operator=(const ColumnReader&) = delete;
    // Disable move copy and move assignment
    ColumnReader(ColumnReader&&) = delete;
    void operator=(ColumnReader&&) = delete;

    Status _init(ColumnMetaPB* meta);

    Status _load_zone_map_index_once();
    Status _load_bitmap_index_once();
    Status _load_bloom_filter_index_once();

    Status _load_zone_map_index(bool use_page_cache, bool kept_in_memory);
    Status _load_ordinal_index(bool use_page_cache, bool kept_in_memory);
    Status _load_bitmap_index(bool use_page_cache, bool kept_in_memory);
    Status _load_bloom_filter_index(bool use_page_cache, bool kept_in_memory);

    static void _parse_zone_map(const ZoneMapPB& zone_map, WrapperField* min_value_container,
                                WrapperField* max_value_container);

    Status _parse_zone_map(const ZoneMapPB& zm, vectorized::ZoneMapDetail* detail) const;

    Status _calculate_row_ranges(const std::vector<uint32_t>& page_indexes, vectorized::SparseRange* row_ranges);

    Status _zone_map_filter(const std::vector<const vectorized::ColumnPredicate*>& predicates,
                            const vectorized::ColumnPredicate* del_predicate,
                            std::unordered_set<uint32_t>* del_partial_filtered_pages, std::vector<uint32_t>* pages);

    // ColumnReader will be resident in memory. When there are many columns in the table,
    // the meta in ColumnReader takes up a lot of memory,
    // and now the content that is not needed in Meta is not saved to ColumnReader
    FieldType _column_type = OLAP_FIELD_TYPE_UNKNOWN;
    PagePointer _dict_page_pointer;
    uint64_t _total_mem_footprint = 0;

    // initialized in init(), used for create PageDecoder
    const EncodingInfo* _encoding_info = nullptr;
    const BlockCompressionCodec* _compress_codec = nullptr; // initialized in init()

    ColumnIndex<ZoneMapIndexPB, ZoneMapIndexReader> _zone_map_index;
    ColumnIndex<OrdinalIndexPB, OrdinalIndexReader> _ordinal_index;
    ColumnIndex<BitmapIndexPB, BitmapIndexReader> _bitmap_index;
    ColumnIndex<BloomFilterIndexPB, BloomFilterIndexReader> _bloom_filter_index;

    std::unique_ptr<ZoneMapPB> _segment_zone_map;

    using SubReaderList = std::vector<std::unique_ptr<ColumnReader>>;
    std::unique_ptr<SubReaderList> _sub_readers;

    // The read operation comprise of compaction, query, checksum and so on.
    // The ordinal index must be loaded before read operation.
    // zonemap, bitmap, bloomfilter is only necessary for query.
    // the other operations can not load these indices.
    StarRocksCallOnce<Status> _ordinal_index_once;
    StarRocksCallOnce<Status> _zonemap_index_once;
    StarRocksCallOnce<Status> _bitmap_index_once;
    StarRocksCallOnce<Status> _bloomfilter_index_once;

    // Pointer to its father segment, as the column reader
    // is never released before the end of the parent's life cycle,
    // so here we just use a normal pointer
    const Segment* _segment = nullptr;

    std::bitset<16> _flags;
};

} // namespace starrocks
