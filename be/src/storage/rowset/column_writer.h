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
//   https://github.com/apache/incubator-doris/blob/master/be/src/olap/rowset/segment_v2/column_writer.h

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

#include <memory> // for unique_ptr

#include "column/vectorized_fwd.h"
#include "common/status.h"      // for Status
#include "gen_cpp/segment.pb.h" // for EncodingTypePB
#include "gutil/strings/substitute.h"
#include "runtime/global_dict/types.h"
#include "storage/inverted/inverted_writer.h"
#include "storage/rowset/binary_dict_page.h"
#include "storage/rowset/common.h"
#include "storage/rowset/page_pointer.h" // for PagePointer
#include "storage/tablet_schema.h"       // for TabletColumn
#include "util/bitmap.h"                 // for BitmapChange
#include "util/slice.h"                  // for OwnedSlice

namespace starrocks {

class TypeInfo;
class BlockCompressionCodec;
class WritableFile;

class Column;

struct ColumnWriterOptions {
    // input and output parameter:
    // - input: column_id/unique_id/type/length/encoding/compression/is_nullable members
    // - output: encoding/indexes/dict_page members
    ColumnMetaPB* meta;
    uint32_t data_page_size = OLAP_PAGE_SIZE;
    uint32_t page_format = 2;
    // store compressed page only when space saving is above the threshold.
    // space saving = 1 - compressed_size / uncompressed_size
    double compression_min_space_saving = 0.1;
    bool need_zone_map = false;
    bool need_bitmap_index = false;
    bool need_bloom_filter = false;
    bool need_inverted_index = false;
    std::unordered_map<IndexType, std::string> standalone_index_file_paths;
    std::unordered_map<IndexType, TabletIndex> tablet_index;

    // for char/varchar will speculate encoding in append
    // for others will decide encoding in init method
    bool need_speculate_encoding = false;

    // when column data is encoding by dict
    // if global_dict is not nullptr, will checkout whether global_dict can cover all data
    GlobalDictMap* global_dict = nullptr;
};

class BitmapIndexWriter;
class EncodingInfo;
class NullMapRLEBuilder;
class NullFlagsBuilder;
class OrdinalIndexWriter;
class PageBuilder;
class BloomFilterIndexWriter;
class ZoneMapIndexWriter;

class ColumnWriter {
public:
    static StatusOr<std::unique_ptr<ColumnWriter>> create(const ColumnWriterOptions& opts, const TabletColumn* column,
                                                          WritableFile* wfile);

    explicit ColumnWriter(TypeInfoPtr type_info, int length, bool is_nullable)
            : _type_info(std::move(type_info)), _length(length), _is_nullable(is_nullable) {}

    virtual ~ColumnWriter() = default;

    virtual Status init() = 0;

    virtual Status append(const Column& column) = 0;

    virtual Status finish_current_page() = 0;

    virtual uint64_t estimate_buffer_size() = 0;

    // finish append data
    virtual Status finish() = 0;

    // write all data into file
    virtual Status write_data() = 0;

    virtual Status write_ordinal_index() = 0;

    virtual Status write_zone_map() = 0;

    virtual Status write_bitmap_index() = 0;

    virtual Status write_bloom_filter_index() = 0;

    virtual Status write_inverted_index() = 0;

    virtual ordinal_t get_next_rowid() const = 0;

    // only invalid in the case of global_dict is not nullptr
    // column is not encoding by dict or append new words that
    // not in global_dict, it will return false
    // return false if type is not string column
    virtual bool is_global_dict_valid() { return false; }

    TypeInfo* type_info() const { return _type_info.get(); }
    int length() const { return _length; }
    bool is_nullable() const { return _is_nullable; }

    virtual uint64_t total_mem_footprint() const = 0;

protected:
    TypeInfoPtr _type_info;
    // NOTE: only used for CHAR/VARCHAR type.
    int _length;
    bool _is_nullable;
};

// Encode one column's data into some memory slice.
// Because some columns would be stored in a file, we should wait
// until all columns has been finished, and then data can be written
// to file
class ScalarColumnWriter final : public ColumnWriter {
public:
    ScalarColumnWriter(const ColumnWriterOptions& opts, TypeInfoPtr type_info, WritableFile* output_file);

    ~ScalarColumnWriter() override;

    Status init() override;

    Status append(const Column& column) override;

    // Write offset column, it's only used in ArrayColumn
    Status append_array_offsets(const Column& column);

    // rebuild char/varchar encoding when _page_builder is empty
    Status set_encoding(const EncodingTypePB& encoding);

    Status finish_current_page() override;

    uint64_t estimate_buffer_size() override;

    // finish append data
    Status finish() override;

    Status write_data() override;
    Status write_ordinal_index() override;
    Status write_zone_map() override;
    Status write_bitmap_index() override;
    Status write_bloom_filter_index() override;

    // TODO: Implement inverted index
    Status write_inverted_index() override;

    ordinal_t get_next_rowid() const override { return _next_rowid; }

    bool is_global_dict_valid() override { return _is_global_dict_valid; }

    uint64_t total_mem_footprint() const override { return _total_mem_footprint; }

private:
    // All Pages will be organized into a linked list
    struct Page {
        // the data vector may contain:
        //     1. one OwnedSlice if the page body is compressed
        //     2. one OwnedSlice if the page body is not compressed and doesn't have nullmap
        //     3. two OwnedSlice if the page body is not compressed and has nullmap
        // use vector for easier management for lifetime of OwnedSlice
        std::vector<OwnedSlice> data;
        PageFooterPB footer;
        Page* next = nullptr;
    };

    struct PageHead {
        Page* head = nullptr;
        Page* tail = nullptr;
    };

    void _push_back_page(Page* page) {
        // add page to pages' tail
        if (_pages.tail != nullptr) {
            _pages.tail->next = page;
        }
        _pages.tail = page;
        if (_pages.head == nullptr) {
            _pages.head = page;
        }
        for (auto& data_slice : page->data) {
            _data_size += data_slice.slice().size;
        }
        // estimate (page footer + footer size + checksum) took 20 bytes
        _data_size += 20;
    }

    Status append(const uint8_t* data, const uint8_t* null_flags, size_t count, bool has_null);

    Status _write_data_page(Page* page);

    ColumnWriterOptions _opts;
    WritableFile* _wfile;
    uint32_t _curr_page_format;
    // total size of data page list
    uint64_t _data_size;

    // cached generated pages,
    PageHead _pages;
    ordinal_t _first_rowid = 0;
    ordinal_t _next_rowid = 0;

    const BlockCompressionCodec* _compress_codec = nullptr;
    const EncodingInfo* _encoding_info = nullptr;

    std::unique_ptr<PageBuilder> _page_builder;

    // Used when _opts.page_format == 1, using Run-Length encoding to build the null map.
    std::unique_ptr<NullMapRLEBuilder> _null_map_builder_v1;

    // Used when _opts.page_format == 2, using bitshuffle encoding to build the null map.
    // Used when _opts.page_format == 3, using lz4 encoding to build the null map.
    std::unique_ptr<NullFlagsBuilder> _null_map_builder_v2;

    std::unique_ptr<OrdinalIndexWriter> _ordinal_index_builder;
    std::unique_ptr<ZoneMapIndexWriter> _zone_map_index_builder;
    std::unique_ptr<BitmapIndexWriter> _bitmap_index_builder;
    std::unique_ptr<BloomFilterIndexWriter> _bloom_filter_index_builder;
    std::unique_ptr<InvertedWriter> _inverted_index_builder;

    // _zone_map_index_builder != NULL || _bitmap_index_builder != NULL || _bloom_filter_index_builder != NULL
    bool _has_index_builder = false;
    int64_t _element_ordinal = 0;
    int64_t _previous_ordinal = 0;
    bool _has_inverted_builder = false;

    bool _is_global_dict_valid = true;

    uint64_t _total_mem_footprint = 0;
};

} // namespace starrocks
