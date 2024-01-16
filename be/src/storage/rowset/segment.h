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
//   https://github.com/apache/incubator-doris/blob/master/be/src/olap/rowset/segment_v2/segment.h

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

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "common/statusor.h"
#include "fs/fs.h"
#include "gen_cpp/olap_file.pb.h"
#include "gen_cpp/segment.pb.h"
#include "gutil/macros.h"
#include "storage/delta_column_group.h"
#include "storage/rowset/page_handle.h"
#include "storage/rowset/page_pointer.h"
#include "storage/short_key_index.h"
#include "storage/tablet_schema.h"
#include "util/faststring.h"
#include "util/once.h"

namespace starrocks {

class ColumnAccessPath;
class TabletSchema;
class ShortKeyIndexDecoder;

class ChunkIterator;
class IndexReadOptions;
class Schema;
class SegmentIterator;
class SegmentReadOptions;

class BitmapIndexIterator;
class ColumnReader;
class ColumnIterator;
class Segment;
using SegmentSharedPtr = std::shared_ptr<Segment>;
using ChunkIteratorPtr = std::shared_ptr<ChunkIterator>;

// A Segment is used to represent a segment in memory format. When segment is
// generated, it won't be modified, so this struct aimed to help read operation.
// It will prepare all ColumnReader to create ColumnIterator as needed.
// And user can create a RowwiseIterator through new_iterator function.
//
// NOTE: This segment is used to a specified TabletSchema, when TabletSchema
// is changed, this segment can not be used any more. For example, after a schema
// change finished, client should disable all cached Segment for old TabletSchema.
class Segment : public std::enable_shared_from_this<Segment> {
public:
    // Like above but share the ownership of |unsafe_tablet_schema_ref|.
    static StatusOr<std::shared_ptr<Segment>> open(std::shared_ptr<FileSystem> fs, FileInfo segment_file_info,
                                                   uint32_t segment_id, TabletSchemaCSPtr tablet_schema,
                                                   size_t* footer_length_hint = nullptr,
                                                   const FooterPointerPB* partial_rowset_footer = nullptr,
                                                   bool skip_fill_local_cache = true,
                                                   lake::TabletManager* tablet_manager = nullptr);

    [[nodiscard]] static StatusOr<size_t> parse_segment_footer(RandomAccessFile* read_file, SegmentFooterPB* footer,
                                                               size_t* footer_length_hint,
                                                               const FooterPointerPB* partial_rowset_footer);

    [[nodiscard]] static Status write_segment_footer(WritableFile* write_file, const SegmentFooterPB& footer);

    Segment(std::shared_ptr<FileSystem> fs, FileInfo segment_file_info, uint32_t segment_id,
            TabletSchemaCSPtr tablet_schema, lake::TabletManager* tablet_manager);

    ~Segment();

    Status open(size_t* footer_length_hint, const FooterPointerPB* partial_rowset_footer, bool skip_fill_local_cache);

    // may return EndOfFile
    StatusOr<ChunkIteratorPtr> new_iterator(const Schema& schema, const SegmentReadOptions& read_options);

    StatusOr<std::shared_ptr<Segment>> new_dcg_segment(const DeltaColumnGroup& dcg, uint32_t idx,
                                                       const TabletSchemaCSPtr& read_tablet_schema);

    uint64_t id() const { return _segment_id; }

    // Creates a new iterator for a specific column in a segment.
    //
    // This function initializes a new iterator object that can be used to traverse
    // the elements in a column of a segment. The iterator starts from the beginning
    // of the column.
    //
    // @param id The unique identifier of the column.
    // @param path A pointer to the access path of the column.
    // @return A new iterator object for the specified column, or NotFound if the segment does not have the column.
    StatusOr<std::unique_ptr<ColumnIterator>> new_column_iterator(ColumnUID id, ColumnAccessPath* path);

    // Creates a new iterator for a specific column in a segment.
    //
    // The main difference from `new_iterator` is, if the segment does not have the
    // column, `new_column_iterator_or_default` will return an iterator that can read
    // the default value of the column, if there is one.
    //
    // Note: If this column does not have a default value defined, but is nullable, then
    // NULL will be used as the default value.
    //
    // @param id The unique identifier of the column.
    // @param path A pointer to the access path of the column.
    // @return A new iterator object for the specified column. If the segment does not have the column and the
    // column does not hava a default value, an error will be returned.
    StatusOr<std::unique_ptr<ColumnIterator>> new_column_iterator_or_default(const TabletColumn& column,
                                                                             ColumnAccessPath* path);

    Status new_bitmap_index_iterator(ColumnUID id, const IndexReadOptions& options, BitmapIndexIterator** iter);

    size_t num_short_keys() const { return _tablet_schema->num_short_key_columns(); }

    uint32_t num_rows_per_block() const {
        DCHECK(invoked(_load_index_once));
        return _sk_index_decoder->num_rows_per_block();
    }

    ShortKeyIndexIterator lower_bound(const Slice& key) const {
        DCHECK(invoked(_load_index_once));
        return _sk_index_decoder->lower_bound(key);
    }

    ShortKeyIndexIterator upper_bound(const Slice& key) const {
        DCHECK(invoked(_load_index_once));
        return _sk_index_decoder->upper_bound(key);
    }

    // This will return the last row block in this segment.
    // NOTE: Before call this function , client should assure that
    // this segment is not empty.
    uint32_t last_block() const {
        DCHECK(invoked(_load_index_once));
        DCHECK(num_rows() > 0);
        return _sk_index_decoder->num_items() - 1;
    }

    size_t num_columns() const { return _column_readers.size(); }

    const ColumnReader* column(size_t i) const {
        auto unique_id = _tablet_schema->column(i).unique_id();
        return _column_readers.count(unique_id) > 0 ? _column_readers.at(unique_id).get() : nullptr;
    }

    const ColumnReader* column_with_uid(size_t uid) const {
        return _column_readers.count(uid) > 0 ? _column_readers.at(uid).get() : nullptr;
    }

    FileSystem* file_system() const { return _fs.get(); }

    const TabletSchema& tablet_schema() const { return *_tablet_schema; }

    const TabletSchemaCSPtr tablet_schema_share_ptr() { return _tablet_schema.schema(); }

    const std::string& file_name() const { return _segment_file_info.path; }

    const FileInfo& file_info() const { return _segment_file_info; }

    uint32_t num_rows() const { return _num_rows; }

    // Load and decode short key index.
    // May be called multiple times, subsequent calls will no op.
    [[nodiscard]] Status load_index(bool skip_fill_local_cache = true);
    bool has_loaded_index() const;

    const ShortKeyIndexDecoder* decoder() const { return _sk_index_decoder.get(); }

    size_t mem_usage() const;

    int64_t get_data_size() const {
        if (_segment_file_info.size.has_value()) {
            return _segment_file_info.size.value();
        }
        return _fs->get_file_size(_segment_file_info.path).value_or(0);
    }

    // read short_key_index, for data check, just used in unit test now
    [[nodiscard]] Status get_short_key_index(std::vector<std::string>* sk_index_values);

    // for cloud native tablet metadata cache.
    // after the segment is inserted into metadata cache, various indexes will be loaded later when used,
    // so the segment size in the cache needs to be updated when indexes are loading.
    void update_cache_size();

    DISALLOW_COPY_AND_MOVE(Segment);

private:
    struct DummyDeleter {
        void operator()(const TabletSchema*) {}
    };

    // TabletSchemaWrapper can work as a raw pointer or shared pointer.
    class TabletSchemaWrapper {
    public:
        // Does not take the ownership of TabletSchema pointed by |raw_ptr|.
        explicit TabletSchemaWrapper(const TabletSchema* raw_ptr) : _schema(raw_ptr, DummyDeleter()) {}

        explicit TabletSchemaWrapper(const TabletSchemaCSPtr* shared_ptr) : _schema(*shared_ptr) {}

        // Shard the ownership of |ptr|.
        explicit TabletSchemaWrapper(TabletSchemaCSPtr ptr) : _schema(std::move(ptr)) {}

        DISALLOW_COPY_AND_MOVE(TabletSchemaWrapper);

        const TabletSchema* operator->() const { return _schema.get(); }

        const TabletSchema& operator*() const { return *_schema; }

        const TabletSchemaCSPtr& schema() { return _schema; };

    private:
        TabletSchemaCSPtr _schema;
    };

    Status _load_index(bool skip_fill_local_cache);

    void _reset();

    size_t _basic_info_mem_usage() const { return sizeof(Segment) + _segment_file_info.path.size(); }

    size_t _short_key_index_mem_usage() const {
        size_t size = _sk_index_handle.mem_usage();
        if (_sk_index_decoder != nullptr) {
            size += _sk_index_decoder->mem_usage();
        }
        return size;
    }

    size_t _column_index_mem_usage() const;

    // open segment file and read the minimum amount of necessary information (footer)
    Status _open(size_t* footer_length_hint, const FooterPointerPB* partial_rowset_footer, bool skip_fill_local_cache);
    Status _create_column_readers(SegmentFooterPB* footer);

    StatusOr<ChunkIteratorPtr> _new_iterator(const Schema& schema, const SegmentReadOptions& read_options);

    bool _use_segment_zone_map_filter(const SegmentReadOptions& read_options);

    friend class SegmentIterator;

    std::shared_ptr<FileSystem> _fs;
    FileInfo _segment_file_info;
    TabletSchemaWrapper _tablet_schema;
    uint32_t _segment_id = 0;
    uint32_t _num_rows = 0;
    PagePointer _short_key_index_page;

    // ColumnReader for each column in TabletSchema. If ColumnReader is nullptr,
    // This means that this segment has no data for that column, which may be added
    // after this segment is generated.
    std::map<int32_t, std::unique_ptr<ColumnReader>> _column_readers;

    // used to guarantee that short key index will be loaded at most once in a thread-safe way
    OnceFlag _load_index_once;
    // used to hold short key index page in memory
    PageHandle _sk_index_handle;
    // short key index decoder
    std::unique_ptr<ShortKeyIndexDecoder> _sk_index_decoder;

    // for cloud native tablet
    lake::TabletManager* _tablet_manager = nullptr;
    // used to guarantee that segment will be opened at most once in a thread-safe way
    OnceFlag _open_once;
};

} // namespace starrocks
