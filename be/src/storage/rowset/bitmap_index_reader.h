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
//   https://github.com/apache/incubator-doris/blob/master/be/src/olap/rowset/segment_v2/bitmap_index_reader.h

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

#include <roaring/roaring.hh>

#include "column/column_helper.h"
#include "common/status.h"
#include "fs/fs.h"
#include "gen_cpp/segment.pb.h"
#include "storage/range.h"
#include "storage/rowset/common.h"
#include "storage/rowset/indexed_column_reader.h"
#include "util/once.h"

namespace starrocks {

class FileSystem;
class TypeInfo;
class BitmapIndexIterator;
class IndexedColumnReader;
class IndexedColumnIterator;

using Roaring = roaring::Roaring;

class BitmapIndexReader {
public:
    BitmapIndexReader(int32_t gram_num = -1, bool with_position = false);
    ~BitmapIndexReader();

    // Load index data into memory.
    //
    // Multiple callers may call this method concurrently, but only the first one
    // can load the data, the others will wait until the first one finished loading
    // data.
    //
    // Return true if the index data was successfully loaded by the caller, false if
    // the data was loaded by another caller.
    StatusOr<bool> load(const IndexReadOptions& opts, const BitmapIndexPB& meta);

    // create a new column iterator. Client should delete returned iterator
    // REQUIRES: the index data has been successfully `load()`ed into memory.
    Status new_iterator(const IndexReadOptions& opts, BitmapIndexIterator** iterator);

    // REQUIRES: the index data has been successfully `load()`ed into memory.
    int64_t bitmap_nums() { return _bitmap_column_reader->num_values(); }

    int32_t gram_num() const { return _gram_num; }

    // REQUIRES: the index data has been successfully `load()`ed into memory.
    int64_t ngram_bitmap_nums() const {
        if (_ngram_bitmap_column_reader != nullptr) {
            return _ngram_bitmap_column_reader->num_values();
        }
        return 0;
    }

    bool with_position() const { return _with_position; }

    const TypeInfoPtr& type_info() { return _typeinfo; }

    bool loaded() const { return invoked(_load_once); }

    size_t mem_usage() const {
        size_t size = sizeof(BitmapIndexReader);
        if (_dict_column_reader != nullptr) {
            size += _dict_column_reader->mem_usage();
        }
        if (_bitmap_column_reader != nullptr) {
            size += _bitmap_column_reader->mem_usage();
        }
        if (_ngram_dict_column_reader != nullptr) {
            size += _ngram_dict_column_reader->mem_usage();
        }
        if (_ngram_bitmap_column_reader != nullptr) {
            size += _ngram_bitmap_column_reader->mem_usage();
        }
        return size;
    }

private:
    friend class BitmapIndexIterator;

    void _reset();

    Status _do_load(const IndexReadOptions& opts, const BitmapIndexPB& meta);

    int32_t _gram_num;
    bool _with_position;

    OnceFlag _load_once;
    TypeInfoPtr _typeinfo;
    std::unique_ptr<IndexedColumnReader> _dict_column_reader;
    std::unique_ptr<IndexedColumnReader> _bitmap_column_reader;
    std::unique_ptr<IndexedColumnReader> _ngram_dict_column_reader;
    std::unique_ptr<IndexedColumnReader> _ngram_bitmap_column_reader;
    std::unique_ptr<IndexedColumnReader> _posting_index_reader;
    std::unique_ptr<IndexedColumnReader> _posting_position_reader;
    bool _has_null = false;
};

class BitmapIndexIterator {
public:
    using DictPredicate = std::function<StatusOr<ColumnPtr>(const Column&)>;

    BitmapIndexIterator(BitmapIndexReader* reader, std::unique_ptr<IndexedColumnIterator> dict_iter,
                        std::unique_ptr<IndexedColumnIterator> bitmap_iter,
                        std::unique_ptr<IndexedColumnIterator> ngram_dict_iter,
                        std::unique_ptr<IndexedColumnIterator> ngram_bitmap_iter,
                        std::unique_ptr<IndexedColumnIterator> posting_index_iter,
                        std::unique_ptr<IndexedColumnIterator> posting_position_iter, bool has_null, rowid_t num_bitmap)
            : _reader(reader),
              _dict_column_iter(std::move(dict_iter)),
              _bitmap_column_iter(std::move(bitmap_iter)),
              _ngram_dict_column_iter(std::move(ngram_dict_iter)),
              _ngram_bitmap_column_iter(std::move(ngram_bitmap_iter)),
              _posting_index_iter(std::move(posting_index_iter)),
              _posting_position_iter(std::move(posting_position_iter)),
              _has_null(has_null),
              _num_bitmap(num_bitmap) {}

    bool has_null_bitmap() const { return _has_null; }

    Status seek_dict_by_ngram(const void* value, roaring::Roaring* roaring) const;

    StatusOr<Buffer<rowid_t>> filter_dict_by_predicate(const roaring::Roaring* rowids,
                                                       const std::function<bool(const Slice*)>& predicate) const;

    // used for test
    Status next_batch_ngram(rowid_t ordinal, size_t* n, Column* column) const;
    Status read_ngram_bitmap(rowid_t ordinal, Roaring* result) const;

    StatusOr<std::vector<roaring::Roaring>> read_positions(rowid_t dict_id, const std::vector<uint64_t>& doc_ranks) const;

    // Seek the dictionary to the first value that is >= the given value.
    //
    // Returns OK when such value exists. The seeked position can be retrieved
    // by `current_ordinal()`, *exact_match is set to indicate whether the
    // seeked value exactly matches `value` or not
    //
    // Returns NotFound when no such value exists (all values in dictionary < `value`).
    // Returns other error status otherwise.
    Status seek_dictionary(const void* value, bool* exact_match);

    StatusOr<Buffer<rowid_t>> seek_dictionary_by_predicate(const DictPredicate& predicate, const Slice& from_value,
                                                           size_t search_size);

    Status next_batch_dictionary(size_t* n, Column* column);

    // Read bitmap at the given ordinal into `result`.
    Status read_bitmap(rowid_t ordinal, Roaring* result);

    Status read_null_bitmap(Roaring* result) {
        if (has_null_bitmap()) {
            // null bitmap is always stored at last
            return read_bitmap(bitmap_nums() - 1, result);
        }
        return Status::OK(); // keep result empty
    }

    // Read and union all bitmaps in range [from, to) into `result`
    Status read_union_bitmap(rowid_t from, rowid_t to, Roaring* result);

    // Read and union all bitmaps in range into `result`.
    //
    // Roaring result;
    // for (size_t i = 0; i < range.size(); i++) {
    //     read_union_bitmap(range[i].begin(), range[i].end(), &result);
    // }
    Status read_union_bitmap(const SparseRange<>& range, Roaring* result);

    Status read_union_bitmap(const Buffer<rowid_t>& rowids, Roaring* result);

    rowid_t bitmap_nums() const { return _num_bitmap; }

    rowid_t ngram_bitmap_nums() const {
        if (_reader != nullptr) {
            return _reader->ngram_bitmap_nums();
        }
        return 0;
    }

    rowid_t current_ordinal() const { return _current_rowid; }

private:
    BitmapIndexReader* _reader;
    std::unique_ptr<IndexedColumnIterator> _dict_column_iter;
    std::unique_ptr<IndexedColumnIterator> _bitmap_column_iter;
    std::unique_ptr<IndexedColumnIterator> _ngram_dict_column_iter;
    std::unique_ptr<IndexedColumnIterator> _ngram_bitmap_column_iter;
    std::unique_ptr<IndexedColumnIterator> _posting_index_iter;
    std::unique_ptr<IndexedColumnIterator> _posting_position_iter;
    bool _has_null;
    rowid_t _num_bitmap;
    rowid_t _current_rowid{0};
};

} // namespace starrocks
