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
    BitmapIndexReader();
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

    const TypeInfoPtr& type_info() { return _typeinfo; }

    bool loaded() const { return invoked(_load_once); }

private:
    friend class BitmapIndexIterator;

    size_t _mem_usage() const {
        size_t size = sizeof(BitmapIndexReader);
        if (_dict_column_reader != nullptr) {
            size += _dict_column_reader->mem_usage();
        }
        if (_bitmap_column_reader != nullptr) {
            size += _bitmap_column_reader->mem_usage();
        }
        return size;
    }

    void _reset();

    Status _do_load(const IndexReadOptions& opts, const BitmapIndexPB& meta);

    OnceFlag _load_once;
    TypeInfoPtr _typeinfo;
    std::unique_ptr<IndexedColumnReader> _dict_column_reader;
    std::unique_ptr<IndexedColumnReader> _bitmap_column_reader;
    bool _has_null = false;
};

class BitmapIndexIterator {
public:
    BitmapIndexIterator(BitmapIndexReader* reader, std::unique_ptr<IndexedColumnIterator> dict_iter,
                        std::unique_ptr<IndexedColumnIterator> bitmap_iter, bool has_null, rowid_t num_bitmap)
            : _reader(reader),
              _dict_column_iter(std::move(dict_iter)),
              _bitmap_column_iter(std::move(bitmap_iter)),
              _has_null(has_null),
              _num_bitmap(num_bitmap) {}

    bool has_null_bitmap() const { return _has_null; }

    // Seek the dictionary to the first value that is >= the given value.
    //
    // Returns OK when such value exists. The seeked position can be retrieved
    // by `current_ordinal()`, *exact_match is set to indicate whether the
    // seeked value exactly matches `value` or not
    //
    // Returns NotFound when no such value exists (all values in dictionary < `value`).
    // Returns other error status otherwise.
    Status seek_dictionary(const void* value, bool* exact_match);

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

    rowid_t bitmap_nums() const { return _num_bitmap; }

    rowid_t current_ordinal() const { return _current_rowid; }

private:
    BitmapIndexReader* _reader;
    std::unique_ptr<IndexedColumnIterator> _dict_column_iter;
    std::unique_ptr<IndexedColumnIterator> _bitmap_column_iter;
    bool _has_null;
    rowid_t _num_bitmap;
    rowid_t _current_rowid{0};
};

} // namespace starrocks
