// This file is made available under Elastic License 2.0.
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
#include "gen_cpp/segment_v2.pb.h"
#include "runtime/mem_pool.h"
#include "storage/column_block.h"
#include "storage/rowset/segment_v2/common.h"
#include "storage/rowset/segment_v2/indexed_column_reader.h"

namespace starrocks {

class TypeInfo;

namespace fs {
class BlockManager;
}

namespace vectorized {
class SparseRange;
}

namespace segment_v2 {

class BitmapIndexIterator;
class IndexedColumnReader;
class IndexedColumnIterator;

class BitmapIndexReader {
public:
    BitmapIndexReader() = default;

    Status load(fs::BlockManager* block_mgr, const std::string& file_name, const BitmapIndexPB* bitmap_index_meta,
                bool use_page_cache, bool kept_in_memory);

    // create a new column iterator. Client should delete returned iterator
    Status new_iterator(BitmapIndexIterator** iterator);

    int64_t bitmap_nums() { return _bitmap_column_reader->num_values(); }

    const TypeInfoPtr& type_info() { return _typeinfo; }

    size_t mem_usage() const {
        size_t size = sizeof(BitmapIndexReader);
        if (_dict_column_reader != nullptr) {
            size += _dict_column_reader->mem_usage();
        }
        if (_bitmap_column_reader != nullptr) {
            size += _bitmap_column_reader->mem_usage();
        }
        return size;
    }

private:
    friend class BitmapIndexIterator;

    TypeInfoPtr _typeinfo;
    bool _has_null = false;
    std::unique_ptr<IndexedColumnReader> _dict_column_reader;
    std::unique_ptr<IndexedColumnReader> _bitmap_column_reader;
};

class BitmapIndexIterator {
public:
    BitmapIndexIterator(BitmapIndexReader* reader, std::unique_ptr<IndexedColumnIterator> dict_iter,
                        std::unique_ptr<IndexedColumnIterator> bitmap_iter, bool has_null, rowid_t num_bitmap)
            : _reader(reader),
              _dict_column_iter(std::move(dict_iter)),
              _bitmap_column_iter(std::move(bitmap_iter)),
              _has_null(has_null),
              _num_bitmap(num_bitmap),
              _current_rowid(0),
              _pool(new MemPool()) {}

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
    Status read_union_bitmap(const vectorized::SparseRange& range, Roaring* result);

    inline rowid_t bitmap_nums() const { return _num_bitmap; }

    inline rowid_t current_ordinal() const { return _current_rowid; }

private:
    BitmapIndexReader* _reader;
    std::unique_ptr<IndexedColumnIterator> _dict_column_iter;
    std::unique_ptr<IndexedColumnIterator> _bitmap_column_iter;
    bool _has_null;
    rowid_t _num_bitmap;
    rowid_t _current_rowid;
    std::unique_ptr<MemPool> _pool;
};

} // namespace segment_v2
} // namespace starrocks
