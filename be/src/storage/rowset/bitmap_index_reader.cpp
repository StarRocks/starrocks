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
//   https://github.com/apache/incubator-doris/blob/master/be/src/olap/rowset/segment_v2/bitmap_index_reader.cpp

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

#include "storage/rowset/bitmap_index_reader.h"

#include <bthread/sys_futex.h>

#include <memory>

#include "column/column_helper.h"
#include "column/column_viewer.h"
#include "storage/chunk_helper.h"
#include "storage/range.h"
#include "storage/types.h"

namespace starrocks {

using Roaring = roaring::Roaring;

BitmapIndexReader::BitmapIndexReader() {
    MEM_TRACKER_SAFE_CONSUME(ExecEnv::GetInstance()->bitmap_index_mem_tracker(), sizeof(BitmapIndexReader));
}

BitmapIndexReader::~BitmapIndexReader() {
    MEM_TRACKER_SAFE_RELEASE(ExecEnv::GetInstance()->bitmap_index_mem_tracker(), _mem_usage());
}

StatusOr<bool> BitmapIndexReader::load(const IndexReadOptions& opts, const BitmapIndexPB& meta) {
    return success_once(_load_once, [&]() {
        Status st = _do_load(opts, meta);
        if (st.ok()) {
            MEM_TRACKER_SAFE_CONSUME(ExecEnv::GetInstance()->bitmap_index_mem_tracker(),
                                     _mem_usage() - sizeof(BitmapIndexReader));
        } else {
            _reset();
        }
        return st;
    });
}

void BitmapIndexReader::_reset() {
    _typeinfo.reset();
    _dict_column_reader.reset();
    _bitmap_column_reader.reset();
    _has_null = false;
}

Status BitmapIndexReader::_do_load(const IndexReadOptions& opts, const BitmapIndexPB& meta) {
    _typeinfo = get_type_info(TYPE_VARCHAR);
    const IndexedColumnMetaPB& dict_meta = meta.dict_column();
    const IndexedColumnMetaPB& bitmap_meta = meta.bitmap_column();
    _has_null = meta.has_null();
    _dict_column_reader = std::make_unique<IndexedColumnReader>(dict_meta);
    _bitmap_column_reader = std::make_unique<IndexedColumnReader>(bitmap_meta);
    RETURN_IF_ERROR(_dict_column_reader->load(opts));
    RETURN_IF_ERROR(_bitmap_column_reader->load(opts));
    return Status::OK();
}

Status BitmapIndexReader::new_iterator(const IndexReadOptions& opts, BitmapIndexIterator** iterator) {
    std::unique_ptr<IndexedColumnIterator> dict_iter;
    std::unique_ptr<IndexedColumnIterator> bitmap_iter;
    RETURN_IF_ERROR(_dict_column_reader->new_iterator(opts, &dict_iter));
    RETURN_IF_ERROR(_bitmap_column_reader->new_iterator(opts, &bitmap_iter));
    *iterator = new BitmapIndexIterator(this, std::move(dict_iter), std::move(bitmap_iter), _has_null, bitmap_nums());
    return Status::OK();
}

Status BitmapIndexIterator::seek_dictionary(const void* value, bool* exact_match) {
    RETURN_IF_ERROR(_dict_column_iter->seek_at_or_after(value, exact_match));
    _current_rowid = _dict_column_iter->get_current_ordinal();
    return Status::OK();
}

Status BitmapIndexIterator::read_bitmap(rowid_t ordinal, Roaring* result) {
    DCHECK(0 <= ordinal && ordinal < _reader->bitmap_nums());

    auto column = ChunkHelper::column_from_field_type(TYPE_VARCHAR, false);
    RETURN_IF_ERROR(_bitmap_column_iter->seek_to_ordinal(ordinal));
    size_t num_to_read = 1;
    size_t num_read = num_to_read;
    RETURN_IF_ERROR(_bitmap_column_iter->next_batch(&num_read, column.get()));
    DCHECK(num_to_read == num_read);

    ColumnViewer<TYPE_VARCHAR> viewer(column);
    auto value = viewer.value(0);

    *result = Roaring::read(value.data, false);
    return Status::OK();
}

Status BitmapIndexIterator::read_union_bitmap(rowid_t from, rowid_t to, Roaring* result) {
    DCHECK(0 <= from && from <= to && to <= _reader->bitmap_nums());

    for (rowid_t pos = from; pos < to; pos++) {
        Roaring bitmap;
        RETURN_IF_ERROR(read_bitmap(pos, &bitmap));
        *result |= bitmap;
    }
    return Status::OK();
}

Status BitmapIndexIterator::read_union_bitmap(const SparseRange<>& range, Roaring* result) {
    for (size_t i = 0; i < range.size(); i++) { // NOLINT
        const Range<>& r = range[i];
        RETURN_IF_ERROR(read_union_bitmap(r.begin(), r.end(), result));
    }
    return Status::OK();
}

} // namespace starrocks
