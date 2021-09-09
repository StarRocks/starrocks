// This file is made available under Elastic License 2.0.
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

#include "storage/rowset/segment_v2/bitmap_index_reader.h"

#include <memory>

#include "storage/types.h"
#include "storage/vectorized/range.h"

namespace starrocks::segment_v2 {

Status BitmapIndexReader::load(fs::BlockManager* block_mgr, const std::string& file_name,
                               const BitmapIndexPB* bitmap_index_meta, bool use_page_cache, bool kept_in_memory) {
    _typeinfo = get_type_info(OLAP_FIELD_TYPE_VARCHAR);
    const IndexedColumnMetaPB& dict_meta = bitmap_index_meta->dict_column();
    const IndexedColumnMetaPB& bitmap_meta = bitmap_index_meta->bitmap_column();
    _has_null = bitmap_index_meta->has_null();

    _dict_column_reader = std::make_unique<IndexedColumnReader>(block_mgr, file_name, dict_meta);
    _bitmap_column_reader = std::make_unique<IndexedColumnReader>(block_mgr, file_name, bitmap_meta);
    RETURN_IF_ERROR(_dict_column_reader->load(use_page_cache, kept_in_memory));
    RETURN_IF_ERROR(_bitmap_column_reader->load(use_page_cache, kept_in_memory));
    return Status::OK();
}

Status BitmapIndexReader::new_iterator(BitmapIndexIterator** iterator) {
    std::unique_ptr<IndexedColumnIterator> dict_iter;
    std::unique_ptr<IndexedColumnIterator> bitmap_iter;
    RETURN_IF_ERROR(_dict_column_reader->new_iterator(&dict_iter));
    RETURN_IF_ERROR(_bitmap_column_reader->new_iterator(&bitmap_iter));
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

    size_t num_to_read = 1;
    std::unique_ptr<ColumnVectorBatch> cvb;
    RETURN_IF_ERROR(ColumnVectorBatch::create(num_to_read, false, _reader->type_info(), nullptr, &cvb));
    ColumnBlock block(cvb.get(), _pool.get());
    ColumnBlockView column_block_view(&block);

    RETURN_IF_ERROR(_bitmap_column_iter->seek_to_ordinal(ordinal));
    size_t num_read = num_to_read;
    RETURN_IF_ERROR(_bitmap_column_iter->next_batch(&num_read, &column_block_view));
    DCHECK(num_to_read == num_read);

    *result = Roaring::read(reinterpret_cast<const Slice*>(block.data())->data, false);
    _pool->clear();
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

Status BitmapIndexIterator::read_union_bitmap(const vectorized::SparseRange& range, Roaring* result) {
    for (size_t i = 0; i < range.size(); i++) {
        const vectorized::Range& r = range[i];
        RETURN_IF_ERROR(read_union_bitmap(r.begin(), r.end(), result));
    }
    return Status::OK();
}

} // namespace starrocks::segment_v2
