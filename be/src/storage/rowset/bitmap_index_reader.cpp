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

#include "bitmap_range_iterator.h"
#include "column/column_helper.h"
#include "column/column_viewer.h"
#include "exprs/function_context.h"
#include "exprs/like_predicate.h"
#include "runtime/exec_env.h"
#include "storage/chunk_helper.h"
#include "storage/posting/encoder.h"
#include "storage/range.h"
#include "storage/types.h"
#include "util/utf8.h"

namespace starrocks {

using Roaring = roaring::Roaring;

BitmapIndexReader::BitmapIndexReader(int32_t gram_num, bool with_position)
        : _gram_num(gram_num), _with_position(with_position) {
    MEM_TRACKER_SAFE_CONSUME(GlobalEnv::GetInstance()->bitmap_index_mem_tracker(), sizeof(BitmapIndexReader));
}

BitmapIndexReader::~BitmapIndexReader() {
    MEM_TRACKER_SAFE_RELEASE(GlobalEnv::GetInstance()->bitmap_index_mem_tracker(), mem_usage());
}

StatusOr<bool> BitmapIndexReader::load(const IndexReadOptions& opts, const BitmapIndexPB& meta) {
    return success_once(_load_once, [&]() {
        Status st = _do_load(opts, meta);
        if (st.ok()) {
            MEM_TRACKER_SAFE_CONSUME(GlobalEnv::GetInstance()->bitmap_index_mem_tracker(),
                                     mem_usage() - sizeof(BitmapIndexReader));
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
    if (meta.has_ngram_dict_column() && meta.has_ngram_bitmap_column()) {
        const IndexedColumnMetaPB& ngram_dict_meta = meta.ngram_dict_column();
        const IndexedColumnMetaPB& ngram_bitmap_meta = meta.ngram_bitmap_column();
        _ngram_dict_column_reader = std::make_unique<IndexedColumnReader>(ngram_dict_meta);
        _ngram_bitmap_column_reader = std::make_unique<IndexedColumnReader>(ngram_bitmap_meta);
        RETURN_IF_ERROR(_ngram_dict_column_reader->load(opts));
        RETURN_IF_ERROR(_ngram_bitmap_column_reader->load(opts));
    } else {
        _ngram_dict_column_reader = nullptr;
        _ngram_bitmap_column_reader = nullptr;
    }
    if (meta.has_posting_index_column() && meta.has_posting_position_column()) {
        const IndexedColumnMetaPB& posting_meta = meta.posting_index_column();
        const IndexedColumnMetaPB& posting_position_meta = meta.posting_position_column();
        _posting_index_reader = std::make_unique<IndexedColumnReader>(posting_meta);
        _posting_position_reader = std::make_unique<IndexedColumnReader>(posting_position_meta);
        RETURN_IF_ERROR(_posting_index_reader->load(opts));
        RETURN_IF_ERROR(_posting_position_reader->load(opts));
    } else {
        _posting_index_reader = nullptr;
        _posting_position_reader = nullptr;
    }
    return Status::OK();
}

Status BitmapIndexReader::new_iterator(const IndexReadOptions& opts, BitmapIndexIterator** iterator) {
    std::unique_ptr<IndexedColumnIterator> dict_iter;
    std::unique_ptr<IndexedColumnIterator> bitmap_iter;
    std::unique_ptr<IndexedColumnIterator> ngram_dict_iter = nullptr;
    std::unique_ptr<IndexedColumnIterator> ngram_bitmap_iter = nullptr;
    std::unique_ptr<IndexedColumnIterator> posting_index_iter = nullptr;
    std::unique_ptr<IndexedColumnIterator> posting_position_iter = nullptr;
    RETURN_IF_ERROR(_dict_column_reader->new_iterator(opts, &dict_iter));
    RETURN_IF_ERROR(_bitmap_column_reader->new_iterator(opts, &bitmap_iter));
    if (_ngram_dict_column_reader != nullptr && _ngram_bitmap_column_reader != nullptr) {
        RETURN_IF_ERROR(_ngram_dict_column_reader->new_iterator(opts, &ngram_dict_iter));
        RETURN_IF_ERROR(_ngram_bitmap_column_reader->new_iterator(opts, &ngram_bitmap_iter));
    }
    if (_posting_index_reader != nullptr && _posting_position_reader != nullptr) {
        RETURN_IF_ERROR(_posting_index_reader->new_iterator(opts, &posting_index_iter));
        RETURN_IF_ERROR(_posting_position_reader->new_iterator(opts, &posting_position_iter));
    }
    *iterator = new BitmapIndexIterator(this, std::move(dict_iter), std::move(bitmap_iter), std::move(ngram_dict_iter),
                                        std::move(ngram_bitmap_iter), std::move(posting_index_iter),
                                        std::move(posting_position_iter), _has_null, bitmap_nums());
    return Status::OK();
}

Status BitmapIndexIterator::seek_dict_by_ngram(const void* value, roaring::Roaring* roaring) const {
    if (_reader->gram_num() <= 0) {
        // _num_bitmap means how many dicts exist. should return all dicts here.
        roaring->addRange(0, _num_bitmap);
        return Status::OK();
    }

    if (_ngram_dict_column_iter == nullptr || _ngram_bitmap_column_iter == nullptr) {
        return Status::InternalError("ngram bitmap index reader is not opened.");
    }
    if (_reader->type_info()->type() != TYPE_VARCHAR && _reader->type_info()->type() != TYPE_CHAR) {
        return Status::NotSupported("ngram seek for dictionary only support string/char type in bitmap index");
    }

    const auto gram_num = _reader->gram_num();
    const auto* slice_val = static_cast<const Slice*>(value);
    if (slice_val->get_size() < gram_num) {
        // _num_bitmap means how many dicts exist. should return all dicts here.
        roaring->addRange(0, _num_bitmap);
        return Status::OK();
    }

    std::vector<size_t> index;
    const size_t slice_gram_num = get_utf8_index(*slice_val, &index);

    std::vector<Slice> ngrams;
    ngrams.reserve(slice_gram_num - gram_num + 1);

    for (size_t j = 0; j + gram_num <= slice_gram_num; ++j) {
        // find next ngram
        size_t cur_ngram_length =
                j + gram_num < slice_gram_num ? index[j + gram_num] - index[j] : slice_val->get_size() - index[j];
        Slice cur_ngram(slice_val->data + index[j], cur_ngram_length);
        ngrams.emplace_back(cur_ngram);
    }
    std::ranges::sort(ngrams);

    bool first = true;
    for (const auto& cur_ngram : ngrams) {
        // search in order.
        bool match = false;
        RETURN_IF_ERROR(_ngram_dict_column_iter->seek_at_or_after(&cur_ngram, &match));
        if (!match) {
            // Clear rowids bitmap here, otherwise the caller might mistakenly treat the remaining values in the
            // bitmap as matched rowids.
            roaring->clear();
            return Status::OK();
        }

        RETURN_IF_ERROR(_ngram_bitmap_column_iter->seek_to_ordinal(_ngram_dict_column_iter->get_current_ordinal()));
        size_t num_to_read = 1;
        size_t num_read = num_to_read;
        auto column = ChunkHelper::column_from_field_type(TYPE_VARCHAR, false);
        RETURN_IF_ERROR(_ngram_bitmap_column_iter->next_batch(&num_read, column.get()));
        if (num_to_read != num_read) {
            return Status::InternalError(fmt::format(
                    "read ngram bitmap column failed, expect {} rows, but got {} rows.", num_to_read, num_read));
        }

        ColumnViewer<TYPE_VARCHAR> viewer(std::move(column));
        const auto str_bitmap = viewer.value(0);

        const auto tmp = Roaring::read(str_bitmap.data, false);
        if (first) {
            *roaring |= tmp;
            first = false;
        } else {
            *roaring &= tmp;
        }

        if (roaring->cardinality() == 0) {
            // no words match, fast fail
            return Status::OK();
        }
    }
    return Status::OK();
}

StatusOr<Buffer<rowid_t>> BitmapIndexIterator::filter_dict_by_predicate(
        const roaring::Roaring* rowids, const std::function<bool(const Slice*)>& predicate) const {
    BitmapRangeIterator it(*rowids);
    uint32_t from, to;
    const auto max_range = rowids->cardinality();
    Buffer<rowid_t> hit_rowids;
    while (it.next_range(max_range, &from, &to)) {
        auto col = ChunkHelper::column_from_field_type(TYPE_VARCHAR, false);
        RETURN_IF_ERROR(_dict_column_iter->seek_to_ordinal(from));
        size_t num_to_read = to - from;
        size_t read = num_to_read;
        RETURN_IF_ERROR(_dict_column_iter->next_batch(&read, col.get()));
        if (num_to_read != read) {
            return Status::InternalError(
                    fmt::format("read dict column failed, expect {} rows, but got {} rows.", num_to_read, read));
        }

        ColumnViewer<TYPE_VARCHAR> viewer(std::move(col));
        for (int i = 0; i < viewer.size(); ++i) {
            auto value = viewer.value(i);
            if (predicate(&value)) {
                hit_rowids.push_back(from + i);
            }
        }
    }
    return std::move(hit_rowids);
}

Status BitmapIndexIterator::next_batch_ngram(rowid_t ordinal, size_t* n, Column* column) const {
    if (_ngram_dict_column_iter != nullptr) {
        if (!(0 <= ordinal && ordinal < _reader->ngram_bitmap_nums())) {
            return Status::InvalidArgument("ordinal is out of range while reading ngram bitmap");
        }
        RETURN_IF_ERROR(_ngram_dict_column_iter->seek_to_ordinal(ordinal));
        return _ngram_dict_column_iter->next_batch(n, column);
    }
    *n = 0;
    return Status::OK();
}

Status BitmapIndexIterator::read_ngram_bitmap(rowid_t ordinal, Roaring* result) const {
    if (_ngram_bitmap_column_iter != nullptr) {
        if (!(0 <= ordinal && ordinal < _reader->ngram_bitmap_nums())) {
            return Status::InvalidArgument("ordinal is out of range while reading ngram bitmap");
        }
        auto column = ChunkHelper::column_from_field_type(TYPE_VARCHAR, false);
        RETURN_IF_ERROR(_ngram_bitmap_column_iter->seek_to_ordinal(ordinal));
        size_t num_to_read = 1;
        size_t num_read = num_to_read;
        RETURN_IF_ERROR(_ngram_bitmap_column_iter->next_batch(&num_read, column.get()));
        if (num_to_read != num_read) {
            return Status::InternalError(fmt::format(
                    "read ngram bitmap column failed, expect {} rows, but got {} rows.", num_to_read, num_read));
        }
        const ColumnViewer<TYPE_VARCHAR> viewer(std::move(column));
        const auto value = viewer.value(0);
        *result = Roaring::read(value.data, false);
    }
    return Status::OK();
}

StatusOr<std::vector<roaring::Roaring>> BitmapIndexIterator::read_positions(
        rowid_t dict_id, const std::vector<uint64_t>& doc_ranks) const {
    if (!_reader->with_position()) {
        return Status::InvalidArgument(fmt::format("Reading positions but position is not enabled."));
    }
    RETURN_IF(_posting_index_iter == nullptr,
              Status::InternalError("Reading positions but no posting index reader provided"));
    RETURN_IF(_posting_position_iter == nullptr,
              Status::InternalError("Reading positions but no posting position reader provided"));

    RETURN_IF_ERROR(_posting_index_iter->seek_to_ordinal(dict_id));

    auto col = ChunkHelper::column_from_field_type(TYPE_INT, false);

    size_t num_to_read = 1;
    size_t num_read = num_to_read;
    RETURN_IF_ERROR(_posting_index_iter->next_batch(&num_read, col.get()));
    RETURN_IF(num_to_read != num_read,
              Status::InternalError(fmt::format("read position index column failed, expect {} rows, but got {} rows.",
                                                num_to_read, num_read)));

    const ColumnViewer<TYPE_INT> viewer(std::move(col));
    const auto offset = viewer.value(0);

    std::vector<roaring::Roaring> result;
    result.reserve(doc_ranks.size());

    const auto encoder = EncoderFactory::createEncoder(EncodingType::VARINT);
    for (const auto& doc_rank : doc_ranks) {
        const ordinal_t ordinal = offset + doc_rank - 1;
        LOG(INFO) << "match_phrase: seek posting position to " << ordinal;
        RETURN_IF_ERROR(_posting_position_iter->seek_to_ordinal(ordinal));

        auto position_col = ChunkHelper::column_from_field_type(TYPE_VARBINARY, false);
        num_to_read = 1;
        num_read = num_to_read;
        RETURN_IF_ERROR(_posting_position_iter->next_batch(&num_read, position_col.get()));
        RETURN_IF(
                num_to_read != num_read,
                Status::InternalError(fmt::format("read position index column failed, expect {} rows, but got {} rows.",
                                                  num_to_read, num_read)));

        const ColumnViewer<TYPE_VARCHAR> position_viewer(std::move(position_col));
        const Slice encoded_positions = position_viewer.value(0);
        const std::vector<uint8_t> positions_bytes(encoded_positions.get_data(),
                                                   encoded_positions.get_data() + encoded_positions.get_size());
        ASSIGN_OR_RETURN(auto positions, encoder->decode(positions_bytes));
        result.push_back(positions);
    }
    return result;
}

Status BitmapIndexIterator::seek_dictionary(const void* value, bool* exact_match) {
    RETURN_IF_ERROR(_dict_column_iter->seek_at_or_after(value, exact_match));
    _current_rowid = _dict_column_iter->get_current_ordinal();
    return Status::OK();
}

Status BitmapIndexIterator::next_batch_dictionary(size_t* n, Column* column) {
    RETURN_IF_ERROR(_dict_column_iter->next_batch(n, column));
    _current_rowid += *n;
    return Status::OK();
}

StatusOr<Buffer<rowid_t>> BitmapIndexIterator::seek_dictionary_by_predicate(const DictPredicate& predicate,
                                                                            const Slice& from_value,
                                                                            size_t search_size) {
    if (_reader->type_info()->type() != TYPE_VARCHAR && _reader->type_info()->type() != TYPE_CHAR) {
        return Status::NotSupported("predicate seek for dictionary only support string/char type bitmap index");
    }
    auto column = ChunkHelper::column_from_field_type(TYPE_VARCHAR, false);
    bool exact_match;
    RETURN_IF_ERROR(seek_dictionary(&from_value, &exact_match));
    size_t beg_rowid = _current_rowid;
    RETURN_IF_ERROR(next_batch_dictionary(&search_size, column.get()));
    ASSIGN_OR_RETURN(auto ret, predicate(*column));

    auto hit_column = down_cast<BooleanColumn*>(ret.get());
    Buffer<rowid_t> hit_rowids;
    for (int i = 0; i < hit_column->size(); ++i) {
        if (hit_column->get_data()[i]) {
            hit_rowids.push_back(beg_rowid + i);
        }
    }
    return hit_rowids;
}

Status BitmapIndexIterator::read_bitmap(rowid_t ordinal, Roaring* result) {
    DCHECK(0 <= ordinal && ordinal < _reader->bitmap_nums());

    auto column = ChunkHelper::column_from_field_type(TYPE_VARCHAR, false);
    RETURN_IF_ERROR(_bitmap_column_iter->seek_to_ordinal(ordinal));
    size_t num_to_read = 1;
    size_t num_read = num_to_read;
    RETURN_IF_ERROR(_bitmap_column_iter->next_batch(&num_read, column.get()));
    DCHECK(num_to_read == num_read);

    ColumnViewer<TYPE_VARCHAR> viewer(std::move(column));
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

Status BitmapIndexIterator::read_union_bitmap(const Buffer<rowid_t>& rowids, Roaring* result) {
    for (const auto& rowid : rowids) {
        Roaring bitmap;
        RETURN_IF_ERROR(read_bitmap(rowid, &bitmap));
        *result |= bitmap;
    }
    return Status::OK();
}

} // namespace starrocks
