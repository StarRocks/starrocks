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

#include "storage/rowset/map_column_iterator.h"

#include "column/map_column.h"
#include "column/nullable_column.h"
#include "storage/rowset/scalar_column_iterator.h"

namespace starrocks {

MapColumnIterator::MapColumnIterator(ColumnIterator* nulls_iterator, ColumnIterator* offsets_iterator,
                                     ColumnIterator* keys_iterator, ColumnIterator* values_iterator) {
    _nulls.reset(nulls_iterator);
    _offsets.reset(offsets_iterator);
    _keys.reset(keys_iterator);
    _values.reset(values_iterator);
}

Status MapColumnIterator::init(const ColumnIteratorOptions& opts) {
    if (_nulls != nullptr) {
        RETURN_IF_ERROR(_nulls->init(opts));
    }
    RETURN_IF_ERROR(_offsets->init(opts));
    RETURN_IF_ERROR(_keys->init(opts));
    RETURN_IF_ERROR(_values->init(opts));
    return Status::OK();
}

Status MapColumnIterator::next_batch(size_t* n, vectorized::Column* dst) {
    vectorized::MapColumn* map_column = nullptr;
    vectorized::NullColumn* null_column = nullptr;
    if (dst->is_nullable()) {
        auto* nullable_column = down_cast<vectorized::NullableColumn*>(dst);

        map_column = down_cast<vectorized::MapColumn*>(nullable_column->data_column().get());
        null_column = down_cast<vectorized::NullColumn*>(nullable_column->null_column().get());
    } else {
        map_column = down_cast<vectorized::MapColumn*>(dst);
    }

    // 1. Read null column
    if (_nulls != nullptr) {
        RETURN_IF_ERROR(_nulls->next_batch(n, null_column));
        down_cast<vectorized::NullableColumn*>(dst)->update_has_null();
    }

    // 2. Read offset column
    // [1, 2, 3], [4, 5, 6]
    // In memory, it will be transformed to actual offset(0, 3, 6)
    // On disk, offset is stored as length array(3, 3)
    auto* offsets = map_column->offsets_column().get();
    auto& data = offsets->get_data();
    size_t end_offset = data.back();

    size_t prev_array_size = offsets->size();
    RETURN_IF_ERROR(_offsets->next_batch(n, offsets));
    size_t curr_array_size = offsets->size();

    size_t num_to_read = end_offset;
    for (size_t i = prev_array_size; i < curr_array_size; ++i) {
        end_offset += data[i];
        data[i] = end_offset;
    }
    num_to_read = end_offset - num_to_read;

    // 3. Read elements
    RETURN_IF_ERROR(_keys->next_batch(&num_to_read, map_column->keys_column().get()));
    RETURN_IF_ERROR(_values->next_batch(&num_to_read, map_column->values_column().get()));

    return Status::OK();
}

Status MapColumnIterator::next_batch(const vectorized::SparseRange& range, vectorized::Column* dst) {
    vectorized::MapColumn* map_column = nullptr;
    vectorized::NullColumn* null_column = nullptr;
    if (dst->is_nullable()) {
        auto* nullable_column = down_cast<vectorized::NullableColumn*>(dst);

        map_column = down_cast<vectorized::MapColumn*>(nullable_column->data_column().get());
        null_column = down_cast<vectorized::NullColumn*>(nullable_column->null_column().get());
    } else {
        map_column = down_cast<vectorized::MapColumn*>(dst);
    }

    CHECK((_nulls == nullptr && null_column == nullptr) || (_nulls != nullptr && null_column != nullptr));

    // 1. Read null column
    if (_nulls != nullptr) {
        RETURN_IF_ERROR(_nulls->next_batch(range, null_column));
        down_cast<vectorized::NullableColumn*>(dst)->update_has_null();
    }

    vectorized::SparseRangeIterator iter = range.new_iterator();
    size_t to_read = range.span_size();

    // array column can be nested, range may be empty
    DCHECK(range.empty() || (range.begin() == _offsets->get_current_ordinal()));
    vectorized::SparseRange element_read_range;
    while (iter.has_more()) {
        vectorized::Range r = iter.next(to_read);

        RETURN_IF_ERROR(_offsets->seek_to_ordinal_and_calc_element_ordinal(r.begin()));
        size_t element_ordinal = _offsets->element_ordinal();
        // if array column in nullable or element of array is empty, element_read_range may be empty.
        // so we should reseek the element_ordinal
        if (element_read_range.span_size() == 0) {
            _keys->seek_to_ordinal(element_ordinal);
            _values->seek_to_ordinal(element_ordinal);
        }
        // 2. Read offset column
        // [1, 2, 3], [4, 5, 6]
        // In memory, it will be transformed to actual offset(0, 3, 6)
        // On disk, offset is stored as length array(3, 3)
        auto* offsets = map_column->offsets_column().get();
        auto& data = offsets->get_data();
        size_t end_offset = data.back();

        size_t prev_array_size = offsets->size();
        vectorized::SparseRange size_read_range(r);
        RETURN_IF_ERROR(_offsets->next_batch(size_read_range, offsets));
        size_t curr_array_size = offsets->size();

        size_t num_to_read = end_offset;
        for (size_t i = prev_array_size; i < curr_array_size; ++i) {
            end_offset += data[i];
            data[i] = end_offset;
        }
        num_to_read = end_offset - num_to_read;

        element_read_range.add(vectorized::Range(element_ordinal, element_ordinal + num_to_read));
    }

    // if array column is nullable, element_read_range may be empty
    DCHECK(element_read_range.empty() || (element_read_range.begin() == _keys->get_current_ordinal()));
    RETURN_IF_ERROR(_keys->next_batch(element_read_range, map_column->keys_column().get()));
    RETURN_IF_ERROR(_values->next_batch(element_read_range, map_column->values_column().get()));

    return Status::OK();
}

Status MapColumnIterator::fetch_values_by_rowid(const rowid_t* rowids, size_t size, vectorized::Column* values) {
    vectorized::MapColumn* map_column = nullptr;
    vectorized::NullColumn* null_column = nullptr;
    // 1. Read null column
    if (_nulls != nullptr) {
        auto* nullable_column = down_cast<vectorized::NullableColumn*>(values);
        map_column = down_cast<vectorized::MapColumn*>(nullable_column->data_column().get());
        null_column = down_cast<vectorized::NullColumn*>(nullable_column->null_column().get());
        RETURN_IF_ERROR(_nulls->fetch_values_by_rowid(rowids, size, null_column));
        nullable_column->update_has_null();
    } else {
        map_column = down_cast<vectorized::MapColumn*>(values);
    }

    // 2. Read offset column
    vectorized::UInt32Column array_size;
    array_size.reserve(size);
    RETURN_IF_ERROR(_offsets->fetch_values_by_rowid(rowids, size, &array_size));

    // [1, 2, 3], [4, 5, 6]
    // In memory, it will be transformed to actual offset(0, 3, 6)
    // On disk, offset is stored as length array(3, 3)
    auto* offsets = map_column->offsets_column().get();
    offsets->reserve(offsets->size() + array_size.size());
    size_t offset = offsets->get_data().back();
    for (size_t i = 0; i < array_size.size(); ++i) {
        offset += array_size.get_data()[i];
        offsets->append(offset);
    }

    // 3. Read elements
    for (size_t i = 0; i < size; ++i) {
        RETURN_IF_ERROR(_offsets->seek_to_ordinal_and_calc_element_ordinal(rowids[i]));
        size_t element_ordinal = _offsets->element_ordinal();
        size_t size_to_read = array_size.get_data()[i];

        RETURN_IF_ERROR(_keys->seek_to_ordinal(element_ordinal));
        RETURN_IF_ERROR(_keys->next_batch(&size_to_read, map_column->keys_column().get()));

        RETURN_IF_ERROR(_values->seek_to_ordinal(element_ordinal));
        RETURN_IF_ERROR(_values->next_batch(&size_to_read, map_column->values_column().get()));
    }
    return Status::OK();
}

Status MapColumnIterator::seek_to_first() {
    if (_nulls != nullptr) {
        RETURN_IF_ERROR(_nulls->seek_to_first());
    }
    RETURN_IF_ERROR(_offsets->seek_to_first());
    RETURN_IF_ERROR(_keys->seek_to_first());
    RETURN_IF_ERROR(_values->seek_to_first());
    return Status::OK();
}

Status MapColumnIterator::seek_to_ordinal(ordinal_t ord) {
    if (_nulls != nullptr) {
        RETURN_IF_ERROR(_nulls->seek_to_ordinal(ord));
    }
    RETURN_IF_ERROR(_offsets->seek_to_ordinal_and_calc_element_ordinal(ord));
    size_t element_ordinal = _offsets->element_ordinal();
    RETURN_IF_ERROR(_keys->seek_to_ordinal(element_ordinal));
    RETURN_IF_ERROR(_values->seek_to_ordinal(element_ordinal));
    return Status::OK();
}

} // namespace starrocks
