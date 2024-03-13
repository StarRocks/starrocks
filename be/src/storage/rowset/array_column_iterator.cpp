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

#include "storage/rowset/array_column_iterator.h"

#include "column/array_column.h"
#include "column/column_access_path.h"
#include "column/const_column.h"
#include "column/nullable_column.h"
#include "storage/rowset/scalar_column_iterator.h"

namespace starrocks {

ArrayColumnIterator::ArrayColumnIterator(ColumnReader* reader, std::unique_ptr<ColumnIterator> null_iterator,
                                         std::unique_ptr<ColumnIterator> array_size_iterator,
                                         std::unique_ptr<ColumnIterator> element_iterator, const ColumnAccessPath* path)
        : _reader(reader),
          _null_iterator(std::move(null_iterator)),
          _array_size_iterator(std::move(array_size_iterator)),
          _element_iterator(std::move(element_iterator)),
          _path(std::move(path)) {}

Status ArrayColumnIterator::init(const ColumnIteratorOptions& opts) {
    if (_null_iterator != nullptr) {
        RETURN_IF_ERROR(_null_iterator->init(opts));
    }
    RETURN_IF_ERROR(_array_size_iterator->init(opts));
    RETURN_IF_ERROR(_element_iterator->init(opts));

    // only offset
    if (_path != nullptr && _path->children().size() == 1 && _path->children()[0]->is_offset()) {
        _access_values = false;
    }

    if (opts.check_dict_encoding) {
        _is_string_element = true;
    }

    return Status::OK();
}

// unpack array column, return: null_column, element_column, offset_column
static inline std::tuple<ArrayColumn*, NullColumn*> unpack_array_column(Column* col) {
    NullColumn* array_null = nullptr;
    ArrayColumn* array_col = nullptr;

    if (col->is_nullable()) {
        auto nullable = down_cast<NullableColumn*>(col);
        array_col = down_cast<ArrayColumn*>(nullable->data_column().get());
        array_null = down_cast<NullColumn*>(nullable->null_column().get());
    } else {
        array_col = down_cast<ArrayColumn*>(col);
    }
    return {array_col, array_null};
}

Status ArrayColumnIterator::next_batch_null_offsets(size_t* n, UInt32Column* offsets, UInt8Column* nulls,
                                                    size_t* element_rows) {
    // 1. Read null column
    if (_null_iterator != nullptr) {
        RETURN_IF_ERROR(_null_iterator->next_batch(n, nulls));
    }

    // 2. Read offset column
    // [1, 2, 3], [4, 5, 6]
    // In memory, it will be transformed to actual offset(0, 3, 6)
    // On disk, offset is stored as length array(3, 3)
    auto& data = offsets->get_data();
    size_t end_offset = data.back();

    size_t prev_array_size = offsets->size();
    RETURN_IF_ERROR(_array_size_iterator->next_batch(n, offsets));
    size_t curr_array_size = offsets->size();

    size_t num_to_read = end_offset;
    for (size_t i = prev_array_size; i < curr_array_size; ++i) {
        end_offset += data[i];
        data[i] = end_offset;
    }
    *element_rows = end_offset - num_to_read;
    return Status::OK();
}

Status ArrayColumnIterator::next_batch(size_t* n, Column* dst) {
    auto [array_column, nulls] = unpack_array_column(dst);
    size_t num_to_read = 0;
    RETURN_IF_ERROR(next_batch_null_offsets(n, array_column->offsets_column().get(), nulls, &num_to_read));

    if (_null_iterator != nullptr) {
        down_cast<NullableColumn*>(dst)->update_has_null();
    }

    // 3. Read elements
    if (_access_values) {
        RETURN_IF_ERROR(_element_iterator->next_batch(&num_to_read, array_column->elements_column().get()));
    } else {
        if (!array_column->elements_column()->is_constant()) {
            array_column->elements_column()->append_default(1);
            array_column->elements_column() = ConstColumn::create(array_column->elements_column(), num_to_read);
        } else {
            array_column->elements_column()->append_default(num_to_read);
        }
    }

    return Status::OK();
}

Status ArrayColumnIterator::next_batch_null_offsets(const SparseRange<>& range, UInt32Column* offsets,
                                                    UInt8Column* nulls, SparseRange<>* element_range,
                                                    size_t* element_rows) {
    // 1. Read null column
    if (_null_iterator != nullptr) {
        RETURN_IF_ERROR(_null_iterator->next_batch(range, nulls));
    }

    SparseRangeIterator<> iter = range.new_iterator();
    size_t to_read = range.span_size();

    // array column can be nested, range may be empty
    DCHECK(range.empty() || (range.begin() == _array_size_iterator->get_current_ordinal()));
    while (iter.has_more()) {
        Range<> r = iter.next(to_read);

        RETURN_IF_ERROR(_array_size_iterator->seek_to_ordinal_and_calc_element_ordinal(r.begin()));
        size_t element_ordinal = _array_size_iterator->element_ordinal();
        // if array column in nullable or element of array is empty, element_range may be empty.
        // so we should reseek the element_ordinal
        if (element_range->span_size() == 0) {
            RETURN_IF_ERROR(_element_iterator->seek_to_ordinal(element_ordinal));
        }
        // 2. Read offset column
        // [1, 2, 3], [4, 5, 6]
        // In memory, it will be transformed to actual offset(0, 3, 6)
        // On disk, offset is stored as length array(3, 3)
        auto& data = offsets->get_data();
        size_t end_offset = data.back();

        size_t prev_array_size = offsets->size();
        SparseRange<> size_read_range(r);
        RETURN_IF_ERROR(_array_size_iterator->next_batch(size_read_range, offsets));
        size_t curr_array_size = offsets->size();

        size_t num_to_read = end_offset;
        for (size_t i = prev_array_size; i < curr_array_size; ++i) {
            end_offset += data[i];
            data[i] = end_offset;
        }
        num_to_read = end_offset - num_to_read;
        *element_rows += num_to_read;

        element_range->add(Range<>(element_ordinal, element_ordinal + num_to_read));
    }

    return Status::OK();
}

Status ArrayColumnIterator::next_batch(const SparseRange<>& range, Column* dst) {
    auto [array_column, null_column] = unpack_array_column(dst);
    CHECK((_null_iterator == nullptr && null_column == nullptr) ||
          (_null_iterator != nullptr && null_column != nullptr));

    SparseRange element_read_range;
    size_t read_rows = 0;
    RETURN_IF_ERROR(next_batch_null_offsets(range, array_column->offsets_column().get(), null_column,
                                            &element_read_range, &read_rows));

    if (_null_iterator != nullptr) {
        down_cast<NullableColumn*>(dst)->update_has_null();
    }

    if (_access_values) {
        // if array column is nullable, element_read_range may be empty
        DCHECK(element_read_range.empty() || (element_read_range.begin() == _element_iterator->get_current_ordinal()));
        RETURN_IF_ERROR(_element_iterator->next_batch(element_read_range, array_column->elements_column().get()));
    } else {
        if (!array_column->elements_column()->is_constant()) {
            array_column->elements_column()->append_default(1);
            array_column->elements_column() = ConstColumn::create(array_column->elements_column(), read_rows);
        } else {
            array_column->elements_column()->append_default(read_rows);
        }
    }

    return Status::OK();
}

Status ArrayColumnIterator::fetch_values_by_rowid(const rowid_t* rowids, size_t size, Column* values) {
    auto [array_column, null_column] = unpack_array_column(values);
    // 1. Read null column
    if (_null_iterator != nullptr) {
        RETURN_IF_ERROR(_null_iterator->fetch_values_by_rowid(rowids, size, null_column));
        down_cast<NullableColumn*>(values)->update_has_null();
    }

    // 2. Read offset column
    UInt32Column array_size;
    array_size.reserve(size);
    RETURN_IF_ERROR(_array_size_iterator->fetch_values_by_rowid(rowids, size, &array_size));

    // [1, 2, 3], [4, 5, 6]
    // In memory, it will be transformed to actual offset(0, 3, 6)
    // On disk, offset is stored as length array(3, 3)
    auto* offsets = array_column->offsets_column().get();
    offsets->reserve(offsets->size() + array_size.size());
    size_t offset = offsets->get_data().back();
    for (size_t i = 0; i < array_size.size(); ++i) {
        offset += array_size.get_data()[i];
        offsets->append(offset);
    }

    // 3. Read elements
    if (_access_values) {
        for (size_t i = 0; i < size; ++i) {
            RETURN_IF_ERROR(_array_size_iterator->seek_to_ordinal_and_calc_element_ordinal(rowids[i]));
            size_t element_ordinal = _array_size_iterator->element_ordinal();
            RETURN_IF_ERROR(_element_iterator->seek_to_ordinal(element_ordinal));
            size_t size_to_read = array_size.get_data()[i];
            RETURN_IF_ERROR(_element_iterator->next_batch(&size_to_read, array_column->elements_column().get()));
        }
    } else {
        if (!array_column->elements_column()->is_constant()) {
            array_column->elements_column()->append_default(1);
            array_column->elements_column() = ConstColumn::create(array_column->elements_column());
        }

        size_t size_to_read = 0;
        for (size_t i = 0; i < size; ++i) {
            RETURN_IF_ERROR(_array_size_iterator->seek_to_ordinal_and_calc_element_ordinal(rowids[i]));
            size_t element_ordinal = _array_size_iterator->element_ordinal();
            RETURN_IF_ERROR(_element_iterator->seek_to_ordinal(element_ordinal));
            size_to_read += array_size.get_data()[i];
        }

        array_column->elements_column()->append_default(size_to_read);
    }

    return Status::OK();
}

Status ArrayColumnIterator::seek_to_first() {
    if (_null_iterator != nullptr) {
        RETURN_IF_ERROR(_null_iterator->seek_to_first());
    }
    RETURN_IF_ERROR(_array_size_iterator->seek_to_first());
    RETURN_IF_ERROR(_element_iterator->seek_to_first());
    return Status::OK();
}

Status ArrayColumnIterator::seek_to_ordinal(ordinal_t ord) {
    if (_null_iterator != nullptr) {
        RETURN_IF_ERROR(_null_iterator->seek_to_ordinal(ord));
    }
    RETURN_IF_ERROR(_array_size_iterator->seek_to_ordinal_and_calc_element_ordinal(ord));
    size_t element_ordinal = _array_size_iterator->element_ordinal();
    RETURN_IF_ERROR(_element_iterator->seek_to_ordinal(element_ordinal));
    return Status::OK();
}

Status ArrayColumnIterator::get_row_ranges_by_zone_map(const std::vector<const ColumnPredicate*>& predicates,
                                                       const ColumnPredicate* del_predicate,
                                                       SparseRange<>* row_ranges) {
    row_ranges->add({0, static_cast<rowid_t>(_reader->num_rows())});
    return Status::OK();
}

bool ArrayColumnIterator::all_page_dict_encoded() const {
    if (_is_string_element) {
        return _element_iterator->all_page_dict_encoded();
    }
    return false;
}

Status ArrayColumnIterator::fetch_all_dict_words(std::vector<Slice>* words) const {
    return _element_iterator->fetch_all_dict_words(words);
}

Status ArrayColumnIterator::next_dict_codes(size_t* n, Column* dst) {
    auto [array_column, nulls] = unpack_array_column(dst);
    size_t num_to_read = 0;
    RETURN_IF_ERROR(next_batch_null_offsets(n, array_column->offsets_column().get(), nulls, &num_to_read));

    if (_null_iterator != nullptr) {
        down_cast<NullableColumn*>(dst)->update_has_null();
    }

    RETURN_IF_ERROR(_element_iterator->next_dict_codes(&num_to_read, array_column->elements_column().get()));
    return Status::OK();
}

Status ArrayColumnIterator::next_dict_codes(const SparseRange<>& range, Column* dst) {
    auto [array_column, null_column] = unpack_array_column(dst);
    CHECK((_null_iterator == nullptr && null_column == nullptr) ||
          (_null_iterator != nullptr && null_column != nullptr));

    SparseRange element_read_range;
    size_t read_rows = 0;
    RETURN_IF_ERROR(next_batch_null_offsets(range, array_column->offsets_column().get(), null_column,
                                            &element_read_range, &read_rows));

    if (_null_iterator != nullptr) {
        down_cast<NullableColumn*>(dst)->update_has_null();
    }

    // if array column is nullable, element_read_range may be empty
    DCHECK(element_read_range.empty() || (element_read_range.begin() == _element_iterator->get_current_ordinal()));
    RETURN_IF_ERROR(_element_iterator->next_dict_codes(element_read_range, array_column->elements_column().get()));

    return Status::OK();
}

Status ArrayColumnIterator::fetch_dict_codes_by_rowid(const rowid_t* rowids, size_t size, Column* values) {
    auto [array_column, null_column] = unpack_array_column(values);
    // 1. Read null column
    if (_null_iterator != nullptr) {
        RETURN_IF_ERROR(_null_iterator->fetch_values_by_rowid(rowids, size, null_column));
        down_cast<NullableColumn*>(values)->update_has_null();
    }

    // 2. Read offset column
    UInt32Column array_size;
    array_size.reserve(size);
    RETURN_IF_ERROR(_array_size_iterator->fetch_values_by_rowid(rowids, size, &array_size));

    auto* offsets = array_column->offsets_column().get();
    offsets->reserve(offsets->size() + array_size.size());
    size_t offset = offsets->get_data().back();
    for (size_t i = 0; i < array_size.size(); ++i) {
        offset += array_size.get_data()[i];
        offsets->append(offset);
    }

    // 3. Read elements
    for (size_t i = 0; i < size; ++i) {
        RETURN_IF_ERROR(_array_size_iterator->seek_to_ordinal_and_calc_element_ordinal(rowids[i]));
        size_t element_ordinal = _array_size_iterator->element_ordinal();
        RETURN_IF_ERROR(_element_iterator->seek_to_ordinal(element_ordinal));
        size_t size_to_read = array_size.get_data()[i];
        RETURN_IF_ERROR(_element_iterator->next_dict_codes(&size_to_read, array_column->elements_column().get()));
    }

    return Status::OK();
}

Status ArrayColumnIterator::decode_dict_codes(const int32_t* codes, size_t size, Column* words) {
    return Status::NotSupported("ArrayColumn don't support local low-cardinality dict optimization");
}

} // namespace starrocks
