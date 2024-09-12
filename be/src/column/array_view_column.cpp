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

#include "column/array_view_column.h"
#include <cstdint>
#include <memory>
#include <stdexcept>
#include "column/array_column.h"
#include "column/chunk.h"
#include "column/vectorized_fwd.h"
#include "gutil/casts.h"

namespace starrocks {

ColumnPtr ArrayViewColumn::replicate(const Buffer<uint32_t>& offsets) {
    // @TODO clone empty???
    // auto dest = this->clone_empty();
    auto dest_size =  offsets.size() - 1;
    auto new_offsets = UInt32Column::create();
    auto new_lengths = UInt32Column::create();
    new_offsets->reserve(offsets.back());
    new_lengths->reserve(offsets.back());

    for (size_t i = 0;i < dest_size;i++) {
        uint32_t repeat_times = offsets[i + 1] - offsets[i];
        new_offsets->append_value_multiple_times(*_offsets, i, repeat_times);
        new_lengths->append_value_multiple_times(*_lengths, i, repeat_times);
    }
    return ArrayViewColumn::create(_elements, new_offsets, new_lengths);
}

void ArrayViewColumn::append(const Column& src, size_t offset, size_t count) {
    const auto& array_view_column = down_cast<const ArrayViewColumn&>(src);
    const auto& src_offsets = array_view_column.offsets();
    const auto& src_lengths = array_view_column.lengths();

    if (_elements == array_view_column._elements) {
        LOG(INFO) << "shared elements, only copy offsets and lengths";
        // if these two array view column share the same elements, just append offset and lengths
        _offsets->append(src_offsets, offset, count);
        _lengths->append(src_lengths, offset, count);
    } else {
        LOG(INFO) << "not shared elements, should copy all";
        // append elements and re-compute offset and length for new data
        // @TODO should optimize
        // @TODO should avoid this copy...
        uint32_t offset = _elements->size();
        for (size_t i = 0;i < count;i++) {
            uint32_t src_offset = src_offsets.get_data()[offset + i];
            uint32_t src_length = src_lengths.get_data()[offset + i];
            DCHECK_LE(src_offset + src_length, array_view_column._elements->size());
            _elements->append(*(array_view_column._elements), src_offset, src_length);
            _offsets->append(src_offset + offset);
            _lengths->append(src_length);
        }
    }
}

void ArrayViewColumn::check_or_die() const {
    DCHECK(_elements);
    DCHECK(_offsets);
    DCHECK(_lengths);
    DCHECK_EQ(_offsets->size(), _lengths->size());
    for (size_t i = 0;i < _offsets->size();i++) {
        uint32_t offset = _offsets->get_data()[i];
        uint32_t length = _lengths->get_data()[i];
        DCHECK_LE(offset + length, _elements->size());
    }
}

// @TODO clone should share elements?
MutableColumnPtr ArrayViewColumn::clone_empty() const {
    return create_mutable(_elements, UInt32Column::create(), UInt32Column::create());
}

StatusOr<ColumnPtr> ArrayViewColumn::to_array_column() const {
    LOG(INFO) << "ArrayViewColumn::to_array_column, cosnt ? " << is_constant();
    // @TODO consider nullable ???
    auto array_elements = _elements->clone_empty();
    auto array_offsets = UInt32Column::create();
    // @TODO reserve elements too?
    LOG(INFO) << "ArrayViewColumn::to_array_column, size: " << _offsets->size();
    array_offsets->reserve(_offsets->size() + 1);
    array_offsets->append(0);
    uint32_t last_offset = 0;
    size_t num_rows = _offsets->size();
    // @TODO maybe copy alot...
    for (size_t i = 0;i < num_rows;i++) {
        uint32_t offset = _offsets->get_data()[i];
        uint32_t length = _lengths->get_data()[i];
        LOG(INFO) << "offset: " << offset << ", len: " << length;
        // append lement
        array_elements->append(*_elements, offset, length);
        array_offsets->append(last_offset + length);
        last_offset += length;
    }
    return ArrayColumn::create(std::move(array_elements), std::move(array_offsets));
}


StatusOr<ColumnPtr> ArrayViewColumn::from_array_column(const ColumnPtr& column) {
    if (!column->is_array()) {
        LOG(INFO) << "from_array_column error...";
        return Status::InternalError("input column must be array column");
    }
    LOG(INFO) << "from_array_column, size: " << column->size();
    auto view_offsets = UInt32Column::create();
    auto view_lengths = UInt32Column::create();
    view_offsets->reserve(column->size());
    view_lengths->reserve(column->size());
    ColumnPtr view_elements;

    // const ArrayColumn* array_column = nullptr;
    if (column->is_nullable()) {
        auto nullable_column = down_cast<const NullableColumn*>(column.get());
        DCHECK(nullable_column != nullptr);
        const auto& null_data = nullable_column->null_column()->get_data();
        auto array_column = down_cast<const ArrayColumn*>(nullable_column->data_column().get());
        const auto& array_offsets = array_column->offsets().get_data();

        view_elements = array_column->elements_column();
        LOG(INFO) << "elements size: " << view_elements->size();
        LOG(INFO) << "null size: " << nullable_column->null_column()->size();
        // array column: [[1,2],null,[],[4]]
        //  null_data [0,1,0,0]
        //  elements column: [1,2,3,4]
        //  offsets column: [0, 2, 2, 2, 3]

        // array view column: [[1,2], null, [], [4]]
        // null_data[0,1,0,0]
        //  elements column: [1,2,3,4]
        //  offsets column: [0,2,2,3]
        //  length column:  [2,0,0,1]
        for (size_t i = 0;i < column->size(); i ++) {
            uint32_t offset = array_offsets[i];
            uint32_t length = null_data[i] ? 0: (array_offsets[i + 1] - offset);
            LOG(INFO) << "append offset: " << offset << ", length: " << length;
            view_offsets->append(offset);
            view_lengths->append(length);
        }
        auto ret = NullableColumn::create(ArrayViewColumn::create(view_elements, view_offsets, view_lengths), nullable_column->null_column());
        ret->check_or_die();
        return ret;
    } 

    auto array_column = down_cast<const ArrayColumn*>(column.get());
    view_elements = array_column->elements_column();
    const auto& array_offsets = array_column->offsets().get_data();

    for (size_t i = 0;i < column->size();i++) {
        uint32_t offset = array_offsets[i];
        uint32_t length = array_offsets[i + 1] - offset;
        view_offsets->append(offset);
        view_lengths->append(length);
    }
    return ArrayViewColumn::create(view_elements, view_offsets, view_lengths);
}

StatusOr<ColumnPtr> ArrayViewColumn::to_array_column(const ColumnPtr& column) {
    if (!column->is_array_view()) {
        LOG(INFO) << "to_array_column error....";
        return Status::InternalError("input column must be array view column");
    }

    if (column->is_nullable()) {
        auto nullable_column = down_cast<const NullableColumn*>(column.get());
        DCHECK(nullable_column != nullptr);
        auto array_view_column = down_cast<const ArrayViewColumn*>(nullable_column->data_column().get());
        LOG(INFO) << "to_array_column";
        ASSIGN_OR_RETURN(auto array_column, array_view_column->to_array_column());
        return NullableColumn::create(std::move(array_column), nullable_column->null_column());
    }
    auto array_view_column = down_cast<const ArrayViewColumn*>(column.get());
    return array_view_column->to_array_column();
}
}