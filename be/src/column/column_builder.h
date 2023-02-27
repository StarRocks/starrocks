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

#pragma once

#include <type_traits>
#include <utility>

#include "column/column_helper.h"
#include "column/type_traits.h"
#include "util/raw_container.h"

namespace starrocks {

template <LogicalType Type>
class ColumnBuilder {
public:
    using DataColumnPtr = typename RunTimeColumnType<Type>::Ptr;
    using NullColumnPtr = NullColumn::Ptr;
    using DatumType = RunTimeCppType<Type>;
    using MovableType = RunTimeCppMovableType<Type>;

    ColumnBuilder(int32_t chunk_size) {
        static_assert(!lt_is_decimal<Type>, "Not support Decimal32/64/128 types");
        _has_null = false;
        _column = RunTimeColumnType<Type>::create();
        _null_column = NullColumn::create();
        reserve(chunk_size);
    }

    ColumnBuilder(int32_t chunk_size, int precision, int scale) {
        _has_null = false;
        _column = RunTimeColumnType<Type>::create();
        _null_column = NullColumn::create();
        reserve(chunk_size);

        if constexpr (lt_is_decimal<Type>) {
            static constexpr auto max_precision = decimal_precision_limit<DatumType>;
            DCHECK(0 <= scale && scale <= precision && precision <= max_precision);
            auto raw_column = ColumnHelper::cast_to_raw<Type>(_column);
            raw_column->set_precision(precision);
            raw_column->set_scale(scale);
        }
    }

    ColumnBuilder(DataColumnPtr column, NullColumnPtr null_column, bool has_null)
            : _column(std::move(column)), _null_column(std::move(null_column)), _has_null(has_null) {}
    //do nothing ctor, members are initialized by its offsprings.
    explicit ColumnBuilder<Type>(void*) {}

    void append(const DatumType& value) {
        _null_column->append(DATUM_NOT_NULL);
        _column->append(value);
    }

    void append(MovableType value) {
        _null_column->append(DATUM_NOT_NULL);
        _column->append(std::move(value));
    }

    void append(const DatumType& value, bool is_null) {
        _has_null = _has_null | is_null;
        _null_column->append(is_null);
        _column->append(value);
    }

    void append(MovableType value, bool is_null) {
        _has_null = _has_null | is_null;
        _null_column->append(is_null);
        _column->append(std::move(value));
    }

    void append_null() {
        _has_null = true;
        _null_column->append(DATUM_NULL);
        _column->append_default();
    }

    ColumnPtr build(bool is_const) {
        if (is_const && _has_null) {
            return ColumnHelper::create_const_null_column(_column->size());
        }

        if (is_const) {
            return ConstColumn::create(_column, _column->size());
        } else if (_has_null) {
            return NullableColumn::create(_column, _null_column);
        } else {
            return _column;
        }
    }

    ColumnPtr build_nullable_column() { return NullableColumn::create(_column, _null_column); }

    void reserve(size_t size) {
        _column->reserve(size);
        _null_column->reserve(size);
    }

    void resize_uninitialized(size_t size) {
        _column->resize_uninitialized(size);
        _null_column->resize_uninitialized(size);
    }

    DataColumnPtr data_column() { return _column; }
    NullColumnPtr null_column() { return _null_column; }
    void set_has_null(bool v) { _has_null = v; }

protected:
    DataColumnPtr _column;
    NullColumnPtr _null_column;
    bool _has_null;
};

class NullableBinaryColumnBuilder : public ColumnBuilder<TYPE_VARCHAR> {
public:
    using ColumnType = RunTimeColumnType<TYPE_VARCHAR>;
    using Offsets = ColumnType::Offsets;
    NullableBinaryColumnBuilder() : ColumnBuilder(nullptr) {
        _column = ColumnType::create();
        _null_column = NullColumn::create();
        _has_null = false;
    }

    // allocate enough room for offsets and null_column
    // reserve bytes_size bytes for Bytes. size of offsets
    // and null_column are deterministic, so proper memory
    // room can be allocated, but bytes' size is non-deterministic,
    // so just reserve moderate memory room. offsets need no
    // initialization(raw::make_room), because it is overwritten
    // fully. null_columns should be zero-out(resize), just
    // slot corresponding to null elements is marked to 1.
    void resize(size_t num_rows, size_t bytes_size) {
        _column->get_bytes().reserve(bytes_size);
        auto& offsets = _column->get_offset();
        raw::make_room(&offsets, num_rows + 1);
        offsets[0] = 0;
        _null_column->get_data().resize(num_rows);
    }

    // mark i-th resulting element is null
    void set_null(size_t i) {
        _has_null = true;
        Bytes& bytes = _column->get_bytes();
        Offsets& offsets = _column->get_offset();
        NullColumn::Container& nulls = _null_column->get_data();
        offsets[i + 1] = bytes.size();
        nulls[i] = 1;
    }

    void append_empty(size_t i) {
        Bytes& bytes = _column->get_bytes();
        Offsets& offsets = _column->get_offset();
        offsets[i + 1] = bytes.size();
    }

    void append(uint8_t* begin, uint8_t* end, size_t i) {
        Bytes& bytes = _column->get_bytes();
        Offsets& offsets = _column->get_offset();
        bytes.insert(bytes.end(), begin, end);
        offsets[i + 1] = bytes.size();
    }
    // for concat and concat_ws, several columns are concatenated
    // together into a string, so append must be invoked as many times
    // as the number of evolving columns; however, the offset is updated
    // only once, so we split the append into append_partial and append_complete
    // as follows
    void append_partial(const uint8_t* begin, const uint8_t* end) {
        Bytes& bytes = _column->get_bytes();
        bytes.insert(bytes.end(), begin, end);
    }

    void append_partial(const Slice& slice) {
        const auto* begin = reinterpret_cast<const uint8_t*>(slice.data);
        const auto* end = begin + slice.size;

        append_partial(begin, end);
    }

    void append_complete(size_t i) {
        Bytes& bytes = _column->get_bytes();
        Offsets& offsets = _column->get_offset();
        offsets[i + 1] = bytes.size();
    }

    // move current ptr backwards for n bytes, used in concat_ws
    void rewind(size_t n) {
        Bytes& bytes = _column->get_bytes();
        bytes.resize(bytes.size() - n);
    }

    NullColumnPtr get_null_column() { return _null_column; }

    NullColumn::Container& get_null_data() { return _null_column->get_data(); }

    // has_null = true means the finally resulting NullableColumn has nulls.
    void set_has_null(bool has_null) { _has_null = has_null; }

private:
};
} // namespace starrocks
