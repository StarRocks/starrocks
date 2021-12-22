// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#pragma once

#include <utility>

#include "column/column_helper.h"
#include "util/raw_container.h"

namespace starrocks {
namespace vectorized {

template <PrimitiveType Type>
class ColumnBuilder {
public:
    using DataColumnPtr = typename RunTimeColumnType<Type>::Ptr;
    using NullColumnPtr = NullColumn::Ptr;
    using DatumType = RunTimeCppType<Type>;

    explicit ColumnBuilder(int reserve_size = config::vector_chunk_size)
            : _column(RunTimeColumnType<Type>::create()), _null_column(nullptr) {
        static_assert(!pt_is_decimal<Type>, "Not support Decimal32/64/128 types");
        reserve(reserve_size);
    }

    ColumnBuilder(int precision, int scale, int reserve_size = config::vector_chunk_size)
            : _column(RunTimeColumnType<Type>::create()), _null_column(nullptr) {
        reserve(reserve_size);
        if constexpr (pt_is_decimal<Type>) {
            static constexpr auto max_precision = decimal_precision_limit<DatumType>;
            DCHECK(0 <= scale && scale <= precision && precision <= max_precision);
            auto raw_column = ColumnHelper::cast_to_raw<Type>(_column);
            raw_column->set_precision(precision);
            raw_column->set_scale(scale);
        }
    }

    ColumnBuilder(DataColumnPtr column, NullColumnPtr null_column, bool has_null)
            : _column(std::move(column)), _null_column(has_null ? std::move(null_column) : nullptr) {}

    void append(const DatumType& value) {
        _column->append(value);
        if (_null_column != nullptr) {
            _null_column->append(DATUM_NOT_NULL);
        }
    }

    void append(const DatumType& value, bool is_null) {
        if (is_null & (_null_column == nullptr)) {
            //      ^ using '&' instead of '&&' intended
            create_null_column();
        }
        if (_null_column != nullptr) {
            _null_column->append(is_null);
        }
        _column->append(value);
    }

    void append_null() {
        if (_null_column == nullptr) {
            create_null_column();
        }
        _null_column->append(DATUM_NULL);
        _column->append_default();
    }

    bool has_null() { return _null_column != nullptr; }

    ColumnPtr build(bool is_const) {
        if (is_const && has_null()) {
            return ColumnHelper::create_const_null_column(_column->size());
        }

        if (is_const) {
            return ConstColumn::create(std::move(_column), _column->size());
        } else if (has_null()) {
            DCHECK_EQ(_column->size(), _null_column->size());
            return NullableColumn::create(std::move(_column), std::move(_null_column));
        } else {
            return std::move(_column);
        }
    }

    void reserve(int size) {
        _column->reserve(size);
        if (_null_column != nullptr) {
            _null_column->reserve(size);
        }
    }

    const DataColumnPtr& data_column() const { return _column; }

protected:
    void create_null_column() {
        _null_column = NullColumn::create();
        _null_column->reserve(_column->capacity());
        _null_column->resize(_column->size());
    }

    DataColumnPtr _column;
    NullColumnPtr _null_column;
};

class NullableBinaryColumnBuilder : public ColumnBuilder<TYPE_VARCHAR> {
public:
    using ColumnType = RunTimeColumnType<TYPE_VARCHAR>;
    using Offsets = ColumnType::Offsets;

    NullableBinaryColumnBuilder() : ColumnBuilder(0) {}

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
        if (_null_column != nullptr) {
            _null_column->get_data().resize(num_rows);
        }
    }

    // mark i-th resulting element is null
    void set_null(size_t i) {
        if (_null_column == nullptr) {
            create_null_column();
        }
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
    void append_partial(uint8_t* begin, uint8_t* end) {
        Bytes& bytes = _column->get_bytes();
        bytes.insert(bytes.end(), begin, end);
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

    NullColumnPtr get_null_column() {
        if (_null_column == nullptr) {
            create_null_column();
        }
        return _null_column;
    }

    NullColumn::Container& get_null_data() {
        if (_null_column == nullptr) {
            create_null_column();
        }
        return _null_column->get_data();
    }

    // has_null = true means the finally resulting NullableColumn has nulls.
    void set_has_null(bool has_null) {
        if (has_null & (_null_column == nullptr)) {
            create_null_column();
        }
    }

private:
};
} // namespace vectorized
} // namespace starrocks
