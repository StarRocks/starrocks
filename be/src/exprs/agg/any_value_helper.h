// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#pragma once

#include <limits>
#include <type_traits>

#include "column/fixed_length_column.h"
#include "column/type_traits.h"
#include "exprs/agg/aggregate.h"
#include "gutil/casts.h"
#include "util/raw_container.h"

namespace starrocks::vectorized {

template <PrimitiveType PT, typename = guard::Guard>
struct AggregateData {};

template <PrimitiveType PT>
struct AggregateData<PT, FixedLengthPTGuard<PT>> {
    using T = RunTimeCppType<PT>;
    using ColumnType = RunTimeColumnType<PT>;

    bool has() { return has_value; }

    void reset() { has_value = false; }

    void change(const Column& column, size_t row_num) {
        const auto& target_column = down_cast<const ColumnType&>(column);
        value = target_column.get_data()[row_num];
    }

    void change(const T& to) {
        has_value = true;
        value = to;
    }

    bool change_if_less(const Column& column, size_t row_num) {
        const auto& target_column = down_cast<const ColumnType&>(column);
        const T target_value = target_column.get_data()[row_num];
        if (!has_value || target_value < value) {
            value = target_value;
            has_value = true;
            return true;
        } else {
            return false;
        }
    }

    bool change_if_less(const T& right) {
        if (!has_value || right < value) {
            value = right;
            has_value = true;
            return true;
        } else {
            return false;
        }
    }

    bool change_if_greater(const Column& column, size_t row_num) {
        const auto& target_column = down_cast<const ColumnType&>(column);
        const T target_value = target_column.get_data()[row_num];
        if (!has_value || target_value > value) {
            value = target_value;
            has_value = true;
            return true;
        } else {
            return false;
        }
    }

    bool change_if_greater(const T& right) {
        if (!has_value || right > value) {
            value = right;
            has_value = true;
            return true;
        } else {
            return false;
        }
    }
    
    T value;
    bool has_value = false;
};

template <PrimitiveType PT>
struct AggregateData<PT, BinaryPTGuard<PT>> {
    using T = RunTimeCppType<PT>;
    using ColumnType = RunTimeColumnType<PT>;

public:
    bool has() { return size > -1; }

    void reset() {
        buffer.clear();
        size = -1;
    }

    Slice get_value() const { return {buffer.data(), buffer.size()}; }

    void change(const Column& column, size_t row_num) {
        DCHECK(column.is_binary());
        Slice right_value = column.get(row_num).get_slice();
        buffer.resize(right_value.size);
        memcpy(buffer.data(), right_value.data, right_value.size);
        size = right_value.size;
    }

    bool change_if_less(const Column& column, size_t row_num) {
        DCHECK(column.is_binary());
        Slice right_value = column.get(row_num).get_slice();
        if (!has() || get_value().compare(right_value) > 0) {
            change(column, row_num);
        } else {
            return false;
        }
    }

    bool change_if_greater(const Column& column, size_t row_num) {
        DCHECK(column.is_binary());
        Slice right_value = column.get(row_num).get_slice();
        if (!has() || get_value().compare(right_value) < 0) {
            change(column, row_num);
        } else {
            return false;
        }
    }

private:
    int32_t size = -1;
    Buffer<uint8_t> buffer;
};

} // namespace starrocks::vectorized
