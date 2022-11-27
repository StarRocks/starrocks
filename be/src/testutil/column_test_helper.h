// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#ifndef STARROCKS_COLUMN_BUILDER_H
#define STARROCKS_COLUMN_BUILDER_H

#include "column/array_column.h"
#include "column/column_helper.h"

namespace starrocks::vectorized {

class ColumnTestHelper {
public:
    template <class T>
    static ColumnPtr build_column(const std::vector<T>& values) {
        if constexpr (std::is_same_v<uint8_t, T>) {
            auto data = UInt8Column::create();
            data->append_numbers(values.data(), values.size() * sizeof(T));
            return data;
        } else if constexpr (std::is_same_v<T, int32_t>) {
            auto data = Int32Column::create();
            data->append_numbers(values.data(), values.size() * sizeof(T));
            return data;
        } else if constexpr (std::is_same_v<T, int64_t>) {
            auto data = Int64Column::create();
            data->append_numbers(values.data(), values.size() * sizeof(T));
            return data;
        } else if constexpr (std::is_same_v<T, Slice>) {
            auto data = BinaryColumn::create();
            data->append_strings(values);
            return data;
        } else if constexpr (std::is_same_v<T, double>) {
            auto data = DoubleColumn ::create();
            data->append_numbers(values.data(), values.size() * sizeof(T));
            return data;
        } else {
            throw std::runtime_error("Type is not supported in build_column.");
        }
    }

    template <class T>
    static ColumnPtr build_nullable_column(const std::vector<T>& values, const std::vector<uint8_t>& nullflags) {
        DCHECK_EQ(values.size(), nullflags.size());
        auto null = NullColumn::create();
        null->append_numbers(nullflags.data(), nullflags.size());
        auto data = build_column<T>(values);
        return NullableColumn::create(std::move(data), std::move(null));
    }

    template <LogicalType TYPE>
    static ColumnPtr create_nullable_column() {
        return NullableColumn::create(RunTimeColumnType<TYPE>::create(), RunTimeColumnType<TYPE_NULL>::create());
    }
};

} // namespace starrocks::vectorized
#endif //STARROCKS_COLUMN_BUILDER_H
