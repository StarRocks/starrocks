// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#pragma once

#include "column/binary_column_base.h"
#include "column/bytes.h"

namespace starrocks::vectorized {

class BinaryColumn final : public ColumnFactory<BinaryColumnBase<uint32_t>, BinaryColumn, Column> {
public:
    using SuperClass = ColumnFactory<BinaryColumnBase<uint32_t>, BinaryColumn, Column>;

    BinaryColumn() = default;

    BinaryColumn(BinaryColumn::Bytes bytes, BinaryColumn::Offsets offsets)
            : SuperClass(std::move(bytes), std::move(offsets)) {}

    BinaryColumn(const BinaryColumn& rhs) : SuperClass(rhs) {}

    BinaryColumn(BinaryColumn&& rhs) noexcept : SuperClass(std::move(rhs)) {}

    // The size limit of a single element is 2^32.
    // The size limit of all elements is 2^32.
    // The number limit of elements is 2^32.
    bool reach_capacity_limit() const override {
        return _bytes.size() >= Column::MAX_CAPACITY_LIMIT || _offsets.size() >= Column::MAX_CAPACITY_LIMIT ||
               _slices.size() >= Column::MAX_CAPACITY_LIMIT;
    }

    std::string get_name() const override { return "binary"; }

    MutableColumnPtr clone_empty() const override { return create_mutable(); }
};

using Offsets = BinaryColumn::Offsets;

} // namespace starrocks::vectorized
