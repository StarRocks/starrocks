// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#pragma once

#include "column/fixed_length_column_base.h"
namespace starrocks::vectorized {
template <typename T>
class FixedLengthColumn final : public ColumnFactory<FixedLengthColumnBase<T>, FixedLengthColumn<T>, Column> {
public:
    using ValueType = T;
    using Container = Buffer<ValueType>;
    using SuperClass = ColumnFactory<FixedLengthColumnBase<T>, FixedLengthColumn<T>, Column>;
    FixedLengthColumn() = default;

    //FixedLengthColumn(const bool pool, const size_t chunk_size, const bool unused): SuperClass(pool, chunk_size) {}

    explicit FixedLengthColumn(const size_t n) : SuperClass(n) {}

    FixedLengthColumn(const size_t n, const ValueType x) : SuperClass(n, x) {}

    FixedLengthColumn(const FixedLengthColumn& src) : SuperClass((const FixedLengthColumnBase<T>&)(src)) {}
    MutableColumnPtr clone_empty() const override { return this->create_mutable(); }

    static typename SuperClass::MutablePtr mutate(typename SuperClass::Ptr ptr) { return ptr->shallowMutate(); }
};
} // namespace starrocks::vectorized
