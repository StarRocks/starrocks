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

#include "column/fixed_length_column_base.h"
namespace starrocks {
template <typename T>
class FixedLengthColumn final : public ColumnFactory<FixedLengthColumnBase<T>, FixedLengthColumn<T>, Column> {
public:
    using ValueType = T;
    using Container = Buffer<ValueType>;
    using SuperClass = ColumnFactory<FixedLengthColumnBase<T>, FixedLengthColumn<T>, Column>;
    FixedLengthColumn() = default;

    explicit FixedLengthColumn(const size_t n) : SuperClass(n) {}

    FixedLengthColumn(const size_t n, const ValueType x) : SuperClass(n, x) {}

    FixedLengthColumn(const FixedLengthColumn& src) : SuperClass((const FixedLengthColumnBase<T>&)(src)) {}
    MutableColumnPtr clone_empty() const override { return this->create_mutable(); }
};
} // namespace starrocks
