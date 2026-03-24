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

#include <types/decimalv3.h>

#include "base/decimal_types.h"
#include "column/column.h"
#include "column/fixed_length_column_base.h"

namespace starrocks {

template <typename T>
class DecimalV3Column final
        : public CowFactory<ColumnFactory<FixedLengthColumnBase<T>, DecimalV3Column<DecimalType<T>>>,
                            DecimalV3Column<DecimalType<T>>, Column> {
    friend class CowFactory<ColumnFactory<FixedLengthColumnBase<T>, DecimalV3Column<DecimalType<T>>>,
                            DecimalV3Column<DecimalType<T>>, Column>;

public:
    using SuperClass =
            CowFactory<ColumnFactory<FixedLengthColumnBase<T>, DecimalV3Column<DecimalType<T>>>,
                       DecimalV3Column<DecimalType<T>>, Column>;

    DecimalV3Column() : DecimalV3Column(memory::get_default_column_allocator()) {}
    explicit DecimalV3Column([[maybe_unused]] memory::Allocator* allocator) : SuperClass(allocator) {}
    explicit DecimalV3Column(size_t num_rows);
    DecimalV3Column([[maybe_unused]] memory::Allocator* allocator, size_t num_rows);
    DecimalV3Column(int precision, int scale);
    DecimalV3Column([[maybe_unused]] memory::Allocator* allocator, int precision, int scale);
    DecimalV3Column(int precision, int scale, size_t num_rows);
    DecimalV3Column([[maybe_unused]] memory::Allocator* allocator, int precision, int scale, size_t num_rows);

    DISALLOW_COPY_TEMPLATE(DecimalV3Column, DecimalV3Column<DecimalType<T>>);

    bool is_decimal() const override;
    bool is_numeric() const override;
    void set_precision(int precision);
    void set_scale(int scale);
    int precision() const;
    int scale() const;

    MutableColumnPtr clone_empty(memory::Allocator* allocator = nullptr) const override {
        memory::Allocator* alloc = allocator ? allocator : memory::get_default_column_allocator();
        return this->create(alloc, _precision, _scale);
    }

    MutableColumnPtr clone(memory::Allocator* allocator = nullptr) const override {
        memory::Allocator* alloc = allocator ? allocator : memory::get_default_column_allocator();
        auto p = clone_empty(alloc);
        p->append(*this, 0, this->size());
        return p;
    }

    void put_mysql_row_buffer(MysqlRowBuffer* buf, size_t idx, bool is_binary_protocol = false) const override;
    std::string debug_item(size_t idx) const override;
    int64_t xor_checksum(uint32_t from, uint32_t to) const override;

private:
    int _precision = decimal_precision_limit<T>;
    int _scale = 0;
};

} // namespace starrocks
