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

#include <runtime/decimalv3.h>

#include "column/column.h"
#include "column/fixed_length_column_base.h"
#include "util/decimal_types.h"
#include "util/mysql_row_buffer.h"

namespace starrocks {

template <typename T>
class DecimalV3Column final : public ColumnFactory<FixedLengthColumnBase<T>, DecimalV3Column<DecimalType<T>>, Column> {
public:
    DecimalV3Column() = default;
    explicit DecimalV3Column(size_t num_rows);
    DecimalV3Column(int precision, int scale);
    DecimalV3Column(int precision, int scale, size_t num_rows);

    DecimalV3Column(DecimalV3Column const&) = default;
    DecimalV3Column& operator=(DecimalV3Column const&) = default;

    bool is_decimal() const override;
    bool is_numeric() const override;
    void set_precision(int precision);
    void set_scale(int scale);
    int precision() const;
    int scale() const;

    MutableColumnPtr clone_empty() const override;

    void put_mysql_row_buffer(MysqlRowBuffer* buf, size_t idx) const override;
    std::string debug_item(size_t idx) const override;
    void crc32_hash(uint32_t* hash, uint32_t from, uint32_t to) const override;
    int64_t xor_checksum(uint32_t from, uint32_t to) const override;

private:
    int _precision;
    int _scale;
};

} // namespace starrocks
