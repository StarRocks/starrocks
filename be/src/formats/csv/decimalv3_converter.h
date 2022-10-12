// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#pragma once

#include "formats/csv/converter.h"
#include "util/decimal_types.h"

namespace starrocks::vectorized::csv {

template <typename T>
class DecimalV3Converter final : public Converter {
    static_assert(is_underlying_type_of_decimal<T>, "not underlying type of decimal");

public:
    DecimalV3Converter(int precision, int scale) : _precision(precision), _scale(scale) {}

    Status write_string(OutputStream* os, const Column& column, size_t row_num, const Options& options) const override;
    Status write_quoted_string(OutputStream* os, const Column& column, size_t row_num,
                               const Options& options) const override;
    bool read_string(Column* column, Slice s, const Options& options) const override;
    bool read_quoted_string(Column* column, Slice s, const Options& options) const override;

private:
    int _precision;
    int _scale;
};

} // namespace starrocks::vectorized::csv
