// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#pragma once

#include "formats/csv/converter.h"

namespace starrocks::vectorized::csv {

class NullableConverter final : public Converter {
public:
    explicit NullableConverter(std::unique_ptr<Converter> base_converter)
            : _base_converter(std::move(base_converter)) {}

    Status write_string(OutputStream* os, const Column& column, size_t row_num, const Options& options) const override;
    Status write_quoted_string(OutputStream* os, const Column& column, size_t row_num,
                               const Options& options) const override;
    bool read_string_for_adaptive_null_column(Column* column, Slice s, const Options& options) const override;
    bool read_string(Column* column, Slice s, const Options& options) const override;
    bool read_quoted_string(Column* column, Slice s, const Options& options) const override;

private:
    std::unique_ptr<Converter> _base_converter;
};

} // namespace starrocks::vectorized::csv
