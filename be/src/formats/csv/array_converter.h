// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#pragma once

#include "formats/csv/array_reader.h"
#include "formats/csv/converter.h"

namespace starrocks::vectorized::csv {

class ArrayConverter final : public Converter {
public:
    explicit ArrayConverter(std::unique_ptr<Converter> elem_converter)
            : _element_converter(std::move(elem_converter)) {}

    Status write_string(OutputStream* os, const Column& column, size_t row_num, const Options& options) const override;
    Status write_quoted_string(OutputStream* os, const Column& column, size_t row_num,
                               const Options& options) const override;
    bool read_string(Column* column, Slice s, const Options& options) const override;
    bool read_quoted_string(Column* column, Slice s, const Options& options) const override;

private:
    mutable std::unique_ptr<ArrayReader> _array_reader;
    std::unique_ptr<Converter> _element_converter;
};

} // namespace starrocks::vectorized::csv
