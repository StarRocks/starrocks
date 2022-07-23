// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#pragma once

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
    static bool _validate_array(Slice s, const Options& options);
    static bool _split_array_elements(Slice s, std::vector<Slice>* elements, const Options& options);
    static bool _split_default_array_elements(Slice s, std::vector<Slice>* elements, const Options& options);
    static bool _split_hive_array_elements(Slice s, std::vector<Slice>* elements, const Options& options);
    std::unique_ptr<Converter> _element_converter;
};

char get_collection_delimiter(char collection_delimiter, char mapkey_delimiter, size_t nested_array_level);

} // namespace starrocks::vectorized::csv
