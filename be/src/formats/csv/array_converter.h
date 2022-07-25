// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#pragma once

#include "formats/csv/array_reader.h"
#include "formats/csv/converter.h"

namespace starrocks::vectorized::csv {

// Hive collection delimiter generate rule is quiet complex,
// if you want to know the details, you can refer to:
// https://github.com/apache/hive/blob/90428cc5f594bd0abb457e4e5c391007b2ad1cb8/serde/src/java/org/apache/hadoop/hive/serde2/lazy/LazySerDeParameters.java#L250
// Next let's begin the story:
// There is a 3D-array [[[1, 2]], [[3, 4], [5, 6]]], in Hive it will be stored as 1^D2^B3^D4^C5^D6 (without '[', ']').
// ^B = (char)2, ^C = (char)3, ^D = (char)4 ....
// In the first level, Hive will use collection_delimiter (user can specify it, default is ^B) as an element separator,
// then origin array split into [[1, 2]] (1^D2) and [[3, 4], [5, 6]] (3^D4^C5^D6).
// In the second level, Hive will use mapkey_delimiter (user can specify it, default is ^C) as a separator, then
// array split into [1, 2], [3, 4] and [5, 6].
// In the third level, Hive will use ^D (user can't specify it) as a separator, then we can get
// each element in this array.
char get_collection_delimiter(char collection_delimiter, char mapkey_delimiter, size_t nested_array_level);

class ArrayConverter final : public Converter {
public:
    ArrayConverter(std::unique_ptr<Converter> elem_converter, const Options& options) {
        _element_converter = std::move(elem_converter);
        if (options.array_format_type == ArrayFormatType::HIVE) {
            char delimiter =
                    get_collection_delimiter(options.array_hive_collection_delimiter,
                                             options.array_hive_mapkey_delimiter, options.array_hive_nested_level);
            _array_reader = std::make_unique<HiveTextArrayReader>(delimiter);
        } else {
            _array_reader = std::make_unique<DefaultArrayReader>();
        }
    }

    Status write_string(OutputStream* os, const Column& column, size_t row_num, const Options& options) const override;
    Status write_quoted_string(OutputStream* os, const Column& column, size_t row_num,
                               const Options& options) const override;
    bool read_string(Column* column, Slice s, const Options& options) const override;
    bool read_quoted_string(Column* column, Slice s, const Options& options) const override;

private:
    std::unique_ptr<ArrayReader> _array_reader;
    std::unique_ptr<Converter> _element_converter;
};

} // namespace starrocks::vectorized::csv
