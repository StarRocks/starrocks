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

#include "formats/csv/converter.h"

namespace starrocks::csv {
class ArrayReader {
public:
    explicit ArrayReader(char array_delimiter) : _array_delimiter(array_delimiter) {}
    virtual ~ArrayReader() = default;
    [[nodiscard]] virtual bool validate(const Slice& s) const = 0;
    [[nodiscard]] virtual bool split_array_elements(const Slice& s, std::vector<Slice>& elements) const = 0;

    // In Hive nested array, string is not quoted, you should set false here if you want
    // to parse Hive array format.
    [[nodiscard]] virtual bool read_quoted_string(const std::unique_ptr<Converter>& elem_converter, Column* column,
                                                  const Slice& s, const Converter::Options& options) const = 0;
    static std::unique_ptr<ArrayReader> create_array_reader(const Converter::Options& options);

protected:
    const char _array_delimiter;
};

// Reader for standard array format: [[1, 2], [3, 4, 5]].
class DefaultArrayReader final : public ArrayReader {
public:
    explicit DefaultArrayReader() : ArrayReader(',') {}
    [[nodiscard]] bool validate(const Slice& s) const override;
    [[nodiscard]] bool split_array_elements(const Slice& s, std::vector<Slice>& elements) const override;
    [[nodiscard]] bool read_quoted_string(const std::unique_ptr<Converter>& elem_converter, Column* column,
                                          const Slice& s, const Converter::Options& options) const override;
};

// Reader for Hive text array format.
// In Hive, array's format is different from standard array. Hive use
// custom array delimiter (aka. collection delimiter).
// In default, 1D array's delimiter is \002(^B), 2D array is \003(^C),.... Detail delimiter rule refer to:
// https://github.com/apache/hive/blob/master/serde/src/java/org/apache/hadoop/hive/serde2/lazy/LazySerDeParameters.java#L250
// For example, a standard array: [[1, 2], [3, 4, 5]], in Hive will be stored as: 1^C2^B3^C4^C5
class HiveTextArrayReader final : public ArrayReader {
public:
    explicit HiveTextArrayReader(const Converter::Options& options)
            : ArrayReader(get_collection_delimiter(options.hive_collection_delimiter,
                                                   options.hive_mapkey_delimiter,
                                                   options.hive_nested_level)) {}
    [[nodiscard]] bool validate(const Slice& s) const override;
    [[nodiscard]] bool split_array_elements(const Slice& s, std::vector<Slice>& elements) const override;
    [[nodiscard]] bool read_quoted_string(const std::unique_ptr<Converter>& elem_converter, Column* column,
                                          const Slice& s, const Converter::Options& options) const override;
};

} // namespace starrocks::csv
