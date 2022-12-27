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
    [[nodiscard]] virtual bool split_array_elements(Slice s, std::vector<Slice>* elements) const = 0;

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
    [[nodiscard]] bool split_array_elements(Slice s, std::vector<Slice>* elements) const override;
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
            : ArrayReader(get_collection_delimiter(options.array_hive_collection_delimiter,
                                                   options.array_hive_mapkey_delimiter,
                                                   options.array_hive_nested_level)) {}
    [[nodiscard]] bool validate(const Slice& s) const override;
    [[nodiscard]] bool split_array_elements(Slice s, std::vector<Slice>* elements) const override;
    [[nodiscard]] bool read_quoted_string(const std::unique_ptr<Converter>& elem_converter, Column* column,
                                          const Slice& s, const Converter::Options& options) const override;

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
    static char get_collection_delimiter(char collection_delimiter, char mapkey_delimiter, size_t nested_array_level);
};

} // namespace starrocks::csv
