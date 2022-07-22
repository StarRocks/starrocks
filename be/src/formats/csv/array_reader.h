// This file is licensed under the Elastic License 2.0. Copyright 2022-present, StarRocks Limited.

#pragma once

#include "formats/csv/converter.h"

namespace starrocks::vectorized::csv {
class ArrayReader {
public:
    explicit ArrayReader(char array_delimiter) : _array_delimiter(array_delimiter) {}
    virtual ~ArrayReader() = default;
    [[nodiscard]] virtual bool read_string(const std::unique_ptr<Converter>& elem_converter, Column* column, Slice s,
                                           const Converter::Options& options) const = 0;
    [[nodiscard]] virtual bool split_array_elements(Slice s, std::vector<Slice>* elements) const = 0;

protected:
    char _array_delimiter;
};

// Reader for standard array format: [[1, 2], [3, 4, 5]].
class StandardArrayReader final : public ArrayReader {
public:
    explicit StandardArrayReader() : ArrayReader(',') {}
    [[nodiscard]] bool read_string(const std::unique_ptr<Converter>& elem_converter, Column* column, Slice s,
                                   const Converter::Options& options) const override;
    [[nodiscard]] bool split_array_elements(Slice s, std::vector<Slice>* elements) const override;
};

// Reader for Hive text array format.
// In Hive, array's format is different from standard array. Hive use
// custom array delimiter (aka. collection delimiter).
// In default, 1D array's delimiter is \002(^B), 2D array is \003(^C),.... Detail delimiter rule refer to:
// https://github.com/apache/hive/blob/90428cc5f594bd0abb457e4e5c391007b2ad1cb8/serde/src/java/org/apache/hadoop/hive/serde2/lazy/LazySerDeParameters.java#L250
// For example, a standard array: [[1, 2], [3, 4, 5]], in Hive will be stored as: 1^C2^B3^C4^C5
class HiveTextArrayReader final : public ArrayReader {
public:
    explicit HiveTextArrayReader(char array_delimiter) : ArrayReader(array_delimiter) {}
    [[nodiscard]] bool read_string(const std::unique_ptr<Converter>& elem_converter, Column* column, Slice s,
                                   const Converter::Options& options) const override;
    [[nodiscard]] bool split_array_elements(Slice s, std::vector<Slice>* elements) const override;
};
} // namespace starrocks::vectorized::csv
