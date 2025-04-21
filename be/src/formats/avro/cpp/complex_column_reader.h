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

#include "formats/avro/cpp/column_reader.h"

namespace starrocks::avrocpp {

class StructColumnReader final : public ColumnReader {
public:
    explicit StructColumnReader(const std::string& col_name, const TypeDescriptor& type_desc,
                                std::vector<ColumnReaderUniquePtr> field_readers);
    ~StructColumnReader() override = default;

    Status read_datum(const avro::GenericDatum& datum, Column* column) override;

private:
    std::vector<ColumnReaderUniquePtr> _field_readers;
};

class ArrayColumnReader final : public ColumnReader {
public:
    explicit ArrayColumnReader(const std::string& col_name, const TypeDescriptor& type_desc,
                               ColumnReaderUniquePtr element_reader)
            : ColumnReader(col_name, type_desc), _element_reader(std::move(element_reader)) {}
    ~ArrayColumnReader() override = default;

    Status read_datum(const avro::GenericDatum& datum, Column* column) override;

private:
    ColumnReaderUniquePtr _element_reader;
};

class MapColumnReader final : public ColumnReader {
public:
    explicit MapColumnReader(const std::string& col_name, const TypeDescriptor& type_desc,
                             ColumnReaderUniquePtr key_reader, ColumnReaderUniquePtr value_reader)
            : ColumnReader(col_name, type_desc),
              _key_reader(std::move(key_reader)),
              _value_reader(std::move(value_reader)) {}
    ~MapColumnReader() override = default;

    Status read_datum(const avro::GenericDatum& datum, Column* column) override;

private:
    ColumnReaderUniquePtr _key_reader;
    ColumnReaderUniquePtr _value_reader;
};

} // namespace starrocks::avrocpp
