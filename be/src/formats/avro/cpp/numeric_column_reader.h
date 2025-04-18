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

#include "column/decimalv3_column.h"
#include "column/fixed_length_column.h"
#include "formats/avro/cpp/column_reader.h"

namespace starrocks::avrocpp {

template <typename T>
class NumericColumnReader final : public ColumnReader {
public:
    explicit NumericColumnReader(const std::string& col_name, const TypeDescriptor& type_desc)
            : ColumnReader(col_name, type_desc) {}
    ~NumericColumnReader() override = default;

    Status read_datum(const avro::GenericDatum& datum, Column* column) override;

private:
    Status read_numeric_value(const avro::GenericDatum& datum, FixedLengthColumn<T>* column);
    Status read_string_value(const avro::GenericDatum& datum, FixedLengthColumn<T>* column);
};

template <typename T>
class DecimalColumnReader final : public ColumnReader {
public:
    explicit DecimalColumnReader(const std::string& col_name, const TypeDescriptor& type_desc)
            : ColumnReader(col_name, type_desc), _precision(type_desc.precision), _scale(type_desc.scale) {}
    ~DecimalColumnReader() override = default;

    Status read_datum(const avro::GenericDatum& datum, Column* column) override;

private:
    Status read_numeric_value(const avro::GenericDatum& datum, DecimalV3Column<T>* column);
    Status read_string_value(const avro::GenericDatum& datum, DecimalV3Column<T>* column);
    Status read_bytes_value(const avro::GenericDatum& datum, DecimalV3Column<T>* column);

    int _precision;
    int _scale;
};

} // namespace starrocks::avrocpp
