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

#include "formats/avro/cpp/column_reader.h"

#include "formats/avro/cpp/binary_column_reader.h"
#include "formats/avro/cpp/complex_column_reader.h"
#include "formats/avro/cpp/date_column_reader.h"
#include "formats/avro/cpp/nullable_column_reader.h"
#include "formats/avro/cpp/numeric_column_reader.h"

namespace starrocks::avrocpp {

ColumnReaderUniquePtr ColumnReader::get_nullable_column_reader(const std::string& col_name,
                                                               const TypeDescriptor& type_desc,
                                                               const cctz::time_zone& timezone, bool invalid_as_null) {
    ColumnReaderUniquePtr reader = nullptr;
    switch (type_desc.type) {
    case TYPE_BOOLEAN:
        reader = std::make_unique<BooleanColumnReader>(col_name, type_desc);
        break;

    case TYPE_TINYINT:
        reader = std::make_unique<NumericColumnReader<int8_t>>(col_name, type_desc);
        break;

    case TYPE_SMALLINT:
        reader = std::make_unique<NumericColumnReader<int16_t>>(col_name, type_desc);
        break;

    case TYPE_INT:
        reader = std::make_unique<NumericColumnReader<int32_t>>(col_name, type_desc);
        break;

    case TYPE_BIGINT:
        reader = std::make_unique<NumericColumnReader<int64_t>>(col_name, type_desc);
        break;

    case TYPE_LARGEINT:
        reader = std::make_unique<NumericColumnReader<int128_t>>(col_name, type_desc);
        break;

    case TYPE_FLOAT:
        reader = std::make_unique<NumericColumnReader<float>>(col_name, type_desc);
        break;

    case TYPE_DOUBLE:
        reader = std::make_unique<NumericColumnReader<double>>(col_name, type_desc);
        break;

    case TYPE_DECIMAL32:
        reader = std::make_unique<DecimalColumnReader<int32_t>>(col_name, type_desc);
        break;

    case TYPE_DECIMAL64:
        reader = std::make_unique<DecimalColumnReader<int64_t>>(col_name, type_desc);
        break;

    case TYPE_DECIMAL128:
        reader = std::make_unique<DecimalColumnReader<int128_t>>(col_name, type_desc);
        break;

    case TYPE_DATE:
        reader = std::make_unique<DateColumnReader>(col_name, type_desc);
        break;

    case TYPE_DATETIME:
        reader = std::make_unique<DatetimeColumnReader>(col_name, type_desc, timezone);
        break;

    case TYPE_STRUCT: {
        std::vector<ColumnReaderUniquePtr> field_readers;
        for (size_t i = 0; i < type_desc.children.size(); ++i) {
            field_readers.emplace_back(get_nullable_column_reader(type_desc.field_names[i], type_desc.children[i],
                                                                  timezone, invalid_as_null));
        }
        reader = std::make_unique<StructColumnReader>(col_name, type_desc, std::move(field_readers));
        break;
    }

    case TYPE_ARRAY: {
        auto element_reader = get_nullable_column_reader(col_name, type_desc.children[0], timezone, invalid_as_null);
        reader = std::make_unique<ArrayColumnReader>(col_name, type_desc, std::move(element_reader));
        break;
    }

    case TYPE_MAP: {
        auto key_reader = get_nullable_column_reader(col_name, type_desc.children[0], timezone, invalid_as_null);
        auto value_reader = get_nullable_column_reader(col_name, type_desc.children[1], timezone, invalid_as_null);
        reader = std::make_unique<MapColumnReader>(col_name, type_desc, std::move(key_reader), std::move(value_reader));
        break;
    }

    default:
        reader = std::make_unique<BinaryColumnReader>(col_name, type_desc, timezone);
        break;
    }

    return std::make_unique<NullableColumnReader>(std::move(reader), invalid_as_null);
}

Status ColumnReader::read_datum_for_adaptive_column(const avro::GenericDatum& datum, Column* column) {
    __builtin_unreachable();
    return Status::OK();
}

} // namespace starrocks::avrocpp
