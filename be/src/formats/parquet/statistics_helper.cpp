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

#include "formats/parquet/statistics_helper.h"

#include "formats/parquet/column_converter.h"
#include "formats/parquet/encoding_plain.h"
#include "formats/parquet/schema.h"

namespace starrocks::parquet {

Status StatisticsHelper::decode_value_into_column(ColumnPtr column, const std::vector<std::string>& values,
                                                  const TypeDescriptor& type, const ParquetField* field,
                                                  const std::string& timezone) {
    std::unique_ptr<ColumnConverter> converter;
    RETURN_IF_ERROR(ColumnConverterFactory::create_converter(*field, type, timezone, &converter));
    bool ret = true;
    switch (field->physical_type) {
    case tparquet::Type::type::INT32: {
        int32_t decode_value = 0;
        if (!converter->need_convert) {
            for (size_t i = 0; i < values.size(); i++) {
                RETURN_IF_ERROR(PlainDecoder<int32_t>::decode(values[i], &decode_value));
                ret &= (column->append_numbers(&decode_value, sizeof(int32_t)) > 0);
            }
        } else {
            ColumnPtr src_column = converter->create_src_column();
            for (size_t i = 0; i < values.size(); i++) {
                RETURN_IF_ERROR(PlainDecoder<int32_t>::decode(values[i], &decode_value));
                ret &= (src_column->append_numbers(&decode_value, sizeof(int32_t)) > 0);
            }
            RETURN_IF_ERROR(converter->convert(src_column, column.get()));
        }
        break;
    }
    case tparquet::Type::type::INT64: {
        int64_t decode_value = 0;
        if (!converter->need_convert) {
            for (size_t i = 0; i < values.size(); i++) {
                RETURN_IF_ERROR(PlainDecoder<int64_t>::decode(values[i], &decode_value));
                ret &= (column->append_numbers(&decode_value, sizeof(int64_t)) > 0);
            }
        } else {
            ColumnPtr src_column = converter->create_src_column();
            for (size_t i = 0; i < values.size(); i++) {
                RETURN_IF_ERROR(PlainDecoder<int64_t>::decode(values[i], &decode_value));
                ret &= (src_column->append_numbers(&decode_value, sizeof(int64_t)) > 0);
            }
            RETURN_IF_ERROR(converter->convert(src_column, column.get()));
        }
        break;
    }
    case tparquet::Type::type::BYTE_ARRAY:
    // todo: FLBA need more test
    case tparquet::Type::type::FIXED_LEN_BYTE_ARRAY: {
        Slice decode_value;
        if (!converter->need_convert) {
            for (size_t i = 0; i < values.size(); i++) {
                RETURN_IF_ERROR(PlainDecoder<Slice>::decode(values[i], &decode_value));
                ret &= column->append_strings(std::vector<Slice>{decode_value});
            }
        } else {
            ColumnPtr src_column = converter->create_src_column();
            for (size_t i = 0; i < values.size(); i++) {
                RETURN_IF_ERROR(PlainDecoder<Slice>::decode(values[i], &decode_value));
                ret &= src_column->append_strings(std::vector<Slice>{decode_value});
            }
            RETURN_IF_ERROR(converter->convert(src_column, column.get()));
        }
        break;
    }
    default:
        return Status::Aborted("Not Supported min/max value type");
    }

    if (UNLIKELY(!ret)) {
        return Status::InternalError("Decode min-max column failed");
    }
    return Status::OK();
}

} // namespace starrocks::parquet