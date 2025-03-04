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

#include <string>

#include "column/column_helper.h"
#include "column/type_traits.h"
#include "column/vectorized_fwd.h"
#include "common/object_pool.h"
#include "common/status.h"
#include "exprs/min_max_predicate.h"
#include "exprs/predicate.h"
#include "formats/parquet/column_converter.h"
#include "formats/parquet/encoding_plain.h"
#include "formats/parquet/schema.h"
#include "gutil/casts.h"
#include "runtime/types.h"
#include "simd/simd.h"
#include "storage/column_predicate.h"
#include "storage/types.h"
#include "storage/uint24.h"
#include "types/date_value.h"
#include "types/large_int_value.h"
#include "types/logical_type.h"

namespace starrocks::parquet {

Status StatisticsHelper::decode_value_into_column(const ColumnPtr& column, const std::vector<std::string>& values,
                                                  const std::vector<bool>& null_pages, const TypeDescriptor& type,
                                                  const ParquetField* field, const std::string& timezone) {
    std::unique_ptr<ColumnConverter> converter;
    RETURN_IF_ERROR(ColumnConverterFactory::create_converter(*field, type, timezone, &converter));
    bool ret = true;
    switch (field->physical_type) {
    case tparquet::Type::type::INT32: {
        int32_t decode_value = 0;
        if (!converter->need_convert) {
            for (size_t i = 0; i < values.size(); i++) {
                if (null_pages[i]) {
                    ret &= column->append_nulls(1);
                } else {
                    RETURN_IF_ERROR(PlainDecoder<int32_t>::decode(values[i], &decode_value));
                    ret &= (column->append_numbers(&decode_value, sizeof(int32_t)) > 0);
                }
            }
        } else {
            ColumnPtr src_column = converter->create_src_column();
            for (size_t i = 0; i < values.size(); i++) {
                if (null_pages[i]) {
                    ret &= src_column->append_nulls(1);
                } else {
                    RETURN_IF_ERROR(PlainDecoder<int32_t>::decode(values[i], &decode_value));
                    ret &= (src_column->append_numbers(&decode_value, sizeof(int32_t)) > 0);
                }
            }
            RETURN_IF_ERROR(converter->convert(src_column, column.get()));
        }
        break;
    }
    case tparquet::Type::type::INT64: {
        int64_t decode_value = 0;
        if (!converter->need_convert) {
            for (size_t i = 0; i < values.size(); i++) {
                if (null_pages[i]) {
                    ret &= column->append_nulls(1);
                } else {
                    RETURN_IF_ERROR(PlainDecoder<int64_t>::decode(values[i], &decode_value));
                    ret &= (column->append_numbers(&decode_value, sizeof(int64_t)) > 0);
                }
            }
        } else {
            ColumnPtr src_column = converter->create_src_column();
            for (size_t i = 0; i < values.size(); i++) {
                if (null_pages[i]) {
                    ret &= src_column->append_nulls(1);
                } else {
                    RETURN_IF_ERROR(PlainDecoder<int64_t>::decode(values[i], &decode_value));
                    ret &= (src_column->append_numbers(&decode_value, sizeof(int64_t)) > 0);
                }
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
                if (null_pages[i]) {
                    ret &= column->append_nulls(1);
                } else {
                    RETURN_IF_ERROR(PlainDecoder<Slice>::decode(values[i], &decode_value));
                    ret &= column->append_strings(std::vector<Slice>{decode_value});
                }
            }
        } else {
            ColumnPtr src_column = converter->create_src_column();
            for (size_t i = 0; i < values.size(); i++) {
                if (null_pages[i]) {
                    ret &= src_column->append_nulls(1);
                } else {
                    RETURN_IF_ERROR(PlainDecoder<Slice>::decode(values[i], &decode_value));
                    ret &= src_column->append_strings(std::vector<Slice>{decode_value});
                }
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

void translate_to_string_value(const ColumnPtr& col, size_t i, std::string& value) {
    if (col->is_date()) {
        value = col->get(i).get_date().to_string();
        return;
    } else if (col->is_timestamp()) {
        value = col->get(i).get_timestamp().to_string();
        return;
    }

    auto v = col->get(i);
    v.visit([&](auto& variant) {
        std::visit(
                [&](auto&& arg) {
                    using T = std::decay_t<decltype(arg)>;
                    if constexpr (std::is_same_v<T, Slice>) {
                        value = arg.to_string();
                    } else if constexpr (std::is_same_v<T, int8_t> || std::is_same_v<T, int16_t> ||
                                         std::is_same_v<T, int32_t> || std::is_same_v<T, int64_t>) {
                        value = std::to_string(arg);
                    } else if constexpr (std::is_same_v<T, int128_t>) {
                        value = LargeIntValue::to_string(arg);
                    } else {
                        // not supported, and should be denied in can_be_used_for_statistics_filter
                        DCHECK(false) << "Unsupported type";
                    }
                },
                variant);
    });
}

Status StatisticsHelper::get_min_max_value(const FileMetaData* file_metadata, const TypeDescriptor& type,
                                           const tparquet::ColumnMetaData* column_meta, const ParquetField* field,
                                           std::vector<std::string>& min_values, std::vector<std::string>& max_values) {
    // When statistics is empty, column_meta->__isset.statistics is still true,
    // but statistics.__isset.xxx may be false, so judgment is required here.
    bool is_set_min_max = (column_meta->statistics.__isset.max && column_meta->statistics.__isset.min) ||
                          (column_meta->statistics.__isset.max_value && column_meta->statistics.__isset.min_value);
    if (!is_set_min_max) {
        return Status::Aborted("No exist min/max");
    }

    DCHECK_EQ(field->physical_type, column_meta->type);
    auto sort_order = sort_order_of_logical_type(type.type);

    if (!has_correct_min_max_stats(file_metadata, *column_meta, sort_order)) {
        return Status::Aborted("The file has incorrect order");
    }

    if (column_meta->statistics.__isset.min_value) {
        min_values.emplace_back(column_meta->statistics.min_value);
        max_values.emplace_back(column_meta->statistics.max_value);
    } else {
        min_values.emplace_back(column_meta->statistics.min);
        max_values.emplace_back(column_meta->statistics.max);
    }

    return Status::OK();
}

bool StatisticsHelper::has_correct_min_max_stats(const FileMetaData* file_metadata,
                                                 const tparquet::ColumnMetaData& column_meta,
                                                 const SortOrder& sort_order) {
    return file_metadata->writer_version().HasCorrectStatistics(column_meta, sort_order);
}

} // namespace starrocks::parquet
