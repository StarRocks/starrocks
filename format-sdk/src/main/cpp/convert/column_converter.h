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

#include <arrow/type.h>

#include "column/column.h"
#include "column/type_traits.h"
#include "types/logical_type.h"

namespace starrocks::lake::format {
class ColumnConverter;

using ColumnConverterVector = std::vector<std::shared_ptr<ColumnConverter>>;

constexpr int32_t ARROW_CONVERTER_ID(arrow::Type::type arrow_type_id, LogicalType sr_logical_type) {
    DCHECK(arrow_type_id >= arrow::Type::NA && arrow_type_id < arrow::Type::MAX_ID)
            << "Invalid arrow type id: " << arrow_type_id;
    DCHECK(sr_logical_type >= starrocks::LogicalType::TYPE_UNKNOWN &&
           sr_logical_type < starrocks::LogicalType::TYPE_MAX_VALUE)
            << "Invalid logical type: " << sr_logical_type;
    return (arrow_type_id << 16) | (sr_logical_type);
}

class ColumnConverter {
public:
    // Create a Converter for the given data type
    static arrow::Result<std::shared_ptr<ColumnConverter>> create(const std::shared_ptr<arrow::DataType>& arrow_type,
                                                                  const std::shared_ptr<Field>& sr_field,
                                                                  const arrow::MemoryPool* pool);

    static arrow::Result<ColumnConverterVector> create_children_converter(
            const std::shared_ptr<arrow::DataType>& arrow_type, const std::shared_ptr<Field>& sr_field,
            const arrow::MemoryPool* pool);

public:
    ColumnConverter(const std::shared_ptr<arrow::DataType>& arrow_type, const std::shared_ptr<Field>& sr_field,
                    const arrow::MemoryPool* pool)
            : _arrow_type(arrow_type), _sr_field(sr_field), _pool(pool){};

    virtual ~ColumnConverter() = default;

    void set_children(ColumnConverterVector converters) { _children = std::move(converters); }

    /**
     * Convert arrow array to starrocks column.
     */
    virtual arrow::Status toSrColumn(std::shared_ptr<arrow::Array> array, ColumnPtr& column) = 0;

    /**
     * Convert starrocks column to arrow array.
     */
    virtual arrow::Result<std::shared_ptr<arrow::Array>> toArrowArray(const std::shared_ptr<Column>& column) = 0;

protected:
    arrow::Result<std::shared_ptr<arrow::Buffer>> convert_null_bitmap(const Buffer<uint8_t>& null_bytes);

    static ColumnPtr get_data_column(const ColumnPtr& column);

protected:
    const std::shared_ptr<arrow::DataType> _arrow_type;

    // when append string data, check length limit will use _sr_field;
    const std::shared_ptr<Field> _sr_field;

    const arrow::MemoryPool* _pool;

    std::vector<std::shared_ptr<ColumnConverter>> _children;
};

} // namespace starrocks::lake::format
