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

// arrow dependencies
#include <arrow/array/array_nested.h>
#include <arrow/buffer.h>
#include <arrow/result.h>
#include <arrow/status.h>
#include <arrow/type.h>
#include <arrow/type_fwd.h>
#include <arrow/util/bitmap_builders.h>

// project dependencies
#include "column_converter.h"

// starrocks dependencies
#include "column/array_column.h"
#include "column/column.h"
#include "column/field.h"
#include "column/map_column.h"
#include "column/struct_column.h"
#include "types/logical_type.h"

namespace starrocks::lake::format {

template <arrow::Type::type ARROW_TYPE_ID, LogicalType SR_TYPE,
          typename = arrow::enable_if_t<
                  arrow::is_var_length_list_type<typename arrow::TypeIdTraits<ARROW_TYPE_ID>::Type>::value ||
                  arrow::is_struct_type<typename arrow::TypeIdTraits<ARROW_TYPE_ID>::Type>::value>>

class NestedConverter : public ColumnConverter {
    using ArrorType = typename arrow::TypeIdTraits<ARROW_TYPE_ID>::Type;
    using ArrowArrayType = typename arrow::TypeTraits<ArrorType>::ArrayType;

    using SrColumnType = RunTimeColumnType<SR_TYPE>;

public:
    NestedConverter(const std::shared_ptr<arrow::DataType> arrow_type, const std::shared_ptr<Field> sr_field,
                    const arrow::MemoryPool* pool)
            : ColumnConverter(arrow_type, sr_field, pool) {}

    arrow::Status toSrColumn(const std::shared_ptr<arrow::Array> array, ColumnPtr& column) override {
        if (!column->is_nullable() && array->null_count() > 0) {
            return arrow::Status::Invalid("Column ", column->get_name(),
                                          " is non-nullable, but there are some null data in array.");
        }

        // copy data column
        const auto& nested_array = arrow::internal::checked_pointer_cast<ArrowArrayType>(array);
        ARROW_ASSIGN_OR_RAISE(arrow::ArrayVector arrow_children_arrays, get_children_arrays(nested_array));

        const auto data_column = arrow::internal::checked_pointer_cast<SrColumnType>(get_data_column(column));
        ARROW_ASSIGN_OR_RAISE(std::vector<starrocks::ColumnPtr> sr_sub_columns, get_children_columns(data_column));

        if (arrow_children_arrays.size() != sr_sub_columns.size()) {
            return arrow::Status::Invalid("Can't convert nested array, the array children size(",
                                          arrow_children_arrays.size(), ") is not same as starrocks sub-column size (",
                                          sr_sub_columns.size(), ")");
        }
        if (_children.size() < arrow_children_arrays.size()) {
            return arrow::Status::Invalid("Converter size (", _children.size(), ") is less than arrow array size(",
                                          arrow_children_arrays.size(), ")");
        }

        // copy data column
        for (size_t idx = 0; idx < arrow_children_arrays.size(); ++idx) {
            ARROW_RETURN_NOT_OK(_children[idx]->toSrColumn(arrow_children_arrays[idx], sr_sub_columns[idx]));
        }
        // for print sr sub column;
        ARROW_ASSIGN_OR_RAISE(std::vector<starrocks::ColumnPtr> sr_sub_columns2, get_children_columns(data_column));

        // copy null bitmap
        if (column->is_nullable()) {
            size_t num_rows = array->length();
            auto nullable = down_cast<NullableColumn*>(column.get());
            auto null_column = down_cast<NullColumn*>(nullable->null_column().get());
            null_column->resize(num_rows);
            for (size_t i = 0; i < num_rows; ++i) {
                nullable->null_column_data()[i] = array->IsNull(i);
            }
            nullable->set_has_null(true);
        }
        return arrow::Status::OK();
    }

private:
    template <class ArrowArrayClass, typename = std::enable_if_t<std::is_same_v<ArrowArrayClass, arrow::ListArray> ||
                                                                 std::is_same_v<ArrowArrayClass, arrow::MapArray> ||
                                                                 std::is_same_v<ArrowArrayClass, arrow::StructArray>>>
    arrow::Result<arrow::ArrayVector> get_children_arrays(const std::shared_ptr<ArrowArrayClass> array) {
        if constexpr (std::is_same_v<ArrowArrayClass, arrow::ListArray>) {
            arrow::ArrayVector all_arrays = {array->offsets(), array->values()};
            return all_arrays;
        } else if constexpr (std::is_same_v<ArrowArrayClass, arrow::MapArray>) {
            arrow::ArrayVector all_arrays = {array->offsets(), array->keys(), array->items()};
            return all_arrays;
        } else if constexpr (std::is_same_v<ArrowArrayClass, arrow::StructArray>) {
            return array->fields();
        } else {
            static_assert(true, "Unsupported type");
        }
    }

    template <class SrColumnClass, typename = std::enable_if_t<std::is_same_v<SrColumnClass, ArrayColumn> ||
                                                               std::is_same_v<SrColumnClass, MapColumn> ||
                                                               std::is_same_v<SrColumnClass, StructColumn>>>
    arrow::Result<Columns> get_children_columns(const std::shared_ptr<SrColumnClass> data_column) {
        if constexpr (std::is_same_v<SrColumnClass, ArrayColumn>) {
            Columns all_sub_columns = {data_column->offsets_column(), data_column->elements_column()};
            return all_sub_columns;
        } else if constexpr (std::is_same_v<SrColumnClass, MapColumn>) {
            Columns all_sub_columns = {data_column->offsets_column(), data_column->keys_column(),
                                       data_column->values_column()};
            return all_sub_columns;
        } else if constexpr (std::is_same_v<SrColumnClass, StructColumn>) {
            return data_column->fields();
        } else {
            static_assert(true, "Unsupported type");
        }
    }

    template <class ArrowArrayClass, typename = std::enable_if_t<std::is_same_v<ArrowArrayClass, arrow::ListArray> ||
                                                                 std::is_same_v<ArrowArrayClass, arrow::MapArray> ||
                                                                 std::is_same_v<ArrowArrayClass, arrow::StructArray>>>
    arrow::Result<std::shared_ptr<arrow::Array>> make_nested_array(const arrow::ArrayVector& arrays,
                                                                   const std::shared_ptr<arrow::Buffer> null_bitmap) {
        if constexpr (std::is_same_v<ArrowArrayClass, arrow::ListArray>) {
            const auto& offsets = *arrays[0];
            const auto& values = *arrays[1];
            return arrow::ListArray::FromArrays(_arrow_type, offsets, values, const_cast<arrow::MemoryPool*>(_pool),
                                                null_bitmap);
        } else if constexpr (std::is_same_v<ArrowArrayClass, arrow::MapArray>) {
            // array[0] is offset, array[1] is key, array[2] is value
            using OffsetArrayType = arrow::TypeTraits<arrow::MapType>::OffsetArrayType;
            const auto& typed_offsets = arrow::internal::checked_pointer_cast<const OffsetArrayType>(arrays[0]);
            return std::make_shared<arrow::MapArray>(_arrow_type, arrays[0]->length() - 1, typed_offsets->values(),
                                                     arrays[1], arrays[2], null_bitmap);
        } else if constexpr (std::is_same_v<ArrowArrayClass, arrow::StructArray>) {
            return arrow::StructArray::Make(arrays, _arrow_type->fields(), null_bitmap);
        } else {
            static_assert(true, "Unsupported type");
        }
    }
};

} // namespace starrocks::lake::format
