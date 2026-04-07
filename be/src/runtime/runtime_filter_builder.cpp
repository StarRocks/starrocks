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

#include "runtime/runtime_filter_builder.h"

#include "column/column.h"
#include "column/column_helper.h"
#include "types/logical_type_infra.h"

namespace starrocks {

template <template <LogicalType> typename FilterType, bool is_skew_join>
struct FilterIniter {
    template <LogicalType LT>
    Status operator()(const ColumnPtr& column, size_t column_offset, RuntimeFilter* expr, bool eq_null) {
        auto* filter = down_cast<FilterType<LT>*>(expr);

        if (column->is_nullable()) {
            auto* nullable_column = ColumnHelper::as_raw_column<NullableColumn>(column);
            const auto& data_array = GetContainer<LT>::get_data(nullable_column->data_column().get());
            if (!nullable_column->has_null()) {
                for (size_t j = column_offset; j < data_array.size(); j++) {
                    if constexpr (is_skew_join) {
                        filter->insert_skew_values(data_array[j]);
                    } else {
                        filter->insert(data_array[j]);
                    }
                }
            } else {
                for (size_t j = column_offset; j < data_array.size(); j++) {
                    if (!nullable_column->is_null(j)) {
                        filter->insert(data_array[j]);
                    } else if (eq_null) {
                        filter->insert_null();
                    }
                }
            }
        } else {
            const auto& data_array = GetContainer<LT>::get_data(column.get());
            for (size_t j = column_offset; j < data_array.size(); j++) {
                if constexpr (is_skew_join) {
                    filter->insert_skew_values(data_array[j]);
                } else {
                    filter->insert(data_array[j]);
                }
            }
        }
        return Status::OK();
    }
};

Status RuntimeFilterBuilder::fill(RuntimeFilter* filter, LogicalType type, const ColumnPtr& column,
                                  size_t column_offset, bool eq_null, bool is_skew_join) {
    if (column == nullptr || filter == nullptr) {
        return Status::InternalError("column or filter is nullptr");
    }
    if (column->has_large_column()) {
        return Status::NotSupported("unsupported build runtime filter for large binary column");
    }

    switch (filter->type()) {
    case RuntimeFilterSerializeType::BLOOM_FILTER:
        if (is_skew_join) {
            return type_dispatch_filter(type, Status::OK(), FilterIniter<ComposedRuntimeBloomFilter, true>(), column,
                                        column_offset, filter, eq_null);
        }
        return type_dispatch_filter(type, Status::OK(), FilterIniter<ComposedRuntimeBloomFilter, false>(), column,
                                    column_offset, filter, eq_null);
    case RuntimeFilterSerializeType::BITSET_FILTER: {
        const auto error_status = Status::NotSupported("runtime bitset filter do not support the logical type: " +
                                                       std::string(logical_type_to_string(type)));
        return type_dispatch_bitset_filter(type, error_status, FilterIniter<ComposedRuntimeBitsetFilter, false>(),
                                           column, column_offset, filter, eq_null);
    }
    case RuntimeFilterSerializeType::EMPTY_FILTER:
        return type_dispatch_filter(type, Status::OK(), FilterIniter<ComposedRuntimeEmptyFilter, false>(), column,
                                    column_offset, filter, eq_null);
    case RuntimeFilterSerializeType::NONE:
    default:
        return Status::NotSupported("unsupported build runtime filter: " + filter->debug_string());
    }
}

Status RuntimeFilterBuilder::fill(RuntimeFilter* filter, LogicalType type, const Columns& columns, size_t column_offset,
                                  bool eq_null, bool is_skew_join) {
    for (const auto& column : columns) {
        RETURN_IF_ERROR(fill(filter, type, column, column_offset, eq_null, is_skew_join));
    }
    return Status::OK();
}

} // namespace starrocks
