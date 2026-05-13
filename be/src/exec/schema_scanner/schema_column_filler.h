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

#include "column/column.h"
#include "column/nullable_column.h"
#include "column/runtime_type_traits.h"
#include "gutil/casts.h"
#include "types/datetime_value.h"
#include "types/logical_type.h"

namespace starrocks {

template <LogicalType SlotType>
void fill_data_column_with_slot(Column* data_column, void* slot) {
    using ColumnType = typename RunTimeTypeTraits<SlotType>::ColumnType;
    using ValueType = typename RunTimeTypeTraits<SlotType>::CppType;

    ColumnType* result = down_cast<ColumnType*>(data_column);
    if constexpr (IsDate<ValueType>) {
        auto* date_time_value = (DateTimeValue*)slot;
        DateValue date_value =
                DateValue::create(date_time_value->year(), date_time_value->month(), date_time_value->day());
        result->append(date_value);
    } else if constexpr (IsTimestamp<ValueType>) {
        auto* date_time_value = (DateTimeValue*)slot;
        TimestampValue timestamp_value =
                TimestampValue::create(date_time_value->year(), date_time_value->month(), date_time_value->day(),
                                       date_time_value->hour(), date_time_value->minute(), date_time_value->second());
        result->append(timestamp_value);
    } else {
        result->append(*(ValueType*)slot);
    }
}

template <LogicalType SlotType>
void fill_column_with_slot(Column* result, void* slot) {
    if (result->is_nullable()) {
        auto* nullable_column = down_cast<NullableColumn*>(result);
        NullData& null_data = nullable_column->null_column_data();
        Column* data_column = nullable_column->data_column_raw_ptr();
        null_data.push_back(0);
        fill_data_column_with_slot<SlotType>(data_column, slot);
    } else {
        fill_data_column_with_slot<SlotType>(result, slot);
    }
}

} // namespace starrocks
