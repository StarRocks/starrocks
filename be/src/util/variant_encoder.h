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
#include "column/column_builder.h"
#include "common/statusor.h"
#include "runtime/types.h"
#include "types/json_value.h"
#include "types/variant_value.h"
#include "velocypack/Slice.h"

namespace starrocks {

class VariantEncoder {
public:
    // cast column to variant column
    static Status encode_column(const ColumnPtr& column, const TypeDescriptor& type,
                                ColumnBuilder<TYPE_VARIANT>* builder, bool allow_throw_exception);
    // cast a single json value to variant value
    static StatusOr<VariantRowValue> encode_json_to_variant(const JsonValue& json);
    // cast a single row possibly shredded column to variant value
    static StatusOr<VariantRowValue> encode_shredded_column_row(const ColumnPtr& column, const TypeDescriptor& type,
                                                                size_t row, VariantEncodingContext* ctx = nullptr);
};

} // namespace starrocks
