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

#include <optional>
#include <string>
#include <variant>
#include <vector>

#include "column/column_viewer.h"
#include "column/mysql_row_buffer.h"
#include "types/decimalv2_value.h"
#include "types/decimalv3.h"
#include "types/logical_type.h"
#include "types/logical_type_infra.h"
#include "types/type_descriptor.h"

namespace starrocks {

using MysqlColumnViewer = std::variant<
#define M(NAME) ColumnViewer<NAME>,
        APPLY_FOR_ALL_SCALAR_TYPE(M)
#undef M
                ColumnViewer<TYPE_NULL>>;

struct MysqlColumnViewerBuilder {
    template <LogicalType ltype>
    std::optional<MysqlColumnViewer> operator()(const ColumnPtr& column) const {
        // JSON/VARIANT need the virtual Column::put_mysql_row_buffer path: it materializes
        // shredded variant rows via try_get_row_ref/get_row_value and uses push_null(false)
        // on serialization failure (so binary-protocol _field_pos is not double-incremented).
        if constexpr (ltype == TYPE_JSON || ltype == TYPE_VARIANT) {
            return std::nullopt;
        } else {
            return MysqlColumnViewer(std::in_place_type<ColumnViewer<ltype>>, column);
        }
    }
};

struct MysqlColumnSerializer {
    template <LogicalType ltype>
    static void serialize(const MysqlColumnViewer& viewer, const TypeDescriptor& type_desc, MysqlRowBuffer* buf,
                          size_t idx, bool is_binary_protocol) {
        const auto& typed_viewer = std::get<ColumnViewer<ltype>>(viewer);
        if (typed_viewer.is_null(idx)) {
            buf->push_null(is_binary_protocol);
            return;
        }

        if (is_binary_protocol) {
            buf->update_field_pos();
        }

        if constexpr (ltype == TYPE_BOOLEAN) {
            buf->push_number<int8_t>(typed_viewer.value(idx) ? 1 : 0, is_binary_protocol);
        } else if constexpr (lt_is_integer<ltype> || ltype == TYPE_LARGEINT) {
            buf->push_number(typed_viewer.value(idx), is_binary_protocol);
        } else if constexpr (ltype == TYPE_FLOAT || ltype == TYPE_DOUBLE) {
            buf->push_number(typed_viewer.value(idx), is_binary_protocol);
        } else if constexpr (ltype == TYPE_DATE) {
            buf->push_date(typed_viewer.value(idx), is_binary_protocol);
        } else if constexpr (ltype == TYPE_DATETIME) {
            buf->push_timestamp(typed_viewer.value(idx), is_binary_protocol);
        } else if constexpr (ltype == TYPE_DECIMALV2) {
            buf->push_decimal(typed_viewer.value(idx).to_string());
        } else if constexpr (lt_is_decimal<ltype>) {
            using CppType = RunTimeCppType<ltype>;
            auto decimal_str =
                    DecimalV3Cast::to_string<CppType>(typed_viewer.value(idx), type_desc.precision, type_desc.scale);
            buf->push_decimal(decimal_str);
        } else if constexpr (lt_is_string<ltype>) {
            auto slice = typed_viewer.value(idx);
            buf->push_string(slice.data, slice.size);
        } else if constexpr (ltype == TYPE_VARBINARY) {
            auto slice = typed_viewer.value(idx);
            buf->push_binary(slice.data, slice.size);
        } else {
            buf->push_null(is_binary_protocol);
        }
    }
};

using MysqlSerializeFn = void (*)(const MysqlColumnViewer&, const TypeDescriptor&, MysqlRowBuffer*, size_t, bool);

struct MysqlSerializerBuilder {
    template <LogicalType ltype>
    MysqlSerializeFn operator()() const {
        return &MysqlColumnSerializer::serialize<ltype>;
    }
};

inline MysqlSerializeFn get_mysql_serializer(LogicalType ltype) {
    return type_dispatch_basic(ltype, MysqlSerializerBuilder());
}

} // namespace starrocks
