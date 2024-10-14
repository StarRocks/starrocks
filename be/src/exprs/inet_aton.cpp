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

#include "exprs/string_functions.h"

#include <algorithm>

#include "column/array_column.h"
#include "column/binary_column.h"
#include "column/column_viewer.h"
#include "common/format_ip.h"

namespace starrocks {

    static inline bool try_parse_ipv4(const char* pos, int64& result_value) {
        return parse_ipv4_whole(pos, reinterpret_cast<unsigned char*>(&result_value));
    }

    template <IPConvertExceptionMode exception_mode, typename ToColumn>
    static ColumnPtr convert_to_ipv4(const ColumnPtr haystack_ptr, const UInt8Column::Container* null_map = nullptr) {

        auto* column_binary = down_cast<BinaryColumn*>(ColumnHelper::get_data_column(haystack_ptr.get()));
        if (!column_binary) {
            throw std::invalid_argument("Illegal column type, expected Binary or String");
        }

        size_t column_size = column_binary->size();

        // Prepare for nullable handling
        UInt8Column::Ptr col_null_map_to;
        UInt8Column::Container * vec_null_map_to = nullptr;

        if constexpr (exception_mode == IPConvertExceptionMode::Null) {
            col_null_map_to = UInt8Column::create(column_size, false);
            vec_null_map_to = &col_null_map_to->get_data();
        }

        auto col_res = ToColumn::create();
        auto& vec_res = col_res->get_data();
        vec_res.resize(column_size);

        // Fetch underlying binary data
        const auto& vec_src = column_binary->get_bytes();
        const auto& offsets_src = column_binary->get_offset();
        size_t prev_offset = 0;

        for (size_t i = 0; i < vec_res.size(); ++i) {
            if (null_map != nullptr && i < null_map->size() && (*null_map)[i]) {
                if constexpr (exception_mode == IPConvertExceptionMode::Throw) {
                    throw std::invalid_argument(
                            "Null Input, you may consider convert it to a valid default IPv4 value "
                            "like '0.0.0.0' first");
                }
                vec_res[i] = 0;
                prev_offset = offsets_src[i];
                if constexpr (exception_mode == IPConvertExceptionMode::Null) {
                    (*vec_null_map_to)[i] = true;
                }
                continue;
            }

            const char* src_start = reinterpret_cast<const char*>(&vec_src[prev_offset]);
            size_t src_length = (i < vec_res.size() - 1) ? (offsets_src[i+1] - prev_offset)
                                                         : (vec_src.size() - prev_offset);

            std::string src(src_start, src_length);
            bool parse_result = try_parse_ipv4(src.c_str(), vec_res[i]);

            if (!parse_result) {
                if constexpr (exception_mode == IPConvertExceptionMode::Throw) {
                    throw std::invalid_argument("Invalid IPv4 value");
                } else if constexpr (exception_mode == IPConvertExceptionMode::Default) {
                    vec_res[i] = 0;
                } else if constexpr (exception_mode == IPConvertExceptionMode::Null) {
                    (*vec_null_map_to)[i] = true;
                    vec_res[i] = 0;
                }
            }

            prev_offset = offsets_src[i] + src_length;
        }

        if constexpr (exception_mode == IPConvertExceptionMode::Null) {
            return NullableColumn::create(std::move(col_res), std::move(col_null_map_to));
        }
        return col_res;
    }

    StatusOr<ColumnPtr> StringFunctions::inet_aton(FunctionContext* context, const Columns& columns) {
        RETURN_IF_COLUMNS_ONLY_NULL(columns);

        ColumnPtr haystack = columns[0];
        const UInt8Column::Container* null_map = nullptr;

        if (haystack->is_nullable()) {
            const auto* column_nullable = down_cast<const NullableColumn*>(haystack.get());
            haystack = column_nullable->data_column();
            null_map = &column_nullable->null_column()->get_data();
        }

        return convert_to_ipv4<IPConvertExceptionMode::Null, Int64Column>(haystack, null_map);
    }
}// namespace starrocks
