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

#include "common/format_ip.h"
#include "exprs/string_functions.h"

namespace starrocks {

static inline bool try_parse_ipv4(const char* pos, size_t str_len, int64& result_value) {
    return parse_ipv4(pos, str_len, result_value);
}

StatusOr<ColumnPtr> StringFunctions::inet_aton(FunctionContext* context, const Columns& columns) {
    RETURN_IF_COLUMNS_ONLY_NULL(columns);

    auto str_viewer = ColumnViewer<TYPE_VARCHAR>(columns[0]);
    auto size = columns[0]->size();

    ColumnBuilder<TYPE_BIGINT> result(size);
    for (int row = 0; row < size; row++) {
        if (str_viewer.is_null(row)) {
            result.append_null();
            continue;
        }

        auto str_value = str_viewer.value(row);
        int64_t parsed_result;
        if (try_parse_ipv4(str_value.get_data(), str_value.get_size(), parsed_result)) {
            result.append(parsed_result);
        } else {
            result.append_null();
        }
    }
    return result.build(ColumnHelper::is_all_const(columns));
}

} // namespace starrocks
