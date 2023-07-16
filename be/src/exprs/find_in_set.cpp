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

#include <algorithm>

#include "column/binary_column.h"
#include "column/column_builder.h"
#include "column/column_helper.h"
#include "column/column_viewer.h"
#include "exprs/binary_function.h"
#include "exprs/string_functions.h"
#include "util/memcmp.h"

namespace starrocks {

// find_in_set
DEFINE_BINARY_FUNCTION_WITH_IMPL(findInSetImpl, str, strlist) {
    if (strlist.size < str.size) {
        return 0;
    }

    char* pos = reinterpret_cast<char*>(memchr(str.data, ',', str.size));
    if (pos != nullptr) {
        return 0;
    }

    int32_t pre_offset = -1;
    int32_t offset = -1;
    int32_t num = 0;
    while (offset < static_cast<int32_t>(strlist.size)) {
        pre_offset = offset;
        size_t n = strlist.size - offset - 1;
        char* pos = reinterpret_cast<char*>(memchr(strlist.data + offset + 1, ',', n));
        if (pos != nullptr) {
            offset = pos - strlist.data;
        } else {
            offset = strlist.size;
        }
        num++;
        bool is_equal = memequal(str.data, str.size, strlist.data + pre_offset + 1, offset - pre_offset - 1);
        if (is_equal) {
            return num;
        }
    }
    return 0;
}

StatusOr<ColumnPtr> StringFunctions::find_in_set(FunctionContext* context, const Columns& columns) {
    return VectorizedStrictBinaryFunction<findInSetImpl>::evaluate<TYPE_VARCHAR, TYPE_INT>(columns[0], columns[1]);
}

} // namespace starrocks
