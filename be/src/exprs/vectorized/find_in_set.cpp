// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#include <algorithm>

#include "column/binary_column.h"
#include "column/column_builder.h"
#include "column/column_helper.h"
#include "column/column_viewer.h"
#include "exprs/vectorized/binary_function.h"
#include "exprs/vectorized/string_functions.h"
#include "util/memcmp.h"

namespace starrocks::vectorized {

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

} // namespace starrocks::vectorized
