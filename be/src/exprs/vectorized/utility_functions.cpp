// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#include "exprs/vectorized/utility_functions.h"

#include "column/column_builder.h"
#include "column/column_viewer.h"
#include "gen_cpp/version.h"
#include "runtime/runtime_state.h"
#include "udf/udf_internal.h"
#include "util/monotime.h"
#include "util/uid_util.h"

namespace starrocks::vectorized {

ColumnPtr UtilityFunctions::version(FunctionContext* context, const Columns& columns) {
    return ColumnHelper::create_const_column<TYPE_VARCHAR>("5.1.0", 1);
}

ColumnPtr UtilityFunctions::current_version(FunctionContext* context, const Columns& columns) {
    static std::string version = std::string(STARROCKS_VERSION) + " " + STARROCKS_COMMIT_HASH;
    return ColumnHelper::create_const_column<TYPE_VARCHAR>(version, 1);
}

ColumnPtr UtilityFunctions::sleep(FunctionContext* context, const Columns& columns) {
    ColumnBuilder<TYPE_BOOLEAN> result;
    ColumnViewer<TYPE_INT> data_column(columns[0]);

    auto size = columns[0]->size();
    for (int row = 0; row < size; ++row) {
        if (data_column.is_null(row)) {
            result.append_null();
            continue;
        }

        auto value = data_column.value(row);
        SleepFor(MonoDelta::FromSeconds(value));
        result.append(true);
    }

    return result.build(ColumnHelper::is_all_const(columns));
}

ColumnPtr UtilityFunctions::last_query_id(FunctionContext* context, const Columns& columns) {
    starrocks::RuntimeState* state = context->impl()->state();
    const std::string& id = state->last_query_id();
    if (!id.empty()) {
        return ColumnHelper::create_const_column<TYPE_VARCHAR>(id, 1);
    } else {
        return ColumnHelper::create_const_null_column(1);
    }
}

ColumnPtr UtilityFunctions::uuid(FunctionContext*, const Columns& columns) {
    int32_t num_rows = ColumnHelper::get_const_value<TYPE_INT>(columns.back());

    ColumnBuilder<TYPE_VARCHAR> result;
    result.reserve(num_rows);
    for (int i = 0; i < num_rows; ++i) {
        result.append(generate_uuid_string());
    }

    return result.build(false);
}

} // namespace starrocks::vectorized
