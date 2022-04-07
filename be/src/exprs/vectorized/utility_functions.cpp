// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#include "exprs/vectorized/utility_functions.h"

#include <cstdint>
#include <cstdlib>
#include <limits>
#include <mutex>
#include <random>
#include <thread>

#include "column/column_builder.h"
#include "column/column_viewer.h"
#include "column/vectorized_fwd.h"
#include "common/config.h"
#include "common/version.h"
#include "runtime/primitive_type.h"
#include "runtime/runtime_state.h"
#include "service/backend_options.h"
#include "udf/udf_internal.h"
#include "util/monotime.h"
#include "util/thread.h"
#include "util/time.h"
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
    ColumnViewer<TYPE_INT> data_column(columns[0]);

    auto size = columns[0]->size();
    ColumnBuilder<TYPE_BOOLEAN> result(size);
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

    ColumnBuilder<TYPE_VARCHAR> result(num_rows);
    for (int i = 0; i < num_rows; ++i) {
        result.append(generate_uuid_string());
    }

    return result.build(false);
}

inline int128_t next_uuid(int64_t timestamp, int16_t backendId, int16_t rand, int16_t tid, int16_t inc) {
    union {
        struct {
            int64_t timestamp;
            int16_t backendId;
            int16_t rand;
            int16_t tid;
            int16_t inc;
        } data;
        int128_t res;
    } v;
    v.data.timestamp = timestamp;
    v.data.backendId = backendId;
    v.data.rand = rand;
    v.data.tid = tid;
    v.data.inc = inc;
    return v.res;
}

// thread ids
// The number of executor threads is fixed.
static std::atomic<int16_t> inc{};
//
static thread_local int uniq_tid = -1;

int16_t get_uniq_tid() {
    if (uniq_tid == -1) {
        uniq_tid = inc.fetch_add(1);
    }
    return uniq_tid;
}

ColumnPtr UtilityFunctions::uuid_numeric(FunctionContext*, const Columns& columns) {
    int32_t num_rows = ColumnHelper::get_const_value<TYPE_INT>(columns.back());
    auto result = Int128Column::create(num_rows);

    static std::random_device rd;
    static std::mt19937 mt(rd());

    std::uniform_int_distribution<int16_t> dist(std::numeric_limits<int16_t>::min(),
                                                std::numeric_limits<int16_t>::max());

    auto& data = result->get_data();

    int backend_id = std::hash<std::string>()(BackendOptions::get_localhost());
    backend_id ^= config::brpc_port;
    // config::brpc_port
    // current thread id
    int tid = get_uniq_tid();
    int64_t timestamp = MonotonicNanos();
    int16_t rand = dist(mt);

    DCHECK_LE(num_rows, std::numeric_limits<int16_t>::max());

    for (int i = 0; i < num_rows; ++i) {
        data[i] = next_uuid(timestamp, backend_id, rand, tid, i);
    }

    return result;
}

} // namespace starrocks::vectorized
