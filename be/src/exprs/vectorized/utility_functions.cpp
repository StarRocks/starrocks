// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#include "exprs/vectorized/utility_functions.h"

#include <emmintrin.h>
#include <unistd.h>

#include <algorithm>
#include <atomic>
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
#include "gen_cpp/version.h"
#include "gutil/casts.h"
#include "runtime/primitive_type.h"
#include "runtime/runtime_state.h"
#include "service/backend_options.h"
#include "udf/udf_internal.h"
#include "util/cidr.h"
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

ColumnPtr UtilityFunctions::uuid(FunctionContext* ctx, const Columns& columns) {
    int32_t num_rows = ColumnHelper::get_const_value<TYPE_INT>(columns.back());

    auto col = UtilityFunctions::uuid_numeric(ctx, columns);
    auto& uuid_data = down_cast<Int128Column*>(col.get())->get_data();

    auto res = BinaryColumn::create();
    auto& bytes = res->get_bytes();
    auto& offsets = res->get_offset();

    offsets.resize(num_rows + 1);
    bytes.resize(36 * num_rows);

    char* ptr = reinterpret_cast<char*>(bytes.data());

    for (int i = 0; i < num_rows; ++i) {
        offsets[i + 1] = offsets[i] + 36;
    }

#ifdef __SSE4_2__
    alignas(16) static constexpr const char hex_chars[16] = {'0', '1', '2', '3', '4', '5', '6', '7',
                                                             '8', '9', 'a', 'b', 'c', 'd', 'e', 'f'};
    const __m128i mask = _mm_set1_epi8(0xF);
    const __m128i chars = _mm_load_si128(reinterpret_cast<const __m128i*>(hex_chars));
#endif

    for (int i = 0; i < num_rows; ++i) {
        char buff[32];
        memset(ptr, '-', 36);
#ifdef __SSE4_2__
        // SIMD::to_hex
        __m128i value = _mm_loadu_si64(reinterpret_cast<const __m128i*>(&uuid_data[i]));
        // 0x1234
        //-> [0x34, 0x12]
        //-> [0x23, 0x01] right shift
        //-> [0x34, 0x23, 0x12, 0x01] pack
        //-> [0x04, 0x03, 0x02, 0x01] mask operator
        //-> shuffle
        value = _mm_and_si128(_mm_unpacklo_epi8(_mm_srli_epi64(value, 4), value), mask);
        value = _mm_shuffle_epi8(chars, value);
        _mm_storeu_si128(reinterpret_cast<__m128i*>(buff), value);

        value = _mm_loadu_si64(reinterpret_cast<const __m128i*>(reinterpret_cast<const int64_t*>(&uuid_data[i]) + 1));
        value = _mm_and_si128(_mm_unpacklo_epi8(_mm_srli_epi64(value, 4), value), mask);
        value = _mm_shuffle_epi8(chars, value);
        _mm_storeu_si128(reinterpret_cast<__m128i*>(buff + 16), value);
#else
        to_hex(uuid_data[i], buff);
        std::reverse(buff, buff + 32);
#endif

        // UUID format 8-4-4-4-12

        memcpy(ptr, buff, 8);
        memcpy(ptr + 8 + 1, buff + 8, 4);
        memcpy(ptr + 8 + 4 + 2, buff + 8 + 4, 4);
        memcpy(ptr + 8 + 4 + 4 + 3, buff + 8 + 4 + 4, 4);
        memcpy(ptr + 8 + 4 + 4 + 4 + 4, buff + 8 + 4 + 4 + 4, 12);

        ptr += 36;
    }

    return res;
}

inline int128_t next_uuid(int64_t timestamp, int16_t backendId, int16_t rand, int16_t tid, int32_t inc) {
    union {
        struct {
            int64_t timestamp : 48;
            int64_t instance : 16;
            int16_t rand;
            int16_t tid;
            int32_t inc;
        } data;
        int128_t res;
    } v;
    v.data.timestamp = timestamp;
    v.data.instance = backendId;
    v.data.rand = rand;
    v.data.tid = tid;
    v.data.inc = inc;
    return v.res;
}

static std::atomic<int32_t> s_counter{};
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

    uint32_t intip;
    CIDR::ip_to_int(BackendOptions::get_localhost(), &intip);
    intip ^= config::brpc_port;
    // current thread id
    int tid = get_uniq_tid();
    int64_t timestamp = GetCurrentTimeMicros();

    int16_t rand = dist(mt);
    int32_t inc = s_counter.fetch_add(num_rows);

    for (int i = 0; i < num_rows; ++i) {
        data[i] = next_uuid(timestamp, intip, rand, tid, inc - i);
    }

    return result;
}

} // namespace starrocks::vectorized
