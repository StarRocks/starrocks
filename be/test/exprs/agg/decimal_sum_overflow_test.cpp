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

// Overflow behaviour of SUM() over DECIMAL, mirroring the binary-operator coverage in
// be/test/exprs/decimal_binary_function_test.cpp.
//
// SUM(DECIMAL32|64|128) accumulates into an int128 and SUM(DECIMAL256) into an int256. Before this
// change every accumulation was a bare `sum += value`, so the accumulator wrapped silently even
// when `sql_mode = 'ERROR_IF_OVERFLOW'` was set.
//
// The overflow is reported through FunctionContext::set_error(), NOT by throwing: several
// aggregation entry points (Analytor::finish_process, SortedStreamingAggregator::_update_states,
// the two spillable aggregate sources) run outside any catch handler, so a thrown exception would
// reach the pipeline driver and terminate the BE. These tests therefore assert on
// ctx->has_error()/ctx->error_msg() and that nothing is thrown.
//
// UBSan: this file is NOT claimed to be free of undefined behaviour. The helper below constructs
// its out-of-range constant in the unsigned domain so that the test code itself does not overflow
// a signed int128, but removable_cumulative_window_does_not_false_positive() drives the PRODUCTION
// accumulator (sum.h, update_state_removable_cumulatively(), the deliberately unchecked bare
// `sum -= value` / `sum += value`) through -1.8 * 10^38, which is outside int128. That transit is
// the very thing the test exists to pin down, so it cannot be removed from the test, and the
// arithmetic is in code this change intentionally leaves unchecked, so it cannot be fixed here
// either. be/CMakeLists.txt builds UBSAN as RELEASE + -fsanitize=undefined with the
// `-fno-sanitize=...` exclusion list commented out, so signed-integer-overflow is live there; that
// one test therefore skips itself when it can detect a sanitizer build (see STARROCKS_TEST_UBSAN
// below - the detection is best-effort, not guaranteed on every compiler).

#include <gtest/gtest.h>

#include <limits>

#include "exprs/agg/base_aggregate_test.h"
#include "exprs/agg/sum.h"
#include "gen_cpp/InternalService_types.h"
#include "runtime/runtime_state.h"
#include "testutil/function_utils.h"

// Best-effort detection of a build with the undefined-behaviour sanitizer enabled. clang exposes
// __has_feature(undefined_behavior_sanitizer); gcc defines __SANITIZE_UNDEFINED__ only from gcc 14
// on. Neither is guaranteed, so a false negative here means the skipped test runs and the
// sanitizer reports the transit described at the top of this file.
#if defined(__has_feature)
#if __has_feature(undefined_behavior_sanitizer)
#define STARROCKS_TEST_UBSAN 1
#endif
#endif
#if defined(__SANITIZE_UNDEFINED__) && !defined(STARROCKS_TEST_UBSAN)
#define STARROCKS_TEST_UBSAN 1
#endif

namespace starrocks {

// 10^38 - 1, the largest DECIMAL(38, 0).
static int128_t decimal38_max() {
    int128_t v = 0;
    for (int i = 0; i < 38; ++i) {
        v = v * 10 + 9;
    }
    return v;
}

// 199999999999999999999999999999999999998 - 2^128, the value the unpatched build returns for
// SUM() over two rows of 10^38-1 (measured on starrocks/allin1-ubuntu:
// SELECT SUM(a) FROM d38 -> -140282366920938463463374607431768211458).
//
// 2 * (10^38 - 1) is above INT128_MAX, so the doubling is done on unsigned __int128, where the
// wraparound is defined, and only the result is converted back. A bare `a + a` would be signed
// overflow, i.e. undefined behaviour that the UBSAN build would abort on.
static int128_t wrapped_two_times_decimal38_max() {
    auto m = static_cast<unsigned __int128>(decimal38_max());
    return static_cast<int128_t>(m + m);
}

// 9 * 10^37, the value used by the sliding-window (D3) case below. 2 * A = 1.8 * 10^38 does not
// fit an int128, whose maximum is ~1.70141 * 10^38.
static int128_t nine_e37() {
    int128_t v = 9;
    for (int i = 0; i < 37; ++i) {
        v *= 10;
    }
    return v;
}

static constexpr const char* kSumOverflowMsg = "The 'sum' operation involving decimal values overflows";

class DecimalSumOverflowTest : public ::testing::Test {
public:
    void SetUp() override {
        TQueryOptions report_error_options;
        report_error_options.__set_overflow_mode(TOverflowMode::REPORT_ERROR);
        TQueryGlobals query_globals;
        _report_error_state = std::make_unique<RuntimeState>(TUniqueId(), report_error_options, query_globals, nullptr);
        _report_error_utils = std::make_unique<FunctionUtils>(_report_error_state.get());

        TQueryOptions default_options;
        default_options.__set_overflow_mode(TOverflowMode::OUTPUT_NULL);
        _default_state = std::make_unique<RuntimeState>(TUniqueId(), default_options, query_globals, nullptr);
        _default_utils = std::make_unique<FunctionUtils>(_default_state.get());
    }

    // ctx->error_if_overflow() == true
    FunctionContext* strict_ctx() { return _report_error_utils->get_fn_ctx(); }
    // ctx->error_if_overflow() == false
    FunctionContext* lenient_ctx() { return _default_utils->get_fn_ctx(); }

private:
    std::unique_ptr<RuntimeState> _report_error_state;
    std::unique_ptr<FunctionUtils> _report_error_utils;
    std::unique_ptr<RuntimeState> _default_state;
    std::unique_ptr<FunctionUtils> _default_utils;
};

static MutableColumnPtr decimal128_column(const std::vector<int128_t>& values) {
    auto column = Decimal128Column::create(38, 0);
    for (auto v : values) {
        column->append(v);
    }
    return column;
}

TEST_F(DecimalSumOverflowTest, contexts_report_the_expected_overflow_mode) {
    ASSERT_TRUE(strict_ctx()->error_if_overflow());
    ASSERT_FALSE(lenient_ctx()->error_if_overflow());
}

// The DEFAULT mode is deliberately unchanged by this patch: it still wraps.
TEST_F(DecimalSumOverflowTest, default_mode_still_wraps_silently) {
    DecimalSumAggregateFunction<TYPE_DECIMAL128> func;
    const AggregateFunction* agg = &func;
    auto aggr_state = ManagedAggrState::create(lenient_ctx(), agg);

    auto column = decimal128_column({decimal38_max(), decimal38_max()});
    const Column* raw = column.get();
    ASSERT_NO_THROW(agg->update(lenient_ctx(), &raw, aggr_state->state(), 0));
    ASSERT_NO_THROW(agg->update(lenient_ctx(), &raw, aggr_state->state(), 1));
    ASSERT_FALSE(lenient_ctx()->has_error());

    auto result = Decimal128Column::create(38, 0);
    agg->finalize_to_column(lenient_ctx(), aggr_state->state(), result.get());
    ASSERT_EQ(wrapped_two_times_decimal38_max(), result->get_data()[0]);
}

TEST_F(DecimalSumOverflowTest, update_reports_overflow_under_error_if_overflow) {
    DecimalSumAggregateFunction<TYPE_DECIMAL128> func;
    const AggregateFunction* agg = &func;
    auto aggr_state = ManagedAggrState::create(strict_ctx(), agg);

    auto column = decimal128_column({decimal38_max(), decimal38_max()});
    const Column* raw = column.get();
    ASSERT_NO_THROW(agg->update(strict_ctx(), &raw, aggr_state->state(), 0));
    ASSERT_FALSE(strict_ctx()->has_error());

    // No exception: the error is recorded on the context, which Aggregator::check_has_error() and
    // Analytor::_check_has_error() poll and turn into Status::RuntimeError.
    ASSERT_NO_THROW(agg->update(strict_ctx(), &raw, aggr_state->state(), 1));
    ASSERT_TRUE(strict_ctx()->has_error());
    ASSERT_STREQ(kSumOverflowMsg, strict_ctx()->error_msg());

    // The state holds the wrapped value, i.e. exactly what the DEFAULT mode would hold. The query
    // is failed by the error flag, not by the state.
    auto result = Decimal128Column::create(38, 0);
    agg->finalize_to_column(strict_ctx(), aggr_state->state(), result.get());
    ASSERT_EQ(wrapped_two_times_decimal38_max(), result->get_data()[0]);
}

// set_error() keeps only the first message, so an overflow repeated over the rest of a chunk does
// not change what check_has_error() reports.
TEST_F(DecimalSumOverflowTest, repeated_overflow_keeps_the_first_message) {
    DecimalSumAggregateFunction<TYPE_DECIMAL128> func;
    const AggregateFunction* agg = &func;
    auto aggr_state = ManagedAggrState::create(strict_ctx(), agg);

    auto column = decimal128_column({decimal38_max(), decimal38_max(), decimal38_max(), decimal38_max()});
    const Column* raw = column.get();
    ASSERT_NO_THROW(agg->update_batch_single_state(strict_ctx(), 4, &raw, aggr_state->state()));
    ASSERT_TRUE(strict_ctx()->has_error());
    ASSERT_STREQ(kSumOverflowMsg, strict_ctx()->error_msg());
}

TEST_F(DecimalSumOverflowTest, update_batch_single_state_reports_overflow) {
    DecimalSumAggregateFunction<TYPE_DECIMAL128> func;
    const AggregateFunction* agg = &func;
    auto aggr_state = ManagedAggrState::create(strict_ctx(), agg);

    auto column = decimal128_column({decimal38_max(), decimal38_max()});
    const Column* raw = column.get();
    ASSERT_NO_THROW(agg->update_batch_single_state(strict_ctx(), 2, &raw, aggr_state->state()));
    ASSERT_TRUE(strict_ctx()->has_error());
    ASSERT_STREQ(kSumOverflowMsg, strict_ctx()->error_msg());
}

TEST_F(DecimalSumOverflowTest, window_frame_update_reports_overflow) {
    DecimalSumAggregateFunction<TYPE_DECIMAL128> func;
    const AggregateFunction* agg = &func;
    auto aggr_state = ManagedAggrState::create(strict_ctx(), agg);

    auto column = decimal128_column({decimal38_max(), decimal38_max()});
    const Column* raw = column.get();
    // The non-incremental frame variant recomputes each frame from scratch, so it only ever holds a
    // real partial sum: the check stays here.
    ASSERT_NO_THROW(agg->update_batch_single_state_with_frame(strict_ctx(), aggr_state->state(), &raw, 0, 2, 0, 2));
    ASSERT_TRUE(strict_ctx()->has_error());
    ASSERT_STREQ(kSumOverflowMsg, strict_ctx()->error_msg());
}

TEST_F(DecimalSumOverflowTest, merge_reports_overflow) {
    DecimalSumAggregateFunction<TYPE_DECIMAL128> func;
    const AggregateFunction* agg = &func;
    auto aggr_state = ManagedAggrState::create(strict_ctx(), agg);

    auto column = decimal128_column({decimal38_max(), decimal38_max()});
    ASSERT_NO_THROW(agg->merge(strict_ctx(), column.get(), aggr_state->state(), 0));
    ASSERT_FALSE(strict_ctx()->has_error());
    ASSERT_NO_THROW(agg->merge(strict_ctx(), column.get(), aggr_state->state(), 1));
    ASSERT_TRUE(strict_ctx()->has_error());
    ASSERT_STREQ(kSumOverflowMsg, strict_ctx()->error_msg());
}

TEST_F(DecimalSumOverflowTest, retract_reports_underflow) {
    DecimalSumAggregateFunction<TYPE_DECIMAL128> func;
    const AggregateFunction* agg = &func;
    auto aggr_state = ManagedAggrState::create(strict_ctx(), agg);

    auto column = decimal128_column({-decimal38_max(), decimal38_max()});
    const Column* raw = column.get();
    ASSERT_NO_THROW(agg->update(strict_ctx(), &raw, aggr_state->state(), 0));
    // state == -(10^38-1); subtracting 10^38-1 underflows the int128 accumulator.
    ASSERT_NO_THROW(agg->retract(strict_ctx(), &raw, aggr_state->state(), 1));
    ASSERT_TRUE(strict_ctx()->has_error());
    ASSERT_STREQ(kSumOverflowMsg, strict_ctx()->error_msg());
}

TEST_F(DecimalSumOverflowTest, no_false_positive_when_the_sum_fits) {
    DecimalSumAggregateFunction<TYPE_DECIMAL128> func;
    const AggregateFunction* agg = &func;
    auto aggr_state = ManagedAggrState::create(strict_ctx(), agg);

    auto column = decimal128_column({decimal38_max(), -decimal38_max(), int128_t(7)});
    const Column* raw = column.get();
    ASSERT_NO_THROW(agg->update_batch_single_state(strict_ctx(), 3, &raw, aggr_state->state()));
    ASSERT_FALSE(strict_ctx()->has_error());

    auto result = Decimal128Column::create(38, 0);
    agg->finalize_to_column(strict_ctx(), aggr_state->state(), result.get());
    ASSERT_EQ(int128_t(7), result->get_data()[0]);
}

// D3. The incremental sliding-window update subtracts the row leaving the frame BEFORE adding the
// row entering it, so the accumulator transits a value that belongs to no frame. With A = 9 * 10^37
// and the rows (A, -A, -A, A) under `ROWS BETWEEN 2 PRECEDING AND CURRENT ROW`, the four frame sums
// are A, 0, -A, -A - all inside int128 - but the transit while computing row 4 is -2A = -1.8*10^38,
// outside it. update_state_removable_cumulatively() must therefore NOT check, or every one of these
// four correct rows would be rejected. Measured on unpatched starrocks/allin1-ubuntu:
//   1  90000000000000000000000000000000000000
//   2  0
//   3 -90000000000000000000000000000000000000
//   4 -90000000000000000000000000000000000000
TEST_F(DecimalSumOverflowTest, removable_cumulative_window_does_not_false_positive) {
#ifdef STARROCKS_TEST_UBSAN
    // The -2A transit below is signed int128 overflow inside the unchecked production path; it is
    // what this test asserts is tolerated, so it cannot be written any other way. Skip rather than
    // trip -fsanitize=signed-integer-overflow.
    GTEST_SKIP() << "drives the unchecked accumulator through -1.8e38 by design; UB under UBSan";
#endif
    const int128_t a = nine_e37();
    DecimalSumAggregateFunction<TYPE_DECIMAL128> func;
    const AggregateFunction* agg = &func;
    auto aggr_state = ManagedAggrState::create(strict_ctx(), agg);

    auto column = decimal128_column({a, -a, -a, a});
    const Column* raw = column.get();

    // `ROWS BETWEEN 2 PRECEDING AND CURRENT ROW` over a 4-row partition.
    const int64_t partition_start = 0;
    const int64_t partition_end = 4;
    const int64_t rows_start_offset = -2;
    const int64_t rows_end_offset = 0;
    const int128_t expected[4] = {a, int128_t(0), -a, -a};

    for (int64_t row = 0; row < 4; ++row) {
        ASSERT_NO_THROW(agg->update_state_removable_cumulatively(strict_ctx(), aggr_state->state(), &raw, row,
                                                                 partition_start, partition_end, rows_start_offset,
                                                                 rows_end_offset, false, false, false));
        auto result = Decimal128Column::create(38, 0);
        agg->finalize_to_column(strict_ctx(), aggr_state->state(), result.get());
        ASSERT_EQ(expected[row], result->get_data()[0]) << "row " << row;
        ASSERT_FALSE(strict_ctx()->has_error()) << "row " << row;
    }
}

// The transit above really is out of range: the same subtraction done through the CHECKED path
// (retract) does report. This is what makes the previous paragraph a false positive and not a
// missed overflow.
TEST_F(DecimalSumOverflowTest, the_window_transit_really_overflows) {
    const int128_t a = nine_e37();
    DecimalSumAggregateFunction<TYPE_DECIMAL128> func;
    const AggregateFunction* agg = &func;
    auto aggr_state = ManagedAggrState::create(strict_ctx(), agg);

    auto column = decimal128_column({-a, a});
    const Column* raw = column.get();
    ASSERT_NO_THROW(agg->update(strict_ctx(), &raw, aggr_state->state(), 0)); // state = -A
    ASSERT_FALSE(strict_ctx()->has_error());
    ASSERT_NO_THROW(agg->retract(strict_ctx(), &raw, aggr_state->state(), 1)); // -A - A = -2A
    ASSERT_TRUE(strict_ctx()->has_error());
    ASSERT_STREQ(kSumOverflowMsg, strict_ctx()->error_msg());
}

// Integer SUM is not checked at all: ERROR_IF_OVERFLOW is documented for DECIMAL only, and the
// generic template is compiled with CheckOverflow=false, so it never inspects the context.
//
// This asserts only the part that is well defined: summing right up to INT64_MAX under
// ERROR_IF_OVERFLOW sets no error. It deliberately does NOT drive the accumulator past INT64_MAX.
// The CheckOverflow=false body is a bare `sum += value`, so an int64 overflow there is signed
// overflow, i.e. undefined behaviour, and a UBSan build would abort on it rather than observe the
// wraparound. MySQL's silent-wraparound semantics for integer SUM are covered where they can be
// observed legitimately, in the SQL regression: `SELECT SUM(b) FROM bigint_t` over two rows of
// INT64_MAX returns -2 on the real server (measured on starrocks/allin1-ubuntu:latest).
TEST_F(DecimalSumOverflowTest, integer_sum_is_never_checked_under_error_if_overflow) {
    SumAggregateFunction<TYPE_BIGINT> func;
    const AggregateFunction* agg = &func;
    auto aggr_state = ManagedAggrState::create(strict_ctx(), agg);

    auto column = Int64Column::create();
    column->append(std::numeric_limits<int64_t>::max() - 1);
    column->append(int64_t(1));
    const Column* raw = column.get();
    ASSERT_NO_THROW(agg->update_batch_single_state(strict_ctx(), 2, &raw, aggr_state->state()));
    ASSERT_FALSE(strict_ctx()->has_error());

    auto result = Int64Column::create();
    agg->finalize_to_column(strict_ctx(), aggr_state->state(), result.get());
    ASSERT_EQ(std::numeric_limits<int64_t>::max(), result->get_data()[0]);
}

} // namespace starrocks
