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

#include <gtest/gtest.h>

#include <cmath>
#include <limits>
#include <memory>

// percentile_approx.h forward-declares FunctionContext and ArrayColumn and is not
// self-contained; pull in their full definitions before it. Order matters, so keep
// clang-format from hoisting percentile_approx.h to the top as the main header.
// clang-format off
#include "column/array_column.h"
#include "column/column_helper.h"
#include "exprs/function_context.h"
#include "exprs/agg/percentile_approx.h"
// clang-format on

#include "exprs/agg/base_aggregate_test.h" // UTRawType + FunctionContext test helpers

namespace starrocks {

// Exposes the protected compression contract of
// PercentileApproxAggregateFunctionBase (clamp + the [MIN, MAX]/DEFAULT
// constants) so it can be asserted directly.
class PercentileCompressionExposer : public PercentileApproxAggregateFunction {
public:
    static double clamp(double c) { return clamp_compression_factor(c); }
    static double min_c() { return MIN_COMPRESSION; }
    static double max_c() { return MAX_COMPRESSION; }
    static double default_c() { return DEFAULT_COMPRESSION_FACTOR; }
};

static ColumnPtr const_double(double v) {
    return ColumnHelper::create_const_column<TYPE_DOUBLE>(v, 1);
}

TEST(PercentileApproxCompressionTest, clampCompressionFactor) {
    const double lo = PercentileCompressionExposer::min_c();
    const double hi = PercentileCompressionExposer::max_c();
    const double def = PercentileCompressionExposer::default_c();

    // In-range values pass through unchanged.
    EXPECT_DOUBLE_EQ(lo, PercentileCompressionExposer::clamp(lo));
    EXPECT_DOUBLE_EQ(hi, PercentileCompressionExposer::clamp(hi));
    EXPECT_DOUBLE_EQ(5000.0, PercentileCompressionExposer::clamp(5000.0));

    // Below MIN, above MAX, non-positive and non-finite all canonicalize to the default.
    EXPECT_DOUBLE_EQ(def, PercentileCompressionExposer::clamp(lo - 1));
    EXPECT_DOUBLE_EQ(def, PercentileCompressionExposer::clamp(hi + 1));
    EXPECT_DOUBLE_EQ(def, PercentileCompressionExposer::clamp(0.0));
    EXPECT_DOUBLE_EQ(def, PercentileCompressionExposer::clamp(-5.0));
    EXPECT_DOUBLE_EQ(def, PercentileCompressionExposer::clamp(std::nan("")));
    EXPECT_DOUBLE_EQ(def, PercentileCompressionExposer::clamp(std::numeric_limits<double>::infinity()));
}

TEST(PercentileApproxCompressionTest, getCompressionFactorApprox) {
    PercentileApproxAggregateFunction fn;
    const double def = PercentileCompressionExposer::default_c();

    // 2-arg call (no explicit compression): legacy/rolling-upgrade path -> default.
    {
        std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context(
                {UTRawType{.type = TYPE_DOUBLE}, UTRawType{.type = TYPE_DOUBLE}}, UTRawType{.type = TYPE_DOUBLE}));
        EXPECT_DOUBLE_EQ(def, fn.get_compression_factor(ctx.get()));
    }
    // 3-arg call, in-range constant compression -> used as-is.
    {
        std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context(
                {UTRawType{.type = TYPE_DOUBLE}, UTRawType{.type = TYPE_DOUBLE}, UTRawType{.type = TYPE_DOUBLE}},
                UTRawType{.type = TYPE_DOUBLE}));
        ctx->set_constant_columns({nullptr, nullptr, const_double(5000.0)});
        EXPECT_DOUBLE_EQ(5000.0, fn.get_compression_factor(ctx.get()));
    }
    // 3-arg call, out-of-range constant compression -> clamped to default.
    {
        std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context(
                {UTRawType{.type = TYPE_DOUBLE}, UTRawType{.type = TYPE_DOUBLE}, UTRawType{.type = TYPE_DOUBLE}},
                UTRawType{.type = TYPE_DOUBLE}));
        ctx->set_constant_columns({nullptr, nullptr, const_double(1.0)});
        EXPECT_DOUBLE_EQ(def, fn.get_compression_factor(ctx.get()));
    }
}

TEST(PercentileApproxCompressionTest, getCompressionFactorWeighted) {
    PercentileApproxWeightedAggregateFunction fn;
    const double def = PercentileCompressionExposer::default_c();

    // Legacy 3-arg form (value, weight, quantile) without compression -> default.
    {
        std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context(
                {UTRawType{.type = TYPE_DOUBLE}, UTRawType{.type = TYPE_BIGINT}, UTRawType{.type = TYPE_DOUBLE}},
                UTRawType{.type = TYPE_DOUBLE}));
        EXPECT_DOUBLE_EQ(def, fn.get_compression_factor(ctx.get()));
    }
    // 4-arg form: compression is the rightmost constant; a non-const weight (nullptr) is skipped.
    {
        std::unique_ptr<FunctionContext> ctx(
                FunctionContext::create_test_context({UTRawType{.type = TYPE_DOUBLE}, UTRawType{.type = TYPE_BIGINT},
                                                      UTRawType{.type = TYPE_DOUBLE}, UTRawType{.type = TYPE_DOUBLE}},
                                                     UTRawType{.type = TYPE_DOUBLE}));
        ctx->set_constant_columns({nullptr, nullptr, const_double(0.5), const_double(5000.0)});
        EXPECT_DOUBLE_EQ(5000.0, fn.get_compression_factor(ctx.get()));
    }
    // 4-arg form, out-of-range compression -> clamped to default.
    {
        std::unique_ptr<FunctionContext> ctx(
                FunctionContext::create_test_context({UTRawType{.type = TYPE_DOUBLE}, UTRawType{.type = TYPE_BIGINT},
                                                      UTRawType{.type = TYPE_DOUBLE}, UTRawType{.type = TYPE_DOUBLE}},
                                                     UTRawType{.type = TYPE_DOUBLE}));
        ctx->set_constant_columns({nullptr, nullptr, const_double(0.5), const_double(20000.0)});
        EXPECT_DOUBLE_EQ(def, fn.get_compression_factor(ctx.get()));
    }
}

} // namespace starrocks
