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

#include "exprs/agg/percentile_approx.h"

#include <gtest/gtest.h>

#include <cmath>
#include <limits>
#include <memory>

#include "column/binary_column.h"
#include "column/column_helper.h"
#include "column/fixed_length_column.h"
#include "column/nullable_column.h"
#include "column/vectorized_fwd.h"
#include "exprs/agg/aggregate_factory.h"
#include "exprs/agg/aggregate_state_allocator.h"
#include "exprs/agg/nullable_aggregate.h"
#include "exprs/function_context.h"
#include "runtime/memory/counting_allocator.h"
#include "runtime/runtime_state.h"
#include "testutil/function_utils.h"
#include "types/logical_type.h"

namespace starrocks {

class PercentileApproxAggTest : public ::testing::Test {
public:
    void SetUp() override {
        _utils = std::make_unique<FunctionUtils>();
        _ctx = _utils->get_fn_ctx();
        _allocator = std::make_unique<CountingAllocatorWithHook>();
        tls_agg_state_allocator = _allocator.get();
    }
    void TearDown() override {
        tls_agg_state_allocator = nullptr;
        _allocator.reset();
        _utils.reset();
    }

protected:
    std::unique_ptr<FunctionUtils> _utils;
    FunctionContext* _ctx{};
    std::unique_ptr<CountingAllocatorWithHook> _allocator;
};

namespace {

// Build a FunctionContext that carries the const columns the aggregate
// expects via ctx->get_constant_column(i). Used for the quantile and
// (optionally) compression parameters.
std::unique_ptr<FunctionContext> make_ctx(std::vector<TypeDescriptor> arg_types, TypeDescriptor return_type,
                                          Columns const_columns) {
    std::unique_ptr<FunctionContext> ctx(
            FunctionContext::create_test_context(std::move(arg_types), std::move(return_type)));
    ctx->set_constant_columns(std::move(const_columns));
    return ctx;
}

struct ManagedState {
    ManagedState(FunctionContext* ctx, const AggregateFunction* func) : _ctx(ctx), _func(func) {
        _state = _mem_pool.allocate_aligned(func->size(), func->alignof_size());
        _func->create(_ctx, _state);
    }
    ~ManagedState() { _func->destroy(_ctx, _state); }
    AggDataPtr state() { return _state; }

    FunctionContext* _ctx;
    const AggregateFunction* _func;
    MemPool _mem_pool;
    AggDataPtr _state;
};

} // namespace

// percentile_approx_weighted with a row whose weight is negative.
// The bug: only weight == 0 was filtered, so the negative-weight row
// entered the digest and produced an incorrect median.
TEST_F(PercentileApproxAggTest, weighted_negative_weight_is_skipped) {
    const AggregateFunction* func =
            get_aggregate_function("percentile_approx_weighted", TYPE_DOUBLE, TYPE_DOUBLE, /*is_nullable=*/false);
    ASSERT_NE(func, nullptr);

    std::vector<TypeDescriptor> arg_types{TypeDescriptor::from_logical_type(TYPE_DOUBLE),
                                          TypeDescriptor::from_logical_type(TYPE_BIGINT),
                                          TypeDescriptor::from_logical_type(TYPE_DOUBLE)};
    auto return_type = TypeDescriptor::from_logical_type(TYPE_DOUBLE);
    auto quantile_const = ColumnHelper::create_const_column<TYPE_DOUBLE>(0.5, 1);
    Columns const_columns{ColumnHelper::create_const_column<TYPE_DOUBLE>(0, 1),
                          ColumnHelper::create_const_column<TYPE_BIGINT>(0, 1), quantile_const};
    auto ctx = make_ctx(arg_types, return_type, const_columns);

    auto values = DoubleColumn::create();
    auto weights = Int64Column::create();
    for (auto v : {10.0, 20.0, 30.0}) {
        values->append(v);
        weights->append(1);
    }
    values->append(100.0);
    weights->append(-10);

    std::vector<const Column*> raw{values.get(), weights.get(), quantile_const.get()};

    ManagedState state(ctx.get(), func);
    for (size_t i = 0; i < values->size(); ++i) {
        func->update(ctx.get(), raw.data(), state.state(), i);
    }

    auto result = DoubleColumn::create();
    func->finalize_to_column(ctx.get(), state.state(), result.get());

    ASSERT_EQ(1U, result->size());
    EXPECT_DOUBLE_EQ(20.0, result->get_data()[0]);
}

// percentile_approx with non-finite values in the input.
// Bug: TDigest::add() rejected NaN but accepted +Inf / -Inf, which
// distorted the digest and the quantile.
TEST_F(PercentileApproxAggTest, scalar_inf_values_are_skipped) {
    const AggregateFunction* func =
            get_aggregate_function("percentile_approx", TYPE_DOUBLE, TYPE_DOUBLE, /*is_nullable=*/false);
    ASSERT_NE(func, nullptr);

    std::vector<TypeDescriptor> arg_types{TypeDescriptor::from_logical_type(TYPE_DOUBLE),
                                          TypeDescriptor::from_logical_type(TYPE_DOUBLE)};
    auto return_type = TypeDescriptor::from_logical_type(TYPE_DOUBLE);
    auto quantile_const = ColumnHelper::create_const_column<TYPE_DOUBLE>(0.5, 1);
    Columns const_columns{ColumnHelper::create_const_column<TYPE_DOUBLE>(0, 1), quantile_const};
    auto ctx = make_ctx(arg_types, return_type, const_columns);

    auto values = DoubleColumn::create();
    for (auto v : {10.0, 20.0, 30.0}) {
        values->append(v);
    }
    values->append(std::numeric_limits<double>::infinity());
    values->append(-std::numeric_limits<double>::infinity());
    values->append(std::numeric_limits<double>::quiet_NaN());

    std::vector<const Column*> raw{values.get(), quantile_const.get()};

    ManagedState state(ctx.get(), func);
    for (size_t i = 0; i < values->size(); ++i) {
        func->update(ctx.get(), raw.data(), state.state(), i);
    }

    auto result = DoubleColumn::create();
    func->finalize_to_column(ctx.get(), state.state(), result.get());

    ASSERT_EQ(1U, result->size());
    const double median = result->get_data()[0];
    EXPECT_TRUE(std::isfinite(median));
    // T-Digest is approximate; the median of {10, 20, 30} should be near 20.
    EXPECT_GE(median, 10.0);
    EXPECT_LE(median, 30.0);
}

// Zero-total-weight state must finalize to SQL NULL via the nullable
// wrapper, not NaN. Reproduces only on the is_nullable=true mapping,
// which is what ALWAYS_NULLABLE_RESULT_AGG_FUNCS forces at runtime.
TEST_F(PercentileApproxAggTest, weighted_empty_state_finalizes_to_null) {
    const AggregateFunction* func =
            get_aggregate_function("percentile_approx_weighted", TYPE_DOUBLE, TYPE_DOUBLE, /*is_nullable=*/true);
    ASSERT_NE(func, nullptr);

    std::vector<TypeDescriptor> arg_types{TypeDescriptor::from_logical_type(TYPE_DOUBLE),
                                          TypeDescriptor::from_logical_type(TYPE_BIGINT),
                                          TypeDescriptor::from_logical_type(TYPE_DOUBLE)};
    auto return_type = TypeDescriptor::from_logical_type(TYPE_DOUBLE);
    auto weight_const = ColumnHelper::create_const_column<TYPE_BIGINT>(0, 1);
    auto quantile_const = ColumnHelper::create_const_column<TYPE_DOUBLE>(0.5, 1);
    Columns const_columns{ColumnHelper::create_const_column<TYPE_DOUBLE>(0, 1), weight_const, quantile_const};
    auto ctx = make_ctx(arg_types, return_type, const_columns);

    auto values = NullableColumn::create(DoubleColumn::create(), NullColumn::create());
    auto* values_data = down_cast<DoubleColumn*>(values->data_column()->as_mutable_raw_ptr());
    for (auto v : {10.0, 20.0, 30.0}) {
        values_data->append(v);
        values->null_column_data().push_back(0);
    }

    auto weights =
            NullableColumn::create(Int64Column::create(values->size(), 0), NullColumn::create(values->size(), 0));

    std::vector<const Column*> raw{values.get(), weights.get(), quantile_const.get()};

    ManagedState state(ctx.get(), func);
    for (size_t i = 0; i < values->size(); ++i) {
        func->update(ctx.get(), raw.data(), state.state(), i);
    }

    auto result = NullableColumn::create(DoubleColumn::create(), NullColumn::create());
    func->finalize_to_column(ctx.get(), state.state(), result.get());

    ASSERT_EQ(1U, result->size());
    EXPECT_TRUE(result->is_null(0));
}

// Non-finite compression must not propagate to TDigest sizing math.
// FE blocks the SQL path, so we drive it directly via FunctionContext to
// exercise the BE-side guard.
TEST_F(PercentileApproxAggTest, non_finite_compression_falls_back_to_default) {
    const AggregateFunction* func =
            get_aggregate_function("percentile_approx", TYPE_DOUBLE, TYPE_DOUBLE, /*is_nullable=*/false);
    ASSERT_NE(func, nullptr);

    std::vector<TypeDescriptor> arg_types{TypeDescriptor::from_logical_type(TYPE_DOUBLE),
                                          TypeDescriptor::from_logical_type(TYPE_DOUBLE),
                                          TypeDescriptor::from_logical_type(TYPE_DOUBLE)};
    auto return_type = TypeDescriptor::from_logical_type(TYPE_DOUBLE);
    auto quantile_const = ColumnHelper::create_const_column<TYPE_DOUBLE>(0.5, 1);
    auto bad_compression = ColumnHelper::create_const_column<TYPE_DOUBLE>(std::numeric_limits<double>::quiet_NaN(), 1);
    Columns const_columns{ColumnHelper::create_const_column<TYPE_DOUBLE>(0, 1), quantile_const, bad_compression};
    auto ctx = make_ctx(arg_types, return_type, const_columns);

    auto values = DoubleColumn::create();
    for (auto v : {10.0, 20.0, 30.0}) {
        values->append(v);
    }
    std::vector<const Column*> raw{values.get(), quantile_const.get(), bad_compression.get()};

    ManagedState state(ctx.get(), func);
    for (size_t i = 0; i < values->size(); ++i) {
        func->update(ctx.get(), raw.data(), state.state(), i);
    }

    auto result = DoubleColumn::create();
    func->finalize_to_column(ctx.get(), state.state(), result.get());

    ASSERT_EQ(1U, result->size());
    const double median = result->get_data()[0];
    EXPECT_TRUE(std::isfinite(median));
    EXPECT_GE(median, 10.0);
    EXPECT_LE(median, 30.0);
}

// ---------------------------------------------------------------------------
// Compact intermediate format (enable_percentile_compact_intermediate).
// ---------------------------------------------------------------------------

// scalar percentile_approx: convert_to_exchange_format -> merge -> finalize must
// produce the same quantile under the compact format as under legacy, and the
// per-row record must shrink to the 9-byte RAW form. The merge phase recovers
// the quantile from the const args [value, quantile, compression].
TEST_F(PercentileApproxAggTest, scalar_compact_roundtrip_matches_legacy) {
    const AggregateFunction* func =
            get_aggregate_function("percentile_approx", TYPE_DOUBLE, TYPE_DOUBLE, /*is_nullable=*/false);
    ASSERT_NE(func, nullptr);

    std::vector<TypeDescriptor> arg_types{TypeDescriptor::from_logical_type(TYPE_DOUBLE),
                                          TypeDescriptor::from_logical_type(TYPE_DOUBLE),
                                          TypeDescriptor::from_logical_type(TYPE_DOUBLE)};
    auto return_type = TypeDescriptor::from_logical_type(TYPE_DOUBLE);
    auto quantile_const = ColumnHelper::create_const_column<TYPE_DOUBLE>(0.5, 1);
    auto compression_const = ColumnHelper::create_const_column<TYPE_DOUBLE>(2048, 1);

    auto run = [&](bool compact, double* median, size_t* row_size) {
        Columns const_columns{ColumnHelper::create_const_column<TYPE_DOUBLE>(0, 1), quantile_const, compression_const};
        auto ctx = make_ctx(arg_types, return_type, const_columns);
        TQueryOptions opts;
        opts.__set_enable_percentile_compact_intermediate(compact);
        RuntimeState rs(TUniqueId(), opts, TQueryGlobals(), nullptr);
        ctx->set_runtime_state(&rs);

        auto values = DoubleColumn::create();
        for (double v : {10.0, 20.0, 30.0, 40.0, 50.0}) {
            values->append(v);
        }
        size_t n = values->size();
        Columns src{ColumnPtr(std::move(values)), quantile_const, compression_const};

        MutableColumnPtr inter = BinaryColumn::create();
        func->convert_to_exchange_format(ctx.get(), src, n, inter);
        ASSERT_EQ(n, inter->size());
        *row_size = down_cast<BinaryColumn*>(inter.get())->get_slice(0).size;

        ManagedState state(ctx.get(), func);
        for (size_t i = 0; i < inter->size(); ++i) {
            func->merge(ctx.get(), inter.get(), state.state(), i);
        }
        ASSERT_FALSE(ctx->has_error());

        auto result = DoubleColumn::create();
        func->finalize_to_column(ctx.get(), state.state(), result.get());
        ASSERT_EQ(1U, result->size());
        *median = result->get_data()[0];
    };

    double legacy_median = 0;
    double compact_median = 0;
    size_t legacy_size = 0;
    size_t compact_size = 0;
    run(false, &legacy_median, &legacy_size);
    run(true, &compact_median, &compact_size);

    EXPECT_DOUBLE_EQ(legacy_median, compact_median);
    EXPECT_EQ(9U, compact_size);          // [tag:1][mean:4][weight:4]
    EXPECT_GT(legacy_size, compact_size); // legacy carries the full TDigest blob
}

// weighted percentile_approx_weighted under the compact exchange format: a
// non-positive weight is written into the RAW record verbatim but dropped at
// merge, so the result matches the set without the bad row.
TEST_F(PercentileApproxAggTest, weighted_compact_drops_non_positive_weight) {
    const AggregateFunction* func =
            get_aggregate_function("percentile_approx_weighted", TYPE_DOUBLE, TYPE_DOUBLE, /*is_nullable=*/false);
    ASSERT_NE(func, nullptr);

    std::vector<TypeDescriptor> arg_types{
            TypeDescriptor::from_logical_type(TYPE_DOUBLE), TypeDescriptor::from_logical_type(TYPE_BIGINT),
            TypeDescriptor::from_logical_type(TYPE_DOUBLE), TypeDescriptor::from_logical_type(TYPE_DOUBLE)};
    auto return_type = TypeDescriptor::from_logical_type(TYPE_DOUBLE);
    auto quantile_const = ColumnHelper::create_const_column<TYPE_DOUBLE>(0.5, 1);
    auto compression_const = ColumnHelper::create_const_column<TYPE_DOUBLE>(2048, 1);
    Columns const_columns{ColumnHelper::create_const_column<TYPE_DOUBLE>(0, 1),
                          ColumnHelper::create_const_column<TYPE_BIGINT>(0, 1), quantile_const, compression_const};
    auto ctx = make_ctx(arg_types, return_type, const_columns);
    TQueryOptions opts;
    opts.__set_enable_percentile_compact_intermediate(true);
    RuntimeState rs(TUniqueId(), opts, TQueryGlobals(), nullptr);
    ctx->set_runtime_state(&rs);

    auto values = DoubleColumn::create();
    auto weights = Int64Column::create();
    for (double v : {10.0, 20.0, 30.0}) {
        values->append(v);
        weights->append(1);
    }
    values->append(100.0);
    weights->append(-10);
    size_t n = values->size();
    Columns src{ColumnPtr(std::move(values)), ColumnPtr(std::move(weights)), quantile_const, compression_const};

    MutableColumnPtr inter = BinaryColumn::create();
    func->convert_to_exchange_format(ctx.get(), src, n, inter);
    ASSERT_EQ(n, inter->size());
    EXPECT_EQ(9U, down_cast<BinaryColumn*>(inter.get())->get_slice(0).size);

    ManagedState state(ctx.get(), func);
    for (size_t i = 0; i < inter->size(); ++i) {
        func->merge(ctx.get(), inter.get(), state.state(), i);
    }
    ASSERT_FALSE(ctx->has_error());

    auto result = DoubleColumn::create();
    func->finalize_to_column(ctx.get(), state.state(), result.get());
    ASSERT_EQ(1U, result->size());
    // median of {10,20,30}; the negative-weight 100 must not enter the digest.
    EXPECT_DOUBLE_EQ(20.0, result->get_data()[0]);
}

// One merge state accepts both a compact RAW record (exchange) and a legacy
// self-contained record (storage), interleaved.
TEST_F(PercentileApproxAggTest, mixed_raw_and_legacy_merge) {
    const AggregateFunction* func =
            get_aggregate_function("percentile_approx", TYPE_DOUBLE, TYPE_DOUBLE, /*is_nullable=*/false);
    ASSERT_NE(func, nullptr);

    std::vector<TypeDescriptor> arg_types{TypeDescriptor::from_logical_type(TYPE_DOUBLE),
                                          TypeDescriptor::from_logical_type(TYPE_DOUBLE),
                                          TypeDescriptor::from_logical_type(TYPE_DOUBLE)};
    auto return_type = TypeDescriptor::from_logical_type(TYPE_DOUBLE);
    auto quantile_const = ColumnHelper::create_const_column<TYPE_DOUBLE>(0.5, 1);
    auto compression_const = ColumnHelper::create_const_column<TYPE_DOUBLE>(2048, 1);
    Columns const_columns{ColumnHelper::create_const_column<TYPE_DOUBLE>(0, 1), quantile_const, compression_const};
    auto ctx = make_ctx(arg_types, return_type, const_columns);
    TQueryOptions opts;
    opts.__set_enable_percentile_compact_intermediate(true);
    RuntimeState rs(TUniqueId(), opts, TQueryGlobals(), nullptr);
    ctx->set_runtime_state(&rs);

    // compact RAW records for {10,20,30} via the exchange path.
    auto raw_values = DoubleColumn::create();
    for (double v : {10.0, 20.0, 30.0}) {
        raw_values->append(v);
    }
    Columns raw_src{ColumnPtr(std::move(raw_values)), quantile_const, compression_const};
    MutableColumnPtr raw_inter = BinaryColumn::create();
    func->convert_to_exchange_format(ctx.get(), raw_src, 3, raw_inter);
    ASSERT_EQ(9U, down_cast<BinaryColumn*>(raw_inter.get())->get_slice(0).size);

    // A legacy self-contained record aggregating {40,50,60} via update + serialize.
    ManagedState src_state(ctx.get(), func);
    {
        auto vals = DoubleColumn::create();
        for (double v : {40.0, 50.0, 60.0}) {
            vals->append(v);
        }
        std::vector<const Column*> raw{vals.get(), quantile_const.get(), compression_const.get()};
        for (size_t i = 0; i < vals->size(); ++i) {
            func->update(ctx.get(), raw.data(), src_state.state(), i);
        }
    }
    MutableColumnPtr digest_inter = BinaryColumn::create();
    func->serialize_to_column(ctx.get(), src_state.state(), digest_inter.get());
    ASSERT_EQ(1U, digest_inter->size());

    // Merge both kinds into one fresh state.
    ManagedState state(ctx.get(), func);
    for (size_t i = 0; i < raw_inter->size(); ++i) {
        func->merge(ctx.get(), raw_inter.get(), state.state(), i);
    }
    func->merge(ctx.get(), digest_inter.get(), state.state(), 0);
    ASSERT_FALSE(ctx->has_error());

    auto result = DoubleColumn::create();
    func->finalize_to_column(ctx.get(), state.state(), result.get());
    ASSERT_EQ(1U, result->size());
    // median of {10,20,30,40,50,60} ~ 35; t-digest is approximate.
    const double median = result->get_data()[0];
    EXPECT_GE(median, 25.0);
    EXPECT_LE(median, 45.0);
}

// A truncated record under the compact format must raise an execution error,
// not read past the slice.
TEST_F(PercentileApproxAggTest, compact_merge_rejects_truncated_record) {
    const AggregateFunction* func =
            get_aggregate_function("percentile_approx", TYPE_DOUBLE, TYPE_DOUBLE, /*is_nullable=*/false);
    ASSERT_NE(func, nullptr);

    std::vector<TypeDescriptor> arg_types{TypeDescriptor::from_logical_type(TYPE_DOUBLE),
                                          TypeDescriptor::from_logical_type(TYPE_DOUBLE)};
    auto return_type = TypeDescriptor::from_logical_type(TYPE_DOUBLE);
    auto quantile_const = ColumnHelper::create_const_column<TYPE_DOUBLE>(0.5, 1);
    Columns const_columns{ColumnHelper::create_const_column<TYPE_DOUBLE>(0, 1), quantile_const};
    auto ctx = make_ctx(arg_types, return_type, const_columns);
    TQueryOptions opts;
    opts.__set_enable_percentile_compact_intermediate(true);
    RuntimeState rs(TUniqueId(), opts, TQueryGlobals(), nullptr);
    ctx->set_runtime_state(&rs);

    // tag = RAW (1) but only 3 bytes total -> shorter than a 9-byte RAW record.
    auto inter = BinaryColumn::create();
    const char truncated[3] = {1, 2, 3};
    inter->append(Slice(truncated, sizeof(truncated)));

    ManagedState state(ctx.get(), func);
    func->merge(ctx.get(), inter.get(), state.state(), 0);
    EXPECT_TRUE(ctx->has_error());
}

// The storage path (convert_to_serialize_format, used by the agg_state
// combinators for persisted values) must stay self-contained legacy regardless
// of the flag, so a value written with the flag ON is read correctly after it is
// turned OFF, and never as a compact RAW record. The reader is given a different
// quantile const (0.99) to prove the quantile comes from the persisted record
// (0.5), not from context.
TEST_F(PercentileApproxAggTest, storage_path_stays_legacy_under_flag) {
    const AggregateFunction* func =
            get_aggregate_function("percentile_approx", TYPE_DOUBLE, TYPE_DOUBLE, /*is_nullable=*/false);
    ASSERT_NE(func, nullptr);

    std::vector<TypeDescriptor> arg_types{TypeDescriptor::from_logical_type(TYPE_DOUBLE),
                                          TypeDescriptor::from_logical_type(TYPE_DOUBLE)};
    auto return_type = TypeDescriptor::from_logical_type(TYPE_DOUBLE);

    // Writer: flag ON, quantile 0.5. The storage path must ignore the flag.
    auto write_quantile = ColumnHelper::create_const_column<TYPE_DOUBLE>(0.5, 1);
    Columns write_const{ColumnHelper::create_const_column<TYPE_DOUBLE>(0, 1), write_quantile};
    auto write_ctx = make_ctx(arg_types, return_type, write_const);
    TQueryOptions on;
    on.__set_enable_percentile_compact_intermediate(true);
    RuntimeState rs_on(TUniqueId(), on, TQueryGlobals(), nullptr);
    write_ctx->set_runtime_state(&rs_on);

    auto values = DoubleColumn::create();
    for (double v : {10.0, 20.0, 30.0, 40.0, 50.0}) {
        values->append(v);
    }
    size_t n = values->size();
    Columns src{ColumnPtr(std::move(values)), write_quantile};
    MutableColumnPtr inter = BinaryColumn::create();
    func->convert_to_serialize_format(write_ctx.get(), src, n, inter);
    // Storage stays self-contained legacy (quantile prefix + TDigest blob), never
    // the 9-byte RAW form, even with the flag on.
    EXPECT_GT(down_cast<BinaryColumn*>(inter.get())->get_slice(0).size, 9U);

    // Reader: flag OFF, with a DIFFERENT quantile constant (0.99). The legacy
    // record is self-contained, so the embedded quantile (0.5) is used and the
    // reader's 0.99 is ignored.
    auto read_quantile = ColumnHelper::create_const_column<TYPE_DOUBLE>(0.99, 1);
    Columns read_const{ColumnHelper::create_const_column<TYPE_DOUBLE>(0, 1), read_quantile};
    auto read_ctx = make_ctx(arg_types, return_type, read_const);
    RuntimeState rs_off(TUniqueId(), TQueryOptions(), TQueryGlobals(), nullptr);
    read_ctx->set_runtime_state(&rs_off);

    ManagedState state(read_ctx.get(), func);
    for (size_t i = 0; i < inter->size(); ++i) {
        func->merge(read_ctx.get(), inter.get(), state.state(), i);
    }
    ASSERT_FALSE(read_ctx->has_error());

    auto result = DoubleColumn::create();
    func->finalize_to_column(read_ctx.get(), state.state(), result.get());
    ASSERT_EQ(1U, result->size());
    // Median (q=0.5 from the record) of {10..50} ~ 30, NOT the q=0.99 (~50) the
    // reader's context would have produced if the quantile leaked from ctx.
    const double median = result->get_data()[0];
    EXPECT_GE(median, 20.0);
    EXPECT_LE(median, 40.0);
}

// Exercise the nullable factory used by SQL, including non-nullable inputs for
// aggregates whose final result can still be NULL (e.g. an empty percentile).
TEST_F(PercentileApproxAggTest, nullable_wrapper_preserves_exchange_and_storage_formats) {
    for (bool weighted : {false, true}) {
        for (bool compact : {false, true}) {
            // 0: non-nullable, 1: nullable without NULLs, 2: mixed, 3: all NULL.
            for (int null_mode : {0, 1, 2, 3}) {
                SCOPED_TRACE(::testing::Message() << weighted << "/" << compact << "/" << null_mode);
                const auto* func = get_aggregate_function(weighted ? "percentile_approx_weighted" : "percentile_approx",
                                                          TYPE_DOUBLE, TYPE_DOUBLE, true);
                ASSERT_NE(nullptr, func);
                auto double_type = TypeDescriptor::from_logical_type(TYPE_DOUBLE);
                std::vector<TypeDescriptor> arg_types{double_type};
                Columns constants{ColumnHelper::create_const_column<TYPE_DOUBLE>(0, 1)};
                auto values = DoubleColumn::create();
                auto weights = Int64Column::create();
                auto nulls = NullColumn::create();
                for (int i = 0; i < 3; ++i) {
                    values->append(10.0);
                    weights->append(1);
                    nulls->append(null_mode == 3 || (null_mode == 2 && i == 1));
                }
                Columns src;
                if (null_mode == 0) {
                    src.emplace_back(std::move(values));
                } else {
                    src.emplace_back(NullableColumn::create(std::move(values), std::move(nulls)));
                }
                if (weighted) {
                    arg_types.push_back(TypeDescriptor::from_logical_type(TYPE_BIGINT));
                    constants.emplace_back(ColumnHelper::create_const_column<TYPE_BIGINT>(1, 1));
                    src.emplace_back(std::move(weights));
                }
                for (double param : {0.5, 2048.0}) {
                    arg_types.push_back(double_type);
                    auto column = ColumnHelper::create_const_column<TYPE_DOUBLE>(param, 3);
                    constants.push_back(column);
                    src.push_back(column);
                }
                auto ctx = make_ctx(arg_types, double_type, constants);
                TQueryOptions opts;
                opts.__set_enable_percentile_compact_intermediate(compact);
                RuntimeState rs(TUniqueId(), opts, TQueryGlobals(), nullptr);
                ctx->set_runtime_state(&rs);
                auto make_intermediate = [&]() -> MutableColumnPtr {
                    if (null_mode == 0) return BinaryColumn::create();
                    return NullableColumn::create(BinaryColumn::create(), NullColumn::create());
                };
                auto exchange = make_intermediate();
                auto storage = make_intermediate();
                func->convert_to_exchange_format(ctx.get(), src, 3, exchange);
                func->convert_to_serialize_format(ctx.get(), src, 3, storage);
                ASSERT_EQ(3U, exchange->size());
                ASSERT_EQ(3U, storage->size());
                const auto* exchange_data =
                        down_cast<const BinaryColumn*>(ColumnHelper::get_data_column(exchange.get()));
                const auto* storage_data = down_cast<const BinaryColumn*>(ColumnHelper::get_data_column(storage.get()));
                ManagedState state(ctx.get(), func);
                for (size_t i = 0; i < 3; ++i) {
                    bool is_null = null_mode == 3 || (null_mode == 2 && i == 1);
                    EXPECT_EQ(is_null, exchange->is_null(i));
                    EXPECT_EQ(is_null, storage->is_null(i));
                    if (!is_null) {
                        EXPECT_GT(storage_data->get_slice(i).size, 9U);
                        if (compact) {
                            EXPECT_EQ(9U, exchange_data->get_slice(i).size);
                        } else {
                            EXPECT_EQ(storage_data->get_slice(i), exchange_data->get_slice(i));
                        }
                    }
                }
                func->merge_batch_single_state(ctx.get(), state.state(), exchange.get(), 0, 3);
                auto result = NullableColumn::create(DoubleColumn::create(), NullColumn::create());
                func->finalize_to_column(ctx.get(), state.state(), result.get());
                ASSERT_EQ(1U, result->size());
                EXPECT_EQ(null_mode == 3, result->is_null(0));
                if (null_mode != 3) {
                    EXPECT_DOUBLE_EQ(10.0, down_cast<DoubleColumn*>(result->data_column_raw_ptr())->get_data()[0]);
                }
                // Aggregated states remain self-contained legacy records even
                // with compact exchange enabled, including through the wrapper.
                auto serialized = make_intermediate();
                func->serialize_to_column(ctx.get(), state.state(), serialized.get());
                ASSERT_EQ(1U, serialized->size());
                if (null_mode != 3) {
                    auto stored = down_cast<const BinaryColumn*>(ColumnHelper::get_data_column(serialized.get()))
                                          ->get_slice(0);
                    EXPECT_GT(stored.size, 9U);
                }
                ASSERT_FALSE(ctx->has_error());
            }
        }
    }
}

TEST_F(PercentileApproxAggTest, nullable_batch_forwards_non_null_ranges) {
    class BatchProbe final : public PercentileApproxAggregateFunction {
    public:
        mutable std::vector<std::pair<size_t, size_t>> ranges;
        void merge_batch_single_state(FunctionContext* ctx, AggDataPtr state, const Column* column, size_t start,
                                      size_t size) const override {
            ranges.emplace_back(start, size);
            PercentileApproxAggregateFunction::merge_batch_single_state(ctx, state, column, start, size);
        }
    } probe;
    using State = NullableAggregateFunctionState<PercentileApproxState, false>;
    NullableAggregateFunctionVariadic<State, PercentileApproxAggEmptyPred> wrapper(&probe);
    auto type = TypeDescriptor::from_logical_type(TYPE_DOUBLE);
    auto q = ColumnHelper::create_const_column<TYPE_DOUBLE>(0.5, 1);
    auto c = ColumnHelper::create_const_column<TYPE_DOUBLE>(5000, 1);
    auto ctx = make_ctx({type, type, type}, type, {nullptr, q, c});
    auto values = DoubleColumn::create();
    for (double v : {1000.0, 10.0, 20.0, 1000.0, 30.0, 1000.0}) values->append(v);
    MutableColumnPtr binary = BinaryColumn::create();
    probe.convert_to_serialize_format(ctx.get(), {values, q, c}, values->size(), binary);
    auto nulls = NullColumn::create();
    for (uint8_t n : {0, 0, 0, 1, 0, 0}) nulls->append(n);
    auto input = NullableColumn::create(std::move(binary), std::move(nulls));
    ManagedState state(ctx.get(), &wrapper);
    wrapper.merge_batch_single_state(ctx.get(), state.state(), input.get(), 1, 4);
    const std::vector<std::pair<size_t, size_t>> expected{{1, 2}, {4, 1}};
    EXPECT_EQ(expected, probe.ranges);
    auto result = NullableColumn::create(DoubleColumn::create(), NullColumn::create());
    wrapper.finalize_to_column(ctx.get(), state.state(), result.get());
    ASSERT_FALSE(result->is_null(0));
    EXPECT_DOUBLE_EQ(20.0, result->get(0).get_double());
    EXPECT_FALSE(ctx->has_error());
    probe.ranges.clear();
    ManagedState empty(ctx.get(), &wrapper);
    wrapper.merge_batch_single_state(ctx.get(), empty.state(), input.get(), 3, 1);
    wrapper.merge_batch_single_state(ctx.get(), empty.state(), input.get(), 0, 0);
    EXPECT_TRUE(probe.ranges.empty());
    result->reset_column();
    wrapper.finalize_to_column(ctx.get(), empty.state(), result.get());
    EXPECT_TRUE(result->is_null(0));
}

TEST_F(PercentileApproxAggTest, tracks_heap_from_creation_through_finalize) {
    for (bool nullable : {false, true}) {
        const auto* func = get_aggregate_function("percentile_approx", TYPE_DOUBLE, TYPE_DOUBLE, nullable);
        ASSERT_NE(nullptr, func);
        auto type = TypeDescriptor::from_logical_type(TYPE_DOUBLE);
        auto q = ColumnHelper::create_const_column<TYPE_DOUBLE>(0.5, 1);
        auto ctx = make_ctx({type, type}, type, {nullptr, q});
        ManagedState state(ctx.get(), func);
        auto* digest_state = nullable
                                     ? &reinterpret_cast<NullableAggregateFunctionState<PercentileApproxState, false>*>(
                                                state.state())
                                                ->_nested_state
                                     : reinterpret_cast<PercentileApproxState*>(state.state());
        EXPECT_EQ(0, ctx->mem_usage());
        EXPECT_EQ(0, digest_state->mem_usage());
        auto values = DoubleColumn::create();
        for (double v : {10.0, 20.0, 30.0}) values->append(v);
        const Column* columns[] = {values.get(), q.get()};
        for (size_t i = 0; i < values->size(); ++i) {
            func->update(ctx.get(), columns, state.state(), i);
            EXPECT_EQ(digest_state->mem_usage(), ctx->mem_usage());
        }
        const int64_t before = ctx->mem_usage();
        auto result = DoubleColumn::create();
        func->finalize_to_column(ctx.get(), state.state(), result.get());
        EXPECT_DOUBLE_EQ(20.0, result->get_data()[0]);
        EXPECT_GT(ctx->mem_usage(), before);
        EXPECT_EQ(digest_state->mem_usage(), ctx->mem_usage());
    }
}

TEST_F(PercentileApproxAggTest, array_exchange_and_persisted_states_preserve_quantiles) {
    auto type = TypeDescriptor::from_logical_type(TYPE_DOUBLE);
    auto array_type = TypeDescriptor::create_array_type(type);
    for (bool weighted : {false, true}) {
        for (bool compact : {false, true}) {
            const auto* func = get_aggregate_function(weighted ? "percentile_approx_weighted" : "percentile_approx",
                                                      TYPE_DOUBLE, TYPE_ARRAY, true);
            ASSERT_NE(nullptr, func);
            auto values = DoubleColumn::create();
            auto weights = Int64Column::create();
            for (double value : {10.0, 20.0, 30.0}) {
                values->append(value);
                weights->append(1);
            }
            auto qs = ColumnHelper::create_column(array_type, false);
            qs->append_datum(DatumArray{Datum(0.0), Datum(0.5), Datum(1.0)});
            ColumnPtr q = ConstColumn::create(std::move(qs), 3);
            auto compression = ColumnHelper::create_const_column<TYPE_DOUBLE>(5000, 3);
            Columns input{values};
            Columns constants{nullptr};
            std::vector<TypeDescriptor> types{type};
            if (weighted) {
                input.push_back(weights);
                constants.push_back(nullptr);
                types.push_back(TypeDescriptor::from_logical_type(TYPE_BIGINT));
            }
            input.insert(input.end(), {q, compression});
            constants.insert(constants.end(), {q, compression});
            types.insert(types.end(), {array_type, type});
            auto ctx = make_ctx(types, array_type, constants);
            TQueryOptions options;
            options.__set_enable_percentile_compact_intermediate(compact);
            RuntimeState runtime(TUniqueId(), options, TQueryGlobals(), nullptr);
            ctx->set_runtime_state(&runtime);
            MutableColumnPtr exchange = BinaryColumn::create();
            func->convert_to_exchange_format(ctx.get(), input, 3, exchange);
            if (compact) {
                EXPECT_EQ(9U, down_cast<BinaryColumn*>(exchange.get())->get_slice(0).size);
            }
            // The merge phase omits the weighted function's non-constant weight argument.
            auto merge_ctx = make_ctx({type, array_type, type}, array_type, {nullptr, q, compression});
            merge_ctx->set_runtime_state(&runtime);
            ManagedState state(merge_ctx.get(), func);
            func->merge_batch_single_state(merge_ctx.get(), state.state(), exchange.get(), 0, 3);
            auto result = ColumnHelper::create_column(array_type, true);
            func->finalize_to_column(merge_ctx.get(), state.state(), result.get());
            ASSERT_FALSE(result->is_null(0));
            auto actual = result->get(0).get_array();
            ASSERT_EQ(3U, actual.size());
            for (size_t i = 0; i < 3; ++i) EXPECT_DOUBLE_EQ(10.0 * (i + 1), actual[i].get_double());
            auto stored = BinaryColumn::create();
            func->serialize_to_column(merge_ctx.get(), state.state(), stored.get());
            EXPECT_GT(stored->get_slice(0).size, 9U);
            auto wrong_qs = ColumnHelper::create_column(array_type, false);
            wrong_qs->append_datum(DatumArray{Datum(0.99)});
            ColumnPtr wrong_q = ConstColumn::create(std::move(wrong_qs), 1);
            auto read_ctx = make_ctx({type, array_type, type}, array_type, {nullptr, wrong_q, compression});
            ManagedState restored(read_ctx.get(), func);
            func->merge(read_ctx.get(), stored.get(), restored.state(), 0);
            result->reset_column();
            func->finalize_to_column(read_ctx.get(), restored.state(), result.get());
            ASSERT_FALSE(result->is_null(0));
            actual = result->get(0).get_array();
            ASSERT_EQ(3U, actual.size());
            for (size_t i = 0; i < 3; ++i) EXPECT_DOUBLE_EQ(10.0 * (i + 1), actual[i].get_double());
            EXPECT_FALSE(ctx->has_error());
            EXPECT_FALSE(merge_ctx->has_error());
            EXPECT_FALSE(read_ctx->has_error());
        }
    }
}

} // namespace starrocks
