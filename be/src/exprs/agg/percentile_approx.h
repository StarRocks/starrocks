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

#pragma once

#include <cmath>

#include "column/array_column.h"
#include "column/column_helper.h"
#include "column/object_column.h"
#include "column/vectorized_fwd.h"
#include "exprs/agg/aggregate.h"
#include "exprs/function_context.h"
#include "gutil/casts.h"
#include "runtime/runtime_state.h"
#include "types/percentile_value.h"
#include "types/tdigest.h"

namespace starrocks {

struct PercentileApproxState {
public:
    PercentileApproxState() : percentile(new PercentileValue()) {}
    explicit PercentileApproxState(double compression)
            : percentile(new PercentileValue(compression)), compression_initialized(true) {}
    ~PercentileApproxState() = default;

    // Reinitialize the PercentileValue with a new compression factor
    // This is used when the state is already constructed (e.g., as part of NullableAggregateFunctionState)
    // but we need to apply a different compression factor from FunctionContext
    void reinit_with_compression(double compression) {
        percentile = std::make_unique<PercentileValue>(compression);
        compression_initialized = true;
    }

    // Heap footprint of the state. Includes the PercentileValue (capacity-
    // based) plus the targetQuantiles vector capacity so push_back / resize
    // of quantiles is also charged to FunctionContext::add_mem_usage.
    int64_t mem_usage() const { return percentile->mem_usage() + targetQuantiles.capacity() * sizeof(double); }

    std::unique_ptr<PercentileValue> percentile;
    bool compression_initialized = false; // Flag to track if compression has been initialized from FunctionContext
    std::vector<double>
            targetQuantiles; // Stores target quantile(s), single value for scalar mode, multiple for array mode
};

// Null predicate for percentile_approx*. A state that received zero total
// weight (no input rows, all inputs filtered out, all weights <= 0, all
// values non-finite) must finalize to SQL NULL instead of NaN. Passed to
// NullableAggregateFunctionVariadic via add_aggregate_mapping_variadic.
struct PercentileApproxAggEmptyPred {
    bool operator()(const PercentileApproxState& state) const { return state.percentile->is_empty(); }
};

class PercentileApproxAggregateFunctionBase
        : public AggregateFunctionBatchHelper<PercentileApproxState, PercentileApproxAggregateFunctionBase> {
protected:
    static constexpr double MIN_COMPRESSION = 2048.0;
    static constexpr double MAX_COMPRESSION = 10000.0;
    // Kept on BE for rolling upgrades from pre-canonicalization FEs. For
    // new-FE calls FunctionAnalyzer is the single source of truth and DEFAULT
    // lives there.
    static constexpr double DEFAULT_COMPRESSION_FACTOR = 10000.0;

    static double clamp_compression_factor(double compression) {
        if (LIKELY(std::isfinite(compression) && compression >= MIN_COMPRESSION && compression <= MAX_COMPRESSION)) {
            return compression;
        }
        // Old FEs can still ship explicit compression literals without the
        // analyzer-side [MIN, MAX] clamp during a rolling upgrade.
        LOG(WARNING) << "Compression factor out of range. Using default compression factor: "
                     << DEFAULT_COMPRESSION_FACTOR;
        return DEFAULT_COMPRESSION_FACTOR;
    }

    // Compact intermediate format for the transient exchange/spill path
    // (enable_percentile_compact_intermediate). Each pass-through sample is
    // serialized as a compact RAW record instead of a full TDigest blob:
    //   RECORD_RAW : [tag:1][mean:f32][weight:f32]   (RAW_RECORD_SIZE bytes)
    // The quantile is NOT embedded; the merge path recovers it from the const
    // arguments (which are forwarded into the merge phase). This is sound only
    // because RAW is produced exclusively by convert_to_exchange_format -- the
    // transient exchange/spill path -- and never by the storage path that the
    // agg_state combinators use, so a RAW record can never be persisted nor read
    // back without the const quantile in context. RAW is private to
    // percentile_approx intermediate state (not a PercentileValue type), so
    // percentile_hash / percentile_union and on-disk percentile columns are
    // unaffected.
    static constexpr uint8_t RECORD_RAW = 1;
    static constexpr size_t RAW_RECORD_SIZE = 1 + sizeof(float) + sizeof(float);

    // Whether to WRITE the compact RAW form. Gated on the flag for rolling-upgrade
    // safety (an old worker must never receive a RAW record). Null-safe: a
    // contextless ctx (unit tests) or an old FE falls back to legacy. The option
    // is global-only on FE, so a query cannot opt in before the cluster is fully
    // upgraded.
    static bool use_compact_intermediate(FunctionContext* ctx) {
        const RuntimeState* state = ctx->state();
        return state != nullptr && state->query_options().__isset.enable_percentile_compact_intermediate &&
               state->query_options().enable_percentile_compact_intermediate;
    }

    // A RAW record is exactly RAW_RECORD_SIZE bytes with the RAW tag at offset 0.
    // A legacy record is always far larger (>= sizeof(double) + an empty
    // PercentileValue blob, ~69 bytes), so the size check is unambiguous; the
    // ctx-quantile branch in merge is therefore reached only for transient
    // exchange records, never for a persisted/legacy one.
    static bool is_raw_record(const Slice& src) {
        return src.size == RAW_RECORD_SIZE && static_cast<uint8_t>(src.data[0]) == RECORD_RAW;
    }

    // The quantile const arg, recovered in the RAW merge path. In the merge phase
    // only the constant original args are forwarded (a non-constant weight is
    // dropped), so the quantile's absolute index shifts; the forwarded const args
    // are, in order, [..., quantile, compression] with compression always last,
    // so the quantile is the second constant column from the right. Works for
    // scalar/weighted (DOUBLE) and array (ARRAY<DOUBLE>).
    static ColumnPtr quantile_const_from_ctx(FunctionContext* ctx) {
        int seen = 0;
        for (int i = ctx->get_num_args() - 1; i >= 0; --i) {
            auto col = ctx->get_constant_column(i);
            if (col != nullptr) {
                if (seen == 1) {
                    return col; // 0 = compression (rightmost), 1 = quantile
                }
                ++seen;
            }
        }
        return nullptr;
    }

    // The compression const arg, recovered in the RAW merge path. RAW exchange
    // records carry no compression and a raw (mean, weight) point cannot supply
    // one; FE injects compression as the last argument, so at the merge phase it
    // is the rightmost forwarded constant (compact is a new-only format, so there
    // is no legacy 3-arg form to disambiguate).
    static ColumnPtr compression_const_from_ctx(FunctionContext* ctx) {
        for (int i = ctx->get_num_args() - 1; i >= 0; --i) {
            auto col = ctx->get_constant_column(i);
            if (col != nullptr) {
                return col; // rightmost const = compression
            }
        }
        return nullptr;
    }

public:
    virtual double get_compression_factor(FunctionContext* ctx) const = 0;

    void merge(FunctionContext* ctx, const Column* column, AggDataPtr __restrict state, size_t row_num) const override {
        const auto* binary_column = down_cast<const BinaryColumn*>(column);
        Slice src = binary_column->get_slice(row_num);
        int64_t prev_memory = data(state).mem_usage();

        if (is_raw_record(src)) {
            // [RECORD_RAW][mean:f32][weight:f32]: one transient pass-through
            // sample. Neither quantile nor compression is embedded; recover both
            // from ctx -- only exchange records are RAW, and the merge phase
            // always carries the const quantile (second from right) and
            // compression (rightmost).
            float mean;
            float weight;
            memcpy(&mean, src.data + 1, sizeof(float));
            memcpy(&weight, src.data + 1 + sizeof(float), sizeof(float));
            if (UNLIKELY(!data(state).compression_initialized)) {
                data(state).reinit_with_compression(clamp_compression_factor(
                        ColumnHelper::get_const_value<TYPE_DOUBLE>(compression_const_from_ctx(ctx))));
            }
            // TDigest::add rejects non-finite mean and weight <= 0.
            data(state).percentile->add(mean, weight);
            if (data(state).targetQuantiles.empty()) {
                data(state).targetQuantiles.push_back(
                        ColumnHelper::get_const_value<TYPE_DOUBLE>(quantile_const_from_ctx(ctx)));
            }
        } else {
            // Legacy self-contained record [quantile:8][PercentileValue blob].
            if (UNLIKELY(src.size < sizeof(double))) {
                ctx->set_error("percentile_approx: truncated intermediate record", false);
                return;
            }
            double quantile;
            memcpy(&quantile, src.data, sizeof(double));
            PercentileApproxState src_percentile;
            if (UNLIKELY(!src_percentile.percentile->deserialize(src.data + sizeof(double),
                                                                 src.size - sizeof(double)))) {
                ctx->set_error("percentile_approx: malformed intermediate record", false);
                return;
            }
            // Compression travels inside the serialized digest; adopt it instead
            // of re-deriving from ctx (arity is unreliable at the merge phase).
            if (UNLIKELY(!data(state).compression_initialized)) {
                data(state).reinit_with_compression(clamp_compression_factor(src_percentile.percentile->compression()));
            }
            merge_digest_into(data(state), src_percentile);
            if (data(state).targetQuantiles.empty()) {
                data(state).targetQuantiles.push_back(quantile);
            }
        }
        ctx->add_mem_usage(data(state).mem_usage() - prev_memory);
    }

protected:
    // Fast-path: when a legacy record's digest carries a single centroid,
    // TDigest::merge() would route it through a priority queue and a batched
    // mergeProcessed/mergeUnprocessed cycle, which is overkill for one centroid.
    // add() pushes directly into the target's _unprocessed buffer.
    static void merge_digest_into(PercentileApproxState& dst, const PercentileApproxState& src) {
        float singleton_mean;
        float singleton_weight;
        if (src.percentile->try_extract_singleton(&singleton_mean, &singleton_weight)) {
            dst.percentile->add(singleton_mean, singleton_weight);
        } else {
            dst.percentile->merge(src.percentile.get());
        }
    }

    // Array variants: the RAW record carries no quantiles, so the merge path
    // recovers the target quantiles from the const ARRAY<DOUBLE> arg
    // (quantile_const_from_ctx). Only reached for transient exchange records.
    static void assign_target_quantiles_from_const_array(PercentileApproxState& s, const Column* const_array_col) {
        const auto* array_column = down_cast<const ArrayColumn*>(ColumnHelper::get_data_column(const_array_col));
        const auto* elements =
                down_cast<const DoubleColumn*>(ColumnHelper::get_data_column(array_column->elements_column().get()));
        auto offsets = array_column->offsets().immutable_data();
        size_t start = offsets[0];
        size_t end = offsets[1];
        auto elements_data = elements->immutable_data();
        auto sub = elements_data.subspan(start, end - start);
        s.targetQuantiles.assign(sub.begin(), sub.end());
    }

public:
    void serialize_to_column(FunctionContext* ctx, ConstAggDataPtr __restrict state, Column* to) const override {
        // Always self-contained legacy [quantile:8][PercentileValue blob],
        // regardless of the flag: serialize_to_column is reused by the agg_state
        // combinator for persisted values, which a later _merge/_union must read
        // without an original quantile in its context. Only pass-through
        // (convert_to_serialize_format) uses the compact RAW form.
        size_t pv_size = data(state).percentile->serialize_size();
        // Avoid a stack VLA: a high-compression digest can serialize to tens of
        // KB of centroids, large enough to risk stack overflow.
        std::vector<uint8_t> result(sizeof(double) + pv_size);
        double quantile = data(state).targetQuantiles.empty() ? 0.0 : data(state).targetQuantiles[0];
        memcpy(result.data(), &quantile, sizeof(double));
        data(state).percentile->serialize(result.data() + sizeof(double));
        down_cast<BinaryColumn*>(to)->append(Slice(result.data(), result.size()));
    }

    void finalize_to_column(FunctionContext* ctx, ConstAggDataPtr __restrict state, Column* to) const override {
        auto* data_column = down_cast<DoubleColumn*>(to);
        double quantile = data(state).targetQuantiles.empty() ? 0.0 : data(state).targetQuantiles[0];
        double result = data(state).percentile->quantile(quantile);
        data_column->append_numbers(&result, sizeof(result));
    }
};

// PercentileApproxAggregateFunction: percentile_approx(expr, DOUBLE p[, DOUBLE compression])
class PercentileApproxAggregateFunction : public PercentileApproxAggregateFunctionBase {
public:
    double get_compression_factor(FunctionContext* ctx) const override {
        // FE FunctionAnalyzer is the single source of truth for new-FE calls:
        // an explicit compression literal is always injected (default or
        // user-supplied) and clamped to [MIN, MAX].
        //
        // Rolling-upgrade fallback: during the upgrade window BEs are upgraded
        // before FEs, so an old FE may still ship 2-arg percentile_approx
        // calls without the canonicalized compression literal. Treat that as
        // the default compression so the old wire format keeps working until
        // the FE side catches up. Remove once we no longer support upgrades
        // from pre-canonicalization FEs.
        if (ctx->get_num_args() <= 2) {
            return DEFAULT_COMPRESSION_FACTOR;
        }
        double compression = ColumnHelper::get_const_value<TYPE_DOUBLE>(ctx->get_constant_column(2));
        return clamp_compression_factor(compression);
    }

    void update(FunctionContext* ctx, const Column** columns, AggDataPtr state, size_t row_num) const override {
        // Lazy initialization of compression factor on first update
        if (UNLIKELY(!data(state).compression_initialized)) {
            double compression = get_compression_factor(ctx);
            data(state).reinit_with_compression(compression);
        }

        // argument 0
        const auto* data_column = down_cast<const DoubleColumn*>(columns[0]);
        // argument 1
        DCHECK(columns[1]->is_constant());
        DCHECK(!columns[1]->is_null(0));
        // Capture before the first-update targetQuantiles push_back so that
        // allocation is charged too (matches the merge path).
        int64_t prev_memory = data(state).mem_usage();
        // first update
        if (UNLIKELY(data(state).targetQuantiles.empty())) {
            data(state).targetQuantiles.push_back(columns[1]->get(0).get_double());
        }
        double column_value = data_column->immutable_data()[row_num];
        data(state).percentile->add(implicit_cast<float>(column_value));
        ctx->add_mem_usage(data(state).mem_usage() - prev_memory);
    }

    // Storage (persisted agg_state) path: self-contained legacy
    // [quantile:8][PercentileValue blob] per row.
    void convert_to_serialize_format(FunctionContext* ctx, const Columns& src, size_t chunk_size,
                                     MutableColumnPtr& dst) const override {
        const auto* data_column = down_cast<const DoubleColumn*>(src[0].get());
        DCHECK(src[1]->is_constant());
        double quantile = down_cast<const ConstColumn*>(src[1].get())->get(0).get_double();
        double compression = get_compression_factor(ctx);
        BinaryColumn* result = down_cast<BinaryColumn*>(dst.get());
        Bytes& bytes = result->get_bytes();
        result->get_offset().resize(chunk_size + 1);
        size_t old_size = bytes.size();
        bytes.reserve(chunk_size * 20);
        for (size_t i = 0; i < chunk_size; ++i) {
            PercentileValue percentile(compression);
            percentile.add(implicit_cast<float>(data_column->immutable_data()[i]));

            size_t new_size = old_size + sizeof(double) + percentile.serialize_size();
            bytes.resize(new_size);
            memcpy(bytes.data() + old_size, &quantile, sizeof(double));
            percentile.serialize(bytes.data() + old_size + sizeof(double));

            result->get_offset()[i + 1] = new_size;
            old_size = new_size;
        }
    }

    // Transient exchange/spill path: compact [RECORD_RAW][mean:f32][weight=1:f32]
    // per row when the flag is on -- no TDigest built, no embedded quantile
    // (recovered from ctx at merge). Falls back to the storage format otherwise.
    void convert_to_exchange_format(FunctionContext* ctx, const Columns& src, size_t chunk_size,
                                    MutableColumnPtr& dst) const override {
        if (!use_compact_intermediate(ctx)) {
            convert_to_serialize_format(ctx, src, chunk_size, dst);
            return;
        }
        const auto* data_column = down_cast<const DoubleColumn*>(src[0].get());
        BinaryColumn* result = down_cast<BinaryColumn*>(dst.get());
        Bytes& bytes = result->get_bytes();
        result->get_offset().resize(chunk_size + 1);
        size_t old_size = bytes.size();
        bytes.resize(old_size + chunk_size * RAW_RECORD_SIZE);
        uint8_t* w = bytes.data() + old_size;
        for (size_t i = 0; i < chunk_size; ++i) {
            const float mean = implicit_cast<float>(data_column->immutable_data()[i]);
            const float weight = 1.0f;
            w[0] = RECORD_RAW;
            memcpy(w + 1, &mean, sizeof(float));
            memcpy(w + 1 + sizeof(float), &weight, sizeof(float));
            w += RAW_RECORD_SIZE;
            result->get_offset()[i + 1] = old_size + (i + 1) * RAW_RECORD_SIZE;
        }
    }
    std::string get_name() const override { return "percentile_approx"; }
};

// PercentileApproxWeightedAggregateFunction: percentile_approx_weighted(expr, weight, DOUBLE p[, DOUBLE compression])
class PercentileApproxWeightedAggregateFunction : public PercentileApproxAggregateFunctionBase {
public:
    // SplitAggregateRule passes the const args to merge phase aggregator for performance.
    // Compression parameter is always the last constant parameter (if provided).
    // FE FunctionAnalyzer pre-clamps the literal to [MIN, MAX] or DEFAULT for
    // new-FE calls.
    double get_compression_factor(FunctionContext* ctx) const override {
        // FE always injects the compression argument; it is the last const arg.
        // SplitAggregateRule passes const args verbatim into the merge phase
        // aggregator, so we still scan from the right to find the first const
        // column (e.g. weight may be a non-const Int64 column).
        //
        // Rolling-upgrade fallback: BEs are upgraded before FEs, so an old FE
        // may ship the legacy 3-arg form `percentile_approx_weighted(expr, w, q)`
        // without the canonicalized compression literal. In that case the
        // rightmost const arg is the quantile (e.g. 0.95), which would
        // misinterpret as compression and break TDigest. Recognize the legacy
        // arity explicitly and return DEFAULT. Remove once we no longer
        // support upgrades from pre-canonicalization FEs.
        int num_args = ctx->get_num_args();
        if (num_args <= 3) {
            return DEFAULT_COMPRESSION_FACTOR;
        }
        for (int i = num_args - 1; i >= 0; i--) {
            auto const_col = ctx->get_constant_column(i);
            if (const_col != nullptr) {
                double compression = ColumnHelper::get_const_value<TYPE_DOUBLE>(const_col);
                return clamp_compression_factor(compression);
            }
        }
        DCHECK(false) << "FE invariant broken: percentile_approx_weighted reached BE "
                         "without an explicit compression argument";
        return DEFAULT_COMPRESSION_FACTOR;
    }

    void update(FunctionContext* ctx, const Column** columns, AggDataPtr state, size_t row_num) const override {
        // Lazy initialization of compression factor on first update
        if (UNLIKELY(!data(state).compression_initialized)) {
            double compression = get_compression_factor(ctx);
            data(state).reinit_with_compression(compression);
        }
        // argument 0
        const auto* data_column = down_cast<const DoubleColumn*>(columns[0]);
        // argument 1: weight can be const or int64 column
        size_t real_row_num = columns[1]->is_constant() ? 0 : row_num;
        int64_t weight = columns[1]->get(real_row_num).get_int64();
        // argument 2
        DCHECK(columns[2]->is_constant());
        DCHECK(!columns[2]->is_null(0));
        // Capture before the first-update targetQuantiles push_back so that
        // allocation is charged too (matches the merge path).
        int64_t prev_memory = data(state).mem_usage();
        if (UNLIKELY(data(state).targetQuantiles.empty())) {
            data(state).targetQuantiles.push_back(columns[2]->get(0).get_double());
        }

        double column_value = data_column->immutable_data()[row_num];
        // add value with weight. Reject w <= 0: a negative weight pushes
        // _processed_weight negative and yields NaN from weightedAverageSorted().
        // TDigest::add() also rejects non-positive weights as a second guard.
        if (LIKELY(weight > 0)) {
            data(state).percentile->add(implicit_cast<float>(column_value), weight);
        }
        ctx->add_mem_usage(data(state).mem_usage() - prev_memory);
    }

    // Storage path: self-contained legacy [quantile:8][PercentileValue blob].
    void convert_to_serialize_format(FunctionContext* ctx, const Columns& src, size_t chunk_size,
                                     MutableColumnPtr& dst) const override {
        // argument 0
        const auto* data_column = down_cast<const DoubleColumn*>(src[0].get());
        // argument 2
        DCHECK(src[2]->is_constant());
        // result
        BinaryColumn* result = down_cast<BinaryColumn*>(dst.get());
        Bytes& bytes = result->get_bytes();
        result->get_offset().resize(chunk_size + 1);
        size_t old_size = bytes.size();

        double quantile = src[2]->get(0).get_double();
        double compression = get_compression_factor(ctx);
        bytes.reserve(chunk_size * 20);

        // argument 1, weight column can be int64 or const column
        // serialize percentile one by one. weight <= 0 must not be added: a
        // negative weight corrupts the digest's running weight totals.
        if (src[1]->is_constant()) {
            int64_t weight = src[1]->get(0).get_int64();
            if (LIKELY(weight > 0)) {
                for (size_t i = 0; i < chunk_size; ++i) {
                    PercentileValue percentile(compression);
                    double value = data_column->immutable_data()[i];
                    percentile.add(implicit_cast<float>(value), weight);
                    size_t new_size = old_size + sizeof(double) + percentile.serialize_size();
                    bytes.resize(new_size);
                    memcpy(bytes.data() + old_size, &quantile, sizeof(double));
                    percentile.serialize(bytes.data() + old_size + sizeof(double));
                    old_size = new_size;
                    result->get_offset()[i + 1] = new_size;
                }
            } else {
                // TODO: optimize for empty weight but should not happen frequently
                PercentileValue empty_percentile(compression);
                size_t delta_size = sizeof(double) + empty_percentile.serialize_size();
                for (size_t i = 0; i < chunk_size; ++i) {
                    size_t new_size = old_size + delta_size;
                    bytes.resize(new_size);
                    memcpy(bytes.data() + old_size, &quantile, sizeof(double));
                    empty_percentile.serialize(bytes.data() + old_size + sizeof(double));
                    old_size = new_size;
                    result->get_offset()[i + 1] = new_size;
                }
            }
        } else {
            const auto* weight_column = down_cast<const Int64Column*>(src[1].get());
            for (size_t i = 0; i < chunk_size; ++i) {
                int64_t weight = weight_column->immutable_data()[i];
                PercentileValue percentile(compression);
                double value = data_column->immutable_data()[i];
                if (LIKELY(weight > 0)) {
                    percentile.add(implicit_cast<float>(value), weight);
                }
                size_t new_size = old_size + sizeof(double) + percentile.serialize_size();
                bytes.resize(new_size);
                memcpy(bytes.data() + old_size, &quantile, sizeof(double));
                percentile.serialize(bytes.data() + old_size + sizeof(double));
                old_size = new_size;
                result->get_offset()[i + 1] = new_size;
            }
        }
    }
    // Transient exchange/spill path: compact [RECORD_RAW][mean:f32][weight:f32]
    // per row when the flag is on. weight <= 0 is written verbatim; merge's add()
    // drops it. The quantile is recovered from ctx at merge.
    void convert_to_exchange_format(FunctionContext* ctx, const Columns& src, size_t chunk_size,
                                    MutableColumnPtr& dst) const override {
        if (!use_compact_intermediate(ctx)) {
            convert_to_serialize_format(ctx, src, chunk_size, dst);
            return;
        }
        const auto* data_column = down_cast<const DoubleColumn*>(src[0].get());
        BinaryColumn* result = down_cast<BinaryColumn*>(dst.get());
        Bytes& bytes = result->get_bytes();
        result->get_offset().resize(chunk_size + 1);
        size_t old_size = bytes.size();
        bytes.resize(old_size + chunk_size * RAW_RECORD_SIZE);
        uint8_t* w = bytes.data() + old_size;
        const bool weight_is_const = src[1]->is_constant();
        const auto* weight_column = weight_is_const ? nullptr : down_cast<const Int64Column*>(src[1].get());
        const float const_weight = weight_is_const ? static_cast<float>(src[1]->get(0).get_int64()) : 0.0f;
        for (size_t i = 0; i < chunk_size; ++i) {
            const float mean = implicit_cast<float>(data_column->immutable_data()[i]);
            const float weight =
                    weight_is_const ? const_weight : static_cast<float>(weight_column->immutable_data()[i]);
            w[0] = RECORD_RAW;
            memcpy(w + 1, &mean, sizeof(float));
            memcpy(w + 1 + sizeof(float), &weight, sizeof(float));
            w += RAW_RECORD_SIZE;
            result->get_offset()[i + 1] = old_size + (i + 1) * RAW_RECORD_SIZE;
        }
    }
    std::string get_name() const override { return "percentile_approx_weighted"; }
};

// PercentileApproxArrayAggregateFunction: percentile_approx(expr, ARRAY<DOUBLE> p[, DOUBLE compression])
// Returns ARRAY<DOUBLE>, using new serialization format
class PercentileApproxArrayAggregateFunction final : public PercentileApproxAggregateFunction {
public:
    void update(FunctionContext* ctx, const Column** columns, AggDataPtr state, size_t row_num) const override {
        // argument 1: array column wrapped in ConstColumn, no need to check is_null
        DCHECK(columns[1]->is_constant());
        // Lazy initialization of compression factor on first update
        if (UNLIKELY(!data(state).compression_initialized)) {
            double compression = get_compression_factor(ctx);
            data(state).reinit_with_compression(compression);

            const auto* array_column = down_cast<const ArrayColumn*>(ColumnHelper::get_data_column(columns[1]));
            const auto* elements = down_cast<const DoubleColumn*>(
                    ColumnHelper::get_data_column(array_column->elements_column().get()));
            auto offsets = array_column->offsets().immutable_data();
            size_t start = offsets[0];
            size_t end = offsets[1];
            DCHECK(end > start) << "Array length cannot be 0";
            auto elements_data = elements->immutable_data();
            auto sub = elements_data.subspan(start, end - start);
            data(state).targetQuantiles.assign(sub.begin(), sub.end());
        }

        // argument 0
        const auto* data_column = down_cast<const DoubleColumn*>(columns[0]);

        double column_value = data_column->immutable_data()[row_num];
        int64_t prev_memory = data(state).mem_usage();
        data(state).percentile->add(implicit_cast<float>(column_value));
        ctx->add_mem_usage(data(state).mem_usage() - prev_memory);
    }

    // Transient exchange RAW record [tag][mean][weight] (quantiles recovered from
    // the const ARRAY arg), or legacy self-contained
    // [count:4][q1..qn:8n][PercentileValue blob] for persisted/agg_state values.
    void merge(FunctionContext* ctx, const Column* column, AggDataPtr __restrict state, size_t row_num) const override {
        const auto* binary_column = down_cast<const BinaryColumn*>(column);
        Slice src = binary_column->get_slice(row_num);
        int64_t prev_memory = data(state).mem_usage();

        if (is_raw_record(src)) {
            float mean;
            float weight;
            memcpy(&mean, src.data + 1, sizeof(float));
            memcpy(&weight, src.data + 1 + sizeof(float), sizeof(float));
            if (UNLIKELY(!data(state).compression_initialized)) {
                data(state).reinit_with_compression(clamp_compression_factor(
                        ColumnHelper::get_const_value<TYPE_DOUBLE>(compression_const_from_ctx(ctx))));
            }
            data(state).percentile->add(mean, weight);
            if (UNLIKELY(data(state).targetQuantiles.empty())) {
                assign_target_quantiles_from_const_array(data(state), quantile_const_from_ctx(ctx).get());
            }
            ctx->add_mem_usage(data(state).mem_usage() - prev_memory);
            return;
        }

        if (UNLIKELY(src.size < sizeof(uint32_t))) {
            ctx->set_error("percentile_approx: truncated intermediate record", false);
            return;
        }
        uint32_t count;
        memcpy(&count, src.data, sizeof(uint32_t));
        size_t header = sizeof(uint32_t) + static_cast<size_t>(count) * sizeof(double);
        if (UNLIKELY(src.size < header)) {
            ctx->set_error("percentile_approx: truncated quantiles header", false);
            return;
        }
        if (UNLIKELY(data(state).targetQuantiles.empty())) {
            data(state).targetQuantiles.resize(count);
            memcpy(data(state).targetQuantiles.data(), (char*)src.data + sizeof(uint32_t), count * sizeof(double));
        }
        // Deserialize the TDigest (skip the quantiles array) with a bounded read.
        PercentileApproxState src_percentile;
        if (UNLIKELY(!src_percentile.percentile->deserialize(src.data + header, src.size - header))) {
            ctx->set_error("percentile_approx: malformed intermediate record", false);
            return;
        }
        // Compression travels inside the serialized digest; adopt it instead of
        // re-deriving from ctx (arity is unreliable at the merge phase).
        if (UNLIKELY(!data(state).compression_initialized)) {
            data(state).reinit_with_compression(clamp_compression_factor(src_percentile.percentile->compression()));
        }
        merge_digest_into(data(state), src_percentile);
        ctx->add_mem_usage(data(state).mem_usage() - prev_memory);
    }

    // Always self-contained legacy [count:4][q1..qn:8n][PercentileValue blob],
    // regardless of the flag (see PercentileApproxAggregateFunctionBase::
    // serialize_to_column for why persisted/agg_state values must stay legacy).
    void serialize_to_column(FunctionContext* ctx, ConstAggDataPtr __restrict state, Column* to) const override {
        size_t pv_size = data(state).percentile->serialize_size();
        uint32_t count = static_cast<uint32_t>(data(state).targetQuantiles.size());
        // Avoid stack VLA (see PercentileApproxAggregateFunctionBase::serialize_to_column).
        std::vector<uint8_t> result(sizeof(uint32_t) + count * sizeof(double) + pv_size);
        memcpy(result.data(), &count, sizeof(uint32_t));
        memcpy(result.data() + sizeof(uint32_t), data(state).targetQuantiles.data(), count * sizeof(double));
        data(state).percentile->serialize(result.data() + sizeof(uint32_t) + count * sizeof(double));
        down_cast<BinaryColumn*>(to)->append(Slice(result.data(), result.size()));
    }

    // Override finalize_to_column method, returns ARRAY<DOUBLE>
    void finalize_to_column(FunctionContext* ctx, ConstAggDataPtr __restrict state, Column* to) const override {
        auto* array_column = down_cast<ArrayColumn*>(to);
        // Calculate all quantiles and build DatumArray
        DatumArray result_array;
        result_array.reserve(data(state).targetQuantiles.size());
        for (double quantile : data(state).targetQuantiles) {
            double result = data(state).percentile->quantile(quantile);
            result_array.emplace_back(result);
        }

        array_column->append_datum(Datum(result_array));
    }

    void convert_to_serialize_format(FunctionContext* ctx, const Columns& src, size_t chunk_size,
                                     MutableColumnPtr& dst) const override {
        // argument 0
        const auto* data_column = down_cast<const DoubleColumn*>(src[0].get());
        // argument 1: ARRAY<DOUBLE>
        DCHECK(src[1]->is_constant());
        BinaryColumn* result = down_cast<BinaryColumn*>(dst.get());
        Bytes& bytes = result->get_bytes();
        result->get_offset().resize(chunk_size + 1);
        size_t old_size = bytes.size();

        // [count:4][q1..qn:8n][PercentileValue blob] per row. Array variants do
        // not use the compact RAW form: embedding count+quantiles to keep RAW
        // self-contained would make it variable-length and indistinguishable from
        // a legacy record by size.
        const auto* array_column = down_cast<const ArrayColumn*>(ColumnHelper::get_data_column(src[1].get()));
        const auto* elements =
                down_cast<const DoubleColumn*>(ColumnHelper::get_data_column(array_column->elements_column().get()));

        auto offsets = array_column->offsets().immutable_data();
        size_t start = offsets[0];
        size_t end = offsets[1];
        uint32_t count = static_cast<uint32_t>(end - start);
        double compression = get_compression_factor(ctx);

        // Extract quantiles
        std::vector<double> quantiles(count);
        auto elements_data = elements->immutable_data();
        for (size_t i = 0; i < count; ++i) {
            quantiles[i] = elements_data[start + i];
        }

        // Calculate estimated size per row: count(4) + quantiles(8*n) + TDigest data(~16 bytes)
        // 20 = sizeof(uint32_t) + percentile.serialize_size()
        size_t estimated_size_per_row = count * sizeof(double) + 20;
        bytes.reserve(chunk_size * estimated_size_per_row);

        // serialize percentile one by one
        for (size_t i = 0; i < chunk_size; ++i) {
            PercentileValue percentile(compression);
            percentile.add(implicit_cast<float>(data_column->immutable_data()[i]));

            size_t new_size = old_size + sizeof(uint32_t) + count * sizeof(double) + percentile.serialize_size();
            bytes.resize(new_size);

            // Write count
            memcpy(bytes.data() + old_size, &count, sizeof(uint32_t));
            // Write quantiles
            memcpy(bytes.data() + old_size + sizeof(uint32_t), quantiles.data(), count * sizeof(double));
            // Write TDigest
            percentile.serialize(bytes.data() + old_size + sizeof(uint32_t) + count * sizeof(double));

            result->get_offset()[i + 1] = new_size;
            old_size = new_size;
        }
    }
};

// PercentileApproxWeightedArrayAggregateFunction: percentile_approx_weighted(expr, weight, ARRAY<DOUBLE> p[, DOUBLE compression])
// Returns ARRAY<DOUBLE>, using new serialization format
class PercentileApproxWeightedArrayAggregateFunction final : public PercentileApproxWeightedAggregateFunction {
public:
    void update(FunctionContext* ctx, const Column** columns, AggDataPtr state, size_t row_num) const override {
        // argument 2: array column wrapped in ConstColumn, no need to check is_null
        DCHECK(columns[2]->is_constant());
        // Lazy initialization of compression factor on first update
        if (UNLIKELY(!data(state).compression_initialized)) {
            double compression = get_compression_factor(ctx);
            data(state).reinit_with_compression(compression);

            const auto* array_column = down_cast<const ArrayColumn*>(ColumnHelper::get_data_column(columns[2]));
            const auto* elements = down_cast<const DoubleColumn*>(
                    ColumnHelper::get_data_column(array_column->elements_column().get()));
            auto offsets = array_column->offsets().immutable_data();
            size_t start = offsets[0];
            size_t end = offsets[1];
            DCHECK(end > start) << "Array length cannot be 0";
            auto elements_data = elements->immutable_data();
            auto sub = elements_data.subspan(start, end - start);
            data(state).targetQuantiles.assign(sub.begin(), sub.end());
        }

        // argument 0
        const auto* data_column = down_cast<const DoubleColumn*>(columns[0]);
        // argument 1: weight can be const or int64 column
        size_t real_row_num = columns[1]->is_constant() ? 0 : row_num;
        int64_t weight = columns[1]->get(real_row_num).get_int64();

        double column_value = data_column->immutable_data()[row_num];
        int64_t prev_memory = data(state).mem_usage();
        // add value with weight. Reject w <= 0: a negative weight pushes
        // _processed_weight negative and yields NaN from weightedAverageSorted().
        // TDigest::add() also rejects non-positive weights as a second guard.
        if (LIKELY(weight > 0)) {
            data(state).percentile->add(implicit_cast<float>(column_value), weight);
        }
        ctx->add_mem_usage(data(state).mem_usage() - prev_memory);
    }

    // Transient exchange RAW record [tag][mean][weight] (quantiles recovered from
    // the const ARRAY arg), or legacy self-contained
    // [count:4][q1..qn:8n][PercentileValue blob] for persisted/agg_state values.
    void merge(FunctionContext* ctx, const Column* column, AggDataPtr __restrict state, size_t row_num) const override {
        const auto* binary_column = down_cast<const BinaryColumn*>(column);
        Slice src = binary_column->get_slice(row_num);
        int64_t prev_memory = data(state).mem_usage();

        if (is_raw_record(src)) {
            float mean;
            float weight;
            memcpy(&mean, src.data + 1, sizeof(float));
            memcpy(&weight, src.data + 1 + sizeof(float), sizeof(float));
            if (UNLIKELY(!data(state).compression_initialized)) {
                data(state).reinit_with_compression(clamp_compression_factor(
                        ColumnHelper::get_const_value<TYPE_DOUBLE>(compression_const_from_ctx(ctx))));
            }
            data(state).percentile->add(mean, weight);
            if (UNLIKELY(data(state).targetQuantiles.empty())) {
                assign_target_quantiles_from_const_array(data(state), quantile_const_from_ctx(ctx).get());
            }
            ctx->add_mem_usage(data(state).mem_usage() - prev_memory);
            return;
        }

        if (UNLIKELY(src.size < sizeof(uint32_t))) {
            ctx->set_error("percentile_approx: truncated intermediate record", false);
            return;
        }
        uint32_t count;
        memcpy(&count, src.data, sizeof(uint32_t));
        size_t header = sizeof(uint32_t) + static_cast<size_t>(count) * sizeof(double);
        if (UNLIKELY(src.size < header)) {
            ctx->set_error("percentile_approx: truncated quantiles header", false);
            return;
        }
        if (UNLIKELY(data(state).targetQuantiles.empty())) {
            data(state).targetQuantiles.resize(count);
            memcpy(data(state).targetQuantiles.data(), (char*)src.data + sizeof(uint32_t), count * sizeof(double));
        }
        // Deserialize the TDigest (skip the quantiles array) with a bounded read.
        PercentileApproxState src_percentile;
        if (UNLIKELY(!src_percentile.percentile->deserialize(src.data + header, src.size - header))) {
            ctx->set_error("percentile_approx: malformed intermediate record", false);
            return;
        }
        // Compression travels inside the serialized digest; adopt it instead of
        // re-deriving from ctx (arity is unreliable at the merge phase).
        if (UNLIKELY(!data(state).compression_initialized)) {
            data(state).reinit_with_compression(clamp_compression_factor(src_percentile.percentile->compression()));
        }
        merge_digest_into(data(state), src_percentile);
        ctx->add_mem_usage(data(state).mem_usage() - prev_memory);
    }

    // Always self-contained legacy [count:4][q1..qn:8n][PercentileValue blob],
    // regardless of the flag (see PercentileApproxAggregateFunctionBase::
    // serialize_to_column for why persisted/agg_state values must stay legacy).
    void serialize_to_column(FunctionContext* ctx, ConstAggDataPtr __restrict state, Column* to) const override {
        size_t pv_size = data(state).percentile->serialize_size();
        uint32_t count = static_cast<uint32_t>(data(state).targetQuantiles.size());
        // Avoid stack VLA (see PercentileApproxAggregateFunctionBase::serialize_to_column).
        std::vector<uint8_t> result(sizeof(uint32_t) + count * sizeof(double) + pv_size);
        memcpy(result.data(), &count, sizeof(uint32_t));
        memcpy(result.data() + sizeof(uint32_t), data(state).targetQuantiles.data(), count * sizeof(double));
        data(state).percentile->serialize(result.data() + sizeof(uint32_t) + count * sizeof(double));
        down_cast<BinaryColumn*>(to)->append(Slice(result.data(), result.size()));
    }

    // Override finalize_to_column method, returns ARRAY<DOUBLE>
    void finalize_to_column(FunctionContext* ctx, ConstAggDataPtr __restrict state, Column* to) const override {
        auto* array_column = down_cast<ArrayColumn*>(to);
        // Calculate all quantiles and build DatumArray
        DatumArray result_array;
        result_array.reserve(data(state).targetQuantiles.size());
        for (double quantile : data(state).targetQuantiles) {
            double result = data(state).percentile->quantile(quantile);
            result_array.emplace_back(result);
        }

        array_column->append_datum(Datum(result_array));
    }

    void convert_to_serialize_format(FunctionContext* ctx, const Columns& src, size_t chunk_size,
                                     MutableColumnPtr& dst) const override {
        // argument 0
        const auto* data_column = down_cast<const DoubleColumn*>(src[0].get());
        // argument 2: ARRAY<DOUBLE>
        DCHECK(src[2]->is_constant());
        // result
        BinaryColumn* result = down_cast<BinaryColumn*>(dst.get());
        Bytes& bytes = result->get_bytes();
        result->get_offset().resize(chunk_size + 1);
        size_t old_size = bytes.size();

        // [count:4][q1..qn:8n][PercentileValue blob] per row. Array variants do
        // not use the compact RAW form (see PercentileApproxArrayAggregateFunction).
        const auto* array_column = down_cast<const ArrayColumn*>(ColumnHelper::get_data_column(src[2].get()));
        const auto* elements =
                down_cast<const DoubleColumn*>(ColumnHelper::get_data_column(array_column->elements_column().get()));

        auto offsets = array_column->offsets().immutable_data();
        size_t start = offsets[0];
        size_t end = offsets[1];
        uint32_t count = static_cast<uint32_t>(end - start);
        double compression = get_compression_factor(ctx);

        // Extract quantiles
        std::vector<double> quantiles(count);
        auto elements_data = elements->immutable_data();
        for (size_t i = 0; i < count; ++i) {
            quantiles[i] = elements_data[start + i];
        }

        // Calculate estimated size per row: count(4) + quantiles(8*n) + TDigest data(~16 bytes)
        // 20 = sizeof(uint32_t) + percentile.serialize_size()
        size_t estimated_size_per_row = count * sizeof(double) + 20;
        bytes.reserve(chunk_size * estimated_size_per_row);

        // argument 1, weight column can be int64 or const column. weight <= 0
        // must not be added (see PercentileApproxWeightedAggregateFunction).
        // serialize percentile one by one
        if (src[1]->is_constant()) {
            int64_t weight = src[1]->get(0).get_int64();
            if (LIKELY(weight > 0)) {
                for (size_t i = 0; i < chunk_size; ++i) {
                    PercentileValue percentile(compression);
                    double value = data_column->immutable_data()[i];
                    percentile.add(implicit_cast<float>(value), weight);

                    size_t new_size =
                            old_size + sizeof(uint32_t) + count * sizeof(double) + percentile.serialize_size();
                    bytes.resize(new_size);

                    // Write count
                    memcpy(bytes.data() + old_size, &count, sizeof(uint32_t));
                    // Write quantiles
                    memcpy(bytes.data() + old_size + sizeof(uint32_t), quantiles.data(), count * sizeof(double));
                    // Write TDigest
                    percentile.serialize(bytes.data() + old_size + sizeof(uint32_t) + count * sizeof(double));

                    old_size = new_size;
                    result->get_offset()[i + 1] = new_size;
                }
            } else {
                // TODO: optimize for empty weight but should not happen frequently
                PercentileValue empty_percentile(compression);
                size_t delta_size = sizeof(uint32_t) + count * sizeof(double) + empty_percentile.serialize_size();
                for (size_t i = 0; i < chunk_size; ++i) {
                    size_t new_size = old_size + delta_size;
                    bytes.resize(new_size);

                    // Write count
                    memcpy(bytes.data() + old_size, &count, sizeof(uint32_t));
                    // Write quantiles
                    memcpy(bytes.data() + old_size + sizeof(uint32_t), quantiles.data(), count * sizeof(double));
                    // Write TDigest
                    empty_percentile.serialize(bytes.data() + old_size + sizeof(uint32_t) + count * sizeof(double));

                    old_size = new_size;
                    result->get_offset()[i + 1] = new_size;
                }
            }
        } else {
            const auto* weight_column = down_cast<const Int64Column*>(src[1].get());
            for (size_t i = 0; i < chunk_size; ++i) {
                int64_t weight = weight_column->immutable_data()[i];
                PercentileValue percentile(compression);
                double value = data_column->immutable_data()[i];
                if (LIKELY(weight > 0)) {
                    percentile.add(implicit_cast<float>(value), weight);
                }

                size_t new_size = old_size + sizeof(uint32_t) + count * sizeof(double) + percentile.serialize_size();
                bytes.resize(new_size);

                // Write count
                memcpy(bytes.data() + old_size, &count, sizeof(uint32_t));
                // Write quantiles
                memcpy(bytes.data() + old_size + sizeof(uint32_t), quantiles.data(), count * sizeof(double));
                // Write TDigest
                percentile.serialize(bytes.data() + old_size + sizeof(uint32_t) + count * sizeof(double));

                old_size = new_size;
                result->get_offset()[i + 1] = new_size;
            }
        }
    }
};

} // namespace starrocks
