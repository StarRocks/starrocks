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

#include "storage/column_aggregate_func.h"

#include "column/array_column.h"
#include "column/map_column.h"
#include "column/struct_column.h"
#include "column/vectorized_fwd.h"
#include "exprs/agg/agg_state_union.h"
#include "exprs/agg/aggregate.h"
#include "exprs/agg/aggregate_state_allocator.h"
#include "exprs/agg/factory/aggregate_resolver.hpp"
#include "runtime/exec_env.h"
#include "runtime/mem_pool.h"
#include "runtime/runtime_state.h"
#include "storage/column_aggregator.h"
#include "util/percentile_value.h"

namespace starrocks {

struct SliceState {
    raw::RawVector<uint8_t> data;
    bool has_value = false;

    Slice slice() { return {data.data(), data.size()}; }

    void update(const Slice& s) {
        has_value = true;
        data.clear();
        data.insert(data.begin(), s.data, s.data + s.size);
    }

    void reset() {
        has_value = false;
        data.clear();
    }
};

// REPLACE
template <typename ColumnType, typename StateType>
class ReplaceAggregator final : public ValueColumnAggregator<ColumnType, StateType> {
public:
    void aggregate_impl(int row, const ColumnPtr& src) override {
        const auto* data = down_cast<const ColumnType*>(src.get())->get_data().data();
        this->data() = data[row];
    }

    void aggregate_batch_impl([[maybe_unused]] int start, int end, const ColumnPtr& src) override {
        aggregate_impl(end - 1, src);
    }

    void append_data(Column* agg) override { down_cast<ColumnType*>(agg)->append(this->data()); }
};

template <>
class ReplaceAggregator<BitmapColumn, BitmapValue> final : public ValueColumnAggregator<BitmapColumn, BitmapValue> {
public:
    void aggregate_impl(int row, const ColumnPtr& src) override {
        const auto* data = down_cast<const BitmapColumn*>(src.get());
        this->data() = *(data->get_object(row));
    }

    void aggregate_batch_impl([[maybe_unused]] int start, int end, const ColumnPtr& src) override {
        aggregate_impl(end - 1, src);
    }

    void append_data(Column* agg) override {
        auto* col = down_cast<BitmapColumn*>(agg);
        auto& bitmap = const_cast<BitmapValue&>(this->data());
        col->append(std::move(bitmap));
    }
};

template <>
class ReplaceAggregator<HyperLogLogColumn, HyperLogLog> final
        : public ValueColumnAggregator<HyperLogLogColumn, HyperLogLog> {
public:
    void aggregate_impl(int row, const ColumnPtr& src) override {
        auto* data = down_cast<const HyperLogLogColumn*>(src.get());
        this->data() = *(data->get_object(row));
    }

    void aggregate_batch_impl([[maybe_unused]] int start, int end, const ColumnPtr& src) override {
        aggregate_impl(end - 1, src);
    }

    void append_data(Column* agg) override {
        auto* col = down_cast<HyperLogLogColumn*>(agg);
        auto& hll = const_cast<HyperLogLog&>(this->data());
        col->append(std::move(hll));
    }
};

template <>
class ReplaceAggregator<PercentileColumn, PercentileValue> final
        : public ValueColumnAggregator<PercentileColumn, PercentileValue> {
public:
    void aggregate_impl(int row, const ColumnPtr& src) override {
        const auto* data = down_cast<const PercentileColumn*>(src.get());
        this->data() = *(data->get_object(row));
    }

    void aggregate_batch_impl([[maybe_unused]] int start, int end, const ColumnPtr& src) override {
        aggregate_impl(end - 1, src);
    }

    void append_data(Column* agg) override {
        auto* col = down_cast<PercentileColumn*>(agg);
        auto& per = const_cast<PercentileValue&>(this->data());
        col->append(std::move(per));
    }
};

template <>
class ReplaceAggregator<BinaryColumn, SliceState> final : public ValueColumnAggregator<BinaryColumn, SliceState> {
public:
    void reset() override { this->data().reset(); }

    void aggregate_impl(int row, const ColumnPtr& src) override {
        const auto* col = down_cast<const BinaryColumn*>(src.get());
        Slice data = col->get_slice(row);
        this->data().update(data);
    }

    void aggregate_batch_impl(int start, int end, const ColumnPtr& src) override {
        auto* col = down_cast<const BinaryColumn*>(src.get());
        Slice data = col->get_slice(end - 1);
        this->data().update(data);
    }

    void append_data(Column* agg) override {
        auto* col = down_cast<BinaryColumn*>(agg);
        // NOTE: assume the storage pointed by |this->data().slice()| not destroyed.
        col->append(this->data().slice());
    }
};

struct ColumnRefState {
    ColumnPtr column;
    int row = 0;

    void reset() noexcept {
        column.reset();
        row = 0;
    }
};

// Array/Map/Struct/Json
template <typename ColumnType>
class ReplaceAggregator<ColumnType, ColumnRefState> final : public ValueColumnAggregator<ColumnType, ColumnRefState> {
public:
    void reset() override { this->data().reset(); }

    void aggregate_impl(int row, const ColumnPtr& src) override {
        this->data().column = src;
        this->data().row = row;
    }

    void aggregate_batch_impl(int start, int end, const ColumnPtr& src) override { aggregate_impl(end - 1, src); }

    void append_data(Column* agg) override {
        auto* col = down_cast<ColumnType*>(agg);
        if (this->data().column) {
            col->append(*this->data().column, this->data().row, 1);
        } else {
            col->append_default();
        }
    }

    bool need_deep_copy() const override { return true; }
};

class ReplaceNullableColumnAggregator final : public ValueColumnAggregatorBase {
public:
    ~ReplaceNullableColumnAggregator() override = default;

    explicit ReplaceNullableColumnAggregator(ValueColumnAggregatorPtr child) : _child(std::move(child)) {
        _null_child = std::make_unique<ReplaceAggregator<NullColumn, uint8_t>>();
    }

    void update_source(const ColumnPtr& src) override {
        _source_column = src;

        const auto* nullable = down_cast<const NullableColumn*>(src.get());
        _child->update_source(nullable->data_column());
        _null_child->update_source(nullable->null_column());
    }

    void update_aggregate(Column* agg) override {
        _aggregate_column = agg;

        auto* n = down_cast<NullableColumn*>(agg);
        _child->update_aggregate(n->data_column().get());
        _null_child->update_aggregate(n->null_column().get());

        reset();
    }

    void aggregate_values(int start, int nums, const uint32* aggregate_loops, bool previous_neq) override {
        _child->aggregate_values(start, nums, aggregate_loops, previous_neq);
        _null_child->aggregate_values(start, nums, aggregate_loops, previous_neq);
    }

    void finalize() override {
        _child->finalize();
        _null_child->finalize();

        auto p = down_cast<NullableColumn*>(_aggregate_column);
        p->set_has_null(SIMD::count_nonzero(p->null_column()->get_data()));
        _aggregate_column = nullptr;
    }

    void reset() override {
        _child->reset();
        _null_child->reset();
    }

    void append_data(Column* agg) override {
        LOG(FATAL) << "append_data is not implemented in ReplaceNullableColumnAggregator";
    }

    void aggregate_impl(int row, const ColumnPtr& data) override {
        LOG(FATAL) << "aggregate_impl is not implemented in ReplaceNullableColumnAggregator";
    }

    void aggregate_batch_impl(int start, int end, const ColumnPtr& data) override {
        LOG(FATAL) << "aggregate_batch_impl is not implemented in ReplaceNullableColumnAggregator";
    }

private:
    ValueColumnAggregatorPtr _null_child;
    ValueColumnAggregatorPtr _child;
};

class AggFuncBasedValueAggregator : public ValueColumnAggregatorBase {
public:
    AggFuncBasedValueAggregator(const AggregateFunction* agg_func) : _agg_func(agg_func) {
        _state = static_cast<AggDataPtr>(std::aligned_alloc(_agg_func->alignof_size(), _agg_func->size()));
        // TODO: create a new FunctionContext by using specific FunctionContext::create_context
        _func_ctx = new FunctionContext();
        _agg_func->create(_func_ctx, _state);
    }

    AggFuncBasedValueAggregator(AggStateDesc* agg_state_desc, std::unique_ptr<AggregateFunction> agg_state_unoin)
            : _agg_func(agg_state_unoin.get()) {
        _agg_state_unoin = std::move(agg_state_unoin);
        _runtime_state = std::make_unique<RuntimeState>(ExecEnv::GetInstance());
        _mem_pool = std::make_unique<MemPool>();
        _func_ctx = FunctionContext::create_context(_runtime_state.get(), _mem_pool.get(),
                                                    agg_state_desc->get_return_type(), agg_state_desc->get_arg_types());
        _state = static_cast<AggDataPtr>(std::aligned_alloc(_agg_func->alignof_size(), _agg_func->size()));
        _agg_func->create(_func_ctx, _state);
    }

    ~AggFuncBasedValueAggregator() override {
        SCOPED_THREAD_LOCAL_AGG_STATE_ALLOCATOR_SETTER(&kDefaultColumnAggregatorAllocator);
        if (_state != nullptr) {
            _agg_func->destroy(_func_ctx, _state);
            std::free(_state);
        }
        if (_func_ctx != nullptr) {
            delete _func_ctx;
        }
    }

    void reset() override {
        _agg_func->destroy(_func_ctx, _state);
        _agg_func->create(_func_ctx, _state);
    }

    void update_aggregate(Column* agg) override {
        _aggregate_column = agg;
        reset();
    }

    void append_data(Column* agg) override { _agg_func->finalize_to_column(_func_ctx, _state, agg); }

    // |data| is readonly.
    void aggregate_impl(int row, const ColumnPtr& data) override {
        _agg_func->merge(_func_ctx, data.get(), _state, row);
    }

    // |data| is readonly.
    void aggregate_batch_impl(int start, int end, const ColumnPtr& input) override {
        _agg_func->merge_batch_single_state(_func_ctx, _state, input.get(), start, end - start);
    }

    bool need_deep_copy() const override { return false; };

    void aggregate_values(int start, int nums, const uint32* aggregate_loops, bool previous_neq) override {
        if (nums <= 0) {
            return;
        }

        // if different with last row in previous chunk
        if (previous_neq) {
            append_data(_aggregate_column);
            reset();
        }

        for (int i = 0; i < nums; ++i) {
            aggregate_batch_impl(start, start + aggregate_loops[i], _source_column);
            start += aggregate_loops[i];
            // If there is another loop, append current state to result column
            if (i < nums - 1) {
                append_data(_aggregate_column);
                reset();
            }
        }
    }

    void finalize() override {
        append_data(_aggregate_column);
        _aggregate_column = nullptr;
    }

private:
    const AggregateFunction* _agg_func;
    FunctionContext* _func_ctx = nullptr;
    AggDataPtr _state{nullptr};

    // used for common aggregate functions
    std::unique_ptr<AggregateFunction> _agg_state_unoin = nullptr;
    std::unique_ptr<RuntimeState> _runtime_state = nullptr;
    std::unique_ptr<MemPool> _mem_pool = nullptr;
};

#define CASE_DEFAULT_WARNING(TYPE)                                             \
    default: {                                                                 \
        CHECK(false) << "unreachable path, unknown key column type: " << TYPE; \
        return nullptr;                                                        \
    }

#define CASE_NEW_KEY_AGGREGATOR(CASE_TYPE, COLUMN_TYPE)              \
    case CASE_TYPE: {                                                \
        return std::make_unique<KeyColumnAggregator<COLUMN_TYPE>>(); \
    }

#define CASE_NEW_VALUE_AGGREGATOR(CASE_TYPE, COLUMN_TYPE, STATE_TYPE, CLASS) \
    case CASE_TYPE: {                                                        \
        return std::make_unique<CLASS<COLUMN_TYPE, STATE_TYPE>>();           \
    }
#define CASE_REPLACE(CASE_TYPE, COLUMN_TYPE, STATE_TYPE) \
    CASE_NEW_VALUE_AGGREGATOR(CASE_TYPE, COLUMN_TYPE, STATE_TYPE, ReplaceAggregator)

ValueColumnAggregatorPtr create_value_aggregator(LogicalType type, StorageAggregateType method) {
    switch (method) {
    case STORAGE_AGGREGATE_REPLACE:
    case STORAGE_AGGREGATE_REPLACE_IF_NOT_NULL: {
        switch (type) {
            CASE_REPLACE(TYPE_TINYINT, Int8Column, int8_t)
            CASE_REPLACE(TYPE_SMALLINT, Int16Column, int16_t)
            CASE_REPLACE(TYPE_INT, Int32Column, int32_t)
            CASE_REPLACE(TYPE_BIGINT, Int64Column, int64_t)
            CASE_REPLACE(TYPE_LARGEINT, Int128Column, int128_t)
            CASE_REPLACE(TYPE_FLOAT, FloatColumn, float)
            CASE_REPLACE(TYPE_DOUBLE, DoubleColumn, double)
            CASE_REPLACE(TYPE_DECIMAL, DecimalColumn, DecimalV2Value)
            CASE_REPLACE(TYPE_DECIMALV2, DecimalColumn, DecimalV2Value)
            CASE_REPLACE(TYPE_DECIMAL32, Decimal32Column, int32_t)
            CASE_REPLACE(TYPE_DECIMAL64, Decimal64Column, int64_t)
            CASE_REPLACE(TYPE_DECIMAL128, Decimal128Column, int128_t)
            CASE_REPLACE(TYPE_DATE_V1, DateColumn, DateValue)
            CASE_REPLACE(TYPE_DATE, DateColumn, DateValue)
            CASE_REPLACE(TYPE_DATETIME_V1, TimestampColumn, TimestampValue)
            CASE_REPLACE(TYPE_DATETIME, TimestampColumn, TimestampValue)
            CASE_REPLACE(TYPE_CHAR, BinaryColumn, SliceState)
            CASE_REPLACE(TYPE_VARCHAR, BinaryColumn, SliceState)
            CASE_REPLACE(TYPE_VARBINARY, BinaryColumn, SliceState)
            CASE_REPLACE(TYPE_BOOLEAN, BooleanColumn, uint8_t)
            CASE_REPLACE(TYPE_ARRAY, ArrayColumn, ColumnRefState)
            CASE_REPLACE(TYPE_MAP, MapColumn, ColumnRefState)
            CASE_REPLACE(TYPE_STRUCT, StructColumn, ColumnRefState)
            CASE_REPLACE(TYPE_HLL, HyperLogLogColumn, HyperLogLog)
            CASE_REPLACE(TYPE_OBJECT, BitmapColumn, BitmapValue)
            CASE_REPLACE(TYPE_PERCENTILE, PercentileColumn, PercentileValue)
            CASE_REPLACE(TYPE_JSON, JsonColumn, ColumnRefState)
            CASE_DEFAULT_WARNING(type)
        }
    }
    default:
    case STORAGE_AGGREGATE_NONE:
        CHECK(false) << "invalid aggregate method: STORAGE_AGGREGATE_NONE";
    case STORAGE_AGGREGATE_UNKNOWN:
        CHECK(false) << "invalid aggregate method: STORAGE_AGGREGATE_UNKNOWN";
    }
    return nullptr;
}

ColumnAggregatorPtr ColumnAggregatorFactory::create_key_column_aggregator(const starrocks::FieldPtr& field) {
    LogicalType type = field->type()->type();
    starrocks::StorageAggregateType method = field->aggregate_method();
    if (method != STORAGE_AGGREGATE_NONE) {
        CHECK(false) << "key column's aggregation method should be NONE";
    }
    switch (type) {
        CASE_NEW_KEY_AGGREGATOR(TYPE_TINYINT, Int8Column)
        CASE_NEW_KEY_AGGREGATOR(TYPE_SMALLINT, Int16Column)
        CASE_NEW_KEY_AGGREGATOR(TYPE_INT, Int32Column)
        CASE_NEW_KEY_AGGREGATOR(TYPE_BIGINT, Int64Column)
        CASE_NEW_KEY_AGGREGATOR(TYPE_LARGEINT, Int128Column)
        CASE_NEW_KEY_AGGREGATOR(TYPE_BOOLEAN, BooleanColumn)
        CASE_NEW_KEY_AGGREGATOR(TYPE_CHAR, BinaryColumn)
        CASE_NEW_KEY_AGGREGATOR(TYPE_VARCHAR, BinaryColumn)
        CASE_NEW_KEY_AGGREGATOR(TYPE_DECIMALV2, DecimalColumn)
        CASE_NEW_KEY_AGGREGATOR(TYPE_DECIMAL32, Decimal32Column)
        CASE_NEW_KEY_AGGREGATOR(TYPE_DECIMAL64, Decimal64Column)
        CASE_NEW_KEY_AGGREGATOR(TYPE_DECIMAL128, Decimal128Column)
        CASE_NEW_KEY_AGGREGATOR(TYPE_DATE, DateColumn)
        CASE_NEW_KEY_AGGREGATOR(TYPE_DATETIME, TimestampColumn)
        CASE_DEFAULT_WARNING(type)
    }
}

ColumnAggregatorPtr ColumnAggregatorFactory::create_value_column_aggregator(const starrocks::FieldPtr& field) {
    LogicalType type = field->type()->type();
    starrocks::StorageAggregateType method = field->aggregate_method();
    if (method == STORAGE_AGGREGATE_NONE) {
        CHECK(false) << "bad agg method NONE for column: " << field->name();
        return nullptr;
    } else if (method == STORAGE_AGGREGATE_REPLACE) {
        auto p = create_value_aggregator(type, method);
        if (field->is_nullable()) {
            return std::make_unique<ReplaceNullableColumnAggregator>(std::move(p));
        } else {
            return p;
        }
    } else if (method == STORAGE_AGGREGATE_REPLACE_IF_NOT_NULL) {
        auto p = create_value_aggregator(type, method);
        if (field->is_nullable()) {
            return std::make_unique<ValueNullableColumnAggregator>(std::move(p));
        } else {
            return p;
        }
    } else if (method == STORAGE_AGGREGATE_AGG_STATE_UNION) {
        if (field->get_agg_state_desc() == nullptr) {
            CHECK(false) << "Bad agg state union method for column: " << field->name()
                         << " for its agg state type is null";
            return nullptr;
        }
        auto* agg_state_desc = field->get_agg_state_desc();
        auto func_name = agg_state_desc->get_func_name();
        DCHECK_EQ(field->is_nullable(), agg_state_desc->is_result_nullable());
        auto* agg_func = AggStateDesc::get_agg_state_func(agg_state_desc);
        CHECK(agg_func != nullptr) << "Unknown aggregate function, name=" << func_name << ", type=" << type
                                   << ", is_nullable=" << field->is_nullable()
                                   << ", agg_state_desc=" << agg_state_desc->debug_string();
        auto agg_state_union = std::make_unique<AggStateUnion>(*agg_state_desc, agg_func);
        return std::make_unique<AggFuncBasedValueAggregator>(agg_state_desc, std::move(agg_state_union));
    } else {
        auto func_name = get_string_by_aggregation_type(method);
        // TODO(alvin): To keep compatible with old code, when type must not be the legacy type,
        // the following convert can be deleted.
        auto normalized_tpe = type;
        switch (type) {
        case TYPE_DATE_V1:
            normalized_tpe = TYPE_DATE;
            break;
        case TYPE_DATETIME_V1:
            normalized_tpe = TYPE_DATETIME;
            break;
        case TYPE_DECIMAL:
            normalized_tpe = TYPE_DECIMALV2;
            break;
        default:
            break;
        }

        auto agg_func = AggregateFuncResolver::instance()->get_aggregate_info(func_name, normalized_tpe, normalized_tpe,
                                                                              false, field->is_nullable());
        CHECK(agg_func != nullptr) << "Unknown aggregate function, name=" << func_name << ", type=" << type
                                   << ", is_nullable=" << field->is_nullable();
        return std::make_unique<AggFuncBasedValueAggregator>(agg_func);
    }
}
} // namespace starrocks
