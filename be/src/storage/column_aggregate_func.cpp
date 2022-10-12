// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#include "storage/column_aggregate_func.h"

#include "column/array_column.h"
#include "column/vectorized_fwd.h"
#include "storage/column_aggregator.h"
#include "util/percentile_value.h"

namespace starrocks::vectorized {

// SUM
template <typename ColumnType, typename StateType>
class SumAggregator final : public ValueColumnAggregator<ColumnType, StateType> {
public:
    void aggregate_impl(int row, const ColumnPtr& src) override {
        auto* data = down_cast<ColumnType*>(src.get())->get_data().data();
        this->data() += data[row];
    }

    void aggregate_batch_impl(int start, int end, const ColumnPtr& src) override {
        auto* data = down_cast<ColumnType*>(src.get())->get_data().data();
        for (int i = start; i < end; ++i) {
            this->data() += data[i];
        }
    }

    void append_data(Column* agg) override { down_cast<ColumnType*>(agg)->append(this->data()); }
};

struct SliceState {
    std::vector<uint8_t> data;
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

template <typename T>
struct MinMaxAggregateData {
    static T min() { return std::numeric_limits<T>::lowest(); }
    static T max() { return std::numeric_limits<T>::max(); }
};

template <>
struct MinMaxAggregateData<DecimalV2Value> {
    static DecimalV2Value min() { return DecimalV2Value::get_min_decimal(); }
    static DecimalV2Value max() { return DecimalV2Value::get_max_decimal(); }
};

template <>
struct MinMaxAggregateData<DateValue> {
    static DateValue min() { return DateValue::MIN_DATE_VALUE; }
    static DateValue max() { return DateValue::MAX_DATE_VALUE; }
};

template <>
struct MinMaxAggregateData<TimestampValue> {
    static TimestampValue min() { return TimestampValue::MIN_TIMESTAMP_VALUE; }
    static TimestampValue max() { return TimestampValue::MAX_TIMESTAMP_VALUE; }
};

// MAX
template <typename ColumnType, typename StateType>
class MaxAggregator final : public ValueColumnAggregator<ColumnType, StateType> {
public:
    void reset() override { this->data() = MinMaxAggregateData<StateType>::min(); }

    void aggregate_impl(int row, const ColumnPtr& src) override {
        auto* data = down_cast<ColumnType*>(src.get())->get_data().data();
        this->data() = std::max<StateType>(this->data(), data[row]);
    }

    void aggregate_batch_impl(int start, int end, const ColumnPtr& src) override {
        auto* data = down_cast<ColumnType*>(src.get())->get_data().data();
        for (int i = start; i < end; ++i) {
            this->data() = std::max<StateType>(this->data(), data[i]);
        }
    }

    void append_data(Column* agg) override { down_cast<ColumnType*>(agg)->append(this->data()); }
};

template <>
class MaxAggregator<BinaryColumn, SliceState> final : public ValueColumnAggregator<BinaryColumn, SliceState> {
public:
    void reset() override { this->data().reset(); }

    void aggregate_impl(int row, const ColumnPtr& src) override {
        auto* col = down_cast<BinaryColumn*>(src.get());

        Slice data = col->get_slice(row);
        if (!this->data().has_value || this->data().slice() < data) {
            this->data().update(data);
        }
    }

    void aggregate_batch_impl(int start, int end, const ColumnPtr& src) override {
        auto* col = down_cast<BinaryColumn*>(src.get());
        Slice data = col->get_slice(start);

        for (int i = start + 1; i < end; ++i) {
            data = std::max<Slice>(data, col->get_slice(i));
        }

        if (!this->data().has_value || this->data().slice() < data) {
            this->data().update(data);
        }
    }

    void append_data(Column* agg) override {
        auto* col = down_cast<BinaryColumn*>(agg);
        col->append(this->data().slice());
    }
};

// MIN
template <typename ColumnType, typename StateType>
class MinAggregator final : public ValueColumnAggregator<ColumnType, StateType> {
public:
    void reset() override { this->data() = MinMaxAggregateData<StateType>::max(); }

    void aggregate_impl(int row, const ColumnPtr& src) override {
        auto* data = down_cast<ColumnType*>(src.get())->get_data().data();
        this->data() = std::min<StateType>(this->data(), data[row]);
    }

    void aggregate_batch_impl(int start, int end, const ColumnPtr& src) override {
        auto* data = down_cast<ColumnType*>(src.get())->get_data().data();
        for (int i = start; i < end; ++i) {
            this->data() = std::min<StateType>(this->data(), data[i]);
        }
    }

    void append_data(Column* agg) override { down_cast<ColumnType*>(agg)->append(this->data()); }
};

template <>
class MinAggregator<BooleanColumn, uint8_t> final : public ValueColumnAggregator<BooleanColumn, uint8_t> {
public:
    // The max value of boolean type is 1 (not uint8_t max).
    void reset() override { this->data() = 1; }

    void aggregate_impl(int row, const ColumnPtr& src) override {
        auto* data = down_cast<BooleanColumn*>(src.get())->get_data().data();
        this->data() = std::min<uint8_t>(this->data(), data[row]);
    }

    void aggregate_batch_impl(int start, int end, const ColumnPtr& src) override {
        auto* data = down_cast<BooleanColumn*>(src.get())->get_data().data();
        for (int i = start; i < end; ++i) {
            this->data() = std::min<uint8_t>(this->data(), data[i]);
        }
    }

    void append_data(Column* agg) override { down_cast<BooleanColumn*>(agg)->append(this->data()); }
};

template <>
class MinAggregator<BinaryColumn, SliceState> final : public ValueColumnAggregator<BinaryColumn, SliceState> {
public:
    void reset() override { this->data().reset(); }

    void aggregate_impl(int row, const ColumnPtr& src) override {
        auto* col = down_cast<BinaryColumn*>(src.get());

        Slice data = col->get_slice(row);
        if (!this->data().has_value || this->data().slice() > data) {
            this->data().update(data);
        }
    }

    void aggregate_batch_impl(int start, int end, const ColumnPtr& src) override {
        auto* col = down_cast<BinaryColumn*>(src.get());
        Slice data = col->get_slice(start);

        for (int i = start + 1; i < end; ++i) {
            data = std::min<Slice>(data, col->get_slice(i));
        }

        if (!this->data().has_value || this->data().slice() > data) {
            this->data().update(data);
        }
    }

    void append_data(Column* agg) override {
        auto* col = down_cast<BinaryColumn*>(agg);
        col->append(this->data().slice());
    }
};

// REPLACE
template <typename ColumnType, typename StateType>
class ReplaceAggregator final : public ValueColumnAggregator<ColumnType, StateType> {
public:
    void aggregate_impl(int row, const ColumnPtr& src) override {
        auto* data = down_cast<ColumnType*>(src.get())->get_data().data();
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
        auto* data = down_cast<BitmapColumn*>(src.get());
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
        auto* data = down_cast<HyperLogLogColumn*>(src.get());
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
        auto* data = down_cast<PercentileColumn*>(src.get());
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
class ReplaceAggregator<JsonColumn, JsonValue> final : public ValueColumnAggregator<JsonColumn, JsonValue> {
public:
    void aggregate_impl(int row, const ColumnPtr& src) override {
        auto* data = down_cast<JsonColumn*>(src.get());
        this->data() = *(data->get_object(row));
    }

    void aggregate_batch_impl([[maybe_unused]] int start, int end, const ColumnPtr& src) override {
        aggregate_impl(end - 1, src);
    }

    void append_data(Column* agg) override {
        auto* col = down_cast<JsonColumn*>(agg);
        auto& per = const_cast<JsonValue&>(this->data());
        col->append(std::move(per));
    }
};

template <>
class ReplaceAggregator<BinaryColumn, SliceState> final : public ValueColumnAggregator<BinaryColumn, SliceState> {
public:
    void reset() override { this->data().reset(); }

    void aggregate_impl(int row, const ColumnPtr& src) override {
        auto* col = down_cast<BinaryColumn*>(src.get());
        Slice data = col->get_slice(row);
        this->data().update(data);
    }

    void aggregate_batch_impl(int start, int end, const ColumnPtr& src) override {
        auto* col = down_cast<BinaryColumn*>(src.get());
        Slice data = col->get_slice(end - 1);
        this->data().update(data);
    }

    void append_data(Column* agg) override {
        auto* col = down_cast<BinaryColumn*>(agg);
        // NOTE: assume the storage pointed by |this->data().slice()| not destroyed.
        col->append(this->data().slice());
    }
};

struct ArrayState {
    ColumnPtr column;
    int row = 0;

    void reset() noexcept {
        column.reset();
        row = 0;
    }
};

template <>
class ReplaceAggregator<ArrayColumn, ArrayState> final : public ValueColumnAggregator<ArrayColumn, ArrayState> {
public:
    void reset() override { this->data().reset(); }

    void aggregate_impl(int row, const ColumnPtr& src) override {
        this->data().column = src;
        this->data().row = row;
    }

    void aggregate_batch_impl(int start, int end, const ColumnPtr& src) override { aggregate_impl(end - 1, src); }

    void append_data(Column* agg) override {
        auto* col = down_cast<ArrayColumn*>(agg);
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

        auto* nullable = down_cast<NullableColumn*>(src.get());
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

// HLL_UNION
class HllUnionAggregator final : public ValueColumnAggregator<HyperLogLogColumn, HyperLogLog> {
public:
    void reset() override {
        this->data().clear();
        _inited = false;
    }

    void aggregate_impl(int row, const ColumnPtr& src) override {
        auto* col = down_cast<HyperLogLogColumn*>(src.get());
        if (!_inited) {
            // First value, directly move it
            _inited = true;
            this->data() = std::move(*col->get_object(row));
        } else {
            this->data().merge(*(col->get_object(row)));
        }
    }

    void aggregate_batch_impl(int start, int end, const ColumnPtr& src) override {
        auto* col = down_cast<HyperLogLogColumn*>(src.get());
        if (!_inited) {
            // First value, directly move it
            _inited = true;
            this->data() = std::move(*col->get_object(start));
            for (int i = start + 1; i < end; ++i) {
                this->data().merge(*(col->get_object(i)));
            }
        } else {
            for (int i = start; i < end; ++i) {
                this->data().merge(*(col->get_object(i)));
            }
        }
    }

    void append_data(Column* agg) override { down_cast<HyperLogLogColumn*>(agg)->append(std::move(this->data())); }

private:
    // The aggregate state whether initialized
    bool _inited = false;
};

// BITMAP_UNION
class BitmapUnionAggregator final : public ValueColumnAggregator<BitmapColumn, BitmapValue> {
public:
    void reset() override {
        this->data().clear();
        _inited = false;
    }

    void aggregate_impl(int row, const ColumnPtr& src) override {
        auto* col = down_cast<BitmapColumn*>(src.get());
        if (!_inited) {
            // First value, directly move it
            _inited = true;
            this->data() = std::move(*col->get_object(row));
        } else {
            this->data() |= *(col->get_object(row));
        }
    }

    void aggregate_batch_impl(int start, int end, const ColumnPtr& src) override {
        auto* col = down_cast<BitmapColumn*>(src.get());
        if (!_inited) {
            // First value, directly move it
            _inited = true;
            this->data() = std::move(*col->get_object(start));
            for (int i = start + 1; i < end; ++i) {
                this->data() |= *(col->get_object(i));
            }
        } else {
            for (int i = start; i < end; ++i) {
                this->data() |= *(col->get_object(i));
            }
        }
    }

    void append_data(Column* agg) override { down_cast<BitmapColumn*>(agg)->append(std::move(this->data())); }

private:
    // The aggregate state whether initialized
    bool _inited = false;
};

// PERCENTILE_UNION
class PercentileUnionAggregator final : public ValueColumnAggregator<PercentileColumn, PercentileValue> {
public:
    void reset() override { this->data() = PercentileValue(); }

    void aggregate_impl(int row, const ColumnPtr& src) override {
        this->data().merge(down_cast<PercentileColumn*>(src.get())->get_object(row));
    }

    void aggregate_batch_impl(int start, int end, const ColumnPtr& src) override {
        auto* col = down_cast<PercentileColumn*>(src.get());
        for (int i = start; i < end; ++i) {
            this->data().merge(col->get_object(i));
        }
    }

    void append_data(Column* agg) override { down_cast<PercentileColumn*>(agg)->append(&this->data()); }
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

#define CASE_SUM(CASE_TYPE, COLUMN_TYPE, STATE_TYPE) \
    CASE_NEW_VALUE_AGGREGATOR(CASE_TYPE, COLUMN_TYPE, STATE_TYPE, SumAggregator)

#define CASE_MAX(CASE_TYPE, COLUMN_TYPE, STATE_TYPE) \
    CASE_NEW_VALUE_AGGREGATOR(CASE_TYPE, COLUMN_TYPE, STATE_TYPE, MaxAggregator)

#define CASE_MIN(CASE_TYPE, COLUMN_TYPE, STATE_TYPE) \
    CASE_NEW_VALUE_AGGREGATOR(CASE_TYPE, COLUMN_TYPE, STATE_TYPE, MinAggregator)

#define CASE_REPLACE(CASE_TYPE, COLUMN_TYPE, STATE_TYPE) \
    CASE_NEW_VALUE_AGGREGATOR(CASE_TYPE, COLUMN_TYPE, STATE_TYPE, ReplaceAggregator)

ValueColumnAggregatorPtr create_value_aggregator(FieldType type, FieldAggregationMethod method) {
    switch (method) {
    case OLAP_FIELD_AGGREGATION_SUM: {
        switch (type) {
            CASE_SUM(OLAP_FIELD_TYPE_TINYINT, Int8Column, int8_t)
            CASE_SUM(OLAP_FIELD_TYPE_SMALLINT, Int16Column, int16_t)
            CASE_SUM(OLAP_FIELD_TYPE_INT, Int32Column, int32_t)
            CASE_SUM(OLAP_FIELD_TYPE_BIGINT, Int64Column, int64_t)
            CASE_SUM(OLAP_FIELD_TYPE_LARGEINT, Int128Column, int128_t)
            CASE_SUM(OLAP_FIELD_TYPE_FLOAT, FloatColumn, float)
            CASE_SUM(OLAP_FIELD_TYPE_DOUBLE, DoubleColumn, double)
            CASE_SUM(OLAP_FIELD_TYPE_DECIMAL, DecimalColumn, DecimalV2Value)
            CASE_SUM(OLAP_FIELD_TYPE_DECIMAL_V2, DecimalColumn, DecimalV2Value)
            CASE_SUM(OLAP_FIELD_TYPE_DECIMAL32, Decimal32Column, int32_t)
            CASE_SUM(OLAP_FIELD_TYPE_DECIMAL64, Decimal64Column, int64_t)
            CASE_SUM(OLAP_FIELD_TYPE_DECIMAL128, Decimal128Column, int128_t)
            CASE_SUM(OLAP_FIELD_TYPE_BOOL, BooleanColumn, uint8_t)
            CASE_DEFAULT_WARNING(type)
        }
    }
    case OLAP_FIELD_AGGREGATION_MAX: {
        switch (type) {
            CASE_MAX(OLAP_FIELD_TYPE_TINYINT, Int8Column, int8_t)
            CASE_MAX(OLAP_FIELD_TYPE_SMALLINT, Int16Column, int16_t)
            CASE_MAX(OLAP_FIELD_TYPE_INT, Int32Column, int32_t)
            CASE_MAX(OLAP_FIELD_TYPE_BIGINT, Int64Column, int64_t)
            CASE_MAX(OLAP_FIELD_TYPE_LARGEINT, Int128Column, int128_t)
            CASE_MAX(OLAP_FIELD_TYPE_FLOAT, FloatColumn, float)
            CASE_MAX(OLAP_FIELD_TYPE_DOUBLE, DoubleColumn, double)
            CASE_MAX(OLAP_FIELD_TYPE_DECIMAL, DecimalColumn, DecimalV2Value)
            CASE_MAX(OLAP_FIELD_TYPE_DECIMAL_V2, DecimalColumn, DecimalV2Value)
            CASE_MAX(OLAP_FIELD_TYPE_DECIMAL32, Decimal32Column, int32_t)
            CASE_MAX(OLAP_FIELD_TYPE_DECIMAL64, Decimal64Column, int64_t)
            CASE_MAX(OLAP_FIELD_TYPE_DECIMAL128, Decimal128Column, int128_t)
            CASE_MAX(OLAP_FIELD_TYPE_DATE, DateColumn, DateValue)
            CASE_MAX(OLAP_FIELD_TYPE_DATE_V2, DateColumn, DateValue)
            CASE_MAX(OLAP_FIELD_TYPE_DATETIME, TimestampColumn, TimestampValue)
            CASE_MAX(OLAP_FIELD_TYPE_TIMESTAMP, TimestampColumn, TimestampValue)
            CASE_MAX(OLAP_FIELD_TYPE_CHAR, BinaryColumn, SliceState)
            CASE_MAX(OLAP_FIELD_TYPE_VARCHAR, BinaryColumn, SliceState)
            CASE_MAX(OLAP_FIELD_TYPE_BOOL, BooleanColumn, uint8_t)
            CASE_DEFAULT_WARNING(type)
        }
    }
    case OLAP_FIELD_AGGREGATION_MIN: {
        switch (type) {
            CASE_MIN(OLAP_FIELD_TYPE_TINYINT, Int8Column, int8_t)
            CASE_MIN(OLAP_FIELD_TYPE_SMALLINT, Int16Column, int16_t)
            CASE_MIN(OLAP_FIELD_TYPE_INT, Int32Column, int32_t)
            CASE_MIN(OLAP_FIELD_TYPE_BIGINT, Int64Column, int64_t)
            CASE_MIN(OLAP_FIELD_TYPE_LARGEINT, Int128Column, int128_t)
            CASE_MIN(OLAP_FIELD_TYPE_FLOAT, FloatColumn, float)
            CASE_MIN(OLAP_FIELD_TYPE_DOUBLE, DoubleColumn, double)
            CASE_MIN(OLAP_FIELD_TYPE_DECIMAL, DecimalColumn, DecimalV2Value)
            CASE_MIN(OLAP_FIELD_TYPE_DECIMAL32, Decimal32Column, int32_t)
            CASE_MIN(OLAP_FIELD_TYPE_DECIMAL64, Decimal64Column, int64_t)
            CASE_MIN(OLAP_FIELD_TYPE_DECIMAL128, Decimal128Column, int128_t)
            CASE_MIN(OLAP_FIELD_TYPE_DECIMAL_V2, DecimalColumn, DecimalV2Value)
            CASE_MIN(OLAP_FIELD_TYPE_DATE, DateColumn, DateValue)
            CASE_MIN(OLAP_FIELD_TYPE_DATE_V2, DateColumn, DateValue)
            CASE_MIN(OLAP_FIELD_TYPE_DATETIME, TimestampColumn, TimestampValue)
            CASE_MIN(OLAP_FIELD_TYPE_TIMESTAMP, TimestampColumn, TimestampValue)
            CASE_MIN(OLAP_FIELD_TYPE_CHAR, BinaryColumn, SliceState)
            CASE_MIN(OLAP_FIELD_TYPE_VARCHAR, BinaryColumn, SliceState)
            CASE_MIN(OLAP_FIELD_TYPE_BOOL, BooleanColumn, uint8_t)
            CASE_DEFAULT_WARNING(type)
        }
    }
    case OLAP_FIELD_AGGREGATION_REPLACE:
    case OLAP_FIELD_AGGREGATION_REPLACE_IF_NOT_NULL: {
        switch (type) {
            CASE_REPLACE(OLAP_FIELD_TYPE_TINYINT, Int8Column, int8_t)
            CASE_REPLACE(OLAP_FIELD_TYPE_SMALLINT, Int16Column, int16_t)
            CASE_REPLACE(OLAP_FIELD_TYPE_INT, Int32Column, int32_t)
            CASE_REPLACE(OLAP_FIELD_TYPE_BIGINT, Int64Column, int64_t)
            CASE_REPLACE(OLAP_FIELD_TYPE_LARGEINT, Int128Column, int128_t)
            CASE_REPLACE(OLAP_FIELD_TYPE_FLOAT, FloatColumn, float)
            CASE_REPLACE(OLAP_FIELD_TYPE_DOUBLE, DoubleColumn, double)
            CASE_REPLACE(OLAP_FIELD_TYPE_DECIMAL, DecimalColumn, DecimalV2Value)
            CASE_REPLACE(OLAP_FIELD_TYPE_DECIMAL_V2, DecimalColumn, DecimalV2Value)
            CASE_REPLACE(OLAP_FIELD_TYPE_DECIMAL32, Decimal32Column, int32_t)
            CASE_REPLACE(OLAP_FIELD_TYPE_DECIMAL64, Decimal64Column, int64_t)
            CASE_REPLACE(OLAP_FIELD_TYPE_DECIMAL128, Decimal128Column, int128_t)
            CASE_REPLACE(OLAP_FIELD_TYPE_DATE, DateColumn, DateValue)
            CASE_REPLACE(OLAP_FIELD_TYPE_DATE_V2, DateColumn, DateValue)
            CASE_REPLACE(OLAP_FIELD_TYPE_DATETIME, TimestampColumn, TimestampValue)
            CASE_REPLACE(OLAP_FIELD_TYPE_TIMESTAMP, TimestampColumn, TimestampValue)
            CASE_REPLACE(OLAP_FIELD_TYPE_CHAR, BinaryColumn, SliceState)
            CASE_REPLACE(OLAP_FIELD_TYPE_VARCHAR, BinaryColumn, SliceState)
            CASE_REPLACE(OLAP_FIELD_TYPE_BOOL, BooleanColumn, uint8_t)
            CASE_REPLACE(OLAP_FIELD_TYPE_ARRAY, ArrayColumn, ArrayState)
            CASE_REPLACE(OLAP_FIELD_TYPE_HLL, HyperLogLogColumn, HyperLogLog)
            CASE_REPLACE(OLAP_FIELD_TYPE_OBJECT, BitmapColumn, BitmapValue)
            CASE_REPLACE(OLAP_FIELD_TYPE_PERCENTILE, PercentileColumn, PercentileValue)
            CASE_REPLACE(OLAP_FIELD_TYPE_JSON, JsonColumn, JsonValue)
            CASE_DEFAULT_WARNING(type)
        }
    }
    case OLAP_FIELD_AGGREGATION_HLL_UNION: {
        switch (type) {
        case OLAP_FIELD_TYPE_HLL: {
            return std::make_unique<HllUnionAggregator>();
        }
            CASE_DEFAULT_WARNING(type)
        }
    }
    case OLAP_FIELD_AGGREGATION_BITMAP_UNION: {
        switch (type) {
        case OLAP_FIELD_TYPE_OBJECT: {
            return std::make_unique<BitmapUnionAggregator>();
        }
            CASE_DEFAULT_WARNING(type)
        }
    }
    case OLAP_FIELD_AGGREGATION_PERCENTILE_UNION: {
        switch (type) {
        case OLAP_FIELD_TYPE_PERCENTILE: {
            return std::make_unique<PercentileUnionAggregator>();
        }
            CASE_DEFAULT_WARNING(type)
        }
    }
    case OLAP_FIELD_AGGREGATION_NONE:
        CHECK(false) << "invalid aggregate method: OLAP_FIELD_AGGREGATION_NONE";
    case OLAP_FIELD_AGGREGATION_UNKNOWN:
        CHECK(false) << "invalid aggregate method: OLAP_FIELD_AGGREGATION_UNKNOWN";
    }
    return nullptr;
}

ColumnAggregatorPtr ColumnAggregatorFactory::create_key_column_aggregator(
        const starrocks::vectorized::FieldPtr& field) {
    FieldType type = field->type()->type();
    starrocks::FieldAggregationMethod method = field->aggregate_method();
    if (method != OLAP_FIELD_AGGREGATION_NONE) {
        CHECK(false) << "key column's aggregation method should be NONE";
    }
    switch (type) {
        CASE_NEW_KEY_AGGREGATOR(OLAP_FIELD_TYPE_TINYINT, Int8Column)
        CASE_NEW_KEY_AGGREGATOR(OLAP_FIELD_TYPE_SMALLINT, Int16Column)
        CASE_NEW_KEY_AGGREGATOR(OLAP_FIELD_TYPE_INT, Int32Column)
        CASE_NEW_KEY_AGGREGATOR(OLAP_FIELD_TYPE_BIGINT, Int64Column)
        CASE_NEW_KEY_AGGREGATOR(OLAP_FIELD_TYPE_LARGEINT, Int128Column)
        CASE_NEW_KEY_AGGREGATOR(OLAP_FIELD_TYPE_BOOL, BooleanColumn)
        CASE_NEW_KEY_AGGREGATOR(OLAP_FIELD_TYPE_CHAR, BinaryColumn)
        CASE_NEW_KEY_AGGREGATOR(OLAP_FIELD_TYPE_VARCHAR, BinaryColumn)
        CASE_NEW_KEY_AGGREGATOR(OLAP_FIELD_TYPE_DECIMAL_V2, DecimalColumn)
        CASE_NEW_KEY_AGGREGATOR(OLAP_FIELD_TYPE_DECIMAL32, Decimal32Column)
        CASE_NEW_KEY_AGGREGATOR(OLAP_FIELD_TYPE_DECIMAL64, Decimal64Column)
        CASE_NEW_KEY_AGGREGATOR(OLAP_FIELD_TYPE_DECIMAL128, Decimal128Column)
        CASE_NEW_KEY_AGGREGATOR(OLAP_FIELD_TYPE_DATE_V2, DateColumn)
        CASE_NEW_KEY_AGGREGATOR(OLAP_FIELD_TYPE_TIMESTAMP, TimestampColumn)
        CASE_DEFAULT_WARNING(type)
    }
}

ColumnAggregatorPtr ColumnAggregatorFactory::create_value_column_aggregator(
        const starrocks::vectorized::FieldPtr& field) {
    FieldType type = field->type()->type();
    starrocks::FieldAggregationMethod method = field->aggregate_method();
    if (method == OLAP_FIELD_AGGREGATION_NONE) {
        CHECK(false) << "bad agg method NONE for column: " << field->name();
        return nullptr;
    } else if (method == OLAP_FIELD_AGGREGATION_REPLACE) {
        auto p = create_value_aggregator(type, method);
        if (field->is_nullable()) {
            return std::make_unique<ReplaceNullableColumnAggregator>(std::move(p));
        } else {
            return p;
        }
    } else {
        auto p = create_value_aggregator(type, method);
        if (field->is_nullable()) {
            return std::make_unique<ValueNullableColumnAggregator>(std::move(p));
        } else {
            return p;
        }
    }
}
} // namespace starrocks::vectorized
