// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#pragma once

#include "column/vectorized_fwd.h"
#include "common/status.h"
#include "runtime/decimalv2_value.h"
#include "storage/decimal12.h"
#include "storage/uint24.h"
#include "util/int96.h"

namespace starrocks {

class ColumnVisitorMutable {
public:
    virtual ~ColumnVisitorMutable() = default;

    // The default implementation of `visit` will return `Status::NotSupported`
    virtual Status visit(vectorized::NullableColumn* column);
    virtual Status visit(vectorized::ConstColumn* column);
    virtual Status visit(vectorized::ArrayColumn* column);
    virtual Status visit(vectorized::MapColumn* column);
    virtual Status visit(vectorized::BinaryColumn* column);
    virtual Status visit(vectorized::LargeBinaryColumn* column);
    virtual Status visit(vectorized::Int8Column* column);
    virtual Status visit(vectorized::UInt8Column* column);
    virtual Status visit(vectorized::Int16Column* column);
    virtual Status visit(vectorized::UInt16Column* column);
    virtual Status visit(vectorized::Int32Column* column);
    virtual Status visit(vectorized::UInt32Column* column);
    virtual Status visit(vectorized::Int64Column* column);
    virtual Status visit(vectorized::UInt64Column* column);
    virtual Status visit(vectorized::Int128Column* column);
    virtual Status visit(vectorized::DoubleColumn* column);
    virtual Status visit(vectorized::FloatColumn* column);
    virtual Status visit(vectorized::DateColumn* column);
    virtual Status visit(vectorized::TimestampColumn* column);
    virtual Status visit(vectorized::DecimalColumn* column);
    virtual Status visit(vectorized::Decimal32Column* column);
    virtual Status visit(vectorized::Decimal64Column* column);
    virtual Status visit(vectorized::Decimal128Column* column);
    virtual Status visit(vectorized::HyperLogLogColumn* column);
    virtual Status visit(vectorized::BitmapColumn* column);
    virtual Status visit(vectorized::PercentileColumn* column);
    virtual Status visit(vectorized::JsonColumn* column);
    virtual Status visit(vectorized::FixedLengthColumn<int96_t>* column);
    virtual Status visit(vectorized::FixedLengthColumn<uint24_t>* column);
    virtual Status visit(vectorized::FixedLengthColumn<decimal12_t>* column);

    // NOTE: Inherited classes normally don't need to implement the following methods, they are
    // defined here mainly for successful compiling.
    virtual Status visit(vectorized::FixedLengthColumnBase<int8_t>* column);
    virtual Status visit(vectorized::FixedLengthColumnBase<uint8_t>* column);
    virtual Status visit(vectorized::FixedLengthColumnBase<int16_t>* column);
    virtual Status visit(vectorized::FixedLengthColumnBase<uint16_t>* column);
    virtual Status visit(vectorized::FixedLengthColumnBase<int32_t>* column);
    virtual Status visit(vectorized::FixedLengthColumnBase<uint32_t>* column);
    virtual Status visit(vectorized::FixedLengthColumnBase<int64_t>* column);
    virtual Status visit(vectorized::FixedLengthColumnBase<uint64_t>* column);
    virtual Status visit(vectorized::FixedLengthColumnBase<vectorized::int128_t>* column);
    virtual Status visit(vectorized::FixedLengthColumnBase<float>* column);
    virtual Status visit(vectorized::FixedLengthColumnBase<double>* column);
    virtual Status visit(vectorized::FixedLengthColumnBase<vectorized::DateValue>* column);
    virtual Status visit(vectorized::FixedLengthColumnBase<DecimalV2Value>* column);
    virtual Status visit(vectorized::FixedLengthColumnBase<vectorized::TimestampValue>* column);
    virtual Status visit(vectorized::FixedLengthColumnBase<int96_t>* column);
    virtual Status visit(vectorized::FixedLengthColumnBase<uint24_t>* column);
    virtual Status visit(vectorized::FixedLengthColumnBase<decimal12_t>* column);
    virtual Status visit(vectorized::ObjectColumn<JsonValue>* column);
};

} // namespace starrocks
