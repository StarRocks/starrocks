// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#pragma once

#include "runtime/decimalv2_value.h"
#include "storage/decimal12.h"
#include "storage/uint24.h"
#include "types/date_value.hpp"
#include "types/timestamp_value.h"
#include "util/int96.h"
#include "util/slice.h"
#include "column/datum.h"

namespace starrocks {

class BitmapValue;
class HyperLogLog;
class PercentileValue;
class JsonValue;

typedef __int128 int128_t;
typedef unsigned __int128 uint128_t;

using DatumArray = std::vector<vectorized::Datum>;

class Row {

public:

    [[nodiscard]] virtual size_t size() const = 0;

    [[nodiscard]] virtual int8_t get_int8(size_t ordinal) const = 0;
    [[nodiscard]] virtual uint8_t get_uint8(size_t ordinal) const = 0;
    [[nodiscard]] virtual int16_t get_int16(size_t ordinal) const = 0;
    [[nodiscard]] virtual uint16_t get_uint16(size_t ordinal) const = 0;
    [[nodiscard]] virtual uint24_t get_uint24(size_t ordinal) const = 0;
    [[nodiscard]] virtual int32_t get_int32(size_t ordinal) const = 0;
    [[nodiscard]] virtual uint32_t get_uint32(size_t ordinal) const = 0;
    [[nodiscard]] virtual int64_t get_int64(size_t ordinal) const = 0;
    [[nodiscard]] virtual uint64_t get_uint64(size_t ordinal) const = 0;
    [[nodiscard]] virtual int96_t get_int96(size_t ordinal) const = 0;
    [[nodiscard]] virtual const int128_t& get_int128(size_t ordinal) const = 0;
    [[nodiscard]] virtual float get_float(size_t ordinal) const = 0;
    [[nodiscard]] virtual double get_double(size_t ordinal) const = 0;
    [[nodiscard]] virtual vectorized::TimestampValue get_timestamp(size_t ordinal) const = 0;
    [[nodiscard]] virtual vectorized::DateValue get_date(size_t ordinal) const = 0;
    [[nodiscard]] virtual const Slice& get_slice(size_t ordinal) const = 0;
    [[nodiscard]] virtual const decimal12_t& get_decimal12(size_t ordinal) const = 0;
    [[nodiscard]] virtual const DecimalV2Value& get_decimal(size_t ordinal) const = 0;
    [[nodiscard]] virtual const DatumArray& get_array(size_t ordinal) const = 0;
    [[nodiscard]] virtual const HyperLogLog* get_hyperloglog(size_t ordinal) const = 0;
    [[nodiscard]] virtual const BitmapValue* get_bitmap(size_t ordinal) const = 0;
    [[nodiscard]] virtual const PercentileValue* get_percentile(size_t ordinal) const = 0;
    [[nodiscard]] virtual const JsonValue* get_json(size_t ordinal) const = 0;

    virtual void set_int8(size_t ordinal, int8_t v) = 0;
    virtual void set_uint8(size_t ordinal, uint8_t v) = 0;
    virtual void set_int16(size_t ordinal, int16_t v) = 0;
    virtual void set_uint16(size_t ordinal, uint16_t v) = 0;
    virtual void set_uint24(size_t ordinal, uint24_t v) = 0;
    virtual void set_int32(size_t ordinal, int32_t v) = 0;
    virtual void set_uint32(size_t ordinal, uint32_t v) = 0;
    virtual void set_int64(size_t ordinal, int64_t v) = 0;
    virtual void set_uint64(size_t ordinal, uint64_t v) = 0;
    virtual void set_int96(size_t ordinal, int96_t v) = 0;
    virtual void set_int128(size_t ordinal, const int128_t& v) = 0;
    virtual void set_float(size_t ordinal, float v) = 0;
    virtual void set_double(size_t ordinal, double v) = 0;
    virtual void set_timestamp(size_t ordinal, vectorized::TimestampValue v) = 0;
    virtual void set_date(size_t ordinal, vectorized::DateValue v) = 0;
    virtual void set_slice(size_t ordinal, const Slice& v) = 0;
    virtual void set_decimal12(size_t ordinal, const decimal12_t& v) = 0;
    virtual void set_decimal(size_t ordinal, const DecimalV2Value& v) = 0;
    virtual void set_array(size_t ordinal, const DatumArray& v) = 0;
    virtual void set_hyperloglog(size_t ordinal, HyperLogLog* v) = 0;
    virtual void set_bitmap(size_t ordinal, BitmapValue* v) = 0;
    virtual void set_percentile(size_t ordinal, PercentileValue* v) = 0;
    virtual void set_json(size_t ordinal, JsonValue* v) = 0;
};

} // namespace starrocks