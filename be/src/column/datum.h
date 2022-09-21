// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#pragma once

#include <variant>

#include "runtime/decimalv2_value.h"
#include "storage/decimal12.h"
#include "storage/uint24.h"
#include "types/date_value.hpp"
#include "types/timestamp_value.h"
#include "types/ipv4_value.h"
#include "util/int96.h"
#include "util/slice.h"

namespace starrocks {
class MemPool;
class Status;
class BitmapValue;
class HyperLogLog;
class PercentileValue;
class JsonValue;
} // namespace starrocks

namespace starrocks::vectorized {

typedef __int128 int128_t;
typedef unsigned __int128 uint128_t;

class Datum;
using DatumArray = std::vector<Datum>;

class Datum {
public:
    Datum() = default;

    template <typename T>
    Datum(T value) {
        set(value);
    }

    int8_t get_int8() const { return get<int8_t>(); }
    uint8_t get_uint8() const { return get<uint8_t>(); }
    int16_t get_int16() const { return get<int16_t>(); }
    uint16_t get_uint16() const { return get<uint16_t>(); }
    uint24_t get_uint24() const { return get<uint24_t>(); }
    int96_t get_int96() const { return get<int96_t>(); }
    int32_t get_int32() const { return get<int32_t>(); }
    uint32_t get_uint32() const { return get<uint32_t>(); }
    int64_t get_int64() const { return get<int64_t>(); }
    uint64_t get_uint64() const { return get<uint64_t>(); }
    float get_float() const { return get<float>(); }
    double get_double() const { return get<double>(); }
    TimestampValue get_timestamp() const { return get<TimestampValue>(); }
    DateValue get_date() const { return get<DateValue>(); }
    const Slice& get_slice() const { return get<Slice>(); }
    const int128_t& get_int128() const { return get<int128_t>(); }
    const decimal12_t& get_decimal12() const { return get<decimal12_t>(); }
    const DecimalV2Value& get_decimal() const { return get<DecimalV2Value>(); }
    const DatumArray& get_array() const { return get<DatumArray>(); }
    const HyperLogLog* get_hyperloglog() const { return get<HyperLogLog*>(); }
    const BitmapValue* get_bitmap() const { return get<BitmapValue*>(); }
    const PercentileValue* get_percentile() const { return get<PercentileValue*>(); }
    const JsonValue* get_json() const { return get<JsonValue*>(); }
    const Ipv4Value* get_ipv4() const { return get<Ipv4Value*>(); }

    void set_int8(int8_t v) { set<decltype(v)>(v); }
    void set_uint8(uint8_t v) { set<decltype(v)>(v); }
    void set_int16(int16_t v) { set<decltype(v)>(v); }
    void set_uint16(uint16_t v) { set<decltype(v)>(v); }
    void set_uint24(uint24_t v) { set<decltype(v)>(v); }
    void set_int32(int32_t v) { set<decltype(v)>(v); }
    void set_uint32(uint32_t v) { set<decltype(v)>(v); }
    void set_int64(int64_t v) { set<decltype(v)>(v); }
    void set_uint64(uint64_t v) { set<decltype(v)>(v); }
    void set_int96(int96_t v) { set<decltype(v)>(v); }
    void set_float(float v) { set<decltype(v)>(v); }
    void set_double(double v) { set<decltype(v)>(v); }
    void set_timestamp(TimestampValue v) { set<decltype(v)>(v); }
    void set_date(DateValue v) { set<decltype(v)>(v); }
    void set_int128(const int128_t& v) { set<decltype(v)>(v); }
    void set_slice(const Slice& v) { set<decltype(v)>(v); }
    void set_decimal12(const decimal12_t& v) { set<decltype(v)>(v); }
    void set_decimal(const DecimalV2Value& v) { set<decltype(v)>(v); }
    void set_array(const DatumArray& v) { set<decltype(v)>(v); }
    void set_hyperloglog(HyperLogLog* v) { set<decltype(v)>(v); }
    void set_bitmap(BitmapValue* v) { set<decltype(v)>(v); }
    void set_percentile(PercentileValue* v) { set<decltype(v)>(v); }
    void set_json(JsonValue* v) { set<decltype(v)>(v); }
    void set_ipv4(Ipv4Value* v) { set<decltype(v)>(v); }

    template <typename T>
    const T& get() const {
        if constexpr (std::is_same_v<DateValue, T>) {
            static_assert(sizeof(DateValue) == sizeof(int32_t));
            return reinterpret_cast<const T&>(std::get<int32_t>(_value));
        } else if constexpr (std::is_same_v<TimestampValue, T>) {
            static_assert(sizeof(TimestampValue) == sizeof(int64_t));
            return reinterpret_cast<const T&>(std::get<int64_t>(_value));
        } else if constexpr (std::is_same_v<Ipv4Value, T>) {
            static_assert(sizeof(Ipv4Value) == sizeof(int64_t));
            return reinterpret_cast<const T&>(std::get<int64_t>(_value));
        } else if constexpr (std::is_same_v<bool, T>) {
            return reinterpret_cast<const T&>(std::get<int8_t>(_value));
        } else if constexpr (std::is_unsigned_v<T>) {
            return reinterpret_cast<const T&>(std::get<std::make_signed_t<T>>(_value));
        } else {
            return std::get<T>(_value);
        }
    }

    template <typename T>
    void set(T value) {
        if constexpr (std::is_same_v<DateValue, T>) {
            _value = value.julian();
        } else if constexpr (std::is_same_v<TimestampValue, T>) {
            _value = value.timestamp();
        } else if constexpr (std::is_same_v<Ipv4Value, T>) {
            _value = value.ip();
        }else if constexpr (std::is_same_v<bool, T>) {
            _value = (int8_t)value;
        } else if constexpr (std::is_unsigned_v<T>) {
            _value = (std::make_signed_t<T>)value;
        } else {
            _value = value;
        }
    }

    bool is_null() const { return _value.index() == 0; }

    void set_null() { _value = std::monostate(); }

    template <class Vistor>
    void visit(Vistor&& vistor) {
        vistor(_value);
    }

private:
    using Variant = std::variant<std::monostate, int8_t, uint8_t, int16_t, uint16_t, uint24_t, int32_t, uint32_t,
                                 int64_t, uint64_t, int96_t, int128_t, Slice, decimal12_t, DecimalV2Value, float,
                                 double, DatumArray, HyperLogLog*, BitmapValue*, PercentileValue*, JsonValue*, Ipv4Value*>;
    Variant _value;
};

static const Datum kNullDatum{};

} // namespace starrocks::vectorized
