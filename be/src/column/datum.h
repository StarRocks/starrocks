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

#include <map>
#include <type_traits>
#include <variant>

#include "runtime/decimalv2_value.h"
#include "storage/decimal12.h"
#include "storage/uint24.h"
#include "types/date_value.hpp"
#include "types/timestamp_value.h"
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

namespace starrocks {

typedef __int128 int128_t;
typedef unsigned __int128 uint128_t;

class Datum;
using DatumArray = std::vector<Datum>;

using DatumKey = std::variant<std::monostate, int8_t, uint8_t, int16_t, uint16_t, uint24_t, int32_t, uint32_t, int64_t,
                              uint64_t, int96_t, int128_t, Slice, decimal12_t, DecimalV2Value, float, double>;
using DatumMap = std::map<DatumKey, Datum>;
using DatumStruct = std::vector<Datum>;

template <class... Ts>
struct overloaded : Ts... {
    using Ts::operator()...;
};
template <class... Ts>
overloaded(Ts...) -> overloaded<Ts...>;

class Datum {
public:
    Datum() = default;

    template <typename T>
    Datum(T value) {
        static_assert(!std::is_same_v<std::string, T>, "should use the Slice as parameter instead of std::string");
        set(value);
    }

    Datum(const DatumKey& datum_key) {
        std::visit(overloaded{[this](auto& arg) { set<decltype(arg)>(arg); }}, datum_key);
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
    const DatumMap& get_map() const { return get<DatumMap>(); }
    const DatumStruct& get_struct() const { return get<DatumStruct>(); }
    const HyperLogLog* get_hyperloglog() const { return get<HyperLogLog*>(); }
    const BitmapValue* get_bitmap() const { return get<BitmapValue*>(); }
    const PercentileValue* get_percentile() const { return get<PercentileValue*>(); }
    const JsonValue* get_json() const { return get<JsonValue*>(); }

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

    template <typename T>
    const T& get() const {
        if constexpr (std::is_same_v<DateValue, T>) {
            static_assert(sizeof(DateValue) == sizeof(int32_t));
            return reinterpret_cast<const T&>(std::get<int32_t>(_value));
        } else if constexpr (std::is_same_v<TimestampValue, T>) {
            static_assert(sizeof(TimestampValue) == sizeof(int64_t));
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
        } else if constexpr (std::is_same_v<bool, T>) {
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

    DatumKey convert2DatumKey() const {
        if (is_null()) {
            return std::monostate();
        }
        return std::visit(
                overloaded{[](const int8_t& arg) { return DatumKey(arg); },
                           [](const uint8_t& arg) { return DatumKey(arg); },
                           [](const int16_t& arg) { return DatumKey(arg); },
                           [](const uint16_t& arg) { return DatumKey(arg); },
                           [](const uint24_t& arg) { return DatumKey(arg); },
                           [](const int32_t& arg) { return DatumKey(arg); },
                           [](const uint32_t& arg) { return DatumKey(arg); },
                           [](const int64_t& arg) { return DatumKey(arg); },
                           [](const uint64_t& arg) { return DatumKey(arg); },
                           [](const int96_t& arg) { return DatumKey(arg); },
                           [](const int128_t& arg) { return DatumKey(arg); },
                           [](const Slice& arg) { return DatumKey(arg); },
                           [](const decimal12_t& arg) { return DatumKey(arg); },
                           [](const DecimalV2Value& arg) { return DatumKey(arg); },
                           [](const float& arg) { return DatumKey(arg); },
                           [](const double& arg) { return DatumKey(arg); }, [](auto& arg) { return DatumKey(); }},
                _value);
    }

    template <typename T>
    bool is_equal(const T& val) const {
        return get<T>() == val;
    }

    bool equal_datum_key(const DatumKey& key) const {
        return std::visit([&](const auto& arg) { return is_equal(arg); }, key);
    }

private:
    using Variant =
            std::variant<std::monostate, int8_t, uint8_t, int16_t, uint16_t, uint24_t, int32_t, uint32_t, int64_t,
                         uint64_t, int96_t, int128_t, Slice, decimal12_t, DecimalV2Value, float, double, DatumArray,
                         DatumMap, HyperLogLog*, BitmapValue*, PercentileValue*, JsonValue*>;
    Variant _value;
};

static const Datum kNullDatum{};

Datum convert2Datum(const DatumKey& key);

} // namespace starrocks
