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

#include "convert_helper.h"

#include <utility>

#include "column/chunk.h"
#include "column/datum_convert.h"
#include "column/decimalv3_column.h"
#include "column/nullable_column.h"
#include "column/schema.h"
#include "gutil/strings/substitute.h"
#include "runtime/datetime_value.h"
#include "runtime/decimalv2_value.h"
#include "runtime/mem_pool.h"
#include "storage/chunk_helper.h"
#include "storage/olap_type_infra.h"
#include "storage/tablet_schema.h"
#include "storage/type_traits.h"
#include "types/bitmap_value.h"
#include "types/hll.h"
#include "types/timestamp_value.h"
#include "util/json.h"
#include "util/percentile_value.h"
#include "util/pred_guard.h"
#include "util/stack_util.h"
#include "util/unaligned_access.h"

namespace starrocks {

using strings::Substitute;

template <typename SrcType, typename DstType>
struct ConvFunction {};
template <>
struct ConvFunction<decimal12_t, int128_t> {
    static inline void apply(const decimal12_t* src, int128_t* dst) {
        auto src_val = unaligned_load<decimal12_t>(src);
        DecimalV2Value tmp;
        tmp.from_olap_decimal(src_val.integer, src_val.fraction);
        unaligned_store<DecimalV2Value>(dst, tmp);
    }
};

template <>
struct ConvFunction<int128_t, decimal12_t> {
    static inline void apply(const int128_t* src, decimal12_t* dst) {
        auto src_val = unaligned_load<DecimalV2Value>(src);
        decimal12_t tmp;
        tmp.integer = src_val.int_value();
        tmp.fraction = src_val.frac_value();
        unaligned_store<decimal12_t>(dst, tmp);
    }
};

template <>
struct ConvFunction<DecimalV2Value, int128_t> {
    static inline void apply(const DecimalV2Value* src, int128_t* dst) {
        strings::memcpy_inlined(dst, src, sizeof(int128_t));
    }
};

template <>
struct ConvFunction<int128_t, DecimalV2Value> {
    static inline void apply(const int128_t* src, DecimalV2Value* dst) {
        strings::memcpy_inlined(dst, src, sizeof(int128_t));
    }
};

template <typename SrcType, typename DstType>
Status to_decimal(const SrcType* src, DstType* dst, int src_precision, int src_scale, int dst_precision,
                  int dst_scale) {
    if (dst_scale < src_scale || dst_precision - dst_scale < src_precision - src_scale) {
        return Status::InternalError("invalid schema");
    }
    int adjust_scale = dst_scale - src_scale;
    if (adjust_scale == 0) {
        DecimalV3Cast::to_decimal_trivial<SrcType, DstType, false>(*src, dst);
    } else if (adjust_scale > 0) {
        const auto scale_factor = get_scale_factor<DstType>(adjust_scale);
        DecimalV3Cast::to_decimal<SrcType, DstType, DstType, true, false>(*src, scale_factor, dst);
    } else {
        const auto scale_factor = get_scale_factor<SrcType>(-adjust_scale);
        DecimalV3Cast::to_decimal<SrcType, DstType, SrcType, false, false>(*src, scale_factor, dst);
    }
    return Status::OK();
};

Status TypeConverter::convert_column(TypeInfo* src_type, const Column& src, TypeInfo* dst_type, Column* dst,
                                     MemPool* mem_pool) const {
    for (size_t i = 0; i < src.size(); i++) {
        Datum old_datum = src.get(i);
        Datum new_datum;
        RETURN_IF_ERROR(convert_datum(src_type, old_datum, dst_type, &new_datum, mem_pool));
        dst->append_datum(new_datum);
    }
    return Status::OK();
}

// Used for schema change
class DatetimeToDateTypeConverter : public TypeConverter {
public:
    DatetimeToDateTypeConverter() = default;
    virtual ~DatetimeToDateTypeConverter() = default;

    Status convert(void* dst, const void* src, MemPool* memPool) const override {
        return Status::InternalError("missing implementation");
    }

    Status convert_datum(TypeInfo* src_typeinfo, const Datum& src, TypeInfo* dst_typeinfo, Datum* dst,
                         MemPool* mem_pool) const override {
        if (src.is_null()) {
            dst->set_null();
            return Status::OK();
        }

        auto src_value = src.get_int64();
        int64_t part1 = (src_value / 1000000L);
        uint24_t year = static_cast<uint24_t>((part1 / 10000L) % 10000);
        uint24_t mon = static_cast<uint24_t>((part1 / 100) % 100);
        uint24_t day = static_cast<uint24_t>(part1 % 100);
        dst->set_uint24((year << 9) + (mon << 5) + day);
        return Status::OK();
    }
};

class TimestampToDateTypeConverter : public TypeConverter {
public:
    TimestampToDateTypeConverter() = default;
    virtual ~TimestampToDateTypeConverter() = default;

    Status convert(void* dst, const void* src, MemPool* memPool) const override {
        return Status::InternalError("missing implementation");
    }

    Status convert_datum(TypeInfo* src_typeinfo, const Datum& src, TypeInfo* dst_typeinfo, Datum* dst,
                         MemPool* mem_pool) const override {
        if (src.is_null()) {
            dst->set_null();
            return Status::OK();
        }
        int year, mon, day, hour, minute, second, usec;
        auto src_value = src.get_timestamp();
        src_value.to_timestamp(&year, &mon, &day, &hour, &minute, &second, &usec);
        dst->set_uint24((year << 9) + (mon << 5) + day);
        return Status::OK();
    }
};

class IntToDateTypeConverter : public TypeConverter {
public:
    IntToDateTypeConverter() = default;
    virtual ~IntToDateTypeConverter() = default;

    Status convert(void* dst, const void* src, MemPool* memPool) const override {
        return Status::InternalError("missing implementation");
    }

    Status convert_datum(TypeInfo* src_typeinfo, const Datum& src, TypeInfo* dst_typeinfo, Datum* dst,
                         MemPool* mem_pool) const override {
        if (src.is_null()) {
            dst->set_null();
            return Status::OK();
        }
        auto src_value = src.get_int32();
        DateTimeValue dt;
        if (!dt.from_date_int64(src_value)) {
            return Status::InternalError("convert int64 to date failed");
        }
        uint24_t year = static_cast<uint24_t>(src_value / 10000);
        uint24_t mon = static_cast<uint24_t>((src_value % 10000) / 100);
        uint24_t day = static_cast<uint24_t>(src_value % 100);
        dst->set_uint24((year << 9) + (mon << 5) + day);
        return Status::OK();
    }
};

class DateV2ToDateTypeConverter : public TypeConverter {
public:
    DateV2ToDateTypeConverter() = default;
    virtual ~DateV2ToDateTypeConverter() = default;

    Status convert(void* dst, const void* src, MemPool* memPool) const override {
        return Status::InternalError("missing implementation");
    }

    Status convert_datum(TypeInfo* src_typeinfo, const Datum& src, TypeInfo* dst_typeinfo, Datum* dst,
                         MemPool* mem_pool) const override {
        if (src.is_null()) {
            dst->set_null();
            return Status::OK();
        }
        auto src_value = src.get_date();
        dst->set_uint24(src_value.to_mysql_date());
        return Status::OK();
    }
};

class TimestampToDateV2TypeConverter : public TypeConverter {
public:
    TimestampToDateV2TypeConverter() = default;
    virtual ~TimestampToDateV2TypeConverter() = default;

    Status convert(void* dst, const void* src, MemPool* memPool) const override {
        unaligned_store<DateValue>(dst, unaligned_load<TimestampValue>(src));
        return Status::OK();
    }

    Status convert_datum(TypeInfo* src_typeinfo, const Datum& src, TypeInfo* dst_typeinfo, Datum* dst,
                         MemPool* mem_pool) const override {
        if (src.is_null()) {
            dst->set_null();
            return Status::OK();
        }
        auto src_value = src.get_timestamp();
        int year, mon, day, hour, minute, second, usec;
        src_value.to_timestamp(&year, &mon, &day, &hour, &minute, &second, &usec);
        DateValue dst_value;
        dst_value.from_date(year, mon, day);
        dst->set_date(dst_value);
        return Status::OK();
    }
};

class DatetimeToDateV2TypeConverter : public TypeConverter {
public:
    DatetimeToDateV2TypeConverter() = default;
    virtual ~DatetimeToDateV2TypeConverter() = default;

    Status convert(void* dst, const void* src, MemPool* memPool) const override {
        TimestampValue timestamp{0};
        timestamp.from_timestamp_literal(unaligned_load<int64_t>(src));
        unaligned_store<DateValue>(dst, timestamp);
        return Status::OK();
    }

    Status convert_datum(TypeInfo* src_typeinfo, const Datum& src, TypeInfo* dst_typeinfo, Datum* dst,
                         MemPool* mem_pool) const override {
        if (src.is_null()) {
            dst->set_null();
            return Status::OK();
        }
        auto src_value = src.get_int64();
        int64_t part1 = (src_value / 1000000L);
        auto year = static_cast<int32_t>((part1 / 10000L) % 10000);
        auto mon = static_cast<int32_t>((part1 / 100) % 100);
        auto day = static_cast<int32_t>(part1 % 100);
        DateValue date_v2;
        date_v2.from_date(year, mon, day);
        dst->set_date(date_v2);
        return Status::OK();
    }
};

class DateToDateV2TypeConverter : public TypeConverter {
public:
    DateToDateV2TypeConverter() = default;
    virtual ~DateToDateV2TypeConverter() = default;

    Status convert(void* dst, const void* src, MemPool* memPool) const override {
        return Status::InternalError("missing implementation");
    }

    Status convert_datum(TypeInfo* src_typeinfo, const Datum& src, TypeInfo* dst_typeinfo, Datum* dst,
                         MemPool* mem_pool) const override {
        if (src.is_null()) {
            dst->set_null();
            return Status::OK();
        }
        DateValue date_v2;
        date_v2.from_mysql_date(src.get_uint24());
        dst->set_date(date_v2);
        return Status::OK();
    }
};

class DateToDatetimeFieldConveter : public TypeConverter {
public:
    DateToDatetimeFieldConveter() = default;
    virtual ~DateToDatetimeFieldConveter() = default;

    Status convert(void* dst, const void* src, MemPool* memPool) const override {
        return Status::InternalError("missing implementation");
    }

    Status convert_datum(TypeInfo* src_typeinfo, const Datum& src, TypeInfo* dst_typeinfo, Datum* dst,
                         MemPool* mem_pool) const override {
        if (src.is_null()) {
            dst->set_null();
            return Status::OK();
        }
        uint32_t src_value = src.get_uint24();
        int day = implicit_cast<int>(src_value & 31u);
        int month = implicit_cast<int>((src_value >> 5u) & 15u);
        int year = implicit_cast<int>(src_value >> 9u);
        dst->set_int64(static_cast<int64>(year * 10000L + month * 100L + day) * 1000000);
        return Status::OK();
    }
};

class DateV2ToDatetimeFieldConveter : public TypeConverter {
public:
    DateV2ToDatetimeFieldConveter() = default;
    virtual ~DateV2ToDatetimeFieldConveter() = default;

    Status convert(void* dst, const void* src, MemPool* memPool) const override {
        return Status::InternalError("missing implementation");
    }

    Status convert_datum(TypeInfo* src_typeinfo, const Datum& src, TypeInfo* dst_typeinfo, Datum* dst,
                         MemPool* mem_pool) const override {
        if (src.is_null()) {
            dst->set_null();
            return Status::OK();
        }
        auto src_value = src.get_date();
        int year, mon, day;
        src_value.to_date(&year, &mon, &day);
        dst->set_int64(static_cast<int64>(year * 10000L + mon * 100L + day) * 1000000);
        return Status::OK();
    }
};

class TimestampToDatetimeTypeConverter : public TypeConverter {
public:
    TimestampToDatetimeTypeConverter() = default;
    virtual ~TimestampToDatetimeTypeConverter() = default;

    Status convert(void* dst, const void* src, MemPool* memPool) const override {
        return Status::InternalError("missing implementation");
    }

    Status convert_datum(TypeInfo* src_typeinfo, const Datum& src, TypeInfo* dst_typeinfo, Datum* dst,
                         MemPool* mem_pool) const override {
        if (src.is_null()) {
            dst->set_null();
            return Status::OK();
        }
        dst->set_int64(src.get_timestamp().to_timestamp_literal());
        return Status::OK();
    }
};

class DateToTimestampTypeConverter : public TypeConverter {
public:
    DateToTimestampTypeConverter() = default;
    virtual ~DateToTimestampTypeConverter() = default;

    Status convert(void* dst, const void* src, MemPool* memPool) const override {
        uint32_t src_value = unaligned_load<uint24_t>(src);
        int day = implicit_cast<int>(src_value & 31u);
        int month = implicit_cast<int>((src_value >> 5u) & 15u);
        int year = implicit_cast<int>(src_value >> 9u);
        unaligned_store<TimestampValue>(dst, TimestampValue::create(year, month, day, 0, 0, 0));
        return Status::OK();
    }

    Status convert_datum(TypeInfo* src_typeinfo, const Datum& src, TypeInfo* dst_typeinfo, Datum* dst,
                         MemPool* mem_pool) const override {
        if (src.is_null()) {
            dst->set_null();
            return Status::OK();
        }
        uint32_t src_value = src.get_uint24();
        int day = implicit_cast<int>(src_value & 31u);
        int month = implicit_cast<int>((src_value >> 5u) & 15u);
        int year = implicit_cast<int>(src_value >> 9u);
        dst->set_timestamp(TimestampValue::create(year, month, day, 0, 0, 0));
        return Status::OK();
    }
};

class DateV2ToTimestampTypeConverter : public TypeConverter {
public:
    DateV2ToTimestampTypeConverter() = default;
    virtual ~DateV2ToTimestampTypeConverter() = default;

    Status convert(void* dst, const void* src, MemPool* memPool) const override {
        auto src_value = unaligned_load<DateValue>(src);
        int year = 0;
        int month = 0;
        int day = 0;
        src_value.to_date(&year, &month, &day);
        unaligned_store<TimestampValue>(dst, TimestampValue::create(year, month, day, 0, 0, 0));
        return Status::OK();
    }

    Status convert_datum(TypeInfo* src_typeinfo, const Datum& src, TypeInfo* dst_typeinfo, Datum* dst,
                         MemPool* mem_pool) const override {
        if (src.is_null()) {
            dst->set_null();
            return Status::OK();
        }
        auto src_value = src.get_date();
        int year = 0;
        int mon = 0;
        int day = 0;
        src_value.to_date(&year, &mon, &day);
        dst->set_timestamp(TimestampValue::create(year, mon, day, 0, 0, 0));
        return Status::OK();
    }
};

class DatetimeToTimestampTypeConverter : public TypeConverter {
public:
    DatetimeToTimestampTypeConverter() = default;
    virtual ~DatetimeToTimestampTypeConverter() = default;

    Status convert(void* dst, const void* src, MemPool* memPool) const override {
        return Status::InternalError("missing implementation");
    }

    Status convert_datum(TypeInfo* src_typeinfo, const Datum& src, TypeInfo* dst_typeinfo, Datum* dst,
                         MemPool* mem_pool) const override {
        if (src.is_null()) {
            dst->set_null();
            return Status::OK();
        }
        TimestampValue timestamp{0};
        timestamp.from_timestamp_literal(src.get_int64());
        dst->set_timestamp(timestamp);
        return Status::OK();
    }
};

class FloatToDoubleTypeConverter : public TypeConverter {
public:
    FloatToDoubleTypeConverter() = default;
    virtual ~FloatToDoubleTypeConverter() = default;

    Status convert(void* dst, const void* src, MemPool* memPool) const override {
        return Status::InternalError("missing implementation");
    }

    Status convert_datum(TypeInfo* src_typeinfo, const Datum& src, TypeInfo* dst_typeinfo, Datum* dst,
                         MemPool* mem_pool) const override {
        if (src.is_null()) {
            dst->set_null();
            return Status::OK();
        }
        char buf[64] = {0};
        snprintf(buf, 64, "%f", src.get_float());
        char* tg;
        dst->set_double(strtod(buf, &tg));
        return Status::OK();
    }
};

class DecimalToDecimal12TypeConverter : public TypeConverter {
public:
    DecimalToDecimal12TypeConverter() = default;
    virtual ~DecimalToDecimal12TypeConverter() = default;

    Status convert(void* dst, const void* src, MemPool* memPool) const override {
        return Status::InternalError("missing implementation");
    }

    Status convert_datum(TypeInfo* src_typeinfo, const Datum& src, TypeInfo* dst_typeinfo, Datum* dst,
                         MemPool* mem_pool) const override {
        if (src.is_null()) {
            dst->set_null();
            return Status::OK();
        }
        decimal12_t dst_value(src.get_decimal().int_value(), src.get_decimal().frac_value());
        dst->set_decimal12(dst_value);
        return Status::OK();
    }
};

class Decimal12ToDecimalTypeConverter : public TypeConverter {
public:
    Decimal12ToDecimalTypeConverter() = default;
    virtual ~Decimal12ToDecimalTypeConverter() = default;

    Status convert(void* dst, const void* src, MemPool* memPool) const override {
        return Status::InternalError("missing implementation");
    }

    Status convert_datum(TypeInfo* src_typeinfo, const Datum& src, TypeInfo* dst_typeinfo, Datum* dst,
                         MemPool* mem_pool) const override {
        if (src.is_null()) {
            dst->set_null();
            return Status::OK();
        }
        DecimalV2Value dst_value;
        dst_value.from_olap_decimal(src.get_decimal12().integer, src.get_decimal12().fraction);
        dst->set_decimal(dst_value);
        return Status::OK();
    }
};

template <LogicalType int_type>
class IntegerToDateV2TypeConverter : public TypeConverter {
public:
    using CppType = typename CppTypeTraits<int_type>::CppType;

    IntegerToDateV2TypeConverter() = default;
    virtual ~IntegerToDateV2TypeConverter() = default;

    Status convert(void* dst, const void* src, MemPool* memPool) const override {
        auto src_value = unaligned_load<CppType>(src);
        DateValue dst_val;
        if (dst_val.from_date_literal_with_check(src_value)) {
            unaligned_store<DateValue>(dst, dst_val);
            return Status::OK();
        }
        return Status::InvalidArgument(Substitute("Can not convert $0 to Date", src_value));
    }

    Status convert_datum(TypeInfo* src_typeinfo, const Datum& src, TypeInfo* dst_typeinfo, Datum* dst,
                         MemPool* mem_pool) const override {
        if (src.is_null()) {
            dst->set_null();
            return Status::OK();
        }
        auto src_value = src.get<CppType>();
        DateValue dst_val;
        if (dst_val.from_date_literal_with_check(src_value)) {
            dst->set_date(dst_val);
            return Status::OK();
        } else {
            return Status::InvalidArgument(Substitute("Can not convert $0 to Date", src_value));
        }
    }
};

template <LogicalType SrcType, LogicalType DstType>
class DecimalTypeConverter : public TypeConverter {
public:
    using SrcCppType = typename CppTypeTraits<SrcType>::CppType;
    using DstCppType = typename CppTypeTraits<DstType>::CppType;

    DecimalTypeConverter() = default;
    virtual ~DecimalTypeConverter() = default;

    Status convert(void* dst, const void* src, MemPool* memPool) const override {
        return Status::InternalError("missing implementation");
    }

    Status convert_datum(TypeInfo* src_typeinfo, const Datum& src, TypeInfo* dst_typeinfo, Datum* dst,
                         MemPool* mem_pool) const override {
        if (src.is_null()) {
            dst->set_null();
            return Status::OK();
        }
        const auto& src_val = src.get<SrcCppType>();
        DstCppType dst_val;
        ConvFunction<SrcCppType, DstCppType>::apply(&src_val, &dst_val);
        dst->set<DstCppType>(dst_val);
        return Status::OK();
    }
};

template <LogicalType SrcType, LogicalType DstType>
class DecimalV3TypeConverter : public TypeConverter {
public:
    using SrcCppType = typename CppTypeTraits<SrcType>::CppType;
    using DstCppType = typename CppTypeTraits<DstType>::CppType;

    DecimalV3TypeConverter() = default;
    virtual ~DecimalV3TypeConverter() = default;

    Status convert(void* dst, const void* src, MemPool* memPool) const override {
        return Status::InternalError("missing implementation");
    }

    Status convert_datum(TypeInfo* src_typeinfo, const Datum& src, TypeInfo* dst_typeinfo, Datum* dst,
                         MemPool* mem_pool) const override {
        if (src.is_null()) {
            dst->set_null();
            return Status::OK();
        }
        DstCppType dst_val = 0;
        SrcCppType src_val = src.get<SrcCppType>();
        auto overflow =
                to_decimal<SrcCppType, DstCppType>(&src_val, &dst_val, src_typeinfo->precision(), src_typeinfo->scale(),
                                                   dst_typeinfo->precision(), dst_typeinfo->scale());
        dst->set<DstCppType>(dst_val);
        return overflow;
    }
};

template <LogicalType Type>
class StringToOtherTypeConverter : public TypeConverter {
public:
    using CppType = typename CppTypeTraits<Type>::CppType;

    StringToOtherTypeConverter() = default;
    virtual ~StringToOtherTypeConverter() = default;

    Status convert(void* dst, const void* src, MemPool* memPool) const override {
        return Status::InternalError("missing implementation");
    }

    Status convert_datum(TypeInfo* src_typeinfo, const Datum& src, TypeInfo* dst_typeinfo, Datum* dst,
                         MemPool* mem_pool) const override {
        std::string source;
        if (src.is_null()) {
            source = "null";
        } else {
            source = src.get_slice().to_string();
        }
        CppType value;
        RETURN_IF_ERROR(dst_typeinfo->from_string(&value, source));
        dst->set(value);
        return Status::OK();
    }
};

// Convert string to json
// JSON needs dynamic memory allocation, which could not fit in TypeInfo::from_string
template <>
class StringToOtherTypeConverter<TYPE_JSON> : public TypeConverter {
public:
    using CppType = typename CppTypeTraits<TYPE_JSON>::CppType;

    StringToOtherTypeConverter() = default;
    virtual ~StringToOtherTypeConverter() = default;

    Status convert(void* dst, const void* src, MemPool* memPool) const override {
        return Status::InternalError("not supported");
    }

    Status convert_column(TypeInfo* src_type, const Column& src, TypeInfo* dst_type, Column* dst,
                          MemPool* mem_pool) const override {
        for (size_t i = 0; i < src.size(); i++) {
            Datum src_datum = src.get(i);
            Slice source;
            if (src_datum.is_null()) {
                source = "null";
            } else {
                source = src_datum.get_slice();
            }
            JsonValue json;
            RETURN_IF_ERROR(JsonValue::parse(source, &json));
            dst->append_datum(&json);
        }
        return Status::OK();
    }

    Status convert_datum(TypeInfo* src_typeinfo, const Datum& src, TypeInfo* dst_typeinfo, Datum* dst,
                         MemPool* mem_pool) const override {
        CHECK(false) << "unreachable";
        return Status::NotSupported("");
    }
};

template <LogicalType Type>
class OtherToStringTypeConverter : public TypeConverter {
public:
    using CppType = typename CppTypeTraits<Type>::CppType;

    OtherToStringTypeConverter() = default;
    virtual ~OtherToStringTypeConverter() = default;

    Status convert(void* dst, const void* src, MemPool* memPool) const override {
        return Status::InternalError("missing implementation");
    }

    Status convert_datum(TypeInfo* src_typeinfo, const Datum& src, TypeInfo* dst_typeinfo, Datum* dst,
                         MemPool* mem_pool) const override {
        std::string source;
        if (src.is_null()) {
            source = "null";
        } else {
            auto value = src.template get<CppType>();
            source = src_typeinfo->to_string(&value);
        }
        Slice slice;
        slice.size = source.size();
        if (mem_pool == nullptr) {
            LOG(WARNING) << "no guarantee of memory security";
            slice.data = (char*)source.data();
        } else {
            slice.data = reinterpret_cast<char*>(mem_pool->allocate(slice.size));
            if (UNLIKELY(slice.data == nullptr)) {
                return Status::MemoryLimitExceeded("Mem usage has exceed the limit of BE");
            }
            memcpy(slice.data, source.data(), slice.size);
        }
        dst->set_slice(slice);
        return Status::OK();
    }
};

template <>
class OtherToStringTypeConverter<TYPE_JSON> : public TypeConverter {
public:
    using CppType = typename CppTypeTraits<TYPE_JSON>::CppType;

    OtherToStringTypeConverter() = default;
    virtual ~OtherToStringTypeConverter() = default;

    Status convert(void* dst, const void* src, MemPool* memPool) const override {
        return Status::InternalError("not supported");
    }

    Status convert_column(TypeInfo* src_type, const Column& src, TypeInfo* dst_type, Column* dst,
                          MemPool* mem_pool) const override {
        for (size_t i = 0; i < src.size(); i++) {
            Datum src_datum = src.get(i);
            if (src_datum.is_null()) {
                dst->append_nulls(1);
            } else {
                const JsonValue* json = src_datum.get_json();
                std::string json_str = json->to_string_uncheck();
                Slice dst_slice = json_str;
                dst_slice.data = reinterpret_cast<char*>(mem_pool->allocate(dst_slice.size));
                RETURN_IF_UNLIKELY_NULL(dst_slice.data, Status::MemoryAllocFailed("mempool exceeded"));
                memcpy(dst_slice.data, json_str.data(), dst_slice.size);
                dst->append_datum(Datum(dst_slice));
            }
        }
        return Status::OK();
    }

    Status convert_datum(TypeInfo* src_typeinfo, const Datum& src_datum, TypeInfo* dst_typeinfo, Datum* dst,
                         MemPool* mem_pool) const override {
        CHECK(false) << "unreachable";
    }
};

const TypeConverter* get_date_converter(LogicalType from_type, LogicalType to_type) {
    switch (from_type) {
    case TYPE_DATETIME_V1: {
        static DatetimeToDateTypeConverter s_converter;
        return &s_converter;
    }
    case TYPE_DATETIME: {
        static TimestampToDateTypeConverter s_converter;
        return &s_converter;
    }
    case TYPE_INT: {
        static IntToDateTypeConverter s_converter;
        return &s_converter;
    }
    case TYPE_DATE: {
        static DateV2ToDateTypeConverter s_converter;
        return &s_converter;
    }
    default:
        return nullptr;
    }
    return nullptr;
}

const TypeConverter* get_datev2_converter(LogicalType from_type, LogicalType to_type) {
    switch (from_type) {
    case TYPE_DATETIME: {
        static TimestampToDateV2TypeConverter s_converter;
        return &s_converter;
    }
    case TYPE_DATETIME_V1: {
        static DatetimeToDateV2TypeConverter s_converter;
        return &s_converter;
    }
    case TYPE_INT: {
        static IntegerToDateV2TypeConverter<TYPE_INT> s_converter;
        return &s_converter;
    }
    case TYPE_DATE_V1: {
        static DateToDateV2TypeConverter s_converter;
        return &s_converter;
    }
    default:
        return nullptr;
    }
}

const TypeConverter* get_datetime_converter(LogicalType from_type, LogicalType to_type) {
    switch (from_type) {
    case TYPE_DATE_V1: {
        static DateToDatetimeFieldConveter s_converter;
        return &s_converter;
    }
    case TYPE_DATE: {
        static DateV2ToDatetimeFieldConveter s_converter;
        return &s_converter;
    }
    case TYPE_DATETIME: {
        static TimestampToDatetimeTypeConverter s_converter;
        return &s_converter;
    }
    default:
        return nullptr;
    }
}

const TypeConverter* get_timestamp_converter(LogicalType from_type, LogicalType to_type) {
    switch (from_type) {
    case TYPE_DATE_V1: {
        static DateToTimestampTypeConverter s_converter;
        return &s_converter;
    }
    case TYPE_DATE: {
        static DateV2ToTimestampTypeConverter s_converter;
        return &s_converter;
    }
    case TYPE_DATETIME_V1: {
        static DatetimeToTimestampTypeConverter s_converter;
        return &s_converter;
    }
    default:
        return nullptr;
    }
}

const TypeConverter* get_decimal_converter(LogicalType from_type, LogicalType to_type) {
    switch (from_type) {
    case TYPE_DECIMALV2: {
        static DecimalToDecimal12TypeConverter s_converter;
        return &s_converter;
    }
    case TYPE_DECIMAL128: {
        static DecimalTypeConverter<TYPE_DECIMAL128, TYPE_DECIMAL> s_converter;
        return &s_converter;
    }
    default:
        return nullptr;
    }
}

const TypeConverter* get_decimalv2_converter(LogicalType from_type, LogicalType to_type) {
    switch (from_type) {
    case TYPE_DECIMAL: {
        static Decimal12ToDecimalTypeConverter s_converter;
        return &s_converter;
    }
    case TYPE_DECIMAL128: {
        static DecimalTypeConverter<TYPE_DECIMAL128, TYPE_DECIMALV2> s_converter;
        return &s_converter;
    }
    default:
        return nullptr;
    }
}

#define DECIMALV3_TYPE_CONVERTER(FromType, ToType)                   \
    case FromType: {                                                 \
        static DecimalV3TypeConverter<FromType, ToType> s_converter; \
        return &s_converter;                                         \
    }

const TypeConverter* get_decimal128_converter(LogicalType from_type, LogicalType to_type) {
    switch (from_type) {
    case TYPE_DECIMAL: {
        static DecimalTypeConverter<TYPE_DECIMAL, TYPE_DECIMAL128> s_converter;
        return &s_converter;
    }
    case TYPE_DECIMALV2: {
        static DecimalTypeConverter<TYPE_DECIMALV2, TYPE_DECIMAL128> s_converter;
        return &s_converter;
    }
        DECIMALV3_TYPE_CONVERTER(TYPE_DECIMAL32, TYPE_DECIMAL128);
        DECIMALV3_TYPE_CONVERTER(TYPE_DECIMAL64, TYPE_DECIMAL128);
        DECIMALV3_TYPE_CONVERTER(TYPE_DECIMAL128, TYPE_DECIMAL128);
    default:
        return nullptr;
    }
}

const TypeConverter* get_decimal64_converter(LogicalType from_type, LogicalType to_type) {
    switch (from_type) {
        DECIMALV3_TYPE_CONVERTER(TYPE_DECIMAL32, TYPE_DECIMAL64);
        DECIMALV3_TYPE_CONVERTER(TYPE_DECIMAL64, TYPE_DECIMAL64);
        DECIMALV3_TYPE_CONVERTER(TYPE_DECIMAL128, TYPE_DECIMAL64);
    default:
        return nullptr;
    }
    return nullptr;
}

const TypeConverter* get_decimal32_converter(LogicalType from_type, LogicalType to_type) {
    switch (from_type) {
        DECIMALV3_TYPE_CONVERTER(TYPE_DECIMAL32, TYPE_DECIMAL32);
        DECIMALV3_TYPE_CONVERTER(TYPE_DECIMAL64, TYPE_DECIMAL32);
        DECIMALV3_TYPE_CONVERTER(TYPE_DECIMAL128, TYPE_DECIMAL32);
    default:
        return nullptr;
    }
}
#undef DECIMALV3_TYPE_CONVERTER

const TypeConverter* get_double_converter(LogicalType from_type, LogicalType to_type) {
    switch (from_type) {
    case TYPE_FLOAT: {
        static FloatToDoubleTypeConverter s_converter;
        return &s_converter;
    }
    default:
        return nullptr;
    }
}

const TypeConverter* get_from_varchar_converter(LogicalType from_type, LogicalType to_type) {
    switch (to_type) {
#define M(ftype)                                              \
    case ftype: {                                             \
        static StringToOtherTypeConverter<ftype> s_converter; \
        return &s_converter;                                  \
    }
        APPLY_FOR_TYPE_CONVERT_FROM_VARCHAR(M)
#undef M
    default:
        return nullptr;
    }
}

const TypeConverter* get_to_varchar_converter(LogicalType from_type, LogicalType to_type) {
    switch (from_type) {
    case TYPE_BOOLEAN: {
        static OtherToStringTypeConverter<TYPE_TINYINT> s_converter;
        return &s_converter;
    }
    case TYPE_CHAR:
    case TYPE_VARCHAR: {
        static OtherToStringTypeConverter<TYPE_VARCHAR> s_converter;
        return &s_converter;
    }
#define M(ftype)                                              \
    case ftype: {                                             \
        static OtherToStringTypeConverter<ftype> s_converter; \
        return &s_converter;                                  \
    }
        APPLY_FOR_TYPE_CONVERT_TO_VARCHAR(M)
#undef M

    default:
        return nullptr;
    }
}

// Datetime, Date and decimal should not be used
// Use Timestamp, Date V2 and decimal v2 to replace
// Should be deleted after storage_format_vesion = 1 is forbid
const TypeConverter* get_type_converter(LogicalType from_type, LogicalType to_type) {
    if (from_type == TYPE_VARCHAR) {
        return get_from_varchar_converter(from_type, to_type);
    }

    switch (to_type) {
    case TYPE_DATE_V1: {
        return get_date_converter(from_type, to_type);
    }
    case TYPE_DATE: {
        return get_datev2_converter(from_type, to_type);
    }
    case TYPE_DATETIME_V1: {
        return get_datetime_converter(from_type, to_type);
    }
    case TYPE_DATETIME: {
        return get_timestamp_converter(from_type, to_type);
    }
    case TYPE_DECIMAL: {
        return get_decimal_converter(from_type, to_type);
    }
    case TYPE_DECIMALV2: {
        return get_decimalv2_converter(from_type, to_type);
    }
    case TYPE_DECIMAL128: {
        return get_decimal128_converter(from_type, to_type);
    }
    case TYPE_DECIMAL64: {
        return get_decimal64_converter(from_type, to_type);
    }
    case TYPE_DECIMAL32: {
        return get_decimal32_converter(from_type, to_type);
    }
    case TYPE_DOUBLE: {
        return get_double_converter(from_type, to_type);
    }
    case TYPE_VARCHAR: {
        return get_to_varchar_converter(from_type, to_type);
    }
    default:
        break;
    }
    return nullptr;
}

template <typename SrcType>
class BitMapTypeConverter : public MaterializeTypeConverter {
public:
    BitMapTypeConverter() = default;
    virtual ~BitMapTypeConverter() = default;

    Status convert_materialized(ColumnPtr src_col, ColumnPtr dst_col, TypeInfo* src_type) const override {
        for (size_t row_index = 0; row_index < src_col->size(); ++row_index) {
            Datum src_datum = src_col->get(row_index);
            Datum dst_datum;
            if (src_datum.is_null()) {
                dst_datum.set_null();
                dst_col->append_datum(dst_datum);
                continue;
            }
            uint64_t origin_value = src_datum.get<SrcType>();
            if (origin_value < 0) {
                LOG(WARNING) << "The input which less than zero "
                             << " is not valid, to_bitmap only support bigint value from 0 to "
                                "18446744073709551615 currently";
                return Status::InternalError("input is invalid");
            }
            BitmapValue bitmap;
            bitmap.add(origin_value);
            dst_datum.set_bitmap(&bitmap);
            dst_col->append_datum(dst_datum);
        }
        return Status::OK();
    }
};

template <typename SrcType>
class HLLTypeConverter : public MaterializeTypeConverter {
public:
    HLLTypeConverter() = default;
    virtual ~HLLTypeConverter() = default;

    Status convert_materialized(ColumnPtr src_col, ColumnPtr dst_col, TypeInfo* src_type) const override {
        for (size_t row_index = 0; row_index < src_col->size(); ++row_index) {
            Datum src_datum = src_col->get(row_index);
            Datum dst_datum;
            if (src_datum.is_null()) {
                dst_datum.set_null();
                dst_col->append_datum(dst_datum);
                continue;
            }
            auto value = src_datum.template get<SrcType>();
            std::string src = src_type->to_string(&value);
            uint64_t hash_value =
                    HashUtil::murmur_hash64A(src.data(), static_cast<int32_t>(src.size()), HashUtil::MURMUR_SEED);
            HyperLogLog hll;
            hll.update(hash_value);
            dst_datum.set_hyperloglog(&hll);
            dst_col->append_datum(dst_datum);
        }
        return Status::OK();
    }
};

template <typename SrcType>
class PercentileTypeConverter : public MaterializeTypeConverter {
public:
    PercentileTypeConverter() = default;
    virtual ~PercentileTypeConverter() = default;

    Status convert_materialized(ColumnPtr src_col, ColumnPtr dst_col, TypeInfo* src_type) const override {
        for (size_t row_index = 0; row_index < src_col->size(); ++row_index) {
            Datum src_datum = src_col->get(row_index);
            Datum dst_datum;
            if (src_datum.is_null()) {
                dst_datum.set_null();
                dst_col->append_datum(dst_datum);
                continue;
            }
            double origin_value = src_datum.get<SrcType>();
            PercentileValue percentile;
            percentile.add(origin_value);
            dst_datum.set_percentile(&percentile);
            dst_col->append_datum(dst_datum);
        }
        return Status::OK();
    }
};

template <LogicalType SrcType>
class DecimalToPercentileTypeConverter : public MaterializeTypeConverter {
public:
    using CppType = typename CppTypeTraits<SrcType>::CppType;
    DecimalToPercentileTypeConverter() = default;
    virtual ~DecimalToPercentileTypeConverter() = default;

    Status convert_materialized(ColumnPtr src_col, ColumnPtr dst_col, TypeInfo* src_type) const override {
        for (size_t row_index = 0; row_index < src_col->size(); ++row_index) {
            Datum src_datum = src_col->get(row_index);
            Datum dst_datum;
            if (src_datum.is_null()) {
                dst_datum.set_null();
                dst_col->append_datum(dst_datum);
                continue;
            }
            double origin_value;
            auto v = src_datum.get<CppType>();
            auto scale_factor = get_scale_factor<CppType>(src_type->scale());
            DecimalV3Cast::to_float<CppType, double>(v, scale_factor, &origin_value);
            PercentileValue percentile;
            percentile.add(static_cast<float>(origin_value));
            dst_datum.set_percentile(&percentile);
            dst_col->append_datum(dst_datum);
        }
        return Status::OK();
    }
};

class CountTypeConverter : public MaterializeTypeConverter {
public:
    CountTypeConverter() = default;
    virtual ~CountTypeConverter() = default;

    Status convert_materialized(ColumnPtr src_col, ColumnPtr dst_col, TypeInfo* src_type) const override {
        for (size_t row_index = 0; row_index < src_col->size(); ++row_index) {
            Datum src_datum = src_col->get(row_index);
            Datum dst_datum;
            int64_t count = src_datum.is_null() ? 0 : 1;
            dst_datum.set_int64(count);
            dst_col->append_datum(dst_datum);
        }
        return Status::OK();
    }
};

#define GET_PERENTILE_CONVERTER(from_type)                     \
    {                                                          \
        static PercentileTypeConverter<from_type> s_converter; \
        return &s_converter;                                   \
    }

#define GET_DECIMAL_PERENTILE_CONVERTER(from_type)                      \
    {                                                                   \
        static DecimalToPercentileTypeConverter<from_type> s_converter; \
        return &s_converter;                                            \
    }

#define GET_HLL_CONVERTER(from_type)                    \
    {                                                   \
        static HLLTypeConverter<from_type> s_converter; \
        return &s_converter;                            \
    }

#define GET_BTIMAP_CONVERTER(from_type)                    \
    {                                                      \
        static BitMapTypeConverter<from_type> s_converter; \
        return &s_converter;                               \
    }

const MaterializeTypeConverter* get_perentile_converter(LogicalType from_type, MaterializeType to_type) {
    switch (from_type) {
    case TYPE_TINYINT:
        GET_PERENTILE_CONVERTER(int8_t);
    case TYPE_UNSIGNED_TINYINT:
        GET_PERENTILE_CONVERTER(uint8_t);
    case TYPE_SMALLINT:
        GET_PERENTILE_CONVERTER(int16_t);
    case TYPE_UNSIGNED_SMALLINT:
        GET_PERENTILE_CONVERTER(uint16_t);
    case TYPE_INT:
        GET_PERENTILE_CONVERTER(int32_t);
    case TYPE_UNSIGNED_INT:
        GET_PERENTILE_CONVERTER(uint32_t);
    case TYPE_BIGINT:
        GET_PERENTILE_CONVERTER(int64_t);
    case TYPE_UNSIGNED_BIGINT:
        GET_PERENTILE_CONVERTER(uint64_t);
    case TYPE_LARGEINT:
        GET_PERENTILE_CONVERTER(int128_t);
    case TYPE_FLOAT:
        GET_PERENTILE_CONVERTER(float);
    case TYPE_DOUBLE:
        GET_PERENTILE_CONVERTER(double);
    case TYPE_DECIMALV2:
        GET_PERENTILE_CONVERTER(DecimalV2Value);
    case TYPE_DECIMAL32:
        GET_DECIMAL_PERENTILE_CONVERTER(TYPE_DECIMAL32);
    case TYPE_DECIMAL64:
        GET_DECIMAL_PERENTILE_CONVERTER(TYPE_DECIMAL64);
    case TYPE_DECIMAL128:
        GET_DECIMAL_PERENTILE_CONVERTER(TYPE_DECIMAL128);
    default:
        LOG(WARNING) << "the column type which was altered from was unsupported."
                     << " from_type=" << from_type;
        return nullptr;
    }
}

const MaterializeTypeConverter* get_hll_converter(LogicalType from_type, MaterializeType to_type) {
    switch (from_type) {
    case TYPE_CHAR:
    case TYPE_VARCHAR:
        GET_HLL_CONVERTER(Slice);
    case TYPE_BOOLEAN:
    case TYPE_TINYINT:
        GET_HLL_CONVERTER(int8_t);
    case TYPE_UNSIGNED_TINYINT:
        GET_HLL_CONVERTER(uint8_t);
    case TYPE_SMALLINT:
        GET_HLL_CONVERTER(int16_t);
    case TYPE_UNSIGNED_SMALLINT:
        GET_HLL_CONVERTER(uint16_t);
    case TYPE_DATE:
    case TYPE_INT:
        GET_HLL_CONVERTER(int32_t);
    case TYPE_UNSIGNED_INT:
        GET_HLL_CONVERTER(uint32_t);
    case TYPE_DATETIME:
    case TYPE_DATETIME_V1:
    case TYPE_BIGINT:
        GET_HLL_CONVERTER(int64_t);
    case TYPE_UNSIGNED_BIGINT:
        GET_HLL_CONVERTER(uint64_t);
    case TYPE_LARGEINT:
        GET_HLL_CONVERTER(int128_t);
    case TYPE_FLOAT:
        GET_HLL_CONVERTER(float);
    case TYPE_DOUBLE:
        GET_HLL_CONVERTER(double);
    case TYPE_DATE_V1:
        GET_HLL_CONVERTER(uint24_t);
    default:
        LOG(WARNING) << "fail to hll hash type : " << from_type;
        return nullptr;
    }
}

const MaterializeTypeConverter* get_bitmap_converter(LogicalType from_type, MaterializeType to_type) {
    switch (from_type) {
    case TYPE_TINYINT:
        GET_BTIMAP_CONVERTER(int8_t);
    case TYPE_UNSIGNED_TINYINT:
        GET_BTIMAP_CONVERTER(uint8_t);
    case TYPE_SMALLINT:
        GET_BTIMAP_CONVERTER(int16_t);
    case TYPE_UNSIGNED_SMALLINT:
        GET_BTIMAP_CONVERTER(uint16_t);
    case TYPE_INT:
        GET_BTIMAP_CONVERTER(int32_t);
    case TYPE_UNSIGNED_INT:
        GET_BTIMAP_CONVERTER(uint32_t);
    case TYPE_BIGINT:
        GET_BTIMAP_CONVERTER(int64_t);
    case TYPE_UNSIGNED_BIGINT:
        GET_BTIMAP_CONVERTER(uint64_t);
    default:
        LOG(WARNING) << "the column type which was altered from was unsupported."
                     << " from_type=" << from_type;
        return nullptr;
    }
}

#undef GET_PERENTILE_CONVERTER
#undef GET_DECIMAL_PERENTILE_CONVERTER
#undef GET_HLL_CONVERTER
#undef GET_BITMAP_CONVERTER

const MaterializeTypeConverter* get_materialized_converter(LogicalType from_type, MaterializeType to_type) {
    switch (to_type) {
    case OLAP_MATERIALIZE_TYPE_PERCENTILE: {
        return get_perentile_converter(from_type, to_type);
    }
    case OLAP_MATERIALIZE_TYPE_HLL: {
        return get_hll_converter(from_type, to_type);
    }
    case OLAP_MATERIALIZE_TYPE_BITMAP: {
        return get_bitmap_converter(from_type, to_type);
    }
    case OLAP_MATERIALIZE_TYPE_COUNT: {
        static CountTypeConverter s_converter;
        return &s_converter;
    }
    default:
        LOG(WARNING) << "unknown materialized type";
        break;
    }
    return nullptr;
}

class SameFieldConverter : public FieldConverter {
public:
    explicit SameFieldConverter(TypeInfoPtr type_info) : _type_info(std::move(type_info)) {}

    ~SameFieldConverter() override = default;

    void convert(void* dst, const void* src) const override { _type_info->shallow_copy(dst, src); }

    void convert(Datum* dst, const Datum& src) const override { *dst = src; }

    ColumnPtr copy_convert(const Column& src) const override { return src.clone_shared(); }

    ColumnPtr move_convert(Column* src) const override {
        auto ret = src->clone_empty();
        ret->swap_column(*src);
        return ret;
    }

private:
    TypeInfoPtr _type_info;
};

class DateToDateV2FieldConverter : public FieldConverter {
public:
    DateToDateV2FieldConverter() = default;
    ~DateToDateV2FieldConverter() override = default;

    void convert(void* dst, const void* src) const override {
        DateValue tmp;
        tmp.from_mysql_date(unaligned_load<uint24_t>(src));
        unaligned_store<DateValue>(dst, tmp);
    }

    void convert(Datum* dst, const Datum& src) const override {
        if (src.is_null()) {
            return;
        }
        DateValue date_v2{0};
        date_v2.from_mysql_date(src.get_uint24());
        dst->set_date(date_v2);
    }

    ColumnPtr copy_convert(const Column& src) const override {
        auto nullable = src.is_nullable();
        auto dst = ChunkHelper::column_from_field_type(TYPE_DATE, nullable);
        int num_items = static_cast<int>(src.size());
        dst->reserve(num_items);
        for (int i = 0; i < num_items; ++i) {
            Datum dst_datum;
            Datum src_datum = src.get(i);
            convert(&dst_datum, src_datum);
            dst->append_datum(dst_datum);
        }
        return dst;
    }

private:
};

class DateV2ToDateFieldConverter : public FieldConverter {
public:
    DateV2ToDateFieldConverter() = default;
    ~DateV2ToDateFieldConverter() override = default;

    void convert(void* dst, const void* src) const override {
        unaligned_store<uint24_t>(dst, unaligned_load<DateValue>(src).to_mysql_date());
    }

    void convert(Datum* dst, const Datum& src) const override {
        if (src.is_null()) {
            dst->set_null();
            return;
        }
        dst->set_uint24(src.get_date().to_mysql_date());
    }

    ColumnPtr copy_convert(const Column& src) const override {
        auto nullable = src.is_nullable();
        auto dst = ChunkHelper::column_from_field_type(TYPE_DATE_V1, nullable);
        int num_items = static_cast<int>(src.size());
        dst->reserve(num_items);
        for (int i = 0; i < num_items; ++i) {
            Datum dst_datum;
            Datum src_datum = src.get(i);
            convert(&dst_datum, src_datum);
            dst->append_datum(dst_datum);
        }
        return dst;
    }

private:
};

class DatetimeToTimestampFieldConverter : public FieldConverter {
public:
    DatetimeToTimestampFieldConverter() = default;
    ~DatetimeToTimestampFieldConverter() override = default;

    void convert(void* dst, const void* src) const override {
        TimestampValue tmp;
        tmp.from_timestamp_literal(unaligned_load<int64_t>(src));
        unaligned_store<TimestampValue>(dst, tmp);
    }

    void convert(Datum* dst, const Datum& src) const override {
        if (src.is_null()) {
            dst->set_null();
            return;
        }
        TimestampValue timestamp{0};
        timestamp.from_timestamp_literal(src.get_int64());
        dst->set_timestamp(timestamp);
    }

    ColumnPtr copy_convert(const Column& src) const override {
        auto nullable = src.is_nullable();
        auto dst = ChunkHelper::column_from_field_type(TYPE_DATETIME, nullable);
        int num_items = static_cast<int>(src.size());
        dst->reserve(num_items);
        for (int i = 0; i < num_items; ++i) {
            Datum dst_datum;
            Datum src_datum = src.get(i);
            convert(&dst_datum, src_datum);
            dst->append_datum(dst_datum);
        }
        return dst;
    }

private:
};

class TimestampToDatetimeFieldConverter : public FieldConverter {
public:
    TimestampToDatetimeFieldConverter() = default;
    ~TimestampToDatetimeFieldConverter() override = default;

    void convert(void* dst, const void* src) const override {
        LOG(INFO) << "convert from datetime to timestamp";
        unaligned_store<int64_t>(dst, unaligned_load<TimestampValue>(src).to_timestamp_literal());
    }

    void convert(Datum* dst, const Datum& src) const override {
        if (src.is_null()) {
            dst->set_null();
            return;
        }
        dst->set_int64(src.get_timestamp().to_timestamp_literal());
    }

    ColumnPtr copy_convert(const Column& src) const override {
        auto nullable = src.is_nullable();
        auto dst = ChunkHelper::column_from_field_type(TYPE_DATETIME_V1, nullable);
        int num_items = static_cast<int>(src.size());
        dst->reserve(num_items);
        for (int i = 0; i < num_items; ++i) {
            Datum dst_datum;
            Datum src_datum = src.get(i);
            convert(&dst_datum, src_datum);
            dst->append_datum(dst_datum);
        }
        return dst;
    }

private:
};

class Decimal12ToDecimalFieldConverter : public FieldConverter {
public:
    Decimal12ToDecimalFieldConverter() = default;
    ~Decimal12ToDecimalFieldConverter() override = default;

    void convert(void* dst, const void* src) const override {
        auto src_val = unaligned_load<decimal12_t>(src);
        DecimalV2Value dst_val;
        dst_val.from_olap_decimal(src_val.integer, src_val.fraction);
        unaligned_store<DecimalV2Value>(dst, dst_val);
    }

    void convert(Datum* dst, const Datum& src) const override {
        if (src.is_null()) {
            dst->set_null();
            return;
        }
        DecimalV2Value dst_val;
        dst_val.from_olap_decimal(src.get_decimal12().integer, src.get_decimal12().fraction);
        dst->set_decimal(dst_val);
    }

    ColumnPtr copy_convert(const Column& src) const override {
        auto nullable = src.is_nullable();
        auto dst = ChunkHelper::column_from_field_type(TYPE_DECIMALV2, nullable);
        int num_items = static_cast<int>(src.size());
        dst->reserve(num_items);
        for (int i = 0; i < num_items; ++i) {
            Datum dst_datum;
            Datum src_datum = src.get(i);
            convert(&dst_datum, src_datum);
            dst->append_datum(dst_datum);
        }
        return dst;
    }

private:
};

class DecimalToDecimal12FieldConverter : public FieldConverter {
public:
    DecimalToDecimal12FieldConverter() = default;
    ~DecimalToDecimal12FieldConverter() override = default;

    void convert(void* dst, const void* src) const override {
        auto src_val = unaligned_load<DecimalV2Value>(src);
        decimal12_t dst_val;
        dst_val.integer = src_val.int_value();
        dst_val.fraction = src_val.frac_value();
        unaligned_store<decimal12_t>(dst, dst_val);
    }

    void convert(Datum* dst, const Datum& src) const override {
        if (src.is_null()) {
            dst->set_null();
            return;
        }
        decimal12_t dst_val(src.get_decimal().int_value(), src.get_decimal().frac_value());
        dst->set_decimal12(dst_val);
    }

    ColumnPtr copy_convert(const Column& src) const override {
        auto nullable = src.is_nullable();
        auto dst = ChunkHelper::column_from_field_type(TYPE_DECIMAL, nullable);
        int num_items = static_cast<int>(src.size());
        dst->reserve(num_items);
        for (int i = 0; i < num_items; ++i) {
            Datum dst_datum;
            Datum src_datum = src.get(i);
            convert(&dst_datum, src_datum);
            dst->append_datum(dst_datum);
        }
        return dst;
    }

private:
};

DEF_PRED_GUARD(DirectlyCopybleGuard, is_directly_copyable, typename, SrcType, typename, DstType)
#define IS_DIRECTLY_COPYABLE_CTOR(SrcType, DstType) DEF_PRED_CASE_CTOR(is_directly_copyable, SrcType, DstType)
#define IS_DIRECTLY_COPYABLE(LT, ...) DEF_BINARY_RELATION_ENTRY_SEP_NONE(IS_DIRECTLY_COPYABLE_CTOR, LT, ##__VA_ARGS__)

IS_DIRECTLY_COPYABLE(DecimalV2Value, int128_t)
IS_DIRECTLY_COPYABLE(int128_t, DecimalV2Value)

template <typename T>
struct ColumnTypeTraits {};
template <>
struct ColumnTypeTraits<DecimalV2Value> {
    using type = DecimalColumn;
};

template <>
struct ColumnTypeTraits<int128_t> {
    using type = Decimal128Column;
};

template <>
struct ColumnTypeTraits<decimal12_t> {
    using type = FixedLengthColumn<decimal12_t>;
};

template <typename T>
using ColumnType = typename ColumnTypeTraits<T>::type;

template <typename SrcType, typename DstType>
class DecimalFieldConverter : public FieldConverter {
public:
    DecimalFieldConverter() = default;
    ~DecimalFieldConverter() override = default;
    void convert(void* dst, const void* src) const override {
        const auto* src_val = reinterpret_cast<const SrcType*>(src);
        auto* dst_val = reinterpret_cast<DstType*>(dst);
        ConvFunction<SrcType, DstType>::apply(src_val, dst_val);
    }

    void convert(Datum* dst, const Datum& src) const override {
        if (src.is_null()) {
            dst->set_null();
            return;
        }
        const auto& src_val = src.get<SrcType>();
        DstType dst_val;
        ConvFunction<SrcType, DstType>::apply(&src_val, &dst_val);
        dst->set<DstType>(dst_val);
    }

    ColumnPtr copy_convert(const Column& src) const override {
        using SrcColumnType = ColumnType<SrcType>;
        using DstColumnType = ColumnType<DstType>;
        // FIXME: precision and scale are lost.
        ColumnPtr dst = DstColumnType::create();
        dst->reserve(src.size());
        if constexpr (is_directly_copyable<SrcType, DstType>) {
            if (!src.is_nullable() && !src.is_constant()) {
                auto* dst_column = down_cast<DstColumnType*>(dst.get());
                auto* src_column = down_cast<const SrcColumnType*>(&src);
                // TODO (by satanson): unsafe abstraction leak
                //  swap std::vector<DecimalV2Value> and std::vector<int128_t>,
                //  raw memory copy is more sound.
                std::swap(dst_column->get_data(), (typename DstColumnType::Container&)(src_column->get_data()));
                return dst;
            } else if (src.is_nullable() && !dst->only_null()) {
                dst = NullableColumn::create(dst, NullColumn::create());
                auto* nullable_dst_column = down_cast<NullableColumn*>(dst.get());
                auto* nullable_src_column = down_cast<const NullableColumn*>(&src);
                auto* dst_column = down_cast<DstColumnType*>(nullable_dst_column->data_column().get());
                auto* src_column = down_cast<SrcColumnType*>(nullable_src_column->data_column().get());
                auto& null_dst_column = nullable_dst_column->null_column();
                auto& null_src_column = nullable_src_column->null_column();
                // TODO (by satanson): unsafe abstraction leak
                //  swap std::vector<DecimalV2Value> and std::vector<int128_t>
                //  raw memory copy is more sound.
                std::swap(dst_column->get_data(), (typename DstColumnType::Container&)(src_column->get_data()));
                std::swap(null_dst_column->get_data(), null_src_column->get_data());
                return dst;
            }
        }
        int num_items = static_cast<int>(src.size());
        for (int i = 0; i < num_items; ++i) {
            Datum dst_datum;
            Datum src_datum = src.get(i);
            convert(&dst_datum, src_datum);
            dst->append_datum(dst_datum);
        }
        return dst;
    }

private:
};

const FieldConverter* get_field_converter(LogicalType from_type, LogicalType to_type) {
#define TYPE_CASE_CLAUSE(type)                                      \
    case type: {                                                    \
        static SameFieldConverter s_converter(get_type_info(type)); \
        return &s_converter;                                        \
    }

    if (from_type == to_type) {
        switch (from_type) {
            TYPE_CASE_CLAUSE(TYPE_BOOLEAN)
            TYPE_CASE_CLAUSE(TYPE_TINYINT)
            TYPE_CASE_CLAUSE(TYPE_SMALLINT)
            TYPE_CASE_CLAUSE(TYPE_INT)
            TYPE_CASE_CLAUSE(TYPE_BIGINT)
            TYPE_CASE_CLAUSE(TYPE_LARGEINT)
            TYPE_CASE_CLAUSE(TYPE_FLOAT)
            TYPE_CASE_CLAUSE(TYPE_DOUBLE)
            TYPE_CASE_CLAUSE(TYPE_DECIMAL)
            TYPE_CASE_CLAUSE(TYPE_DECIMALV2)
            TYPE_CASE_CLAUSE(TYPE_CHAR)
            TYPE_CASE_CLAUSE(TYPE_VARCHAR)
            TYPE_CASE_CLAUSE(TYPE_DATE_V1)
            TYPE_CASE_CLAUSE(TYPE_DATE)
            TYPE_CASE_CLAUSE(TYPE_DATETIME_V1)
            TYPE_CASE_CLAUSE(TYPE_DATETIME)
            TYPE_CASE_CLAUSE(TYPE_HLL)
            TYPE_CASE_CLAUSE(TYPE_OBJECT)
            TYPE_CASE_CLAUSE(TYPE_PERCENTILE)
            TYPE_CASE_CLAUSE(TYPE_JSON)
            TYPE_CASE_CLAUSE(TYPE_VARBINARY)
        case TYPE_DECIMAL32:
        case TYPE_DECIMAL64:
        case TYPE_DECIMAL128:
        case TYPE_ARRAY:
        case TYPE_UNSIGNED_TINYINT:
        case TYPE_UNSIGNED_SMALLINT:
        case TYPE_UNSIGNED_INT:
        case TYPE_UNSIGNED_BIGINT:
        case TYPE_UNKNOWN:
        case TYPE_MAP:
        case TYPE_STRUCT:
        case TYPE_DISCRETE_DOUBLE:
        case TYPE_NONE:
        case TYPE_NULL:
        case TYPE_FUNCTION:
        case TYPE_TIME:
        case TYPE_BINARY:
        case TYPE_MAX_VALUE:
            return nullptr;
        }
        DCHECK(false) << "unreachable path";
        return nullptr;
    }
#undef TYPE_CASE_CLAUSE

    if (to_type == TYPE_DATE && from_type == TYPE_DATE_V1) {
        static DateToDateV2FieldConverter s_converter;
        return &s_converter;
    } else if (to_type == TYPE_DATE_V1 && from_type == TYPE_DATE) {
        static DateV2ToDateFieldConverter s_converter;
        return &s_converter;
    } else if (to_type == TYPE_DATETIME && from_type == TYPE_DATETIME_V1) {
        static DatetimeToTimestampFieldConverter s_converter;
        return &s_converter;
    } else if (to_type == TYPE_DATETIME_V1 && from_type == TYPE_DATETIME) {
        static TimestampToDatetimeFieldConverter s_converter;
        return &s_converter;
    } else if (to_type == TYPE_DECIMALV2 && from_type == TYPE_DECIMAL) {
        static Decimal12ToDecimalFieldConverter s_converter;
        return &s_converter;
    } else if (to_type == TYPE_DECIMAL && from_type == TYPE_DECIMALV2) {
        static DecimalToDecimal12FieldConverter s_converter;
        return &s_converter;
    } else if (to_type == TYPE_DECIMAL128 && from_type == TYPE_DECIMAL) {
        static DecimalFieldConverter<decimal12_t, int128_t> s_converter;
        return &s_converter;
    } else if (to_type == TYPE_DECIMAL && from_type == TYPE_DECIMAL128) {
        static DecimalFieldConverter<int128_t, decimal12_t> s_converter;
        return &s_converter;
    } else if (to_type == TYPE_DECIMAL128 && from_type == TYPE_DECIMALV2) {
        static DecimalFieldConverter<DecimalV2Value, int128_t> s_converter;
        return &s_converter;
    } else if (to_type == TYPE_DECIMALV2 && from_type == TYPE_DECIMAL128) {
        static DecimalFieldConverter<int128_t, DecimalV2Value> s_converter;
        return &s_converter;
    }
    return nullptr;
}

Status RowConverter::init(const TabletSchema& in_schema, const TabletSchema& out_schema) {
    auto num_columns = in_schema.num_columns();
    _converters.resize(num_columns);
    _cids.resize(num_columns, 0);
    for (int i = 0; i < in_schema.num_columns(); ++i) {
        _cids[i] = i;
        _converters[i] = get_field_converter(in_schema.column(i).type(), out_schema.column(i).type());
        if (_converters[i] == nullptr) {
            return Status::NotSupported("Cannot get field converter");
        }
    }
    return Status::OK();
}

Status RowConverter::init(const Schema& in_schema, const Schema& out_schema) {
    auto num_columns = in_schema.num_fields();
    _converters.resize(num_columns);
    for (int i = 0; i < num_columns; ++i) {
        _converters[i] = get_field_converter(in_schema.field(i)->type()->type(), out_schema.field(i)->type()->type());
        if (_converters[i] == nullptr) {
            return Status::NotSupported("Cannot get field converter");
        }
    }
    return Status::OK();
}

void RowConverter::convert(std::vector<Datum>* dst, const std::vector<Datum>& src) const {
    size_t num_datums = src.size();
    dst->resize(num_datums);
    for (size_t i = 0; i < num_datums; ++i) {
        _converters[i]->convert(&(*dst)[i], src[i]);
    }
}

Status ChunkConverter::init(const Schema& in_schema, const Schema& out_schema) {
    DCHECK_EQ(in_schema.num_fields(), out_schema.num_fields());
    DCHECK_EQ(in_schema.num_key_fields(), out_schema.num_key_fields());
    auto num_columns = in_schema.num_fields();
    _converters.resize(num_columns, nullptr);
    for (int i = 0; i < num_columns; ++i) {
        auto& f1 = in_schema.field(i);
        auto& f2 = out_schema.field(i);
        DCHECK_EQ(f1->id(), f2->id());
        _converters[i] = get_field_converter(f1->type()->type(), f2->type()->type());
        if (_converters[i] == nullptr) {
            return Status::NotSupported("Cannot get field converter");
        }
    }
    _out_schema = std::make_shared<Schema>(out_schema);
    return Status::OK();
}

std::unique_ptr<Chunk> ChunkConverter::copy_convert(const Chunk& from) const {
    auto dest = std::make_unique<Chunk>(Columns{}, std::make_shared<Schema>());
    auto num_columns = _converters.size();
    DCHECK_EQ(num_columns, from.num_columns());
    for (int i = 0; i < num_columns; ++i) {
        auto f = _out_schema->field(i);
        auto c = _converters[i]->copy_convert(*from.get_column_by_id(f->id()));
        dest->append_column(std::move(c), f);
    }
    return dest;
}

std::unique_ptr<Chunk> ChunkConverter::move_convert(Chunk* from) const {
    auto dest = std::make_unique<Chunk>(Columns{}, std::make_shared<Schema>());
    auto num_columns = _converters.size();
    DCHECK_EQ(num_columns, from->num_columns());
    for (int i = 0; i < num_columns; ++i) {
        auto f = _out_schema->field(i);
        auto c = _converters[i]->move_convert(from->get_column_by_id(f->id()).get());
        dest->append_column(std::move(c), f);
    }
    return dest;
}

} // namespace starrocks
