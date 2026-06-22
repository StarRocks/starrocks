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

#include "column/type_converter.h"

#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <string>

#include "base/hash/unaligned_access.h"
#include "column/type_converter_detail.h"
#include "gutil/strings/substitute.h"
#include "types/datetime_value.h"
#include "types/decimalv3.h"
#include "types/json_value.h"
#include "types/olap_type_infra.h"
#include "types/storage_type_traits.h"
#include "types/timestamp_value.h"
#include "types/type_info.h"

namespace starrocks {

using strings::Substitute;

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
                                     const TypeInfoAllocator* allocator) const {
    for (size_t i = 0; i < src.size(); i++) {
        Datum old_datum = src.get(i);
        Datum new_datum;
        RETURN_IF_ERROR(convert_datum(src_type, old_datum, dst_type, &new_datum, allocator));
        dst->append_datum(new_datum);
    }
    return Status::OK();
}

// Used for schema change
class DatetimeToDateTypeConverter : public TypeConverter {
public:
    DatetimeToDateTypeConverter() = default;
    ~DatetimeToDateTypeConverter() override = default;

    Status convert(void* dst, const void* src, const TypeInfoAllocator* allocator) const override {
        return Status::InternalError("missing implementation");
    }

    Status convert_datum(TypeInfo* src_typeinfo, const Datum& src, TypeInfo* dst_typeinfo, Datum* dst,
                         const TypeInfoAllocator* allocator) const override {
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
    ~TimestampToDateTypeConverter() override = default;

    Status convert(void* dst, const void* src, const TypeInfoAllocator* allocator) const override {
        return Status::InternalError("missing implementation");
    }

    Status convert_datum(TypeInfo* src_typeinfo, const Datum& src, TypeInfo* dst_typeinfo, Datum* dst,
                         const TypeInfoAllocator* allocator) const override {
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
    ~IntToDateTypeConverter() override = default;

    Status convert(void* dst, const void* src, const TypeInfoAllocator* allocator) const override {
        return Status::InternalError("missing implementation");
    }

    Status convert_datum(TypeInfo* src_typeinfo, const Datum& src, TypeInfo* dst_typeinfo, Datum* dst,
                         const TypeInfoAllocator* allocator) const override {
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
    ~DateV2ToDateTypeConverter() override = default;

    Status convert(void* dst, const void* src, const TypeInfoAllocator* allocator) const override {
        return Status::InternalError("missing implementation");
    }

    Status convert_datum(TypeInfo* src_typeinfo, const Datum& src, TypeInfo* dst_typeinfo, Datum* dst,
                         const TypeInfoAllocator* allocator) const override {
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
    ~TimestampToDateV2TypeConverter() override = default;

    Status convert(void* dst, const void* src, const TypeInfoAllocator* allocator) const override {
        unaligned_store<DateValue>(dst, unaligned_load<TimestampValue>(src));
        return Status::OK();
    }

    Status convert_datum(TypeInfo* src_typeinfo, const Datum& src, TypeInfo* dst_typeinfo, Datum* dst,
                         const TypeInfoAllocator* allocator) const override {
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
    ~DatetimeToDateV2TypeConverter() override = default;

    Status convert(void* dst, const void* src, const TypeInfoAllocator* allocator) const override {
        TimestampValue timestamp{0};
        timestamp.from_timestamp_literal(unaligned_load<int64_t>(src));
        unaligned_store<DateValue>(dst, timestamp);
        return Status::OK();
    }

    Status convert_datum(TypeInfo* src_typeinfo, const Datum& src, TypeInfo* dst_typeinfo, Datum* dst,
                         const TypeInfoAllocator* allocator) const override {
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
    ~DateToDateV2TypeConverter() override = default;

    Status convert(void* dst, const void* src, const TypeInfoAllocator* allocator) const override {
        return Status::InternalError("missing implementation");
    }

    Status convert_datum(TypeInfo* src_typeinfo, const Datum& src, TypeInfo* dst_typeinfo, Datum* dst,
                         const TypeInfoAllocator* allocator) const override {
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
    ~DateToDatetimeFieldConveter() override = default;

    Status convert(void* dst, const void* src, const TypeInfoAllocator* allocator) const override {
        return Status::InternalError("missing implementation");
    }

    Status convert_datum(TypeInfo* src_typeinfo, const Datum& src, TypeInfo* dst_typeinfo, Datum* dst,
                         const TypeInfoAllocator* allocator) const override {
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
    ~DateV2ToDatetimeFieldConveter() override = default;

    Status convert(void* dst, const void* src, const TypeInfoAllocator* allocator) const override {
        return Status::InternalError("missing implementation");
    }

    Status convert_datum(TypeInfo* src_typeinfo, const Datum& src, TypeInfo* dst_typeinfo, Datum* dst,
                         const TypeInfoAllocator* allocator) const override {
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
    ~TimestampToDatetimeTypeConverter() override = default;

    Status convert(void* dst, const void* src, const TypeInfoAllocator* allocator) const override {
        return Status::InternalError("missing implementation");
    }

    Status convert_datum(TypeInfo* src_typeinfo, const Datum& src, TypeInfo* dst_typeinfo, Datum* dst,
                         const TypeInfoAllocator* allocator) const override {
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
    ~DateToTimestampTypeConverter() override = default;

    Status convert(void* dst, const void* src, const TypeInfoAllocator* allocator) const override {
        uint32_t src_value = unaligned_load<uint24_t>(src);
        int day = implicit_cast<int>(src_value & 31u);
        int month = implicit_cast<int>((src_value >> 5u) & 15u);
        int year = implicit_cast<int>(src_value >> 9u);
        unaligned_store<TimestampValue>(dst, TimestampValue::create(year, month, day, 0, 0, 0));
        return Status::OK();
    }

    Status convert_datum(TypeInfo* src_typeinfo, const Datum& src, TypeInfo* dst_typeinfo, Datum* dst,
                         const TypeInfoAllocator* allocator) const override {
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
    ~DateV2ToTimestampTypeConverter() override = default;

    Status convert(void* dst, const void* src, const TypeInfoAllocator* allocator) const override {
        auto src_value = unaligned_load<DateValue>(src);
        int year = 0;
        int month = 0;
        int day = 0;
        src_value.to_date(&year, &month, &day);
        unaligned_store<TimestampValue>(dst, TimestampValue::create(year, month, day, 0, 0, 0));
        return Status::OK();
    }

    Status convert_datum(TypeInfo* src_typeinfo, const Datum& src, TypeInfo* dst_typeinfo, Datum* dst,
                         const TypeInfoAllocator* allocator) const override {
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
    ~DatetimeToTimestampTypeConverter() override = default;

    Status convert(void* dst, const void* src, const TypeInfoAllocator* allocator) const override {
        return Status::InternalError("missing implementation");
    }

    Status convert_datum(TypeInfo* src_typeinfo, const Datum& src, TypeInfo* dst_typeinfo, Datum* dst,
                         const TypeInfoAllocator* allocator) const override {
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
    ~FloatToDoubleTypeConverter() override = default;

    Status convert(void* dst, const void* src, const TypeInfoAllocator* allocator) const override {
        return Status::InternalError("missing implementation");
    }

    Status convert_datum(TypeInfo* src_typeinfo, const Datum& src, TypeInfo* dst_typeinfo, Datum* dst,
                         const TypeInfoAllocator* allocator) const override {
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
    ~DecimalToDecimal12TypeConverter() override = default;

    Status convert(void* dst, const void* src, const TypeInfoAllocator* allocator) const override {
        return Status::InternalError("missing implementation");
    }

    Status convert_datum(TypeInfo* src_typeinfo, const Datum& src, TypeInfo* dst_typeinfo, Datum* dst,
                         const TypeInfoAllocator* allocator) const override {
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
    ~Decimal12ToDecimalTypeConverter() override = default;

    Status convert(void* dst, const void* src, const TypeInfoAllocator* allocator) const override {
        return Status::InternalError("missing implementation");
    }

    Status convert_datum(TypeInfo* src_typeinfo, const Datum& src, TypeInfo* dst_typeinfo, Datum* dst,
                         const TypeInfoAllocator* allocator) const override {
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
    using CppType = StorageCppType<int_type>;

    IntegerToDateV2TypeConverter() = default;
    ~IntegerToDateV2TypeConverter() override = default;

    Status convert(void* dst, const void* src, const TypeInfoAllocator* allocator) const override {
        auto src_value = unaligned_load<CppType>(src);
        DateValue dst_val;
        if (dst_val.from_date_literal_with_check(src_value)) {
            unaligned_store<DateValue>(dst, dst_val);
            return Status::OK();
        }
        return Status::InvalidArgument(Substitute("Can not convert $0 to Date", src_value));
    }

    Status convert_datum(TypeInfo* src_typeinfo, const Datum& src, TypeInfo* dst_typeinfo, Datum* dst,
                         const TypeInfoAllocator* allocator) const override {
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
    using SrcCppType = StorageCppType<SrcType>;
    using DstCppType = StorageCppType<DstType>;

    DecimalTypeConverter() = default;
    ~DecimalTypeConverter() override = default;

    Status convert(void* dst, const void* src, const TypeInfoAllocator* allocator) const override {
        return Status::InternalError("missing implementation");
    }

    Status convert_datum(TypeInfo* src_typeinfo, const Datum& src, TypeInfo* dst_typeinfo, Datum* dst,
                         const TypeInfoAllocator* allocator) const override {
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
    using SrcCppType = StorageCppType<SrcType>;
    using DstCppType = StorageCppType<DstType>;

    DecimalV3TypeConverter() = default;
    ~DecimalV3TypeConverter() override = default;

    Status convert(void* dst, const void* src, const TypeInfoAllocator* allocator) const override {
        return Status::InternalError("missing implementation");
    }

    Status convert_datum(TypeInfo* src_typeinfo, const Datum& src, TypeInfo* dst_typeinfo, Datum* dst,
                         const TypeInfoAllocator* allocator) const override {
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
    using CppType = StorageCppType<Type>;

    StringToOtherTypeConverter() = default;
    ~StringToOtherTypeConverter() override = default;

    Status convert(void* dst, const void* src, const TypeInfoAllocator* allocator) const override {
        return Status::InternalError("missing implementation");
    }

    Status convert_datum(TypeInfo* src_typeinfo, const Datum& src, TypeInfo* dst_typeinfo, Datum* dst,
                         const TypeInfoAllocator* allocator) const override {
        if (src.is_null()) {
            dst->set_null();
            return Status::OK();
        }
        std::string source = src.get_slice().to_string();

        CppType value;
        RETURN_IF_ERROR(dst_typeinfo->from_string(&value, source));
        dst->set(value);
        return Status::OK();
    }
};

// Convert string to json
// JSON needs dynamic memory allocation, which could not fit in TypeInfo::from_string
template <>
class StringToOtherTypeConverter<TYPE_JSON> final : public TypeConverter {
public:
    using CppType = StorageCppType<TYPE_JSON>;

    StringToOtherTypeConverter() = default;
    ~StringToOtherTypeConverter() override = default;

    Status convert(void* dst, const void* src, const TypeInfoAllocator* allocator) const override {
        return Status::InternalError("not supported");
    }

    Status convert_column(TypeInfo* src_type, const Column& src, TypeInfo* dst_type, Column* dst,
                          const TypeInfoAllocator* allocator) const override {
        for (size_t i = 0; i < src.size(); i++) {
            Datum src_datum = src.get(i);
            if (src_datum.is_null()) {
                dst->append_nulls(1);
            } else {
                Slice source = src_datum.get_slice();
                JsonValue json;
                RETURN_IF_ERROR(JsonValue::parse(source, &json));
                dst->append_datum(&json);
            }
        }
        return Status::OK();
    }

    Status convert_datum(TypeInfo* src_typeinfo, const Datum& src, TypeInfo* dst_typeinfo, Datum* dst,
                         const TypeInfoAllocator* allocator) const override {
        CHECK(false) << "unreachable";
        return Status::NotSupported("");
    }
};

template <LogicalType Type>
class OtherToStringTypeConverter final : public TypeConverter {
public:
    using CppType = StorageCppType<Type>;

    OtherToStringTypeConverter() = default;
    ~OtherToStringTypeConverter() override = default;

    Status convert(void* dst, const void* src, const TypeInfoAllocator* allocator) const override {
        return Status::InternalError("missing implementation");
    }

    Status convert_datum(TypeInfo* src_typeinfo, const Datum& src, TypeInfo* dst_typeinfo, Datum* dst,
                         const TypeInfoAllocator* allocator) const override {
        if (src.is_null()) {
            dst->set_null();
            return Status::OK();
        }
        auto value = src.get<CppType>();
        std::string source = src_typeinfo->to_string(&value);
        Slice slice;
        slice.size = source.size();
        if (allocator == nullptr) {
            LOG(WARNING) << "no guarantee of memory security";
            slice.data = (char*)source.data();
        } else {
            slice.data = reinterpret_cast<char*>(allocator->allocate(slice.size));
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
    using CppType = StorageCppType<TYPE_JSON>;

    OtherToStringTypeConverter() = default;
    ~OtherToStringTypeConverter() override = default;

    Status convert(void* dst, const void* src, const TypeInfoAllocator* allocator) const override {
        return Status::InternalError("not supported");
    }

    Status convert_column(TypeInfo* src_type, const Column& src, TypeInfo* dst_type, Column* dst,
                          const TypeInfoAllocator* allocator) const override {
        for (size_t i = 0; i < src.size(); i++) {
            Datum src_datum = src.get(i);
            if (src_datum.is_null()) {
                dst->append_nulls(1);
            } else {
                const JsonValue* json = src_datum.get_json();
                std::string json_str = json->to_string_uncheck();
                Slice dst_slice = json_str;
                RETURN_IF_UNLIKELY_NULL(allocator, Status::MemoryAllocFailed("missing type info allocator"));
                dst_slice.data = reinterpret_cast<char*>(allocator->allocate(dst_slice.size));
                RETURN_IF_UNLIKELY_NULL(dst_slice.data, Status::MemoryAllocFailed("type info allocator exceeded"));
                memcpy(dst_slice.data, json_str.data(), dst_slice.size);
                dst->append_datum(Datum(dst_slice));
            }
        }
        return Status::OK();
    }

    Status convert_datum(TypeInfo* src_typeinfo, const Datum& src_datum, TypeInfo* dst_typeinfo, Datum* dst,
                         const TypeInfoAllocator* allocator) const override {
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

} // namespace starrocks
