// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.
#include "column/datum_convert.h"

#include "gutil/strings/substitute.h"
#include "runtime/decimalv2_value.h"
#include "runtime/mem_pool.h"
#include "storage/types.h"

namespace starrocks::vectorized {

using strings::Substitute;

template <FieldType TYPE>
Status datum_from_string(TypeInfo* type_info, Datum* dst, const std::string& str) {
    typename CppTypeTraits<TYPE>::CppType value;
    if (type_info->from_string(&value, str) != OLAP_SUCCESS) {
        return Status::InvalidArgument(Substitute("Failed to convert $0 to type $1", str, TYPE));
    }
    dst->set(value);
    return Status::OK();
}

Status datum_from_string(TypeInfo* type_info, Datum* dst, const std::string& str, MemPool* mem_pool) {
    const auto type = type_info->type();
    switch (type) {
    case OLAP_FIELD_TYPE_BOOL: {
        bool v;
        auto st = type_info->from_string(&v, str);
        if (st != OLAP_SUCCESS) {
            return Status::InvalidArgument(Substitute("Failed to conert $0 to Bool type", str));
        }
        dst->set_int8(v);
        return Status::OK();
    }
    case OLAP_FIELD_TYPE_TINYINT:
        return datum_from_string<OLAP_FIELD_TYPE_TINYINT>(type_info, dst, str);
    case OLAP_FIELD_TYPE_SMALLINT:
        return datum_from_string<OLAP_FIELD_TYPE_SMALLINT>(type_info, dst, str);
    case OLAP_FIELD_TYPE_INT:
        return datum_from_string<OLAP_FIELD_TYPE_INT>(type_info, dst, str);
    case OLAP_FIELD_TYPE_BIGINT:
        return datum_from_string<OLAP_FIELD_TYPE_BIGINT>(type_info, dst, str);
    case OLAP_FIELD_TYPE_LARGEINT:
        return datum_from_string<OLAP_FIELD_TYPE_LARGEINT>(type_info, dst, str);
    case OLAP_FIELD_TYPE_FLOAT:
        return datum_from_string<OLAP_FIELD_TYPE_FLOAT>(type_info, dst, str);
    case OLAP_FIELD_TYPE_DOUBLE:
        return datum_from_string<OLAP_FIELD_TYPE_DOUBLE>(type_info, dst, str);
    case OLAP_FIELD_TYPE_DATE:
        return datum_from_string<OLAP_FIELD_TYPE_DATE>(type_info, dst, str);
    case OLAP_FIELD_TYPE_DATE_V2:
        return datum_from_string<OLAP_FIELD_TYPE_DATE_V2>(type_info, dst, str);
    case OLAP_FIELD_TYPE_DATETIME:
        return datum_from_string<OLAP_FIELD_TYPE_DATETIME>(type_info, dst, str);
    case OLAP_FIELD_TYPE_TIMESTAMP:
        return datum_from_string<OLAP_FIELD_TYPE_TIMESTAMP>(type_info, dst, str);
    case OLAP_FIELD_TYPE_DECIMAL:
        return datum_from_string<OLAP_FIELD_TYPE_DECIMAL>(type_info, dst, str);
    case OLAP_FIELD_TYPE_DECIMAL_V2:
        return datum_from_string<OLAP_FIELD_TYPE_DECIMAL_V2>(type_info, dst, str);
    case OLAP_FIELD_TYPE_DECIMAL32:
        return datum_from_string<OLAP_FIELD_TYPE_DECIMAL32>(type_info, dst, str);
    case OLAP_FIELD_TYPE_DECIMAL64:
        return datum_from_string<OLAP_FIELD_TYPE_DECIMAL64>(type_info, dst, str);
    case OLAP_FIELD_TYPE_DECIMAL128:
        return datum_from_string<OLAP_FIELD_TYPE_DECIMAL128>(type_info, dst, str);
        /* Type need memory allocated */
    case OLAP_FIELD_TYPE_CHAR:
    case OLAP_FIELD_TYPE_VARCHAR: {
        /* Type need memory allocated */
        Slice slice;
        slice.size = str.size();
        if (mem_pool == nullptr) {
            slice.data = (char*)str.data();
        } else {
            slice.data = reinterpret_cast<char*>(mem_pool->allocate(slice.size));
            if (UNLIKELY(slice.data == nullptr)) {
                return Status::InternalError("Mem usage has exceed the limit of BE");
            }
            memcpy(slice.data, str.data(), slice.size);
        }
        // If type is OLAP_FIELD_TYPE_CHAR, strip its tailing '\0'
        if (type == OLAP_FIELD_TYPE_CHAR) {
            slice.size = strnlen(slice.data, slice.size);
        }
        dst->set_slice(slice);
        break;
    }
    default:
        return Status::NotSupported(Substitute("Type $0 not supported", type));
    }

    return Status::OK();
}

template <FieldType TYPE>
std::string datum_to_string(TypeInfo* type_info, const Datum& datum) {
    using CppType = typename CppTypeTraits<TYPE>::CppType;
    auto value = datum.template get<CppType>();
    return type_info->to_string(&value);
}

std::string datum_to_string(TypeInfo* type_info, const Datum& datum) {
    if (datum.is_null()) {
        return "null";
    }
    const auto type = type_info->type();
    switch (type) {
    case OLAP_FIELD_TYPE_BOOL:
    case OLAP_FIELD_TYPE_TINYINT:
        return datum_to_string<OLAP_FIELD_TYPE_TINYINT>(type_info, datum);
    case OLAP_FIELD_TYPE_SMALLINT:
        return datum_to_string<OLAP_FIELD_TYPE_SMALLINT>(type_info, datum);
    case OLAP_FIELD_TYPE_INT:
        return datum_to_string<OLAP_FIELD_TYPE_INT>(type_info, datum);
    case OLAP_FIELD_TYPE_BIGINT:
        return datum_to_string<OLAP_FIELD_TYPE_BIGINT>(type_info, datum);
    case OLAP_FIELD_TYPE_LARGEINT:
        return datum_to_string<OLAP_FIELD_TYPE_LARGEINT>(type_info, datum);
    case OLAP_FIELD_TYPE_FLOAT:
        return datum_to_string<OLAP_FIELD_TYPE_FLOAT>(type_info, datum);
    case OLAP_FIELD_TYPE_DOUBLE:
        return datum_to_string<OLAP_FIELD_TYPE_DOUBLE>(type_info, datum);
    case OLAP_FIELD_TYPE_DATE:
        return datum_to_string<OLAP_FIELD_TYPE_DATE>(type_info, datum);
    case OLAP_FIELD_TYPE_DATE_V2:
        return datum_to_string<OLAP_FIELD_TYPE_DATE_V2>(type_info, datum);
    case OLAP_FIELD_TYPE_DATETIME:
        return datum_to_string<OLAP_FIELD_TYPE_DATETIME>(type_info, datum);
    case OLAP_FIELD_TYPE_TIMESTAMP:
        return datum_to_string<OLAP_FIELD_TYPE_TIMESTAMP>(type_info, datum);
    case OLAP_FIELD_TYPE_DECIMAL:
        return datum_to_string<OLAP_FIELD_TYPE_DECIMAL>(type_info, datum);
    case OLAP_FIELD_TYPE_DECIMAL_V2:
        return datum_to_string<OLAP_FIELD_TYPE_DECIMAL_V2>(type_info, datum);
    case OLAP_FIELD_TYPE_DECIMAL32:
        return datum_to_string<OLAP_FIELD_TYPE_DECIMAL32>(type_info, datum);
    case OLAP_FIELD_TYPE_DECIMAL64:
        return datum_to_string<OLAP_FIELD_TYPE_DECIMAL64>(type_info, datum);
    case OLAP_FIELD_TYPE_DECIMAL128:
        return datum_to_string<OLAP_FIELD_TYPE_DECIMAL128>(type_info, datum);
    case OLAP_FIELD_TYPE_CHAR:
    case OLAP_FIELD_TYPE_VARCHAR:
        return datum_to_string<OLAP_FIELD_TYPE_VARCHAR>(type_info, datum);
    default:
        return "";
    }
}

Status convert_to_date(FieldType src_type, const Datum& src_datum, FieldType dst_type, Datum& dst_datum) {
    if (src_datum.is_null()) {
        dst_datum.set_null();
        return Status::OK();
    }

    if (src_type == OLAP_FIELD_TYPE_DATETIME) {
        auto src_value = src_datum.get_int64();
        int64_t part1 = (src_value / 1000000L);
        uint24_t year = static_cast<uint24_t>((part1 / 10000L) % 10000);
        uint24_t mon = static_cast<uint24_t>((part1 / 100) % 100);
        uint24_t day = static_cast<uint24_t>(part1 % 100);
        dst_datum.set_uint24((year << 9) + (mon << 5) + day);
        return Status::OK();
    }

    if (src_type == OLAP_FIELD_TYPE_TIMESTAMP) {
        int year, mon, day, hour, minute, second, usec;
        auto src_value = src_datum.get_timestamp();
        src_value.to_timestamp(&year, &mon, &day, &hour, &minute, &second, &usec);
        dst_datum.set_uint24((year << 9) + (mon << 5) + day);
        return Status::OK();
    }

    if (src_type == OLAP_FIELD_TYPE_INT) {
        auto src_value = src_datum.get_int32();
        DateTimeValue dt;
        if (!dt.from_date_int64(src_value)) {
            return Status::InternalError("invalid schema");
        }
        uint24_t year = static_cast<uint24_t>(src_value / 10000);
        uint24_t mon = static_cast<uint24_t>((src_value % 10000) / 100);
        uint24_t day = static_cast<uint24_t>(src_value % 100);
        dst_datum.set_uint24((year << 9) + (mon << 5) + day);
        return Status::OK();
    }

    if (src_type == OLAP_FIELD_TYPE_DATE_V2) {
        auto src_value = src_datum.get_date();
        dst_datum.set_uint24(src_value.to_mysql_date());
        return Status::OK();
    }

    std::stringstream ss;
    ss << "does not support change " << field_type_to_string(src_type) << " to OLAP_FIELD_TYPE_DATE";
    return Status::InternalError(ss.str());
}

Status convert_to_date_v2(FieldType src_type, const Datum& src_datum, FieldType dst_type, Datum& dst_datum) {
    if (src_datum.is_null()) {
        dst_datum.set_null();
        return Status::OK();
    }

    if (src_type == OLAP_FIELD_TYPE_TIMESTAMP) {
        auto src_value = src_datum.get_timestamp();
        dst_datum.set_date(src_value);
        return Status::OK();
    }

    if (src_type == OLAP_FIELD_TYPE_DATETIME) {
        TimestampValue timestamp{0};
        timestamp.from_timestamp_literal(src_datum.get_int64());
        dst_datum.set_date(timestamp);
    }

    if (src_type == OLAP_FIELD_TYPE_INT) {
        auto src_value = src_datum.get_int32();
        DateValue dst_val;
        if (dst_val.from_date_literal_with_check(src_value)) {
            dst_datum.set_date(dst_val);
            return Status::OK();
        } else {
            return Status::InvalidArgument(Substitute("Can not convert $0 to Date", src_value));
        }
    }

    if (src_type == OLAP_FIELD_TYPE_DATE) {
        DateValue tmp;
        tmp.from_mysql_date(src_datum.get_uint24());
        dst_datum.set_date(tmp);
        return Status::OK();
    }

    std::stringstream ss;
    ss << "does not support change " << field_type_to_string(src_type) << " to OLAP_FIELD_TYPE_DATE_V2";
    return Status::InternalError(ss.str());
}

Status convert_to_datetime(FieldType src_type, const Datum& src_datum, FieldType dst_type, Datum& dst_datum) {
    if (src_datum.is_null()) {
        dst_datum.set_null();
        return Status::OK();
    }

    if (src_type == OLAP_FIELD_TYPE_DATE) {
        auto src_value = src_datum.get_uint24();
        int day = static_cast<int>(src_value & 31);
        int mon = static_cast<int>(src_value >> 5 & 15);
        int year = static_cast<int>(src_value >> 9);
        dst_datum.set_int64(static_cast<int64>(year * 10000L + mon * 100L + day) * 1000000);
        return Status::OK();
    }

    if (src_type == OLAP_FIELD_TYPE_DATE_V2) {
        auto src_value = src_datum.get_date();
        int year, mon, day;
        src_value.to_date(&year, &mon, &day);
        dst_datum.set_int64(static_cast<int64>(year * 10000L + mon * 100L + day) * 1000000);
        return Status::OK();
    }

    if (src_type == OLAP_FIELD_TYPE_TIMESTAMP) {
        dst_datum.set_int64(src_datum.get_timestamp().to_timestamp_literal());
    }

    std::stringstream ss;
    ss << "does not support change " << field_type_to_string(src_type) << " to OLAP_FIELD_TYPE_DATETIME";
    return Status::InternalError(ss.str());
}

Status convert_to_timestamp(FieldType src_type, const Datum& src_datum, FieldType dst_type, Datum& dst_datum) {
    if (src_datum.is_null()) {
        dst_datum.set_null();
        return Status::OK();
    }

    if (src_type == OLAP_FIELD_TYPE_DATE) {
        auto src_value = src_datum.get_uint24();
        int day = implicit_cast<int>(src_value & 31u);
        int mon = implicit_cast<int>((src_value >> 5u) & 15u);
        int year = implicit_cast<int>(src_value >> 9u);
        dst_datum.set_timestamp(TimestampValue::create(year, mon, day, 0, 0, 0));
        return Status::OK();
    }

    if (src_type == OLAP_FIELD_TYPE_DATE_V2) {
        auto src_value = src_datum.get_date();
        int year = 0;
        int mon = 0;
        int day = 0;
        src_value.to_date(&year, &mon, &day);
        dst_datum.set_timestamp(TimestampValue::create(year, mon, day, 0, 0, 0));
        return Status::OK();
    }

    if (src_type == OLAP_FIELD_TYPE_DATETIME) {
        TimestampValue timestamp{0};
        timestamp.from_timestamp_literal(src_datum.get_int64());
        dst_datum.set_timestamp(timestamp);
        return Status::OK();
    }

    std::stringstream ss;
    ss << "does not support change " << field_type_to_string(src_type) << " to " << field_type_to_string(dst_type);
    return Status::InternalError(ss.str());
}

Status convert_to_double(FieldType src_type, const Datum& src_datum, FieldType dst_type, Datum& dst_datum) {
    if (src_datum.is_null()) {
        dst_datum.set_null();
        return Status::OK();
    }

    if (src_type == OLAP_FIELD_TYPE_FLOAT) {
        char buf[64] = {0};
        snprintf(buf, 64, "%f", src_datum.get_float());
        char* tg;
        dst_datum.set_double(strtod(buf, &tg));
        return Status::OK();
    }

    std::stringstream ss;
    ss << "does not support change " << field_type_to_string(src_type) << " to " << field_type_to_string(dst_type);
    return Status::InternalError(ss.str());
}

Status convert_to_decimal(FieldType src_type, const Datum& src_datum, FieldType dst_type, Datum& dst_datum) {
    if (src_datum.is_null()) {
        dst_datum.set_null();
        return Status::OK();
    }

    if (src_type == OLAP_FIELD_TYPE_DECIMAL_V2) {
        decimal12_t dst_value(src_datum.get_decimal().int_value(), src_datum.get_decimal().frac_value());
        dst_datum.set_decimal12(dst_value);
        return Status::OK();
    }

    if (src_type == OLAP_FIELD_TYPE_DECIMAL128) {
        const auto& src_value = src_datum.get_decimal();
        decimal12_t tmp;
        tmp.integer = src_value.int_value();
        tmp.fraction = src_value.frac_value();
        dst_datum.set_decimal12(tmp);
        return Status::OK();
    }

    std::stringstream ss;
    ss << "does not support change " << field_type_to_string(src_type) << " to " << field_type_to_string(dst_type);
    return Status::InternalError(ss.str());
}

Status convert_to_decimal_v2(FieldType src_type, const Datum& src_datum, FieldType dst_type, Datum& dst_datum) {
    if (src_datum.is_null()) {
        dst_datum.set_null();
        return Status::OK();
    }

    if (src_type == OLAP_FIELD_TYPE_DECIMAL) {
        DecimalV2Value dst_value;
        dst_value.from_olap_decimal(src_datum.get_decimal12().integer, src_datum.get_decimal12().fraction);
        dst_datum.set_decimal(dst_value);
        return Status::OK();
    }

    if (src_type == OLAP_FIELD_TYPE_DECIMAL128) {
        auto src_value = src_datum.get_int128();
        DecimalV2Value dst_value;
        strings::memcpy_inlined(&dst_value, &src_value, sizeof(int128_t));
        dst_datum.set_decimal(dst_value);
    }

    std::stringstream ss;
    ss << "does not support change " << field_type_to_string(src_type) << " to " << field_type_to_string(dst_type);
    return Status::InternalError(ss.str());
}

Status convert_to_decimal128(FieldType src_type, const Datum& src_datum, FieldType dst_type, Datum& dst_datum) {
    if (src_datum.is_null()) {
        dst_datum.set_null();
        return Status::OK();
    }

    if (src_type == OLAP_FIELD_TYPE_DECIMAL) {
        auto src_value = src_datum.get_decimal12();
        DecimalV2Value dst_value;
        dst_value.from_olap_decimal(src_value.integer, src_value.fraction);
        dst_datum.set_int128(dst_value);
        return Status::OK();
    }

    if (src_type == OLAP_FIELD_TYPE_DECIMAL_V2) {
        auto src_value = src_datum.get_decimal();
        int128_t dst_value;
        strings::memcpy_inlined(&dst_value, &src_value, sizeof(int128_t));
        dst_datum.set_int128(dst_value);
        return Status::OK();
    }

    std::stringstream ss;
    ss << "does not support change " << field_type_to_string(src_type) << " to " << field_type_to_string(dst_type);
    return Status::InternalError(ss.str());
}

Status convert_datum(TypeInfo* src_type_info, const Datum& src_datum, TypeInfo* dst_type_info, Datum& dst_datum) {
    const auto src_type = src_type_info->type();
    const auto dst_type = dst_type_info->type();
    if (src_type == OLAP_FIELD_TYPE_VARCHAR) {
        std::string source = src_datum.get_slice().to_string();
        return datum_from_string(dst_type_info, &dst_datum, source, nullptr);
    }

    if (dst_type == OLAP_FIELD_TYPE_VARCHAR) {
        std::string source = datum_to_string(src_type_info, src_datum);
        if (source == "") {
            std::stringstream ss;
            ss << "does not support change " << field_type_to_string(src_type) << " to VARCHAR";
            return Status::InternalError(ss.str());
        }
        Slice slice(source.c_str(), source.length());
        dst_datum.set_slice(slice);
        return Status::OK();
    }

    if (dst_type == OLAP_FIELD_TYPE_DATE) {
        return convert_to_date(src_type, src_datum, dst_type, dst_datum);
    }

    if (dst_type == OLAP_FIELD_TYPE_DATE_V2) {
        return convert_to_date_v2(src_type, src_datum, dst_type, dst_datum);
    }

    if (dst_type == OLAP_FIELD_TYPE_DATETIME) {
        return convert_to_datetime(src_type, src_datum, dst_type, dst_datum);
    }

    if (dst_type == OLAP_FIELD_TYPE_TIMESTAMP) {
        return convert_to_timestamp(src_type, src_datum, dst_type, dst_datum);
    }

    if (dst_type == OLAP_FIELD_TYPE_DOUBLE) {
        return convert_to_double(src_type, src_datum, dst_type, dst_datum);
    }

    if (dst_type == OLAP_FIELD_TYPE_DECIMAL) {
        return convert_to_decimal(src_type, src_datum, dst_type, dst_datum);
    }

    if (dst_type == OLAP_FIELD_TYPE_DECIMAL_V2) {
        return convert_to_decimal_v2(src_type, src_datum, dst_type, dst_datum);
    }

    if (dst_type == OLAP_FIELD_TYPE_DECIMAL128) {
        return convert_to_decimal128(src_type, src_datum, dst_type, dst_datum);
    }

    return Status::InternalError("failed");
}

} // namespace starrocks::vectorized
