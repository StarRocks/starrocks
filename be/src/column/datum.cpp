// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#include "column/datum.h"

#include "gutil/strings/substitute.h"
#include "runtime/mem_pool.h"
#include "storage/types.h"

namespace starrocks::vectorized {

using strings::Substitute;

template <FieldType type>
Status datum_from_string(Datum* dst, const std::string& str) {
    static TypeInfoPtr type_info = get_type_info(type);
    typename CppTypeTraits<type>::CppType value;
    if (type_info->from_string(&value, str) != OLAP_SUCCESS) {
        return Status::InvalidArgument(Substitute("Failed to convert $0 to type $1", str, type));
    }
    dst->set(value);
    return Status::OK();
}

Status datum_from_string(Datum* dst, FieldType type, const std::string& str, MemPool* mem_pool) {
    switch (type) {
    case OLAP_FIELD_TYPE_BOOL: {
        static TypeInfoPtr type_info = get_type_info(OLAP_FIELD_TYPE_BOOL);
        bool v;
        auto st = type_info->from_string(&v, str);
        if (st != OLAP_SUCCESS) {
            return Status::InvalidArgument(Substitute("Failed to conert $0 to Bool type", str));
        }
        dst->set_int8(v);
        return Status::OK();
    }
    case OLAP_FIELD_TYPE_TINYINT:
        return datum_from_string<OLAP_FIELD_TYPE_TINYINT>(dst, str);
    case OLAP_FIELD_TYPE_SMALLINT:
        return datum_from_string<OLAP_FIELD_TYPE_SMALLINT>(dst, str);
    case OLAP_FIELD_TYPE_INT:
        return datum_from_string<OLAP_FIELD_TYPE_INT>(dst, str);
    case OLAP_FIELD_TYPE_BIGINT:
        return datum_from_string<OLAP_FIELD_TYPE_BIGINT>(dst, str);
    case OLAP_FIELD_TYPE_LARGEINT:
        return datum_from_string<OLAP_FIELD_TYPE_LARGEINT>(dst, str);
    case OLAP_FIELD_TYPE_FLOAT:
        return datum_from_string<OLAP_FIELD_TYPE_FLOAT>(dst, str);
    case OLAP_FIELD_TYPE_DOUBLE:
        return datum_from_string<OLAP_FIELD_TYPE_DOUBLE>(dst, str);
    case OLAP_FIELD_TYPE_DATE:
        return datum_from_string<OLAP_FIELD_TYPE_DATE>(dst, str);
    case OLAP_FIELD_TYPE_DATE_V2:
        return datum_from_string<OLAP_FIELD_TYPE_DATE_V2>(dst, str);
    case OLAP_FIELD_TYPE_DATETIME:
        return datum_from_string<OLAP_FIELD_TYPE_DATETIME>(dst, str);
    case OLAP_FIELD_TYPE_TIMESTAMP:
        return datum_from_string<OLAP_FIELD_TYPE_TIMESTAMP>(dst, str);
    case OLAP_FIELD_TYPE_DECIMAL:
        return datum_from_string<OLAP_FIELD_TYPE_DECIMAL>(dst, str);
    case OLAP_FIELD_TYPE_DECIMAL_V2:
        return datum_from_string<OLAP_FIELD_TYPE_DECIMAL_V2>(dst, str);
    case OLAP_FIELD_TYPE_CHAR:
    case OLAP_FIELD_TYPE_VARCHAR: {
        /* Type need memory allocated */
        Slice slice;
        slice.size = str.size();
        if (mem_pool == nullptr) {
            slice.data = (char*)str.data();
        } else {
            slice.data = reinterpret_cast<char*>(mem_pool->allocate(slice.size));
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

template <typename T, FieldType type>
std::string datum_to_string(const T& datum) {
    using CppType = typename CppTypeTraits<type>::CppType;
    static TypeInfoPtr type_info = get_type_info(type);
    auto value = datum.template get<CppType>();
    return type_info->to_string(&value);
}

std::string datum_to_string(const Datum& datum, FieldType type) {
    if (datum.is_null()) {
        return "null";
    }
    switch (type) {
    case OLAP_FIELD_TYPE_BOOL:
    case OLAP_FIELD_TYPE_TINYINT:
        return datum_to_string<Datum, OLAP_FIELD_TYPE_TINYINT>(datum);
    case OLAP_FIELD_TYPE_SMALLINT:
        return datum_to_string<Datum, OLAP_FIELD_TYPE_SMALLINT>(datum);
    case OLAP_FIELD_TYPE_INT:
        return datum_to_string<Datum, OLAP_FIELD_TYPE_INT>(datum);
    case OLAP_FIELD_TYPE_BIGINT:
        return datum_to_string<Datum, OLAP_FIELD_TYPE_BIGINT>(datum);
    case OLAP_FIELD_TYPE_LARGEINT:
        return datum_to_string<Datum, OLAP_FIELD_TYPE_LARGEINT>(datum);
    case OLAP_FIELD_TYPE_FLOAT:
        return datum_to_string<Datum, OLAP_FIELD_TYPE_FLOAT>(datum);
    case OLAP_FIELD_TYPE_DOUBLE:
        return datum_to_string<Datum, OLAP_FIELD_TYPE_DOUBLE>(datum);
    case OLAP_FIELD_TYPE_DATE:
        return datum_to_string<Datum, OLAP_FIELD_TYPE_DATE>(datum);
    case OLAP_FIELD_TYPE_DATE_V2:
        return datum_to_string<Datum, OLAP_FIELD_TYPE_DATE_V2>(datum);
    case OLAP_FIELD_TYPE_DATETIME:
        return datum_to_string<Datum, OLAP_FIELD_TYPE_DATETIME>(datum);
    case OLAP_FIELD_TYPE_TIMESTAMP:
        return datum_to_string<Datum, OLAP_FIELD_TYPE_TIMESTAMP>(datum);
    case OLAP_FIELD_TYPE_DECIMAL:
        return datum_to_string<Datum, OLAP_FIELD_TYPE_DECIMAL>(datum);
    case OLAP_FIELD_TYPE_DECIMAL_V2:
        return datum_to_string<Datum, OLAP_FIELD_TYPE_DECIMAL_V2>(datum);
    case OLAP_FIELD_TYPE_CHAR:
    case OLAP_FIELD_TYPE_VARCHAR:
        return datum_to_string<Datum, OLAP_FIELD_TYPE_VARCHAR>(datum);
    default:
        return "";
    }
}

} // namespace starrocks::vectorized
