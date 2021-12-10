// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.
#include "column/datum_convert.h"

#include "gutil/strings/substitute.h"
#include "runtime/mem_pool.h"

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
            RETURN_IF_UNLIKELY_NULL(slice.data, Status::MemoryAllocFailed("alloc mem for varchar field failed"));
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

} // namespace starrocks::vectorized
