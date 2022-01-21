// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.
#include "column/datum_convert.h"

#include "exec/vectorized/join_hash_map.h"
#include "gutil/strings/substitute.h"
#include "runtime/mem_pool.h"
#include "storage/olap_common.h"
#include "storage/olap_type_infra.h"

namespace starrocks::vectorized {

using strings::Substitute;

struct DatumFromStringBuilder {
    template <FieldType ftype>
    Status operator()(TypeInfo* type_info, Datum* dst, const std::string& str, MemPool* mem_pool) {
        switch (ftype) {
        case OLAP_FIELD_TYPE_BOOL: {
            bool v;
            RETURN_IF_ERROR(type_info->from_string(&v, str));
            dst->set_int8(v);
            return Status::OK();
        }
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
            if (ftype == OLAP_FIELD_TYPE_CHAR) {
                slice.size = strnlen(slice.data, slice.size);
            }
            dst->set_slice(slice);
            return Status::OK();
        }
        default: {
            typename CppTypeTraits<ftype>::CppType value;
            RETURN_IF_ERROR(type_info->from_string(&value, str));
            dst->set(value);
            return Status::OK();
        }
        }
    }
};

Status datum_from_string(TypeInfo* type_info, Datum* dst, const std::string& str, MemPool* mem_pool) {
    const auto type = type_info->type();
    
    return field_type_dispatch_supported(type, DatumFromStringBuilder(), type_info, dst, str, mem_pool);
}

struct DatumToStringBuilder {
    
    template <FieldType ftype>
    std::string operator()(TypeInfo* type_info, const Datum& datum) {
        using CppType = typename CppTypeTraits<ftype>::CppType;
        auto value = datum.template get<CppType>();
        return type_info->to_string(&value);
    }
};

std::string datum_to_string(TypeInfo* type_info, const Datum& datum) {
    if (datum.is_null()) {
        return "null";
    }
    return field_type_dispatch_supported(type_info->type(), DatumToStringBuilder(), type_info, datum);
}

} // namespace starrocks::vectorized
