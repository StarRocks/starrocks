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

#include "column/datum_convert.h"

#include <cstring>

#include "gutil/strings/substitute.h"
#include "types/logical_type.h"
#include "types/storage_type_traits.h"

namespace starrocks {

using strings::Substitute;

template <LogicalType TYPE>
Status datum_from_string(TypeInfo* type_info, Datum* dst, const std::string& str) {
    StorageCppType<TYPE> value;
    RETURN_IF_ERROR(type_info->from_string(&value, str));
    dst->set(value);
    return Status::OK();
}

Status datum_from_string(TypeInfo* type_info, Datum* dst, const std::string& str,
                         const TypeInfoAllocator* type_info_allocator) {
    const auto type = type_info->type();
    switch (type) {
    case TYPE_TINYINT:
        return datum_from_string<TYPE_TINYINT>(type_info, dst, str);
    case TYPE_SMALLINT:
        return datum_from_string<TYPE_SMALLINT>(type_info, dst, str);
    case TYPE_BIGINT:
        return datum_from_string<TYPE_BIGINT>(type_info, dst, str);
    case TYPE_LARGEINT:
        return datum_from_string<TYPE_LARGEINT>(type_info, dst, str);
    case TYPE_INT:
        return datum_from_string<TYPE_INT>(type_info, dst, str);
    case TYPE_INT256:
        return datum_from_string<TYPE_INT256>(type_info, dst, str);
    case TYPE_DATE_V1:
        return datum_from_string<TYPE_DATE_V1>(type_info, dst, str);
    case TYPE_DATE:
        return datum_from_string<TYPE_DATE>(type_info, dst, str);
    case TYPE_DATETIME_V1:
        return datum_from_string<TYPE_DATETIME_V1>(type_info, dst, str);
    case TYPE_DATETIME:
        return datum_from_string<TYPE_DATETIME>(type_info, dst, str);
    case TYPE_DECIMAL:
        return datum_from_string<TYPE_DECIMAL>(type_info, dst, str);
    case TYPE_DECIMALV2:
        return datum_from_string<TYPE_DECIMALV2>(type_info, dst, str);
    case TYPE_DECIMAL32:
        return datum_from_string<TYPE_DECIMAL32>(type_info, dst, str);
    case TYPE_DECIMAL64:
        return datum_from_string<TYPE_DECIMAL64>(type_info, dst, str);
    case TYPE_DECIMAL128:
        return datum_from_string<TYPE_DECIMAL128>(type_info, dst, str);
    case TYPE_DECIMAL256:
        return datum_from_string<TYPE_DECIMAL256>(type_info, dst, str);
    case TYPE_FLOAT:
        return datum_from_string<TYPE_FLOAT>(type_info, dst, str);
    case TYPE_DOUBLE:
        return datum_from_string<TYPE_DOUBLE>(type_info, dst, str);
    case TYPE_BOOLEAN: {
        bool v;
        RETURN_IF_ERROR(type_info->from_string(&v, str));
        dst->set_int8(v);
        return Status::OK();
    }
        /* Type need memory allocated */
    case TYPE_VARBINARY:
    case TYPE_CHAR:
    case TYPE_VARCHAR: {
        /* Type need memory allocated */
        Slice slice;
        slice.size = str.size();
        if (type_info_allocator == nullptr) {
            slice.data = (char*)str.data();
        } else {
            slice.data = reinterpret_cast<char*>(type_info_allocator->allocate(slice.size));
            RETURN_IF_UNLIKELY_NULL(slice.data, Status::MemoryAllocFailed("alloc mem for varchar field failed"));
            memcpy(slice.data, str.data(), slice.size);
        }
        // If type is TYPE_CHAR, strip its tailing '\0'
        if (type == TYPE_CHAR) {
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

template <LogicalType TYPE>
std::string datum_to_string(TypeInfo* type_info, const Datum& datum) {
    using CppType = StorageCppType<TYPE>;
    auto value = datum.get<CppType>();
    return type_info->to_string(&value);
}

std::string datum_to_string(TypeInfo* type_info, const Datum& datum) {
    if (datum.is_null()) {
        return "null";
    }
    const auto type = type_info->type();
    switch (type) {
    case TYPE_BOOLEAN:
        return datum_to_string<TYPE_TINYINT>(type_info, datum);
    case TYPE_VARBINARY:
    case TYPE_CHAR:
    case TYPE_VARCHAR:
        return datum_to_string<TYPE_VARCHAR>(type_info, datum);
    case TYPE_TINYINT:
        return datum_to_string<TYPE_TINYINT>(type_info, datum);
    case TYPE_SMALLINT:
        return datum_to_string<TYPE_SMALLINT>(type_info, datum);
    case TYPE_BIGINT:
        return datum_to_string<TYPE_BIGINT>(type_info, datum);
    case TYPE_LARGEINT:
        return datum_to_string<TYPE_LARGEINT>(type_info, datum);
    case TYPE_INT:
        return datum_to_string<TYPE_INT>(type_info, datum);
    case TYPE_INT256:
        return datum_to_string<TYPE_INT256>(type_info, datum);
    case TYPE_DATE_V1:
        return datum_to_string<TYPE_DATE_V1>(type_info, datum);
    case TYPE_DATE:
        return datum_to_string<TYPE_DATE>(type_info, datum);
    case TYPE_DATETIME_V1:
        return datum_to_string<TYPE_DATETIME_V1>(type_info, datum);
    case TYPE_DATETIME:
        return datum_to_string<TYPE_DATETIME>(type_info, datum);
    case TYPE_DECIMAL:
        return datum_to_string<TYPE_DECIMAL>(type_info, datum);
    case TYPE_DECIMALV2:
        return datum_to_string<TYPE_DECIMALV2>(type_info, datum);
    case TYPE_DECIMAL32:
        return datum_to_string<TYPE_DECIMAL32>(type_info, datum);
    case TYPE_DECIMAL64:
        return datum_to_string<TYPE_DECIMAL64>(type_info, datum);
    case TYPE_DECIMAL128:
        return datum_to_string<TYPE_DECIMAL128>(type_info, datum);
    case TYPE_DECIMAL256:
        return datum_to_string<TYPE_DECIMAL256>(type_info, datum);
    case TYPE_FLOAT:
        return datum_to_string<TYPE_FLOAT>(type_info, datum);
    case TYPE_JSON:
        return datum_to_string<TYPE_JSON>(type_info, datum);
    case TYPE_DOUBLE:
        return datum_to_string<TYPE_DOUBLE>(type_info, datum);
    default:
        return "";
    }
}

} // namespace starrocks
