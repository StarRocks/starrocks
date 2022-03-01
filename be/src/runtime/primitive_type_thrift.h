// This file is made available under Elastic License 2.0.

#pragma once

#include "gen_cpp/Exprs_types.h"
#include "gen_cpp/Types_types.h"
#include "runtime/primitive_type.h"
#include "runtime/primitive_type_infra.h"


// Convert between primitive type and thrift type

namespace starrocks {
    
inline TExprOpcode::type to_in_opcode(PrimitiveType t) {
    return TExprOpcode::FILTER_IN;
}

inline TColumnType to_tcolumn_type_thrift(TPrimitiveType::type ttype) {
    TColumnType t;
    t.__set_type(ttype);
    return t;
}

inline PrimitiveType thrift_to_type(TPrimitiveType::type ttype) {
    switch (ttype) {
    // TODO(mofei) rename these two type
    case TPrimitiveType::INVALID_TYPE:
        return INVALID_TYPE;
    case TPrimitiveType::NULL_TYPE:
        return TYPE_NULL;
#define M(ttype)                \
    case TPrimitiveType::ttype: \
        return TYPE_##ttype;
        APPLY_FOR_SCALAR_THRIFT_TYPE(M)
#undef M
    }

    return INVALID_TYPE;
}

inline TPrimitiveType::type to_thrift(PrimitiveType ptype) {
    switch (ptype) {
    // TODO(mofei) rename these two type
    case INVALID_TYPE:
        return TPrimitiveType::INVALID_TYPE;
    case TYPE_NULL:
        return TPrimitiveType::NULL_TYPE;

#define M(thrift_name)       \
    case TYPE_##thrift_name: \
        return TPrimitiveType::thrift_name;
        APPLY_FOR_SCALAR_THRIFT_TYPE(M)
#undef M

    case TYPE_ARRAY:
    case TYPE_MAP:
    case TYPE_STRUCT:
        return TPrimitiveType::INVALID_TYPE;
    }
    return TPrimitiveType::INVALID_TYPE;
}

    
} // namespace starrocks