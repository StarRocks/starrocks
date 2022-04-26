// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/be/src/runtime/types.h

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#ifndef STARROCKS_BE_RUNTIME_TYPES_H
#define STARROCKS_BE_RUNTIME_TYPES_H

#include <string>
#include <vector>

#include "common/config.h"
#include "gen_cpp/Types_types.h" // for TPrimitiveType
#include "gen_cpp/types.pb.h"    // for PTypeDesc
#include "runtime/primitive_type.h"
#include "storage/hll.h"
#include "thrift/protocol/TDebugProtocol.h"
#include "util/json.h"

namespace starrocks {

class TypeInfo;

// Describes a type. Includes the enum, children types, and any type-specific metadata
// (e.g. precision and scale for decimals).
struct TypeDescriptor {
    PrimitiveType type{INVALID_TYPE};
    /// Only meaningful for type TYPE_CHAR/TYPE_VARCHAR/TYPE_HLL
    int len{-1};
    static constexpr int MAX_VARCHAR_LENGTH = 1048576;
    static constexpr int MAX_CHAR_LENGTH = 255;
    static constexpr int MAX_CHAR_INLINE_LENGTH = 128;
    static constexpr int DEFAULT_BITMAP_LENGTH = 128;

    /// Only set if type == TYPE_DECIMAL
    int precision{-1};
    int scale{-1};

    /// Must be kept in sync with FE's max precision/scale.
    static const int MAX_PRECISION = 38;
    static const int MAX_SCALE = MAX_PRECISION;

    /// The maximum precision representable by a 4-byte decimal (Decimal4Value)
    static const int MAX_DECIMAL4_PRECISION = 9;
    /// The maximum precision representable by a 8-byte decimal (Decimal8Value)
    static const int MAX_DECIMAL8_PRECISION = 18;

    /// Empty for scalar types
    std::vector<TypeDescriptor> children;

    /// Only set if type == TYPE_STRUCT. The field name of each child.
    std::vector<std::string> field_names;

    TypeDescriptor() {}

    explicit TypeDescriptor(PrimitiveType type) : type(type), len(-1), precision(-1), scale(-1) {}

    static TypeDescriptor create_char_type(int len) {
        DCHECK_GE(len, 1);
        DCHECK_LE(len, MAX_CHAR_LENGTH);
        TypeDescriptor ret;
        ret.type = TYPE_CHAR;
        ret.len = len;
        return ret;
    }

    static TypeDescriptor create_varchar_type(int len) {
        DCHECK_GE(len, 1);
        DCHECK_LE(len, MAX_VARCHAR_LENGTH);
        TypeDescriptor ret;
        ret.type = TYPE_VARCHAR;
        ret.len = len;
        return ret;
    }

    static TypeDescriptor create_json_type() {
        TypeDescriptor res;
        res.type = TYPE_JSON;
        res.len = kJsonDefaultSize;
        return res;
    }

    static TypeDescriptor create_hll_type() {
        TypeDescriptor ret;
        ret.type = TYPE_HLL;
        ret.len = HLL_COLUMN_DEFAULT_LEN;
        return ret;
    }

    static TypeDescriptor create_decimal_type(int precision, int scale) {
        DCHECK_LE(precision, MAX_PRECISION);
        DCHECK_LE(scale, MAX_SCALE);
        DCHECK_GE(precision, 0);
        DCHECK_LE(scale, precision);
        TypeDescriptor ret;
        ret.type = TYPE_DECIMAL;
        ret.precision = precision;
        ret.scale = scale;
        return ret;
    }

    static TypeDescriptor create_decimalv2_type(int precision, int scale) {
        DCHECK_LE(precision, MAX_PRECISION);
        DCHECK_LE(scale, MAX_SCALE);
        DCHECK_GE(precision, 0);
        DCHECK_LE(scale, precision);
        TypeDescriptor ret;
        ret.type = TYPE_DECIMALV2;
        ret.precision = precision;
        ret.scale = scale;
        return ret;
    }

    static TypeDescriptor create_decimalv3_type(PrimitiveType type, int precision, int scale) {
        DCHECK(type == TYPE_DECIMAL32 || type == TYPE_DECIMAL64 || type == TYPE_DECIMAL128);
        DCHECK_LE(precision, MAX_PRECISION);
        DCHECK_LE(scale, MAX_SCALE);
        DCHECK_GE(precision, 0);
        DCHECK_LE(scale, precision);
        TypeDescriptor ret;
        ret.type = type;
        ret.precision = precision;
        ret.scale = scale;
        return ret;
    }

    static TypeDescriptor create_bitmap_type() {
        TypeDescriptor ret(TYPE_OBJECT);
        ret.len = DEFAULT_BITMAP_LENGTH;
        return ret;
    }

    static TypeDescriptor from_primtive_type(PrimitiveType type,
                                             [[maybe_unused]] int len = TypeDescriptor::MAX_VARCHAR_LENGTH,
                                             [[maybe_unused]] int precision = 27, [[maybe_unused]] int scale = 9) {
        switch (type) {
        case TYPE_CHAR:
            return TypeDescriptor::create_char_type(MAX_CHAR_LENGTH);
        case TYPE_VARCHAR:
            return TypeDescriptor::create_varchar_type(MAX_VARCHAR_LENGTH);
        case TYPE_HLL:
            return TypeDescriptor::create_hll_type();
        case TYPE_DECIMAL:
            return TypeDescriptor::create_decimal_type(precision, scale);
        case TYPE_DECIMALV2:
            return TypeDescriptor::create_decimalv2_type(precision, scale);
        case TYPE_DECIMAL32:
            return TypeDescriptor::create_decimalv3_type(TYPE_DECIMAL32, precision, scale);
        case TYPE_DECIMAL64:
            return TypeDescriptor::create_decimalv3_type(TYPE_DECIMAL64, precision, scale);
        case TYPE_DECIMAL128:
            return TypeDescriptor::create_decimalv3_type(TYPE_DECIMAL128, precision, scale);
        case TYPE_JSON:
            return TypeDescriptor::create_json_type();
        case TYPE_OBJECT:
            return TypeDescriptor::create_bitmap_type();
        default:
            return TypeDescriptor(type);
        }
    }
    static TypeDescriptor from_thrift(const TTypeDesc& t) {
        int idx = 0;
        TypeDescriptor result(t.types, &idx);
        DCHECK_EQ(idx, t.types.size());
        return result;
    }

    static TypeDescriptor from_storage_type_info(TypeInfo* type_info);

    static TypeDescriptor from_protobuf(const PTypeDesc& ptype) {
        int idx = 0;
        TypeDescriptor result(ptype.types(), &idx);
        DCHECK_EQ(idx, ptype.types_size());
        return result;
    }

    bool is_assignable(const TypeDescriptor& o) const {
        if (is_complex_type()) {
            if ((type != o.type) || (children.size() != o.children.size())) {
                return false;
            }
            for (int i = 0; i < children.size(); i++) {
                if (!children[i].is_assignable(o.children[i])) {
                    return false;
                }
            }
            return true;
        }
        if (is_decimal_type()) {
            return type == o.type && scale == o.scale;
        } else {
            return type == o.type;
        }
    }

    bool is_implicit_castable(const TypeDescriptor& from) const {
        if (is_decimal_type()) {
            return precision == from.precision && scale == from.scale;
        }
        return false;
    }

    bool operator==(const TypeDescriptor& o) const {
        if (type != o.type) {
            return false;
        }
        if (children != o.children) {
            return false;
        }
        if (type == TYPE_CHAR) {
            return len == o.len;
        }
        if (is_decimal_type()) {
            return precision == o.precision && scale == o.scale;
        }
        return true;
    }

    bool operator!=(const TypeDescriptor& other) const { return !(*this == other); }

    TTypeDesc to_thrift() const {
        TTypeDesc thrift_type;
        to_thrift(&thrift_type);
        return thrift_type;
    }

    PTypeDesc to_protobuf() const {
        PTypeDesc proto_type;
        to_protobuf(&proto_type);
        return proto_type;
    }

    inline bool is_string_type() const {
        return type == TYPE_VARCHAR || type == TYPE_CHAR || type == TYPE_HLL || type == TYPE_OBJECT ||
               type == TYPE_PERCENTILE;
    }

    inline bool is_date_type() const { return type == TYPE_DATE || type == TYPE_DATETIME; }

    inline bool is_decimalv3_type() const {
        return (type == TYPE_DECIMAL32 || type == TYPE_DECIMAL64 || type == TYPE_DECIMAL128);
    }

    inline bool is_decimal_type() const {
        return (type == TYPE_DECIMAL || type == TYPE_DECIMALV2 || is_decimalv3_type());
    }

    inline bool is_var_len_string_type() const {
        return type == TYPE_VARCHAR || type == TYPE_HLL || type == TYPE_CHAR || type == TYPE_OBJECT ||
               type == TYPE_PERCENTILE;
    }

    inline bool is_complex_type() const { return type == TYPE_STRUCT || type == TYPE_ARRAY || type == TYPE_MAP; }

    inline bool is_collection_type() const { return type == TYPE_ARRAY || type == TYPE_MAP; }

    // For some types with potential huge length, whose memory consumption is far more than normal types,
    // they need a different chunk_size setting
    bool is_huge_type() const { return len >= 128; }

    /// Returns the size of a slot for this type.
    inline int get_slot_size() const {
        switch (type) {
        case TYPE_CHAR:
        case TYPE_VARCHAR:
        case TYPE_HLL:
        case TYPE_OBJECT:
        case TYPE_PERCENTILE:
        case TYPE_JSON:
            return sizeof(StringValue);

        case TYPE_NULL:
        case TYPE_BOOLEAN:
        case TYPE_TINYINT:
            return 1;

        case TYPE_SMALLINT:
            return 2;

        case TYPE_INT:
        case TYPE_FLOAT:
        case TYPE_DECIMAL32:
            return 4;

        case TYPE_BIGINT:
        case TYPE_DOUBLE:
        case TYPE_TIME:
        case TYPE_DECIMAL64:
            return 8;

        case TYPE_DATE:
        case TYPE_DATETIME:
            // This is the size of the slot, the actual size of the data is 12.
            return sizeof(DateTimeValue);

        case TYPE_DECIMAL:
            return sizeof(DecimalValue);

        case TYPE_LARGEINT:
        case TYPE_DECIMALV2:
        case TYPE_DECIMAL128:
            return 16;
        case TYPE_ARRAY:
            return sizeof(void*); // sizeof(Collection*)
        case INVALID_TYPE:
        case TYPE_BINARY:
        case TYPE_STRUCT:
        case TYPE_MAP:
            DCHECK(false);
            break;
        }
        // For llvm complain
        return -1;
    }

    static inline int get_decimal_byte_size(int precision) {
        DCHECK_GT(precision, 0);
        if (precision <= MAX_DECIMAL4_PRECISION) {
            return 4;
        }
        if (precision <= MAX_DECIMAL8_PRECISION) {
            return 8;
        }
        return 16;
    }

    std::string debug_string() const;

    /// Recursive implementation of ToThrift() that populates 'thrift_type' with the
    /// TTypeNodes for this type and its children.
    void to_thrift(TTypeDesc* thrift_type) const;

private:
    /// Used to create a possibly nested type from the flattened Thrift representation.
    ///
    /// 'idx' is an in/out parameter that is initially set to the index of the type in
    /// 'types' being constructed, and is set to the index of the next type in 'types' that
    /// needs to be processed (or the size 'types' if all nodes have been processed).
    TypeDescriptor(const std::vector<TTypeNode>& types, int* idx);
    TypeDescriptor(const google::protobuf::RepeatedPtrField<PTypeNode>& types, int* idx);
    void to_protobuf(PTypeDesc* proto_type) const;
};

inline std::ostream& operator<<(std::ostream& os, const TypeDescriptor& type) {
    os << type.debug_string();
    return os;
}

} // namespace starrocks

#endif
