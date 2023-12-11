// Copyright 2023-present StarRocks, Inc. All rights reserved.
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

#pragma once

#include <string>
#include <vector>

#include "common/logging.h"
#include "gen_cpp/Types_types.h" // for TPrimitiveType
#include "gen_cpp/types.pb.h"    // for PTypeDesc
#include "thrift/protocol/TDebugProtocol.h"
#include "types/constexpr.h"
#include "types/logical_type.h"

namespace starrocks {

class TypeInfo;

// Describes a type. Includes the enum, children types, and any type-specific metadata
// (e.g. precision and scale for decimals).
struct TypeDescriptor {
    LogicalType type{TYPE_UNKNOWN};
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

    TypeDescriptor() = default;

    explicit TypeDescriptor(LogicalType type) : type(type) {}

    static TypeDescriptor create_char_type(int len) {
        TypeDescriptor ret;
        ret.type = TYPE_CHAR;
        ret.len = len;
        return ret;
    }

    static TypeDescriptor create_varchar_type(int len) {
        TypeDescriptor ret;
        ret.type = TYPE_VARCHAR;
        ret.len = len;
        return ret;
    }

    static TypeDescriptor create_varbinary_type(int len) {
        TypeDescriptor ret;
        ret.type = TYPE_VARBINARY;
        ret.len = len;
        return ret;
    }

    static TypeDescriptor create_json_type() {
        TypeDescriptor res;
        res.type = TYPE_JSON;
        res.len = kJsonDefaultSize;
        return res;
    }

    static TypeDescriptor create_array_type(const TypeDescriptor& children) {
        TypeDescriptor res;
        res.type = TYPE_ARRAY;
        res.children.push_back(children);
        return res;
    }

    static TypeDescriptor create_map_type(const TypeDescriptor& key, const TypeDescriptor& value) {
        TypeDescriptor res;
        res.type = TYPE_MAP;
        res.children.push_back(key);
        res.children.push_back(value);
        return res;
    }

    static TypeDescriptor create_struct_type(const std::vector<std::string> field_names,
                                             const std::vector<TypeDescriptor>& filed_types) {
        TypeDescriptor res;
        res.type = TYPE_STRUCT;
        res.field_names = field_names;
        res.children = filed_types;
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

    static TypeDescriptor create_decimalv3_type(LogicalType type, int precision, int scale) {
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

    static TypeDescriptor from_logical_type(LogicalType type,
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

    static TypeDescriptor from_protobuf(const PTypeDesc& ltype) {
        int idx = 0;
        TypeDescriptor result(ltype.types(), &idx);
        DCHECK_EQ(idx, ltype.types_size());
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
            return type == o.type && precision == o.precision && scale == o.scale;
        } else {
            return type == o.type;
        }
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
               type == TYPE_PERCENTILE || type == TYPE_VARBINARY;
    }

    inline bool is_date_type() const { return type == TYPE_DATE || type == TYPE_DATETIME; }

    inline bool is_decimalv3_type() const {
        return (type == TYPE_DECIMAL32 || type == TYPE_DECIMAL64 || type == TYPE_DECIMAL128);
    }

    inline bool is_decimal_type() const {
        return (type == TYPE_DECIMAL || type == TYPE_DECIMALV2 || is_decimalv3_type());
    }

    inline bool is_unknown_type() const { return type == TYPE_UNKNOWN; }

    inline bool is_complex_type() const { return type == TYPE_STRUCT || type == TYPE_ARRAY || type == TYPE_MAP; }

    inline bool is_struct_type() const { return type == TYPE_STRUCT; }

    inline bool is_array_type() const { return type == TYPE_ARRAY; }

    inline bool is_map_type() const { return type == TYPE_MAP; }

    inline bool is_collection_type() const { return type == TYPE_ARRAY || type == TYPE_MAP; }

    // Could this type be used at join on conjuncts
    bool support_join() const;
    // Could this type be used at order by clause
    bool support_orderby() const;
    // Could this type be used at group by clause
    bool support_groupby() const;

    // For some types with potential huge length, whose memory consumption is far more than normal types,
    // they need a different chunk_size setting
    bool is_huge_type() const { return type == TYPE_JSON || type == TYPE_OBJECT || type == TYPE_HLL; }

    /// Returns the size of a slot for this type.
    int get_slot_size() const;

    size_t get_flat_size() const;

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

    size_t get_array_depth_limit() const;

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
