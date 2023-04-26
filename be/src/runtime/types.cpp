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

// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/be/src/runtime/types.cpp

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

#include "runtime/types.h"

#include <ostream>

#include "gutil/strings/substitute.h"
#include "runtime/datetime_value.h"
#include "runtime/string_value.h"
#include "storage/types.h"
#include "types/array_type_info.h"
#include "types/logical_type.h"

namespace starrocks {

TypeDescriptor::TypeDescriptor(const std::vector<TTypeNode>& types, int* idx) {
    DCHECK_GE(*idx, 0);
    DCHECK_LT(*idx, types.size());
    const TTypeNode& node = types[*idx];
    switch (node.type) {
    case TTypeNodeType::SCALAR: {
        DCHECK(node.__isset.scalar_type);
        ++(*idx);
        const TScalarType scalar_type = node.scalar_type;
        type = thrift_to_type(scalar_type.type);
        len = (scalar_type.__isset.len) ? scalar_type.len : -1;
        scale = (scalar_type.__isset.scale) ? scalar_type.scale : -1;
        precision = (scalar_type.__isset.precision) ? scalar_type.precision : -1;

        if (type == TYPE_DECIMAL || type == TYPE_DECIMALV2 || type == TYPE_DECIMAL32 || type == TYPE_DECIMAL64 ||
            type == TYPE_DECIMAL128) {
            DCHECK(scalar_type.__isset.precision);
            DCHECK(scalar_type.__isset.scale);
        }
        break;
    }
    case TTypeNodeType::STRUCT:
        type = TYPE_STRUCT;
        ++(*idx);
        for (const auto& struct_field : node.struct_fields) {
            field_names.push_back(struct_field.name);
            children.push_back(TypeDescriptor(types, idx));
        }
        DCHECK_EQ(field_names.size(), children.size());
        break;
    case TTypeNodeType::ARRAY:
        DCHECK(!node.__isset.scalar_type);
        DCHECK_LT(*idx, types.size() - 1);
        type = TYPE_ARRAY;
        ++(*idx);
        children.push_back(TypeDescriptor(types, idx));
        break;
    case TTypeNodeType::MAP:
        DCHECK(!node.__isset.scalar_type);
        DCHECK_LT(*idx, types.size() - 2);
        type = TYPE_MAP;
        ++(*idx);
        children.push_back(TypeDescriptor(types, idx));
        children.push_back(TypeDescriptor(types, idx));
        break;
    }
}

void TypeDescriptor::to_thrift(TTypeDesc* thrift_type) const {
    thrift_type->__isset.types = true;
    thrift_type->types.emplace_back();
    TTypeNode& curr_node = thrift_type->types.back();
    if (type == TYPE_ARRAY) {
        curr_node.__set_type(TTypeNodeType::ARRAY);
        DCHECK_EQ(1, children.size());
        children[0].to_thrift(thrift_type);
    } else if (type == TYPE_MAP) {
        curr_node.__set_type(TTypeNodeType::MAP);
        DCHECK_EQ(2, children.size());
        children[0].to_thrift(thrift_type);
        children[1].to_thrift(thrift_type);
    } else if (type == TYPE_STRUCT) {
        DCHECK_EQ(type, TYPE_STRUCT);
        curr_node.__set_type(TTypeNodeType::STRUCT);
        curr_node.__set_struct_fields(std::vector<TStructField>());
        for (const auto& field_name : field_names) {
            curr_node.struct_fields.emplace_back();
            curr_node.struct_fields.back().__set_name(field_name);
        }
        DCHECK_EQ(children.size(), field_names.size());
        for (const TypeDescriptor& child : children) {
            child.to_thrift(thrift_type);
        }
    } else {
        curr_node.type = TTypeNodeType::SCALAR;
        curr_node.__set_scalar_type(TScalarType());
        TScalarType& scalar_type = curr_node.scalar_type;
        scalar_type.__set_type(starrocks::to_thrift(type));
        if (len != -1) {
            scalar_type.__set_len(len);
        }
        if (scale != -1) {
            scalar_type.__set_scale(scale);
        }
        if (precision != -1) {
            scalar_type.__set_precision(precision);
        }
    }
}

void TypeDescriptor::to_protobuf(PTypeDesc* proto_type) const {
    PTypeNode* node = proto_type->add_types();
    if (type == TYPE_ARRAY) {
        node->set_type(TTypeNodeType::ARRAY);
        DCHECK_EQ(1, children.size());
        children[0].to_protobuf(proto_type);
    } else if (type == TYPE_MAP) {
        node->set_type(TTypeNodeType::MAP);
        children[0].to_protobuf(proto_type);
        children[1].to_protobuf(proto_type);
        DCHECK_EQ(2, children.size());
    } else if (type == TYPE_STRUCT) {
        node->set_type(TTypeNodeType::STRUCT);
        DCHECK_EQ(field_names.size(), children.size());
        for (size_t i = 0; i < field_names.size(); i++) {
            node->add_struct_fields()->set_name(field_names[i]);
        }
        for (const TypeDescriptor& child : children) {
            child.to_protobuf(proto_type);
        }
    } else {
        node->set_type(TTypeNodeType::SCALAR);
        PScalarType* scalar_type = node->mutable_scalar_type();
        scalar_type->set_type(starrocks::to_thrift(type));
        if (len != -1) {
            scalar_type->set_len(len);
        }
        if (scale != -1) {
            scalar_type->set_scale(scale);
        }
        if (precision != -1) {
            scalar_type->set_precision(precision);
        }
    }
}

TypeDescriptor::TypeDescriptor(const google::protobuf::RepeatedPtrField<PTypeNode>& types, int* idx) {
    DCHECK_GE(*idx, 0);
    DCHECK_LT(*idx, types.size());

    const PTypeNode& node = types.Get(*idx);
    auto node_type = static_cast<TTypeNodeType::type>(node.type());
    switch (node_type) {
    case TTypeNodeType::SCALAR: {
        DCHECK(node.has_scalar_type());
        ++(*idx);
        const PScalarType& scalar_type = node.scalar_type();
        type = thrift_to_type((TPrimitiveType::type)scalar_type.type());
        len = scalar_type.has_len() ? scalar_type.len() : -1;
        scale = scalar_type.has_scale() ? scalar_type.scale() : -1;
        precision = scalar_type.has_precision() ? scalar_type.precision() : -1;

        if (type == TYPE_CHAR || type == TYPE_VARCHAR || type == TYPE_HLL) {
            DCHECK(scalar_type.has_len());
        } else if (type == TYPE_DECIMAL || type == TYPE_DECIMALV2) {
            DCHECK(scalar_type.has_precision());
            DCHECK(scalar_type.has_scale());
        }
        break;
    }
    case TTypeNodeType::STRUCT:
        type = TYPE_STRUCT;
        ++(*idx);
        for (int i = 0; i < node.struct_fields().size(); ++i) {
            children.push_back(TypeDescriptor(types, idx));
            field_names.push_back(node.struct_fields(i).name());
        }
        break;
    case TTypeNodeType::ARRAY:
        DCHECK(!node.has_scalar_type());
        DCHECK_LT(*idx, types.size() - 1);
        ++(*idx);
        type = TYPE_ARRAY;
        children.push_back(TypeDescriptor(types, idx));
        break;
    case TTypeNodeType::MAP:
        DCHECK(!node.has_scalar_type());
        DCHECK_LT(*idx, types.size() - 2);
        ++(*idx);
        type = TYPE_MAP;
        children.push_back(TypeDescriptor(types, idx));
        children.push_back(TypeDescriptor(types, idx));
        break;
    }
}

std::string TypeDescriptor::debug_string() const {
    switch (type) {
    case TYPE_CHAR:
        return strings::Substitute("CHAR($0)", len);
    case TYPE_VARCHAR:
        return strings::Substitute("VARCHAR($0)", len);
    case TYPE_VARBINARY:
        return strings::Substitute("VARBINARY($0)", len);
    case TYPE_DECIMAL:
        return strings::Substitute("DECIMAL($0, $1)", precision, scale);
    case TYPE_DECIMALV2:
        return strings::Substitute("DECIMALV2($0, $1)", precision, scale);
    case TYPE_DECIMAL32:
        return strings::Substitute("DECIMAL32($0, $1)", precision, scale);
    case TYPE_DECIMAL64:
        return strings::Substitute("DECIMAL64($0, $1)", precision, scale);
    case TYPE_DECIMAL128:
        return strings::Substitute("DECIMAL128($0, $1)", precision, scale);
    case TYPE_ARRAY:
        return strings::Substitute("ARRAY<$0>", children[0].debug_string());
    case TYPE_MAP:
        return strings::Substitute("MAP<$0, $1>", children[0].debug_string(), children[1].debug_string());
    case TYPE_STRUCT: {
        std::stringstream ss;
        ss << "STRUCT{";
        for (size_t i = 0; i < field_names.size(); i++) {
            ss << field_names[i] << " " << children[i].debug_string();
            if (i + 1 < field_names.size()) {
                ss << ", ";
            }
        }
        ss << "}";
        return ss.str();
    }
    default:
        return type_to_string(type);
    }
}

bool TypeDescriptor::support_join() const {
    return type != TYPE_JSON && type != TYPE_OBJECT && type != TYPE_PERCENTILE && type != TYPE_HLL &&
           type != TYPE_MAP && type != TYPE_STRUCT && type != TYPE_ARRAY;
}

bool TypeDescriptor::support_orderby() const {
    return type != TYPE_JSON && type != TYPE_OBJECT && type != TYPE_PERCENTILE && type != TYPE_HLL &&
           type != TYPE_MAP && type != TYPE_STRUCT && type != TYPE_ARRAY;
}

bool TypeDescriptor::support_groupby() const {
    return type != TYPE_JSON && type != TYPE_OBJECT && type != TYPE_PERCENTILE && type != TYPE_HLL &&
           type != TYPE_MAP && type != TYPE_STRUCT;
}

TypeDescriptor TypeDescriptor::from_storage_type_info(TypeInfo* type_info) {
    LogicalType ftype = type_info->type();

    bool is_array = false;
    if (ftype == TYPE_ARRAY) {
        is_array = true;
        type_info = get_item_type_info(type_info).get();
        ftype = type_info->type();
    }

    LogicalType ltype = scalar_field_type_to_logical_type(ftype);
    DCHECK(ltype != TYPE_UNKNOWN);
    int len = TypeDescriptor::MAX_VARCHAR_LENGTH;
    int precision = type_info->precision();
    int scale = type_info->scale();
    TypeDescriptor ret = TypeDescriptor::from_logical_type(ltype, len, precision, scale);

    if (is_array) {
        TypeDescriptor arr;
        arr.type = TYPE_ARRAY;
        arr.children.emplace_back(ret);
        return arr;
    }
    return ret;
}

/// Returns the size of a slot for this type.
int TypeDescriptor::get_slot_size() const {
    switch (type) {
    case TYPE_CHAR:
    case TYPE_VARCHAR:
    case TYPE_HLL:
    case TYPE_OBJECT:
    case TYPE_PERCENTILE:
    case TYPE_JSON:
    case TYPE_VARBINARY:
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
        return 40;

    case TYPE_LARGEINT:
    case TYPE_DECIMALV2:
    case TYPE_DECIMAL128:
        return 16;
    case TYPE_ARRAY:
    case TYPE_MAP:
        return sizeof(void*); // sizeof(Collection*)
    case TYPE_STRUCT: {
        int struct_size = 0;
        for (const TypeDescriptor& type_descriptor : children) {
            struct_size += type_descriptor.get_slot_size();
        }
        return struct_size;
    }
    case TYPE_UNKNOWN:
    case TYPE_BINARY:
    case TYPE_FUNCTION:
    case TYPE_UNSIGNED_TINYINT:
    case TYPE_UNSIGNED_SMALLINT:
    case TYPE_UNSIGNED_INT:
    case TYPE_UNSIGNED_BIGINT:
    case TYPE_DISCRETE_DOUBLE:
    case TYPE_DATE_V1:
    case TYPE_DATETIME_V1:
    case TYPE_NONE:
    case TYPE_MAX_VALUE:
        DCHECK(false);
        break;
    }
    // For llvm complain
    return -1;
}

size_t TypeDescriptor::get_array_depth_limit() const {
    int depth = 1;
    const TypeDescriptor* type = this;
    while (type->children.size() > 0) {
        type = &type->children[0];
        depth++;
    }
    return depth;
}

} // namespace starrocks
