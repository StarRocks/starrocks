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

#include "orc_schema_builder.h"

#include "fmt/format.h"

namespace starrocks {

static Status get_orc_type_from_scalar_type(const orc::Type* typ, TypeDescriptor* desc);
static Status get_orc_type_from_list(const orc::Type* typ, TypeDescriptor* desc);
static Status get_orc_type_from_map(const orc::Type* typ, TypeDescriptor* desc);
static Status get_orc_type_from_struct(const orc::Type* typ, TypeDescriptor* desc);
static Status get_orc_type_from_union(const orc::Type* typ, TypeDescriptor* desc);

Status get_orc_type(const orc::Type* typ, TypeDescriptor* desc) {
    switch (typ->getKind()) {
    case orc::TypeKind::LIST:
        return get_orc_type_from_list(typ, desc);
    case orc::TypeKind::MAP:
        return get_orc_type_from_map(typ, desc);
    case orc::TypeKind::STRUCT:
        return get_orc_type_from_struct(typ, desc);
    case orc::TypeKind::UNION:
        return get_orc_type_from_union(typ, desc);
    default:
        return get_orc_type_from_scalar_type(typ, desc);
    }
}

static Status get_orc_type_from_scalar_type(const orc::Type* typ, TypeDescriptor* desc) {
    switch (typ->getKind()) {
    case orc::TypeKind::BOOLEAN:
        *desc = TypeDescriptor::from_logical_type(TYPE_BOOLEAN);
        break;

    case orc::TypeKind::BYTE:
        *desc = TypeDescriptor::from_logical_type(TYPE_TINYINT);
        break;

    case orc::TypeKind::SHORT:
        *desc = TypeDescriptor::from_logical_type(TYPE_SMALLINT);
        break;

    case orc::TypeKind::INT:
        *desc = TypeDescriptor::from_logical_type(TYPE_INT);
        break;

    case orc::TypeKind::LONG:
        *desc = TypeDescriptor::from_logical_type(TYPE_BIGINT);
        break;

    case orc::TypeKind::FLOAT:
        *desc = TypeDescriptor::from_logical_type(TYPE_FLOAT);
        break;

    case orc::TypeKind::DOUBLE:
        *desc = TypeDescriptor::from_logical_type(TYPE_DOUBLE);
        break;

    case orc::TypeKind::STRING:
        *desc = TypeDescriptor::create_varchar_type(TypeDescriptor::MAX_VARCHAR_LENGTH);
        break;

    case orc::TypeKind::BINARY:
        *desc = TypeDescriptor::create_varbinary_type(TypeDescriptor::MAX_VARCHAR_LENGTH);
        break;

    case orc::TypeKind::TIMESTAMP:
        *desc = TypeDescriptor::from_logical_type(TYPE_DATETIME);
        break;

    case orc::TypeKind::LIST:
    case orc::TypeKind::MAP:
    case orc::TypeKind::STRUCT:
    case orc::TypeKind::UNION:
        // NEVER REACH.
        // scalar types only.
        return Status::NotSupported(fmt::format("Unkown supported orc type: {}", typ->getKind()));

    case orc::TypeKind::DECIMAL:
        *desc = TypeDescriptor::create_decimalv3_type(TYPE_DECIMAL128, typ->getPrecision(), typ->getScale());
        break;

    case orc::TypeKind::DATE:
        *desc = TypeDescriptor::from_logical_type(TYPE_DATE);
        break;

    case orc::TypeKind::VARCHAR:
        *desc = TypeDescriptor::create_varchar_type(typ->getMaximumLength());
        break;

    case orc::TypeKind::CHAR:
        *desc = TypeDescriptor::create_char_type(typ->getMaximumLength());
        break;

    default:
        // treat other type as varchar.
        *desc = TypeDescriptor::create_varchar_type(TypeDescriptor::MAX_VARCHAR_LENGTH);
    }
    return Status::OK();
}

static Status get_orc_type_from_list(const orc::Type* typ, TypeDescriptor* desc) {
    DCHECK(typ->getKind() == orc::TypeKind::LIST);

    if (typ->getSubtypeCount() != 1) {
        return Status::Corruption(fmt::format("expect 1 element in orc list type, got: {}", typ->getSubtypeCount()));
    }

    auto subtype = typ->getSubtype(0);

    TypeDescriptor child_desc;
    RETURN_IF_ERROR(get_orc_type(subtype, &child_desc));
    *desc = TypeDescriptor::create_array_type(child_desc);

    return Status::OK();
}

static Status get_orc_type_from_map(const orc::Type* typ, TypeDescriptor* desc) {
    DCHECK(typ->getKind() == orc::TypeKind::MAP);
    if (typ->getSubtypeCount() != 2) {
        return Status::Corruption(fmt::format("expect key and value in orc map type, got: {}", typ->getSubtypeCount()));
    }

    auto key_type = typ->getSubtype(0);
    TypeDescriptor key_desc;
    RETURN_IF_ERROR(get_orc_type(key_type, &key_desc));

    auto value_type = typ->getSubtype(1);
    TypeDescriptor value_desc;
    RETURN_IF_ERROR(get_orc_type(value_type, &value_desc));

    *desc = TypeDescriptor::create_map_type(key_desc, value_desc);

    return Status::OK();
}

static Status get_orc_type_from_struct(const orc::Type* typ, TypeDescriptor* desc) {
    DCHECK(typ->getKind() == orc::TypeKind::STRUCT);
    if (typ->getSubtypeCount() == 0) {
        return Status::Corruption("no fields in orc struct type");
    }

    std::vector<std::string> field_names;
    std::vector<TypeDescriptor> field_types;
    for (size_t i = 0; i < typ->getSubtypeCount(); ++i) {
        auto name = typ->getFieldName(i);
        field_names.emplace_back(std::move(name));

        TypeDescriptor field_desc;
        RETURN_IF_ERROR(get_orc_type(typ->getSubtype(i), &field_desc));
        field_types.emplace_back(std::move(field_desc));
    }

    *desc = TypeDescriptor::create_struct_type(field_names, field_types);
    return Status::OK();
}

static Status get_orc_type_from_union(const orc::Type* typ, TypeDescriptor* desc) {
    *desc = TypeDescriptor::create_varchar_type(TypeDescriptor::MAX_VARCHAR_LENGTH);
    return Status::OK();
}

} // namespace starrocks
