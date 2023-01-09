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

#include <memory>
#include <string>

#include "arrow/array.h"
#include "arrow/type.h"
#include "arrow/type_fwd.h"
#include "arrow/type_traits.h"
#include "column/json_column.h"
#include "common/statusor.h"
#include "gutil/casts.h"
#include "gutil/strings/substitute.h"
#include "util/json.h"

namespace starrocks {

using namespace arrow;
using ArrowStatus = Status;

// Convert multi-row Array to JsonColumn
static Status convert_multi_arrow_list(const ListArray* array, JsonColumn* output, size_t array_start_idx,
                                       size_t num_elements);
static Status convert_multi_arrow_struct(const StructArray* array, JsonColumn* output, size_t array_start_idx,
                                         size_t num_elements);
static Status convert_multi_arrow_map(const MapArray* array, JsonColumn* output, size_t array_start_idx,
                                      size_t num_elements);
static Status convert_multi_arrow_primitive(const Array* array, JsonColumn* output, size_t array_start_idx,
                                            size_t num_elements);

// Convert a single row in Array to json
static Status convert_single_arrow_list(const ListArray* array, int offset, vpack::Builder* builder);
static Status convert_single_arrow_struct(const StructArray* array, int offset, vpack::Builder* builder);
static Status convert_single_arrow_map(const MapArray* array, int offset, vpack::Builder* builder);

// Convert the whole array to json array
static Status convert_arrow_to_json_array(const Array* array, Type::type value_type, vpack::Builder* output);
static Status convert_arrow_to_json_array(const StringArray* array, vpack::Builder* builder);
static Status convert_arrow_to_json_array(const BooleanArray* array, vpack::Builder* builder);

// Convert the element at offset to a json element, which could be both primitive or nested
static Status convert_arrow_to_json_element(const Array* array, Type::type type_id, int offset, const std::string& name,
                                            vpack::Builder* output);

template <class TypeClass, class CType = typename TypeClass::c_type>
static enable_if_number<TypeClass, Status> convert_arrow_to_json_array(const NumericArray<TypeClass>* array,
                                                                       vpack::Builder* builder) {
    builder->openArray();
    for (int i = 0; i < array->length(); i++) {
        CType value = array->Value(i);
        builder->add(vpack::Value(value));
    }
    builder->close();

    return Status::OK();
}

#define APPLY_FOR_ALL_NUMERIC(M) \
    M(Type::INT8)                \
    M(Type::INT16)               \
    M(Type::INT32)               \
    M(Type::INT64)               \
    M(Type::UINT8)               \
    M(Type::UINT16)              \
    M(Type::UINT32)              \
    M(Type::UINT64)              \
    M(Type::FLOAT)               \
    M(Type::DOUBLE)

#define APPLY_FOR_ALL_TEMPORAL(M) \
    M(Type::DATE32)               \
    M(Type::DATE64)               \
    M(Type::TIME32)               \
    M(Type::TIME64)               \
    M(Type::TIMESTAMP)

static bool inline is_physical_temporal(Type::type type) {
    switch (type) {
#define M(tt) \
    case tt:  \
        return true;
        APPLY_FOR_ALL_TEMPORAL(M)
#undef M
    default:
        return false;
    }
}
static bool inline is_physical_signed(Type::type type) {
    if (arrow::is_signed_integer(type) || is_physical_temporal(type)) {
        return true;
    }
    return false;
}

static Status convert_multi_arrow_list(const ListArray* array, JsonColumn* output, size_t array_start_idx,
                                       size_t num_elements) {
    for (int i = array_start_idx; i < array_start_idx + num_elements; i++) {
        vpack::Builder builder;
        RETURN_IF_ERROR(convert_single_arrow_list(array, i, &builder));
        JsonValue json(builder.slice());
        output->append(std::move(json));
    }
    return Status::OK();
}

static Status convert_multi_arrow_struct(const StructArray* array, JsonColumn* output, size_t array_start_idx,
                                         size_t num_elements) {
    for (int i = array_start_idx; i < array_start_idx + num_elements; i++) {
        vpack::Builder builder;
        RETURN_IF_ERROR(convert_single_arrow_struct(array, i, &builder));
        JsonValue json(builder.slice());
        output->append(std::move(json));
    }
    return Status::OK();
}

static Status convert_multi_arrow_map(const MapArray* array, JsonColumn* output, size_t array_start_idx,
                                      size_t num_elements) {
    for (int i = array_start_idx; i < array_start_idx + num_elements; i++) {
        vpack::Builder builder;
        RETURN_IF_ERROR(convert_single_arrow_map(array, i, &builder));
        JsonValue json(builder.slice());
        output->append(std::move(json));
    }
    return Status::OK();
}

static Status convert_multi_arrow_primitive(const Array* array, JsonColumn* output, size_t array_start_idx,
                                            size_t num_elements) {
    auto type_id = array->type_id();

#define M(type)                                                                  \
    case type: {                                                                 \
        using TypeClass = TypeIdTraits<type>::Type;                              \
        using ArrayType = TypeTraits<TypeClass>::ArrayType;                      \
        auto real_array = down_cast<const ArrayType*>(array);                    \
        for (int i = array_start_idx; i < array_start_idx + num_elements; i++) { \
            vpack::Builder builder;                                              \
            if (is_physical_signed(type)) {                                      \
                JsonValue json = JsonValue::from_int(real_array->Value(i));      \
                output->append(std::move(json));                                 \
            } else if (arrow::is_unsigned_integer(type)) {                       \
                JsonValue json = JsonValue::from_uint(real_array->Value(i));     \
                output->append(std::move(json));                                 \
            } else if (is_floating(type)) {                                      \
                JsonValue json = JsonValue::from_double(real_array->Value(i));   \
                output->append(std::move(json));                                 \
            }                                                                    \
        }                                                                        \
        break;                                                                   \
    }

    switch (type_id) {
        APPLY_FOR_ALL_NUMERIC(M)
        APPLY_FOR_ALL_TEMPORAL(M)
#undef M

    case Type::BOOL: {
        auto real_array = down_cast<const BooleanArray*>(array);
        for (int i = 0; i < array->length(); i++) {
            vpack::Builder builder;
            JsonValue json = JsonValue::from_bool(real_array->Value(i));
            output->append(std::move(json));
        }
        break;
    }
    case Type::STRING: {
        auto real_array = down_cast<const StringArray*>(array);
        for (int i = 0; i < array->length(); i++) {
            vpack::Builder builder;
            auto view = real_array->GetView(i);
            ASSIGN_OR_RETURN(auto json, JsonValue::parse_json_or_string({view.data(), view.length()}));
            output->append(std::move(json));
        }
        break;
    }
    default: {
        return Status::NotSupported(strings::Substitute("type $0 is not supported", type_id));
    }
    }

    return Status::OK();
}

static Status convert_single_arrow_list(const ListArray* array, int offset, vpack::Builder* builder) {
    std::shared_ptr<Array> slice = array->value_slice(offset);
    return convert_arrow_to_json_array(slice.get(), array->value_type()->id(), builder);
}

static Status convert_single_arrow_struct(const StructArray* array, int offset, vpack::Builder* builder) {
    builder->openObject();
    const StructType* struct_type = array->struct_type();
    for (int i = 0; i < array->num_fields(); i++) {
        auto field = array->field(i);
        RETURN_IF_ERROR(convert_arrow_to_json_element(field.get(), field->type_id(), offset,
                                                      struct_type->field(i)->name(), builder));
    }
    builder->close();

    return Status::OK();
}

// Support numeric types and string type
static StatusOr<std::string> convert_array_element_to_string(const Array* array, int offset) {
    switch (array->type_id()) {
    case Type::STRING: {
        auto key_array = down_cast<const StringArray*>(array);
        return key_array->GetString(offset);
    }
#define M(type)                                                          \
    case type: {                                                         \
        using TypeClass = TypeIdTraits<type>::Type;                      \
        using ArrayType = TypeTraits<TypeClass>::ArrayType;              \
        using CType = TypeTraits<TypeClass>::CType;                      \
        CType value = down_cast<const ArrayType*>(array)->Value(offset); \
        return std::to_string(value);                                    \
    }

        APPLY_FOR_ALL_NUMERIC(M)
        APPLY_FOR_ALL_TEMPORAL(M)
#undef M

    default:
        return Status::NotSupported(strings::Substitute("key type of map not supported: $0", array->type()->name()));
    }
}

static Status convert_single_arrow_map(const MapArray* array, int offset, vpack::Builder* builder) {
    auto item_array = array->items();

    builder->openObject();
    Type::type item_type = item_array->type_id();
    for (int i = array->value_offset(offset); i < array->value_offset(offset + 1); i++) {
        ASSIGN_OR_RETURN(std::string field_name, convert_array_element_to_string(array->keys().get(), i));
        RETURN_IF_ERROR(convert_arrow_to_json_element(item_array.get(), item_type, i, field_name, builder));
    }
    builder->close();
    return Status::OK();
}

static Status convert_arrow_to_json_element(const Array* array, Type::type type_id, int offset,
                                            const std::string& field_name, vpack::Builder* builder) {
#define M(type)                                                          \
    case type: {                                                         \
        using TypeClass = TypeIdTraits<type>::Type;                      \
        using ArrayType = TypeTraits<TypeClass>::ArrayType;              \
        using CType = TypeTraits<TypeClass>::CType;                      \
        CType value = down_cast<const ArrayType*>(array)->Value(offset); \
        builder->add(field_name, vpack::Value(value));                   \
        break;                                                           \
    }

    switch (type_id) {
        APPLY_FOR_ALL_NUMERIC(M)
#undef M

    case Type::BOOL: {
        bool value = down_cast<const BooleanArray*>(array)->Value(offset);
        builder->add(field_name, vpack::Value(value));
        break;
    }
    case Type::STRING: {
        auto value = down_cast<const StringArray*>(array)->Value(offset);
        builder->add(field_name, vpack::Value(std::string_view(value.data(), value.length())));
        break;
    }
    case Type::STRUCT: {
        vpack::Builder sub_builder;
        RETURN_IF_ERROR(convert_single_arrow_struct(down_cast<const StructArray*>(array), offset, &sub_builder));
        builder->add(field_name, sub_builder.slice());
        break;
    }
    case Type::LIST: {
        vpack::Builder sub_builder;
        RETURN_IF_ERROR(convert_single_arrow_list(down_cast<const ListArray*>(array), offset, &sub_builder));
        builder->add(field_name, sub_builder.slice());
        break;
    }
    case Type::MAP: {
        vpack::Builder sub_builder;
        RETURN_IF_ERROR(convert_single_arrow_map(down_cast<const MapArray*>(array), offset, &sub_builder));
        builder->add(field_name, sub_builder.slice());
        break;
    }

    default:
        return Status::NotSupported(strings::Substitute("arrow type $0 is not supported", type_id));
    }

    return Status::OK();
}

// Convert a generic array to json array
static Status convert_arrow_to_json_array(const Array* array, Type::type value_type, vpack::Builder* builder) {
    switch (value_type) {
#define M(t)                                                                             \
    case t: {                                                                            \
        using TypeClass = TypeIdTraits<t>::Type;                                         \
        using ArrayType = TypeTraits<TypeClass>::ArrayType;                              \
        return convert_arrow_to_json_array(down_cast<const ArrayType*>(array), builder); \
    }
        APPLY_FOR_ALL_NUMERIC(M)
#undef M

    case Type::BOOL:
        return convert_arrow_to_json_array(down_cast<const BooleanArray*>(array), builder);
    case Type::STRING:
        return convert_arrow_to_json_array(down_cast<const StringArray*>(array), builder);
    case Type::STRUCT: {
        auto real_array = down_cast<const StructArray*>(array);
        builder->openArray();
        for (int i = 0; i < array->length(); i++) {
            RETURN_IF_ERROR(convert_single_arrow_struct(real_array, i, builder));
        }
        builder->close();
        break;
    }
    case Type::LIST: {
        auto real_array = down_cast<const ListArray*>(array);
        builder->openArray();
        for (int i = 0; i < array->length(); i++) {
            RETURN_IF_ERROR(convert_single_arrow_list(real_array, i, builder));
        }
        builder->close();
        break;
    }
    case Type::MAP: {
        auto real_array = down_cast<const MapArray*>(array);
        builder->openArray();
        for (int i = 0; i < array->length(); i++) {
            RETURN_IF_ERROR(convert_single_arrow_map(real_array, i, builder));
        }
        builder->close();
        break;
    }
    default:
        return Status::NotSupported(strings::Substitute("arrow type $0 is not supported", array->type()->ToString()));
    }

    return Status::OK();
}

static Status convert_arrow_to_json_array(const StringArray* array, vpack::Builder* builder) {
    builder->openArray();
    for (int i = 0; i < array->length(); i++) {
        auto value = array->Value(i);
        builder->add(vpack::Value(std::string_view{value.data(), value.length()}));
    }
    builder->close();
    return Status::OK();
}

static Status convert_arrow_to_json_array(const BooleanArray* array, vpack::Builder* builder) {
    builder->openArray();
    for (int i = 0; i < array->length(); i++) {
        auto value = array->Value(i);
        builder->add(vpack::Value(value));
    }
    builder->close();
    return Status::OK();
}

// Convert array to a json column
// Support all arrow types, including primitive types and nested data types
Status convert_arrow_to_json(const Array* array, JsonColumn* output, size_t array_start_idx, size_t num_elements) {
    // std::cerr << "convert_arrow_to_json: " << array->type()->ToString() << std::endl;
    auto type = array->type_id();
    switch (type) {
    case Type::LIST:
        return convert_multi_arrow_list(down_cast<const ListArray*>(array), output, array_start_idx, num_elements);
    case Type::STRUCT:
        return convert_multi_arrow_struct(down_cast<const StructArray*>(array), output, array_start_idx, num_elements);
    case Type::MAP:
        return convert_multi_arrow_map(down_cast<const MapArray*>(array), output, array_start_idx, num_elements);

    case Type::STRING:
    case Type::BOOL:
#define M(type) \
    case type:  \
        return convert_multi_arrow_primitive(array, output, array_start_idx, num_elements);

        APPLY_FOR_ALL_NUMERIC(M)
        APPLY_FOR_ALL_TEMPORAL(M)
#undef M

    default:
        return Status::NotSupported(strings::Substitute("arrow type $0 not supported", array->type()->name()));
    }

    return Status::OK();
}

} // namespace starrocks
