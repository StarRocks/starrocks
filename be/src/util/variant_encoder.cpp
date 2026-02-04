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

#include "util/variant_encoder.h"

#include <algorithm>
#include <cstdint>
#include <limits>
#include <map>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "arrow/util/endian.h"
#include "base/string/slice.h"
#include "column/array_column.h"
#include "column/binary_column.h"
#include "column/column_helper.h"
#include "column/column_viewer.h"
#include "column/datum_convert.h"
#include "column/fixed_length_column.h"
#include "column/json_column.h"
#include "column/map_column.h"
#include "column/nullable_column.h"
#include "column/struct_column.h"
#include "common/status.h"
#include "runtime/time_types.h"
#include "runtime/types.h"
#include "storage/types.h"
#include "util/decimal_types.h"
#include "util/variant.h"
#include "velocypack/vpack.h"

namespace starrocks {

constexpr uint8_t kVariantMetadataVersion = 1;
constexpr uint8_t kVariantMetadataSortedMask = 0b10000;
constexpr uint8_t kVariantMetadataOffsetSizeShift = 6;

// --- Internal Encoding Helpers ---

// Apache Parquet Variant Shredding Support
// Reference: https://github.com/apache/parquet-format/blob/master/VariantShredding.md
//
// Variant shredding allows storing variant values in a columnar format with typed columns
// for better compression and query performance. The spec defines:
//
// 1. Each variant has metadata (field dictionary) + value (variant-encoded data)
// 2. Optionally, a typed_value can store the value in a specific Parquet type
// 3. Mutual exclusivity rules:
//    - For primitives/arrays: value OR typed_value (not both)
//    - For objects: both can be non-null (partially shredded object)
// 4. Interpretation:
//    - (null, null) = missing value (valid only for object fields)
//    - (non-null, null) = value present, any type including null
//    - (null, non-null) = value present, matches shredded type
//    - (non-null, non-null) = partially shredded object

// --- Internal Encoding Helpers ---

static bool is_fully_static_type(const TypeDescriptor& type) {
    if (type.type == TYPE_MAP || type.type == TYPE_JSON || type.type == TYPE_VARIANT) {
        return false;
    }
    if (type.type == TYPE_STRUCT) {
        for (const auto& name : type.field_names) {
            if (name == "typed_value" || name == "value") return false;
        }
    }
    for (const auto& child : type.children) {
        if (!is_fully_static_type(child)) return false;
    }
    return true;
}

static void collect_keys_from_static_type(const TypeDescriptor& type, VariantEncodingContext* ctx) {
    if (type.type == TYPE_STRUCT) {
        for (size_t i = 0; i < type.field_names.size(); ++i) {
            ctx->keys.insert(type.field_names[i]);
            collect_keys_from_static_type(type.children[i], ctx);
        }
    } else if (type.type == TYPE_ARRAY) {
        collect_keys_from_static_type(type.children[0], ctx);
    }
}

template <typename T>
static inline void append_little_endian(std::string* out, T value) {
    T le_value = arrow::bit_util::ToLittleEndian(value);
    out->append(reinterpret_cast<const char*>(&le_value), sizeof(T));
}

static inline void append_uint_le(std::string* out, uint32_t value, uint8_t size) {
    uint32_t le_value = arrow::bit_util::ToLittleEndian(value);
    out->append(reinterpret_cast<const char*>(&le_value), size);
}

static inline void append_int128_le(std::string* out, int128_t value) {
    int64_t low = static_cast<int64_t>(value);
    int64_t high = static_cast<int64_t>(value >> 64);
    append_little_endian<int64_t>(out, low);
    append_little_endian<int64_t>(out, high);
}

static inline uint8_t minimal_uint_size(uint32_t value) {
    if (value <= 0xFF) return 1;
    if (value <= 0xFFFF) return 2;
    if (value <= 0xFFFFFF) return 3;
    return 4;
}

static inline char encode_variant_header(VariantType type, VariantValue::BasicType basic) {
    return static_cast<char>((static_cast<uint8_t>(type) << VariantValue::kValueHeaderBitShift) |
                             static_cast<uint8_t>(basic));
}

static std::string encode_variant_null() {
    return std::string(VariantValue::kEmptyValue);
}

static std::string encode_variant_boolean(bool value) {
    const auto type = value ? VariantType::BOOLEAN_TRUE : VariantType::BOOLEAN_FALSE;
    return std::string(1, encode_variant_header(type, VariantValue::BasicType::PRIMITIVE));
}

template <typename T>
static std::string encode_variant_primitive(VariantType type, T value) {
    std::string out;
    out.reserve(VariantValue::kHeaderSizeBytes + sizeof(T));
    out.push_back(encode_variant_header(type, VariantValue::BasicType::PRIMITIVE));
    append_little_endian<T>(&out, value);
    return out;
}

static std::string encode_variant_decimal(VariantType type, int128_t value, uint8_t scale) {
    std::string out;
    const size_t val_size = (type == VariantType::DECIMAL4) ? 4 : (type == VariantType::DECIMAL8 ? 8 : 16);
    out.reserve(VariantValue::kHeaderSizeBytes + 1 + val_size);
    out.push_back(encode_variant_header(type, VariantValue::BasicType::PRIMITIVE));
    out.push_back(static_cast<char>(scale));
    if (type == VariantType::DECIMAL4) {
        append_little_endian<int32_t>(&out, static_cast<int32_t>(value));
    } else if (type == VariantType::DECIMAL8) {
        append_little_endian<int64_t>(&out, static_cast<int64_t>(value));
    } else {
        append_int128_le(&out, value);
    }
    return out;
}

static std::string encode_variant_string(const Slice& value) {
    std::string out;
    const size_t len = value.size;
    if (len <= 63) {
        uint8_t header = static_cast<uint8_t>((len << VariantValue::kValueHeaderBitShift) |
                                              static_cast<uint8_t>(VariantValue::BasicType::SHORT_STRING));
        out.push_back(static_cast<char>(header));
        out.append(value.data, len);
    } else {
        out.push_back(encode_variant_header(VariantType::STRING, VariantValue::BasicType::PRIMITIVE));
        append_uint_le(&out, static_cast<uint32_t>(len), sizeof(uint32_t));
        out.append(value.data, len);
    }
    return out;
}

static std::string encode_variant_binary(const Slice& value) {
    std::string out;
    out.reserve(VariantValue::kHeaderSizeBytes + sizeof(uint32_t) + value.size);
    out.push_back(encode_variant_header(VariantType::BINARY, VariantValue::BasicType::PRIMITIVE));
    append_little_endian<uint32_t>(&out, static_cast<uint32_t>(value.size));
    out.append(value.data, value.size);
    return out;
}

static std::string encode_variant_array(const std::vector<std::string>& elements) {
    const uint32_t num_elements = static_cast<uint32_t>(elements.size());
    uint64_t total_data_size = 0;
    for (const auto& element : elements) {
        total_data_size += element.size();
    }
    const uint8_t offset_size = minimal_uint_size(static_cast<uint32_t>(total_data_size));
    const bool is_large = num_elements > 255;
    const uint8_t num_elements_size = is_large ? 4 : 1;

    uint8_t vheader = static_cast<uint8_t>((offset_size - 1) | (is_large << 2));
    std::string out;
    out.reserve(1 + num_elements_size + (num_elements + 1) * offset_size + total_data_size);
    out.push_back(encode_variant_header(static_cast<VariantType>(vheader), VariantValue::BasicType::ARRAY));
    append_uint_le(&out, num_elements, num_elements_size);

    uint32_t offset = 0;
    append_uint_le(&out, offset, offset_size);
    for (const auto& element : elements) {
        offset += static_cast<uint32_t>(element.size());
        append_uint_le(&out, offset, offset_size);
    }
    for (const auto& element : elements) {
        out.append(element);
    }
    return out;
}

static std::string encode_variant_object(const std::vector<uint32_t>& field_ids,
                                         const std::vector<std::string>& field_values, uint32_t max_field_id) {
    const uint32_t num_fields = static_cast<uint32_t>(field_ids.size());
    uint64_t total_data_size = 0;
    for (const auto& value : field_values) {
        total_data_size += value.size();
    }
    const uint8_t field_id_size = minimal_uint_size(max_field_id);
    const uint8_t field_offset_size = minimal_uint_size(static_cast<uint32_t>(total_data_size));
    const bool is_large = num_fields > 255;
    const uint8_t num_elements_size = is_large ? 4 : 1;

    uint8_t vheader =
            static_cast<uint8_t>((field_offset_size - 1) | ((field_id_size - 1) << 2) | ((is_large ? 1 : 0) << 4));
    std::string out;
    out.reserve(1 + num_elements_size + num_fields * field_id_size + (num_fields + 1) * field_offset_size +
                total_data_size);
    out.push_back(encode_variant_header(static_cast<VariantType>(vheader), VariantValue::BasicType::OBJECT));
    append_uint_le(&out, num_fields, num_elements_size);

    for (uint32_t id : field_ids) {
        append_uint_le(&out, id, field_id_size);
    }

    uint32_t offset = 0;
    append_uint_le(&out, offset, field_offset_size);
    for (const auto& value : field_values) {
        offset += static_cast<uint32_t>(value.size());
        append_uint_le(&out, offset, field_offset_size);
    }
    for (const auto& value : field_values) {
        out.append(value);
    }
    return out;
}

static std::string encode_variant_object_from_map(const std::map<uint32_t, std::string>& fields) {
    std::vector<uint32_t> ids;
    std::vector<std::string> values;
    uint32_t max_id = 0;
    for (const auto& [id, value] : fields) {
        ids.push_back(id);
        values.push_back(value);
        max_id = std::max(max_id, id);
    }
    return encode_variant_object(ids, values, max_id);
}

// --- JSON Key Collection & Value Encoding ---

static Status collect_json_slice_keys(const vpack::Slice& slice, VariantEncodingContext* ctx) {
    try {
        if (slice.isObject()) {
            for (vpack::ObjectIterator it(slice); it.valid(); it.next()) {
                auto current = *it;
                auto key = current.key.stringView();
                ctx->keys.emplace(key.data(), key.size());
                RETURN_IF_ERROR(collect_json_slice_keys(current.value, ctx));
            }
        } else if (slice.isArray()) {
            for (vpack::ArrayIterator it(slice); it.valid(); it.next()) {
                RETURN_IF_ERROR(collect_json_slice_keys(it.value(), ctx));
            }
        }
    } catch (const vpack::Exception& e) {
        return fromVPackException(e);
    }
    return Status::OK();
}

static StatusOr<std::string> encode_json_to_variant_value(const vpack::Slice& slice, VariantEncodingContext* ctx) {
    try {
        if (slice.isNone() || slice.isNull()) {
            return encode_variant_null();
        }
        if (slice.isBool()) {
            return encode_variant_boolean(slice.getBool());
        }
        if (slice.isInt() || slice.isSmallInt()) {
            return encode_variant_primitive<int64_t>(VariantType::INT64, slice.getIntUnchecked());
        }
        if (slice.isUInt()) {
            uint64_t value = slice.getUIntUnchecked();
            if (value <= static_cast<uint64_t>(std::numeric_limits<int64_t>::max())) {
                return encode_variant_primitive<int64_t>(VariantType::INT64, static_cast<int64_t>(value));
            }
            return encode_variant_primitive<double>(VariantType::DOUBLE, static_cast<double>(value));
        }
        if (slice.isDouble()) {
            return encode_variant_primitive<double>(VariantType::DOUBLE, slice.getDouble());
        }
        if (slice.isString() || slice.isBinary()) {
            vpack::ValueLength len;
            const char* str = slice.getStringUnchecked(len);
            return encode_variant_string(Slice(str, len));
        }
        if (slice.isArray()) {
            std::vector<std::string> elements;
            for (vpack::ArrayIterator it(slice); it.valid(); it.next()) {
                ASSIGN_OR_RETURN(std::string element, encode_json_to_variant_value(it.value(), ctx));
                elements.emplace_back(std::move(element));
            }
            return encode_variant_array(elements);
        }
        if (slice.isObject()) {
            std::map<uint32_t, std::string> fields;
            for (vpack::ObjectIterator it(slice); it.valid(); it.next()) {
                auto current = *it;
                auto key = current.key.stringView();
                auto it_key = ctx->key_to_id.find(std::string_view(key.data(), key.size()));
                if (it_key == ctx->key_to_id.end()) {
                    return Status::VariantError("Variant metadata missing field: " +
                                                std::string(key.data(), key.size()));
                }
                ASSIGN_OR_RETURN(std::string field_value, encode_json_to_variant_value(current.value, ctx));
                fields[it_key->second] = std::move(field_value);
            }
            return encode_variant_object_from_map(fields);
        }
    } catch (const vpack::Exception& e) {
        return fromVPackException(e);
    }
    return Status::NotSupported(
            fmt::format("Unsupported JSON type {} for variant encoding type", valueTypeName(slice.type())));
}

// --- Metadata Handling ---

static StatusOr<std::string> build_variant_metadata(VariantEncodingContext* ctx) {
    if (ctx->keys.empty()) {
        return std::string(VariantMetadata::kEmptyMetadata);
    }

    std::vector<std::string_view> sorted_keys;
    sorted_keys.reserve(ctx->keys.size());
    for (const auto& key : ctx->keys) {
        sorted_keys.push_back(key);
    }
    std::sort(sorted_keys.begin(), sorted_keys.end());

    uint32_t total_string_size = 0;
    for (const auto& key : sorted_keys) {
        if (key.size() > std::numeric_limits<uint32_t>::max() - total_string_size) {
            return Status::VariantError("Variant metadata string size overflow");
        }
        total_string_size += static_cast<uint32_t>(key.size());
    }

    const uint32_t dict_size = static_cast<uint32_t>(sorted_keys.size());
    const uint32_t max_value = std::max(dict_size, total_string_size);
    const uint8_t offset_size = minimal_uint_size(max_value);

    uint8_t header = kVariantMetadataVersion;
    header |= kVariantMetadataSortedMask;
    header |= static_cast<uint8_t>((offset_size - 1) << kVariantMetadataOffsetSizeShift);

    std::string metadata;
    metadata.reserve(1 + offset_size * (dict_size + 2) + total_string_size);
    metadata.push_back(static_cast<char>(header));

    append_uint_le(&metadata, dict_size, offset_size);

    uint32_t offset = 0;
    append_uint_le(&metadata, offset, offset_size);
    for (const auto& key : sorted_keys) {
        offset += static_cast<uint32_t>(key.size());
        append_uint_le(&metadata, offset, offset_size);
    }

    for (const auto& key : sorted_keys) {
        metadata.append(key.data(), key.size());
    }

    ctx->key_to_id.reserve(sorted_keys.size());
    for (uint32_t i = 0; i < sorted_keys.size(); ++i) {
        ctx->key_to_id.emplace(sorted_keys[i], i);
    }

    return metadata;
}

// --- Column Processing ---

static bool find_struct_field_idx(const std::vector<std::string>& names, const std::string& target, size_t* idx) {
    for (size_t i = 0; i < names.size(); ++i) {
        if (names[i] == target) {
            *idx = i;
            return true;
        }
    }
    return false;
}

static bool is_shredded_wrapper(const StructColumn* struct_col, size_t* typed_idx, size_t* value_idx) {
    const auto& names = struct_col->field_names();
    if (names.size() != 2) return false;
    return find_struct_field_idx(names, "typed_value", typed_idx) && find_struct_field_idx(names, "value", value_idx);
}

static Status collect_keys_from_column_row(const ColumnPtr& column, const TypeDescriptor& type, size_t row,
                                           VariantEncodingContext* ctx) {
    if (column->is_nullable() && down_cast<const NullableColumn*>(column.get())->is_null(row)) {
        return Status::OK();
    }
    ColumnPtr data_col = ColumnHelper::get_data_column(column);

    switch (type.type) {
    case TYPE_STRUCT: {
        const auto* struct_col = down_cast<const StructColumn*>(data_col.get());
        size_t typed_idx = 0;
        size_t value_idx = 0;
        if (is_shredded_wrapper(struct_col, &typed_idx, &value_idx)) {
            const ColumnPtr& typed_col = struct_col->get_column_by_idx(typed_idx);
            if (typed_col->is_nullable() && down_cast<const NullableColumn*>(typed_col.get())->is_null(row)) {
                return Status::OK();
            }
            size_t type_typed_idx = typed_idx;
            if (!type.field_names.empty()) find_struct_field_idx(type.field_names, "typed_value", &type_typed_idx);
            return collect_keys_from_column_row(typed_col, type.children[type_typed_idx], row, ctx);
        }

        const auto& field_names = !type.field_names.empty() ? type.field_names : struct_col->field_names();
        for (size_t i = 0; i < struct_col->fields_size(); ++i) {
            ctx->keys.insert(field_names[i]);
            RETURN_IF_ERROR(collect_keys_from_column_row(struct_col->get_column_by_idx(i), type.children[i], row, ctx));
        }
        return Status::OK();
    }
    case TYPE_ARRAY: {
        const auto* array_col = down_cast<const ArrayColumn*>(data_col.get());
        const auto* offsets = array_col->offsets_column_raw_ptr();
        const auto& offset_data = offsets->get_data();
        const ColumnPtr& elements = array_col->elements_column();
        for (uint32_t i = offset_data[row]; i < offset_data[row + 1]; ++i) {
            RETURN_IF_ERROR(collect_keys_from_column_row(elements, type.children[0], i, ctx));
        }
        return Status::OK();
    }
    case TYPE_MAP: {
        const auto* map_col = down_cast<const MapColumn*>(data_col.get());
        const auto* offsets = map_col->offsets_column_raw_ptr();
        const auto& offset_data = offsets->get_data();
        const ColumnPtr& keys_col = map_col->keys_column();
        const ColumnPtr& values_col = map_col->values_column();
        const TypeDescriptor& key_type = type.children[0];
        const TypeDescriptor& value_type = type.children[1];
        const TypeInfoPtr key_type_info = get_type_info(key_type);
        for (uint32_t i = offset_data[row]; i < offset_data[row + 1]; ++i) {
            const Datum key_datum = ColumnHelper::get_data_column(keys_col)->get(i);
            if (!key_datum.is_null()) {
                ctx->keys.insert(datum_to_string(key_type_info.get(), key_datum));
            }
            RETURN_IF_ERROR(collect_keys_from_column_row(values_col, value_type, i, ctx));
        }
        return Status::OK();
    }
    case TYPE_JSON: {
        const auto* json_col = down_cast<const JsonColumn*>(data_col.get());
        const JsonValue* json = json_col->get_object(row);
        if (json != nullptr && !json->is_null_or_none()) {
            return collect_json_slice_keys(json->to_vslice(), ctx);
        }
        return Status::OK();
    }
    default:
        return Status::OK();
    }
}

static StatusOr<std::string> encode_value_from_column_row(const ColumnPtr& column, const TypeDescriptor& type,
                                                          size_t row, VariantEncodingContext* ctx) {
    if (column->is_nullable() && down_cast<const NullableColumn*>(column.get())->is_null(row)) {
        return encode_variant_null();
    }
    ColumnPtr data_col = ColumnHelper::get_data_column(column);

    switch (type.type) {
    case TYPE_NULL:
        return encode_variant_null();
#define ENCODE_PRIMITIVE_CASE(LTYPE, CTYPE, VTYPE)                                    \
    case LTYPE: {                                                                     \
        const auto* col = down_cast<const FixedLengthColumn<CTYPE>*>(data_col.get()); \
        return encode_variant_primitive<CTYPE>(VTYPE, col->get_data()[row]);          \
    }
        ENCODE_PRIMITIVE_CASE(TYPE_BOOLEAN, uint8_t,
                              (col->get_data()[row] ? VariantType::BOOLEAN_TRUE : VariantType::BOOLEAN_FALSE))
        ENCODE_PRIMITIVE_CASE(TYPE_TINYINT, int8_t, VariantType::INT8)
        ENCODE_PRIMITIVE_CASE(TYPE_SMALLINT, int16_t, VariantType::INT16)
        ENCODE_PRIMITIVE_CASE(TYPE_INT, int32_t, VariantType::INT32)
        ENCODE_PRIMITIVE_CASE(TYPE_BIGINT, int64_t, VariantType::INT64)
        ENCODE_PRIMITIVE_CASE(TYPE_FLOAT, float, VariantType::FLOAT)
        ENCODE_PRIMITIVE_CASE(TYPE_DOUBLE, double, VariantType::DOUBLE)
#undef ENCODE_PRIMITIVE_CASE
    case TYPE_CHAR:
    case TYPE_VARCHAR: {
        const auto* col = down_cast<const BinaryColumn*>(data_col.get());
        return encode_variant_string(col->get_slice(row));
    }
    case TYPE_VARBINARY: {
        const auto* col = down_cast<const BinaryColumn*>(data_col.get());
        return encode_variant_binary(col->get_slice(row));
    }
    case TYPE_DATE: {
        const auto* col = down_cast<const FixedLengthColumn<DateValue>*>(data_col.get());
        return encode_variant_primitive<int32_t>(VariantType::DATE, col->get_data()[row].to_days_since_unix_epoch());
    }
    case TYPE_DATETIME: {
        const auto* col = down_cast<const FixedLengthColumn<TimestampValue>*>(data_col.get());
        return encode_variant_primitive<int64_t>(VariantType::TIMESTAMP_NTZ,
                                                 col->get_data()[row].to_unix_microsecond());
    }
    case TYPE_TIME: {
        const auto* col = down_cast<const FixedLengthColumn<double>*>(data_col.get());
        return encode_variant_primitive<int64_t>(VariantType::TIME_NTZ,
                                                 static_cast<int64_t>(col->get_data()[row] * USECS_PER_SEC));
    }
    case TYPE_LARGEINT: {
        const auto* col = down_cast<const FixedLengthColumn<int128_t>*>(data_col.get());
        return encode_variant_decimal(VariantType::DECIMAL16, col->get_data()[row], 0);
    }
    case TYPE_DECIMALV2: {
        const auto* col = down_cast<const FixedLengthColumn<DecimalV2Value>*>(data_col.get());
        return encode_variant_decimal(VariantType::DECIMAL16, col->get_data()[row].value(), DecimalV2Value::SCALE);
    }
    case TYPE_DECIMAL32: {
        const auto* col = down_cast<const FixedLengthColumn<int32_t>*>(data_col.get());
        return encode_variant_decimal(VariantType::DECIMAL4, col->get_data()[row], type.scale);
    }
    case TYPE_DECIMAL64: {
        const auto* col = down_cast<const FixedLengthColumn<int64_t>*>(data_col.get());
        return encode_variant_decimal(VariantType::DECIMAL8, col->get_data()[row], type.scale);
    }
    case TYPE_DECIMAL128: {
        const auto* col = down_cast<const FixedLengthColumn<int128_t>*>(data_col.get());
        return encode_variant_decimal(VariantType::DECIMAL16, col->get_data()[row], type.scale);
    }
    case TYPE_JSON: {
        const auto* col = down_cast<const JsonColumn*>(data_col.get());
        const JsonValue* json = col->get_object(row);
        if (json == nullptr || json->is_null_or_none()) return encode_variant_null();
        return encode_json_to_variant_value(json->to_vslice(), ctx);
    }
    case TYPE_ARRAY: {
        const auto* array_col = down_cast<const ArrayColumn*>(data_col.get());
        const auto* offsets = array_col->offsets_column_raw_ptr();
        const auto& offset_data = offsets->get_data();
        const ColumnPtr& elements = array_col->elements_column();
        std::vector<std::string> encoded_elements;
        for (uint32_t i = offset_data[row]; i < offset_data[row + 1]; ++i) {
            ASSIGN_OR_RETURN(auto encoded, encode_value_from_column_row(elements, type.children[0], i, ctx));
            encoded_elements.emplace_back(std::move(encoded));
        }
        return encode_variant_array(encoded_elements);
    }
    case TYPE_MAP: {
        const auto* map_col = down_cast<const MapColumn*>(data_col.get());
        const auto* offsets = map_col->offsets_column_raw_ptr();
        const auto& offset_data = offsets->get_data();
        const ColumnPtr& keys_col = map_col->keys_column();
        const ColumnPtr& values_col = map_col->values_column();
        const TypeDescriptor& key_type = type.children[0];
        const TypeDescriptor& value_type = type.children[1];
        const TypeInfoPtr key_type_info = get_type_info(key_type);
        std::map<uint32_t, std::string> fields;
        for (uint32_t i = offset_data[row]; i < offset_data[row + 1]; ++i) {
            const Datum key_datum = ColumnHelper::get_data_column(keys_col)->get(i);
            if (key_datum.is_null()) continue;
            std::string key_str = datum_to_string(key_type_info.get(), key_datum);
            auto it = ctx->key_to_id.find(key_str);
            if (it == ctx->key_to_id.end()) return Status::VariantError("Variant metadata missing field: " + key_str);
            ASSIGN_OR_RETURN(auto encoded_value, encode_value_from_column_row(values_col, value_type, i, ctx));
            fields[it->second] = std::move(encoded_value);
        }
        return encode_variant_object_from_map(fields);
    }
    case TYPE_STRUCT: {
        const auto* struct_col = down_cast<const StructColumn*>(data_col.get());
        size_t typed_idx = 0;
        size_t value_idx = 0;
        // Check if this is a shredded wrapper struct (has exactly "value" and "typed_value" fields)
        // This pattern is used by Parquet Variant Shredding to store variant data
        if (is_shredded_wrapper(struct_col, &typed_idx, &value_idx)) {
            const ColumnPtr& typed_col = struct_col->get_column_by_idx(typed_idx);
            const ColumnPtr& value_col = struct_col->get_column_by_idx(value_idx);

            const bool has_typed =
                    !(typed_col->is_nullable() && down_cast<const NullableColumn*>(typed_col.get())->is_null(row));
            const bool has_value =
                    !(value_col->is_nullable() && down_cast<const NullableColumn*>(value_col.get())->is_null(row));

            // Per Apache Parquet Variant Shredding Spec:
            // - If typed_value is non-null: encode from typed_value (shredded representation)
            // - Else if value is non-null: use value directly (raw variant encoding)
            // - Else both null: return variant null
            //
            // Special case: For partially shredded objects, both can be non-null.
            // In this case, we need to merge fields from both:
            // - value contains non-shredded fields (raw variant-encoded object)
            // - typed_value contains shredded fields (struct columns)
            if (has_typed && has_value) {
                // Check if typed_value is an object (struct type)
                // Map column index to type index: in case type.field_names has different ordering than struct_col
                // (though for our wrapper structs constructed by the reader, they should match)
                size_t type_typed_idx = typed_idx;
                if (!type.field_names.empty()) find_struct_field_idx(type.field_names, "typed_value", &type_typed_idx);
                const TypeDescriptor& typed_value_type = type.children[type_typed_idx];

                if (typed_value_type.type == TYPE_STRUCT) {
                    // Partially shredded object: merge value and typed_value
                    ColumnPtr value_data = ColumnHelper::get_data_column(value_col);
                    Slice value_slice = down_cast<const BinaryColumn*>(value_data.get())->get_slice(row);

                    if (!value_slice.empty()) {
                        // Decode value to extract non-shredded fields
                        VariantValue value_variant(value_slice);
                        if (value_variant.type() == VariantType::OBJECT) {
                            // Get field IDs from value (non-shredded fields)
                            ASSIGN_OR_RETURN(auto value_obj_info, value_variant.get_object_info());

                            // Build merged field map
                            std::map<uint32_t, std::string> merged_fields;

                            // Add non-shredded fields from value
                            for (uint32_t i = 0; i < value_obj_info.num_elements; ++i) {
                                uint32_t field_id = VariantUtil::read_little_endian_unsigned32(
                                        value_slice.data + value_obj_info.id_start_offset + i * value_obj_info.id_size,
                                        value_obj_info.id_size);

                                uint32_t field_start = VariantUtil::read_little_endian_unsigned32(
                                        value_slice.data + value_obj_info.offset_start_offset +
                                                i * value_obj_info.offset_size,
                                        value_obj_info.offset_size);
                                uint32_t field_end = VariantUtil::read_little_endian_unsigned32(
                                        value_slice.data + value_obj_info.offset_start_offset +
                                                (i + 1) * value_obj_info.offset_size,
                                        value_obj_info.offset_size);

                                const char* field_data =
                                        value_slice.data + value_obj_info.data_start_offset + field_start;
                                size_t field_size = field_end - field_start;
                                merged_fields[field_id] = std::string(field_data, field_size);
                            }

                            // Add shredded fields from typed_value (these override if there's conflict)
                            ColumnPtr typed_data = ColumnHelper::get_data_column(typed_col);
                            const auto* typed_struct = down_cast<const StructColumn*>(typed_data.get());
                            const auto& typed_field_names = !typed_value_type.field_names.empty()
                                                                    ? typed_value_type.field_names
                                                                    : typed_struct->field_names();

                            for (size_t i = 0; i < typed_struct->fields_size(); ++i) {
                                const std::string& field_name = typed_field_names[i];
                                auto it = ctx->key_to_id.find(field_name);
                                if (it == ctx->key_to_id.end()) {
                                    return Status::VariantError("Variant metadata missing field: " + field_name);
                                }
                                ASSIGN_OR_RETURN(auto encoded_value,
                                                 encode_value_from_column_row(typed_struct->get_column_by_idx(i),
                                                                              typed_value_type.children[i], row, ctx));
                                merged_fields[it->second] = std::move(encoded_value);
                            }

                            return encode_variant_object_from_map(merged_fields);
                        }
                    }
                }
                // Fall through to normal typed_value encoding if not a partially shredded object
            }

            if (has_typed) {
                size_t type_typed_idx = typed_idx;
                if (!type.field_names.empty()) find_struct_field_idx(type.field_names, "typed_value", &type_typed_idx);
                return encode_value_from_column_row(typed_col, type.children[type_typed_idx], row, ctx);
            }
            if (has_value) {
                ColumnPtr value_data = ColumnHelper::get_data_column(value_col);
                Slice value_slice = down_cast<const BinaryColumn*>(value_data.get())->get_slice(row);
                if (value_slice.empty()) return encode_variant_null();
                // Return raw variant encoding from value field
                return std::string(value_slice.data, value_slice.size);
            }
            return encode_variant_null();
        }

        const auto& field_names = !type.field_names.empty() ? type.field_names : struct_col->field_names();
        std::map<uint32_t, std::string> fields;
        for (size_t i = 0; i < struct_col->fields_size(); ++i) {
            const std::string& field_name = field_names[i];
            auto it = ctx->key_to_id.find(field_name);
            if (it == ctx->key_to_id.end())
                return Status::VariantError("Variant metadata missing field: " + field_name);
            ASSIGN_OR_RETURN(auto encoded_value, encode_value_from_column_row(struct_col->get_column_by_idx(i),
                                                                              type.children[i], row, ctx));
            fields[it->second] = std::move(encoded_value);
        }
        return encode_variant_object_from_map(fields);
    }
    default:
        return Status::NotSupported("Unsupported variant column type");
    }
}

// --- Public APIs ---

StatusOr<VariantRowValue> VariantEncoder::encode_json_to_variant(const JsonValue& json) {
    vpack::Slice slice = json.to_vslice();
    VariantEncodingContext ctx;
    RETURN_IF_ERROR(collect_json_slice_keys(slice, &ctx));
    ASSIGN_OR_RETURN(auto metadata, build_variant_metadata(&ctx));
    ASSIGN_OR_RETURN(std::string value, encode_json_to_variant_value(slice, &ctx));
    return VariantRowValue::create(metadata, value);
}

StatusOr<VariantRowValue> VariantEncoder::encode_shredded_column_row(const ColumnPtr& column,
                                                                     const TypeDescriptor& type, size_t row,
                                                                     VariantEncodingContext* ctx) {
    if (ctx != nullptr) {
        // Shredded path: Metadata already provided (e.g. from Parquet)
        DCHECK(ctx->metadata_built);
        ASSIGN_OR_RETURN(auto value, encode_value_from_column_row(column, type, row, ctx));
        return VariantRowValue::create(ctx->metadata_raw, value);
    }

    // Discovery path: Need to find keys and build metadata for this row
    VariantEncodingContext local_ctx;
    RETURN_IF_ERROR(collect_keys_from_column_row(column, type, row, &local_ctx));
    ASSIGN_OR_RETURN(std::string metadata, build_variant_metadata(&local_ctx));
    ASSIGN_OR_RETURN(auto value, encode_value_from_column_row(column, type, row, &local_ctx));
    return VariantRowValue::create(metadata, value);
}

template <LogicalType LT, typename EncodeFn>
static Status encode_column_with_viewer(const ColumnPtr& column, ColumnBuilder<TYPE_VARIANT>* builder, bool allow_throw,
                                        EncodeFn&& encode_fn) {
    const size_t num_rows = column->size();
    ColumnPtr data_col = column;
    bool is_const = false;

    if (data_col->is_constant()) {
        const auto* const_col = down_cast<const ConstColumn*>(data_col.get());
        if (const_col->only_null()) {
            builder->append_nulls(static_cast<int>(num_rows));
            return Status::OK();
        }
        data_col = const_col->data_column();
        is_const = true;
    }

    const NullableColumn* nullable =
            data_col->is_nullable() ? down_cast<const NullableColumn*>(data_col.get()) : nullptr;
    if (nullable) data_col = nullable->data_column();

    ColumnViewer<LT> viewer(data_col);
    for (size_t i = 0; i < num_rows; ++i) {
        const size_t row = is_const ? 0 : i;
        if (nullable && nullable->is_null(row)) {
            builder->append_null();
            continue;
        }
        StatusOr<std::string> encoded = encode_fn(viewer, row);
        if (!encoded.ok()) {
            if (allow_throw) return encoded.status();
            builder->append_null();
            continue;
        }
        builder->append(VariantRowValue::create(VariantMetadata::kEmptyMetadata, std::move(encoded.value())).value());
    }
    return Status::OK();
}

Status VariantEncoder::encode_column(const ColumnPtr& column, const TypeDescriptor& type,
                                     ColumnBuilder<TYPE_VARIANT>* builder, bool allow_throw) {
    const size_t num_rows = column->size();
    if (num_rows == 0) return Status::OK();

    // Hot path for complex types (Array/Map/Struct) and JSON
    if (type.type == TYPE_ARRAY || type.type == TYPE_MAP || type.type == TYPE_STRUCT || type.type == TYPE_JSON) {
        ColumnPtr data_col = column;
        bool is_const = false;
        if (data_col->is_constant()) {
            const auto* const_col = down_cast<const ConstColumn*>(data_col.get());
            if (const_col->only_null()) {
                builder->append_nulls(static_cast<int>(num_rows));
                return Status::OK();
            }
            data_col = const_col->data_column();
            is_const = true;
        }
        const NullableColumn* nullable =
                data_col->is_nullable() ? down_cast<const NullableColumn*>(data_col.get()) : nullptr;
        if (nullable) data_col = nullable->data_column();

        VariantEncodingContext ctx;
        if (is_fully_static_type(type)) {
            collect_keys_from_static_type(type, &ctx);
            ASSIGN_OR_RETURN(auto metadata, build_variant_metadata(&ctx));
            for (size_t i = 0; i < num_rows; ++i) {
                const size_t row = is_const ? 0 : i;
                if (nullable && nullable->is_null(row)) {
                    builder->append_null();
                    continue;
                }
                auto res = encode_value_from_column_row(data_col, type, row, &ctx);
                if (!res.ok()) {
                    if (allow_throw) return res.status();
                    builder->append_null();
                    continue;
                }
                builder->append(VariantRowValue::create(metadata, res.value()).value());
            }
        } else {
            for (size_t i = 0; i < num_rows; ++i) {
                const size_t row = is_const ? 0 : i;
                if (nullable && nullable->is_null(row)) {
                    builder->append_null();
                    continue;
                }
                // Reuse encode_shredded_column_row for row-level discovery and encoding
                auto res = VariantEncoder::encode_shredded_column_row(data_col, type, row, nullptr);
                if (!res.ok()) {
                    if (allow_throw) return res.status();
                    builder->append_null();
                    continue;
                }
                builder->append(std::move(res.value()));
            }
        }
        return Status::OK();
    }

    // Fast path for primitive types (No metadata required)
    switch (type.type) {
    case TYPE_NULL:
        builder->append_nulls(num_rows);
        return Status::OK();
    case TYPE_BOOLEAN:
        return encode_column_with_viewer<TYPE_BOOLEAN>(column, builder, allow_throw, [](const auto& v, size_t r) {
            return encode_variant_boolean(v.value(r));
        });
    case TYPE_TINYINT:
        return encode_column_with_viewer<TYPE_TINYINT>(column, builder, allow_throw, [](const auto& v, size_t r) {
            return encode_variant_primitive<int8_t>(VariantType::INT8, v.value(r));
        });
    case TYPE_SMALLINT:
        return encode_column_with_viewer<TYPE_SMALLINT>(column, builder, allow_throw, [](const auto& v, size_t r) {
            return encode_variant_primitive<int16_t>(VariantType::INT16, v.value(r));
        });
    case TYPE_INT:
        return encode_column_with_viewer<TYPE_INT>(column, builder, allow_throw, [](const auto& v, size_t r) {
            return encode_variant_primitive<int32_t>(VariantType::INT32, v.value(r));
        });
    case TYPE_BIGINT:
        return encode_column_with_viewer<TYPE_BIGINT>(column, builder, allow_throw, [](const auto& v, size_t r) {
            return encode_variant_primitive<int64_t>(VariantType::INT64, v.value(r));
        });
    case TYPE_FLOAT:
        return encode_column_with_viewer<TYPE_FLOAT>(column, builder, allow_throw, [](const auto& v, size_t r) {
            return encode_variant_primitive<float>(VariantType::FLOAT, v.value(r));
        });
    case TYPE_DOUBLE:
        return encode_column_with_viewer<TYPE_DOUBLE>(column, builder, allow_throw, [](const auto& v, size_t r) {
            return encode_variant_primitive<double>(VariantType::DOUBLE, v.value(r));
        });
    case TYPE_LARGEINT:
        return encode_column_with_viewer<TYPE_LARGEINT>(column, builder, allow_throw, [](const auto& v, size_t r) {
            return encode_variant_decimal(VariantType::DECIMAL16, v.value(r), 0);
        });
    case TYPE_DECIMALV2:
        return encode_column_with_viewer<TYPE_DECIMALV2>(column, builder, allow_throw, [](const auto& v, size_t r) {
            return encode_variant_decimal(VariantType::DECIMAL16, v.value(r).value(), DecimalV2Value::SCALE);
        });
    case TYPE_DECIMAL32:
        return encode_column_with_viewer<TYPE_DECIMAL32>(column, builder, allow_throw, [&](const auto& v, size_t r) {
            return encode_variant_decimal(VariantType::DECIMAL4, v.value(r), type.scale);
        });
    case TYPE_DECIMAL64:
        return encode_column_with_viewer<TYPE_DECIMAL64>(column, builder, allow_throw, [&](const auto& v, size_t r) {
            return encode_variant_decimal(VariantType::DECIMAL8, v.value(r), type.scale);
        });
    case TYPE_DECIMAL128:
        return encode_column_with_viewer<TYPE_DECIMAL128>(column, builder, allow_throw, [&](const auto& v, size_t r) {
            return encode_variant_decimal(VariantType::DECIMAL16, v.value(r), type.scale);
        });
    case TYPE_CHAR:
    case TYPE_VARCHAR:
        return encode_column_with_viewer<TYPE_VARCHAR>(column, builder, allow_throw, [](const auto& v, size_t r) {
            return encode_variant_string(v.value(r));
        });
    case TYPE_DATE:
        return encode_column_with_viewer<TYPE_DATE>(column, builder, allow_throw, [](const auto& v, size_t r) {
            return encode_variant_primitive<int32_t>(VariantType::DATE, v.value(r).to_days_since_unix_epoch());
        });
    case TYPE_DATETIME:
        return encode_column_with_viewer<TYPE_DATETIME>(column, builder, allow_throw, [](const auto& v, size_t r) {
            return encode_variant_primitive<int64_t>(VariantType::TIMESTAMP_NTZ, v.value(r).to_unix_microsecond());
        });
    case TYPE_TIME:
        return encode_column_with_viewer<TYPE_TIME>(column, builder, allow_throw, [](const auto& v, size_t r) {
            return encode_variant_primitive<int64_t>(VariantType::TIME_NTZ,
                                                     static_cast<int64_t>(v.value(r) * USECS_PER_SEC));
        });
    default:
        if (allow_throw) return Status::NotSupported("Unsupported variant cast: " + type.debug_string());
        builder->append_nulls(num_rows);
        return Status::OK();
    }
}

} // namespace starrocks
