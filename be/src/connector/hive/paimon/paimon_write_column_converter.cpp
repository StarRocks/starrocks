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

#include "connector/hive/paimon/paimon_write_column_converter.h"

#include <arrow/array.h>
#include <arrow/builder.h>
#include <arrow/memory_pool.h>
#include <arrow/record_batch.h>
#include <arrow/type.h>
#include <arrow/util/key_value_metadata.h>
#include <cctz/time_zone.h>

#include <algorithm>
#include <charconv>
#include <chrono>
#include <cstdint>
#include <limits>
#include <string_view>
#include <unordered_set>
#include <utility>

#include "base/string/utf8_check.h"
#include "base/time/timezone_utils.h"
#include "column/array_column.h"
#include "column/binary_column.h"
#include "column/chunk.h"
#include "column/const_column.h"
#include "column/decimalv3_column.h"
#include "column/fixed_length_column.h"
#include "column/map_column.h"
#include "column/nullable_column.h"
#include "column/struct_column.h"
#include "column/variant_column.h"
#include "exprs/expr.h"
#include "exprs/expr_context.h"
#include "types/decimalv2_value.h"
#include "types/timestamp_value.h"
#include "types/variant_value.h"

namespace starrocks {
namespace {

constexpr size_t kMaxDepth = 128;
constexpr size_t kMaxVariantWork = 1 << 20;
constexpr size_t kMaxVariantBytes = 128 << 20;

Status from_arrow(const arrow::Status& status) {
    return status.ok() ? Status::OK() : Status::InvalidArgument(status.ToString());
}

std::string folded_name(std::string_view name) {
    std::string result(name);
    for (char& c : result) {
        if (c >= 'A' && c <= 'Z') c += 'a' - 'A';
    }
    return result;
}

// All callers establish the byte range before using this existing little-endian helper.
uint32_t read_uint(std::string_view bytes, size_t offset, uint8_t width) {
    return VariantUtil::read_little_endian_unsigned32(bytes.data() + offset, width);
}

// VariantRowValue::validate_metadata checks the version/minimum size, but does not validate the
// dictionary offsets, UTF-8, uniqueness, or trailing bytes. Do those before using any accessor.
StatusOr<std::vector<std::string_view>> validate_variant_metadata(std::string_view metadata) {
    RETURN_IF_ERROR(VariantRowValue::validate_metadata(metadata));
    const uint8_t header = static_cast<uint8_t>(metadata[0]);
    if ((header & 0x20) != 0) return Status::InvalidArgument("Reserved VARIANT metadata header bit");
    const uint8_t width = (header >> 6) + 1;
    if (metadata.size() < size_t{1} + width) return Status::InvalidArgument("Truncated VARIANT dictionary size");
    const uint32_t count = read_uint(metadata, 1, width);
    if (count > kMaxVariantWork) return Status::InvalidArgument("VARIANT dictionary work limit exceeded");
    const size_t data_start = 1 + size_t{width} * (size_t{count} + 2);
    if (data_start > metadata.size()) return Status::InvalidArgument("Truncated VARIANT dictionary offsets");
    if (read_uint(metadata, 1 + width, width) != 0 ||
        read_uint(metadata, 1 + size_t{width} * (size_t{count} + 1), width) != metadata.size() - data_start) {
        return Status::InvalidArgument("Invalid VARIANT dictionary extent");
    }
    std::vector<std::string_view> keys;
    keys.reserve(count);
    std::unordered_set<std::string_view> unique;
    for (uint32_t i = 0; i < count; ++i) {
        const uint32_t begin = read_uint(metadata, 1 + size_t{width} * (i + 1), width);
        const uint32_t end = read_uint(metadata, 1 + size_t{width} * (size_t{i} + 2), width);
        if (end < begin || end > metadata.size() - data_start) {
            return Status::InvalidArgument("Invalid VARIANT dictionary offset");
        }
        const auto key = metadata.substr(data_start + begin, end - begin);
        if (!validate_utf8(key.data(), key.size()) || !unique.emplace(key).second ||
            ((header & 0x10) != 0 && !keys.empty() && keys.back() >= key)) {
            return Status::InvalidArgument("Invalid VARIANT dictionary string or ordering");
        }
        keys.emplace_back(key);
    }
    return keys;
}

// Validate exact public bytes, not JSON or a narrowed decimal representation. In objects the
// field-id order is lexical, but payload offsets may be nonmonotonic: sort offsets only to find
// the physical extent of each child, without rewriting either ids, offsets, or payload bytes.
Status validate_variant_value(std::string_view raw, const std::vector<std::string_view>& keys, size_t depth,
                              size_t* work) {
    if (depth > kMaxDepth || *work == 0) return Status::InvalidArgument("VARIANT nesting/work limit exceeded");
    --*work;
    if (raw.empty()) return Status::InvalidArgument("Empty VARIANT value");
    const uint8_t header = static_cast<uint8_t>(raw[0]);
    const uint8_t basic = header & 3;
    const uint8_t info = header >> 2;
    if (basic == 1) {
        if (raw.size() != size_t{info} + 1 || !validate_utf8(raw.data() + 1, info)) {
            return Status::InvalidArgument("Invalid VARIANT short string");
        }
        return Status::OK();
    }
    if (basic == 0) {
        size_t expected = 0;
        switch (info) {
        case 0:
        case 1:
        case 2:
            expected = 1;
            break;
        case 3:
            expected = 2;
            break;
        case 4:
            expected = 3;
            break;
        case 5:
        case 11:
        case 14:
            expected = 5;
            break;
        case 6:
        case 7:
        case 12:
        case 13:
            expected = 9;
            break;
        case 8:
        case 9:
        case 10:
            expected = info == 8 ? 6 : (info == 9 ? 10 : 18);
            if (raw.size() < 2 || static_cast<uint8_t>(raw[1]) > (info == 8 ? 9 : (info == 9 ? 18 : 38))) {
                return Status::InvalidArgument("Invalid VARIANT decimal scale");
            }
            // Deliberately do not constrain the signed integer payload to decimal(38). Existing
            // DECIMAL16 bytes can contain the full signed int128 range and must remain exact.
            break;
        case 15:
        case 16: {
            if (raw.size() < 5) return Status::InvalidArgument("Truncated VARIANT binary/string length");
            expected = size_t{5} + read_uint(raw, 1, 4);
            if (expected != raw.size() || (info == 16 && !validate_utf8(raw.data() + 5, raw.size() - 5))) {
                return Status::InvalidArgument("Invalid VARIANT binary/string extent");
            }
            break;
        }
        case 20:
            expected = 17;
            break;
        default:
            // StarRocks also has TIME_NTZ and nanosecond timestamp tags. Public Paimon 0.3
            // does not support them; never silently reinterpret them as another primitive.
            return Status::NotSupported("VARIANT primitive is not supported by public Paimon 0.3");
        }
        return raw.size() == expected ? Status::OK() : Status::InvalidArgument("Invalid VARIANT primitive extent");
    }

    const bool object = basic == 2;
    if ((object && (info & 0x20) != 0) || (!object && (info & 0x38) != 0)) {
        return Status::InvalidArgument("Reserved VARIANT container header bits");
    }
    uint32_t count;
    size_t offsets_start;
    size_t data_start;
    uint8_t width;
    uint8_t id_width = 0;
    size_t ids_start = 0;
    const VariantValue value(raw);
    if (object) {
        ASSIGN_OR_RETURN(const auto shape, value.get_object_info());
        count = shape.num_elements;
        offsets_start = shape.offset_start_offset;
        data_start = shape.data_start_offset;
        width = shape.offset_size;
        id_width = shape.id_size;
        ids_start = shape.id_start_offset;
    } else {
        ASSIGN_OR_RETURN(const auto shape, value.get_array_info());
        count = shape.num_elements;
        offsets_start = shape.offset_start_offset;
        data_start = shape.data_start_offset;
        width = shape.offset_size;
    }
    if (count > *work) return Status::InvalidArgument("VARIANT container work limit exceeded");
    const uint32_t total = read_uint(raw, offsets_start + size_t{count} * width, width);
    if (total != raw.size() - data_start) return Status::InvalidArgument("Invalid VARIANT container extent");
    std::vector<uint32_t> offsets;
    offsets.reserve(size_t{count} + 1);
    std::string_view previous_key;
    for (uint32_t i = 0; i < count; ++i) {
        if (object) {
            const uint32_t id = read_uint(raw, ids_start + size_t{i} * id_width, id_width);
            if (id >= keys.size() || (i > 0 && previous_key >= keys[id])) {
                return Status::InvalidArgument("Invalid VARIANT object field id/order");
            }
            previous_key = keys[id];
        }
        const uint32_t offset = read_uint(raw, offsets_start + size_t{i} * width, width);
        if (offset >= total || (!object && !offsets.empty() && offsets.back() >= offset)) {
            return Status::InvalidArgument("Invalid VARIANT child offset");
        }
        offsets.emplace_back(offset);
    }
    if (object) std::sort(offsets.begin(), offsets.end());
    if ((count == 0 && total != 0) || (count > 0 && offsets.front() != 0)) {
        return Status::InvalidArgument("Invalid VARIANT first payload offset");
    }
    offsets.emplace_back(total);
    for (uint32_t i = 0; i < count; ++i) {
        if (offsets[i] >= offsets[i + 1]) return Status::InvalidArgument("Overlapping VARIANT payload offsets");
        RETURN_IF_ERROR(validate_variant_value(raw.substr(data_start + offsets[i], offsets[i + 1] - offsets[i]), keys,
                                               depth + 1, work));
    }
    return Status::OK();
}

Status validate_variant(std::string_view metadata, std::string_view value) {
    if (metadata.size() > kMaxVariantBytes || value.size() > kMaxVariantBytes) {
        return Status::InvalidArgument("VARIANT byte limit exceeded");
    }
    ASSIGN_OR_RETURN(const auto keys, validate_variant_metadata(metadata));
    size_t work = kMaxVariantWork - keys.size();
    return validate_variant_value(value, keys, 0, &work);
}

// Do not inspect nullable payloads to determine row count: their children may be inaccessible
// under a NULL ancestor. StructColumn::size() itself dereferences its first child.
StatusOr<size_t> logical_size(const Column* column) {
    if (column == nullptr) return Status::InvalidArgument("Missing source column");
    if (column->is_constant()) return column->size();
    if (const auto* nullable = dynamic_cast<const NullableColumn*>(column)) return nullable->null_column()->size();
    if (const auto* array = dynamic_cast<const ArrayColumn*>(column)) {
        if (array->offsets().size() == 0) return Status::InvalidArgument("Missing ARRAY offsets");
        return array->offsets().size() - 1;
    }
    if (const auto* map = dynamic_cast<const MapColumn*>(column)) {
        if (map->offsets().size() == 0) return Status::InvalidArgument("Missing MAP offsets");
        return map->offsets().size() - 1;
    }
    if (const auto* structure = dynamic_cast<const StructColumn*>(column)) {
        if (structure->fields_size() == 0) return Status::InvalidArgument("Empty source STRUCT shape");
        return logical_size(structure->field_column_raw_ptr(0));
    }
    if (const auto* variant = dynamic_cast<const VariantColumn*>(column)) {
        if (variant->has_metadata_column()) return variant->metadata_column()->size();
        if (!variant->typed_columns().empty()) return logical_size(variant->typed_columns()[0].get());
        return size_t{0};
    }
    return column->size();
}

struct Cell {
    const Column* data;
    size_t row;
    bool is_null;
};

StatusOr<Cell> resolve_cell(const Column* column, size_t row) {
    for (size_t depth = 0; depth <= kMaxDepth; ++depth) {
        ASSIGN_OR_RETURN(const size_t size, logical_size(column));
        if (row >= size) return Status::InvalidArgument("Source row outside column bounds");
        if (const auto* constant = dynamic_cast<const ConstColumn*>(column)) {
            column = constant->data_column().get();
            row = 0;
        } else if (const auto* nullable = dynamic_cast<const NullableColumn*>(column)) {
            if (nullable->has_null() && nullable->immutable_null_column_data()[row] != 0) {
                return Cell{nullptr, row, true};
            }
            column = nullable->data_column().get();
        } else {
            return Cell{column, row, column->only_null()};
        }
    }
    return Status::InvalidArgument("Source column wrapper nesting limit exceeded");
}

struct Conversion {
    TypeDescriptor source;
    std::shared_ptr<arrow::Field> target;
    std::unique_ptr<arrow::ArrayBuilder> builder;
    std::vector<Conversion> children;
    std::vector<size_t> source_fields;
    int64_t length = -1;
    int timestamp_precision = -1;
    cctz::time_zone session_zone;
};

StatusOr<Conversion> make_conversion(const TypeDescriptor& source, const std::shared_ptr<arrow::Field>& target,
                                     const std::string& time_zone, arrow::MemoryPool* pool, size_t depth = 0) {
    if (target == nullptr || target->type() == nullptr || depth > kMaxDepth) {
        return Status::InvalidArgument("Invalid or excessively nested Paimon target field");
    }
    Conversion result;
    result.source = source;
    result.target = target;
    const auto id = target->type()->id();
    bool compatible = false;
    switch (source.type) {
    case TYPE_NULL:
        compatible = true;
        break;
    case TYPE_BOOLEAN:
        compatible = id == arrow::Type::BOOL;
        break;
    case TYPE_TINYINT:
        compatible = id == arrow::Type::INT8;
        break;
    case TYPE_SMALLINT:
        compatible = id == arrow::Type::INT16;
        break;
    case TYPE_INT:
        compatible = id == arrow::Type::INT32;
        break;
    case TYPE_BIGINT:
        compatible = id == arrow::Type::INT64;
        break;
    case TYPE_FLOAT:
        compatible = id == arrow::Type::FLOAT;
        break;
    case TYPE_DOUBLE:
        compatible = id == arrow::Type::DOUBLE;
        break;
    case TYPE_CHAR:
    case TYPE_VARCHAR:
    case TYPE_VARBINARY: {
        compatible = id == (source.type == TYPE_VARBINARY ? arrow::Type::BINARY : arrow::Type::STRING);
        result.length = source.len;
        if (target->metadata() != nullptr) {
            const int index = target->metadata()->FindKey("starrocks.paimon.length");
            if (index >= 0) {
                const auto& text = target->metadata()->value(index);
                int64_t bound;
                const auto parsed = std::from_chars(text.data(), text.data() + text.size(), bound);
                if (parsed.ec != std::errc() || parsed.ptr != text.data() + text.size() || bound <= 0) {
                    return Status::InvalidArgument("Invalid starrocks.paimon.length metadata");
                }
                result.length = source.len > 0 ? std::min<int64_t>(source.len, bound) : bound;
            }
        }
        break;
    }
    case TYPE_DATE:
        compatible = id == arrow::Type::DATE32;
        break;
    case TYPE_DATETIME:
        compatible = id == arrow::Type::TIMESTAMP;
        if (compatible) {
            const auto& timestamp = static_cast<const arrow::TimestampType&>(*target->type());
            if (target->metadata()) {
                const int index = target->metadata()->FindKey("starrocks.paimon.timestamp.precision");
                if (index >= 0) {
                    const auto& text = target->metadata()->value(index);
                    int precision = -1;
                    auto parsed = std::from_chars(text.data(), text.data() + text.size(), precision);
                    if (parsed.ec != std::errc() || parsed.ptr != text.data() + text.size() || precision < 0 ||
                        precision > 9) {
                        return Status::InvalidArgument("Invalid native Paimon timestamp precision");
                    }
                    const auto unit = precision == 0 ? arrow::TimeUnit::SECOND
                                                     : precision <= 3 ? arrow::TimeUnit::MILLI
                                                                      : precision <= 6 ? arrow::TimeUnit::MICRO
                                                                                       : arrow::TimeUnit::NANO;
                    if (timestamp.unit() != unit) {
                        return Status::InvalidArgument("Native Paimon timestamp precision and Arrow unit disagree");
                    }
                    result.timestamp_precision = precision;
                }
            }
            if (!timestamp.timezone().empty()) {
                cctz::time_zone target_zone;
                if (!TimezoneUtils::find_cctz_time_zone(timestamp.timezone(), target_zone) ||
                    !TimezoneUtils::find_cctz_time_zone(time_zone, result.session_zone)) {
                    return Status::InvalidArgument("Invalid target/session timestamp timezone");
                }
            }
        }
        break;
    case TYPE_DECIMALV2:
    case TYPE_DECIMAL32:
    case TYPE_DECIMAL64:
    case TYPE_DECIMAL128:
    case TYPE_LARGEINT:
        compatible = id == arrow::Type::DECIMAL128;
        if (compatible) {
            const auto& decimal = static_cast<const arrow::Decimal128Type&>(*target->type());
            const int precision = source.type == TYPE_LARGEINT ? 38 : source.precision;
            const int scale = source.type == TYPE_LARGEINT ? 0 : source.scale;
            compatible = decimal.precision() == precision && decimal.scale() == scale &&
                         (source.type != TYPE_DECIMALV2 || scale == DecimalV2Value::SCALE);
        }
        break;
    case TYPE_ARRAY:
        compatible = id == arrow::Type::LIST && source.children.size() == 1;
        if (compatible) {
            const auto& list = static_cast<const arrow::ListType&>(*target->type());
            ASSIGN_OR_RETURN(auto child,
                             make_conversion(source.children[0], list.value_field(), time_zone, pool, depth + 1));
            result.children.emplace_back(std::move(child));
        }
        break;
    case TYPE_MAP:
        compatible = id == arrow::Type::MAP && source.children.size() == 2;
        if (compatible) {
            const auto& map = static_cast<const arrow::MapType&>(*target->type());
            // StarRocks MAP has no sorted-key guarantee; never advertise one without sorting.
            if (map.key_field()->nullable() || map.keys_sorted()) {
                return Status::NotSupported("Nullable or sorted Paimon MAP keys are not supported");
            }
            ASSIGN_OR_RETURN(auto key,
                             make_conversion(source.children[0], map.key_field(), time_zone, pool, depth + 1));
            ASSIGN_OR_RETURN(auto item,
                             make_conversion(source.children[1], map.item_field(), time_zone, pool, depth + 1));
            result.children.emplace_back(std::move(key));
            result.children.emplace_back(std::move(item));
        }
        break;
    case TYPE_STRUCT:
        compatible = id == arrow::Type::STRUCT && source.children.size() == source.field_names.size();
        if (compatible) {
            const auto& structure = static_cast<const arrow::StructType&>(*target->type());
            if (structure.num_fields() != source.children.size()) {
                return Status::InvalidArgument("Paimon STRUCT field count mismatch");
            }
            std::unordered_set<std::string> source_names;
            for (const auto& name : source.field_names) {
                if (!source_names.emplace(folded_name(name)).second) {
                    return Status::InvalidArgument("Ambiguous case-insensitive source STRUCT fields");
                }
            }
            std::unordered_set<std::string> target_names;
            for (const auto& field : structure.fields()) {
                const auto name = folded_name(field->name());
                if (!target_names.emplace(name).second) {
                    return Status::InvalidArgument("Ambiguous case-insensitive target STRUCT fields");
                }
                size_t index = 0;
                while (index < source.field_names.size() && folded_name(source.field_names[index]) != name) ++index;
                if (index == source.field_names.size()) return Status::InvalidArgument("Missing Paimon STRUCT field");
                ASSIGN_OR_RETURN(auto child,
                                 make_conversion(source.children[index], field, time_zone, pool, depth + 1));
                result.source_fields.emplace_back(index);
                result.children.emplace_back(std::move(child));
            }
        }
        break;
    case TYPE_VARIANT:
        compatible = id == arrow::Type::STRUCT;
        if (compatible) {
            const auto& structure = static_cast<const arrow::StructType&>(*target->type());
            const auto metadata = target->metadata();
            const int marker = metadata == nullptr ? -1 : metadata->FindKey("paimon.extension.type");
            compatible = structure.num_fields() == 2 && structure.field(0)->name() == "value" &&
                         structure.field(1)->name() == "metadata" &&
                         structure.field(0)->type()->id() == arrow::Type::BINARY &&
                         structure.field(1)->type()->id() == arrow::Type::BINARY && !structure.field(0)->nullable() &&
                         !structure.field(1)->nullable() && marker >= 0 &&
                         metadata->value(marker) == "paimon.type.variant";
        }
        break;
    default:
        return Status::NotSupported("Unsupported StarRocks type for native Paimon write: " + source.debug_string());
    }
    if (!compatible) return Status::InvalidArgument("Source type does not match authoritative Paimon Arrow field");
    if (depth == 0) RETURN_IF_ERROR(from_arrow(arrow::MakeBuilder(pool, target->type(), &result.builder)));
    return result;
}

// Append a valid, non-null placeholder without reading any source. In particular Arrow's
// StructBuilder::AppendNull alone is insufficient: required nested children must not gain nulls.
Status append_placeholder(arrow::ArrayBuilder* builder) {
    switch (builder->type()->id()) {
    case arrow::Type::STRUCT: {
        auto* structure = static_cast<arrow::StructBuilder*>(builder);
        RETURN_IF_ERROR(from_arrow(structure->Append()));
        for (int i = 0; i < structure->num_fields(); ++i) {
            RETURN_IF_ERROR(append_placeholder(structure->field_builder(i)));
        }
        return Status::OK();
    }
    case arrow::Type::LIST:
        return from_arrow(static_cast<arrow::ListBuilder*>(builder)->Append());
    case arrow::Type::MAP:
        return from_arrow(static_cast<arrow::MapBuilder*>(builder)->Append());
    default:
        return from_arrow(builder->AppendEmptyValue());
    }
}

Status append_null(arrow::ArrayBuilder* builder) {
    if (builder->type()->id() == arrow::Type::STRUCT) {
        auto* structure = static_cast<arrow::StructBuilder*>(builder);
        RETURN_IF_ERROR(from_arrow(structure->Append(false)));
        for (int i = 0; i < structure->num_fields(); ++i) {
            RETURN_IF_ERROR(append_placeholder(structure->field_builder(i)));
        }
        return Status::OK();
    }
    return from_arrow(builder->AppendNull());
}

Status append_inactive(const Conversion& conversion, arrow::ArrayBuilder* builder, bool is_null) {
    if (conversion.source.type == TYPE_VARIANT) {
        auto* structure = static_cast<arrow::StructBuilder*>(builder);
        RETURN_IF_ERROR(from_arrow(structure->Append(!is_null)));
        RETURN_IF_ERROR(from_arrow(
                static_cast<arrow::BinaryBuilder*>(structure->field_builder(0))->Append(VariantValue::kEmptyValue)));
        return from_arrow(static_cast<arrow::BinaryBuilder*>(structure->field_builder(1))
                                  ->Append(VariantMetadata::kEmptyMetadata));
    }
    if (conversion.source.type == TYPE_STRUCT) {
        auto* structure = static_cast<arrow::StructBuilder*>(builder);
        RETURN_IF_ERROR(from_arrow(structure->Append(!is_null)));
        for (size_t i = 0; i < conversion.children.size(); ++i) {
            RETURN_IF_ERROR(append_inactive(conversion.children[i], structure->field_builder(i), false));
        }
        return Status::OK();
    }
    return is_null ? append_null(builder) : append_placeholder(builder);
}

template <typename Source, typename Builder>
Status append_number(const Cell& cell, arrow::ArrayBuilder* builder) {
    const auto* column = dynamic_cast<const Source*>(cell.data);
    if (column == nullptr) return Status::InvalidArgument("Source scalar physical column type mismatch");
    return from_arrow(static_cast<Builder*>(builder)->Append(column->immutable_data()[cell.row]));
}

StatusOr<int64_t> timestamp_value(const Cell& cell, const Conversion& conversion) {
    const auto* column = dynamic_cast<const TimestampColumn*>(cell.data);
    if (column == nullptr) return Status::InvalidArgument("Source DATETIME physical column type mismatch");
    const auto value = column->immutable_data()[cell.row];
    if (!value.is_valid()) return Status::InvalidArgument("Invalid source DATETIME");
    const auto& target = static_cast<const arrow::TimestampType&>(*conversion.target->type());
    int64_t micros = value.to_unix_microsecond();
    if (!target.timezone().empty()) {
        int year, month, day, hour, minute, second, usec;
        value.to_timestamp(&year, &month, &day, &hour, &minute, &second, &usec);
        // Target timezone marks an instant, not another civil wall clock. Resolve the source in
        // the session zone using historical/DST rules; UTC/fixed-offset arithmetic is not enough.
        const auto lookup = conversion.session_zone.lookup(cctz::civil_second(year, month, day, hour, minute, second));
        if (lookup.kind == cctz::time_zone::civil_lookup::SKIPPED) {
            return Status::InvalidArgument("DATETIME falls in a skipped session-timezone interval");
        }
        const int64_t seconds = std::chrono::duration_cast<std::chrono::seconds>(lookup.pre.time_since_epoch()).count();
        if (__builtin_mul_overflow(seconds, int64_t{1000000}, &micros) ||
            __builtin_add_overflow(micros, int64_t{usec}, &micros)) {
            return Status::InvalidArgument("Paimon timestamp microsecond overflow");
        }
    }
    if (conversion.timestamp_precision >= 0 && conversion.timestamp_precision < 6) {
        int64_t tick = 1;
        for (int i = conversion.timestamp_precision; i < 6; ++i) tick *= 10;
        if (micros % tick != 0) {
            return Status::InvalidArgument("DATETIME loses fractional precision in the native Paimon target");
        }
    }
    switch (target.unit()) {
    case arrow::TimeUnit::SECOND:
        return micros / 1000000 - (micros % 1000000 < 0);
    case arrow::TimeUnit::MILLI:
        return micros / 1000 - (micros % 1000 < 0);
    case arrow::TimeUnit::MICRO:
        return micros;
    case arrow::TimeUnit::NANO: {
        int64_t nanos;
        if (__builtin_mul_overflow(micros, int64_t{1000}, &nanos)) {
            return Status::InvalidArgument("Paimon timestamp nanosecond overflow");
        }
        return nanos;
    }
    }
    return Status::InvalidArgument("Unsupported Paimon timestamp unit");
}

Status append_decimal(const Cell& cell, const Conversion& conversion, arrow::ArrayBuilder* builder) {
    int128_t value;
    switch (conversion.source.type) {
    case TYPE_DECIMAL32: {
        const auto* column = dynamic_cast<const Decimal32Column*>(cell.data);
        if (column == nullptr || column->scale() != conversion.source.scale ||
            column->precision() != conversion.source.precision) {
            return Status::InvalidArgument("Source DECIMAL32 physical type/precision/scale mismatch");
        }
        value = column->immutable_data()[cell.row];
        break;
    }
    case TYPE_DECIMAL64: {
        const auto* column = dynamic_cast<const Decimal64Column*>(cell.data);
        if (column == nullptr || column->scale() != conversion.source.scale ||
            column->precision() != conversion.source.precision) {
            return Status::InvalidArgument("Source DECIMAL64 physical type/precision/scale mismatch");
        }
        value = column->immutable_data()[cell.row];
        break;
    }
    case TYPE_DECIMAL128: {
        const auto* column = dynamic_cast<const Decimal128Column*>(cell.data);
        if (column == nullptr || column->scale() != conversion.source.scale ||
            column->precision() != conversion.source.precision) {
            return Status::InvalidArgument("Source DECIMAL128 physical type/precision/scale mismatch");
        }
        value = column->immutable_data()[cell.row];
        break;
    }
    case TYPE_DECIMALV2: {
        const auto* column = dynamic_cast<const DecimalColumn*>(cell.data);
        if (column == nullptr) return Status::InvalidArgument("Source DECIMALV2 physical column type mismatch");
        value = column->immutable_data()[cell.row].value();
        break;
    }
    default: {
        const auto* column = dynamic_cast<const Int128Column*>(cell.data);
        if (column == nullptr) return Status::InvalidArgument("Source LARGEINT physical column type mismatch");
        value = column->immutable_data()[cell.row];
        break;
    }
    }
    const arrow::Decimal128 decimal(static_cast<int64_t>(value >> 64), static_cast<uint64_t>(value));
    const auto& target = static_cast<const arrow::Decimal128Type&>(*conversion.target->type());
    if (!decimal.FitsInPrecision(target.precision())) {
        return Status::InvalidArgument("Paimon decimal precision overflow");
    }
    return from_arrow(static_cast<arrow::Decimal128Builder*>(builder)->Append(decimal));
}

Status append_variant(const Cell& cell, arrow::ArrayBuilder* builder) {
    const auto* column = dynamic_cast<const VariantColumn*>(cell.data);
    if (column == nullptr) return Status::InvalidArgument("Source VARIANT physical column type mismatch");
    // Never use query-side try_materialize_row: a failed overlay merge there may fall
    // back to a partial residual. Strict materialization validates the base before overlays.
    ASSIGN_OR_RETURN(auto materialized, column->materialize_row_strict(cell.row));
    const auto metadata = materialized.get_metadata().raw();
    const auto value = materialized.get_value().raw();
    RETURN_IF_ERROR(validate_variant(metadata, value));
    auto* structure = static_cast<arrow::StructBuilder*>(builder);
    RETURN_IF_ERROR(from_arrow(structure->Append()));
    RETURN_IF_ERROR(from_arrow(
            static_cast<arrow::BinaryBuilder*>(structure->field_builder(0))->Append(value.data(), value.size())));
    return from_arrow(
            static_cast<arrow::BinaryBuilder*>(structure->field_builder(1))->Append(metadata.data(), metadata.size()));
}

Status append_cell(const Column* column, size_t row, const Conversion& conversion, arrow::ArrayBuilder* builder) {
    ASSIGN_OR_RETURN(const auto cell, resolve_cell(column, row));
    if (cell.is_null || conversion.source.type == TYPE_NULL) {
        if (!conversion.target->nullable()) return Status::InvalidArgument("SQL NULL in required Paimon field");
        return append_inactive(conversion, builder, true);
    }
    switch (conversion.source.type) {
    case TYPE_BOOLEAN:
        return append_number<BooleanColumn, arrow::BooleanBuilder>(cell, builder);
    case TYPE_TINYINT:
        return append_number<Int8Column, arrow::Int8Builder>(cell, builder);
    case TYPE_SMALLINT:
        return append_number<Int16Column, arrow::Int16Builder>(cell, builder);
    case TYPE_INT:
        return append_number<Int32Column, arrow::Int32Builder>(cell, builder);
    case TYPE_BIGINT:
        return append_number<Int64Column, arrow::Int64Builder>(cell, builder);
    case TYPE_FLOAT:
        return append_number<FloatColumn, arrow::FloatBuilder>(cell, builder);
    case TYPE_DOUBLE:
        return append_number<DoubleColumn, arrow::DoubleBuilder>(cell, builder);
    case TYPE_CHAR:
    case TYPE_VARCHAR:
    case TYPE_VARBINARY: {
        const auto* binary = dynamic_cast<const BinaryColumn*>(cell.data);
        if (binary == nullptr) return Status::InvalidArgument("Source binary physical column type mismatch");
        const auto value = binary->get_slice(cell.row);
        if ((conversion.length >= 0 && value.size > conversion.length) ||
            value.size > std::numeric_limits<int32_t>::max()) {
            return Status::InvalidArgument("Paimon string/binary byte length exceeded");
        }
        if (conversion.source.type != TYPE_VARBINARY && !validate_utf8(value.data, value.size)) {
            return Status::InvalidArgument("Invalid UTF-8 in Paimon string");
        }
        if (conversion.source.type == TYPE_VARBINARY) {
            return from_arrow(static_cast<arrow::BinaryBuilder*>(builder)->Append(value.data, value.size));
        }
        return from_arrow(static_cast<arrow::StringBuilder*>(builder)->Append(value.data, value.size));
    }
    case TYPE_DATE: {
        const auto* dates = dynamic_cast<const DateColumn*>(cell.data);
        if (dates == nullptr) return Status::InvalidArgument("Source DATE physical column type mismatch");
        const auto value = dates->immutable_data()[cell.row];
        if (!value.is_valid()) return Status::InvalidArgument("Invalid source DATE");
        return from_arrow(static_cast<arrow::Date32Builder*>(builder)->Append(value.to_days_since_unix_epoch()));
    }
    case TYPE_DATETIME: {
        ASSIGN_OR_RETURN(const auto value, timestamp_value(cell, conversion));
        return from_arrow(static_cast<arrow::TimestampBuilder*>(builder)->Append(value));
    }
    case TYPE_DECIMALV2:
    case TYPE_DECIMAL32:
    case TYPE_DECIMAL64:
    case TYPE_DECIMAL128:
    case TYPE_LARGEINT:
        return append_decimal(cell, conversion, builder);
    case TYPE_ARRAY: {
        const auto* array = dynamic_cast<const ArrayColumn*>(cell.data);
        if (array == nullptr) return Status::InvalidArgument("Source ARRAY physical column type mismatch");
        const auto offsets = array->offsets().immutable_data();
        const uint32_t begin = offsets[cell.row];
        const uint32_t end = offsets[cell.row + 1];
        if (begin > end) return Status::InvalidArgument("Invalid source ARRAY offsets");
        auto* list = static_cast<arrow::ListBuilder*>(builder);
        if (begin == end) return from_arrow(list->Append());
        ASSIGN_OR_RETURN(const auto size, logical_size(array->elements_column_raw_ptr()));
        if (end > size) return Status::InvalidArgument("Invalid source ARRAY offsets");
        RETURN_IF_ERROR(from_arrow(list->Append()));
        for (size_t i = begin; i < end; ++i) {
            RETURN_IF_ERROR(
                    append_cell(array->elements_column_raw_ptr(), i, conversion.children[0], list->value_builder()));
        }
        return Status::OK();
    }
    case TYPE_MAP: {
        const auto* map = dynamic_cast<const MapColumn*>(cell.data);
        if (map == nullptr) return Status::InvalidArgument("Source MAP physical column type mismatch");
        const auto offsets = map->offsets().immutable_data();
        const uint32_t begin = offsets[cell.row];
        const uint32_t end = offsets[cell.row + 1];
        if (begin > end) return Status::InvalidArgument("Invalid source MAP offsets");
        auto* output = static_cast<arrow::MapBuilder*>(builder);
        if (begin == end) return from_arrow(output->Append());
        ASSIGN_OR_RETURN(const auto keys_size, logical_size(map->keys_column_raw_ptr()));
        ASSIGN_OR_RETURN(const auto values_size, logical_size(map->values_column_raw_ptr()));
        if (end > keys_size || end > values_size) return Status::InvalidArgument("Invalid source MAP offsets");
        RETURN_IF_ERROR(from_arrow(output->Append()));
        for (size_t i = begin; i < end; ++i) {
            RETURN_IF_ERROR(append_cell(map->keys_column_raw_ptr(), i, conversion.children[0], output->key_builder()));
            RETURN_IF_ERROR(
                    append_cell(map->values_column_raw_ptr(), i, conversion.children[1], output->item_builder()));
        }
        return Status::OK();
    }
    case TYPE_STRUCT: {
        const auto* structure = dynamic_cast<const StructColumn*>(cell.data);
        if (structure == nullptr || structure->fields_size() != conversion.source.children.size()) {
            return Status::InvalidArgument("Source STRUCT physical shape mismatch");
        }
        auto* output = static_cast<arrow::StructBuilder*>(builder);
        RETURN_IF_ERROR(from_arrow(output->Append()));
        for (size_t i = 0; i < conversion.children.size(); ++i) {
            RETURN_IF_ERROR(append_cell(structure->field_column_raw_ptr(conversion.source_fields[i]), cell.row,
                                        conversion.children[i], output->field_builder(i)));
        }
        return Status::OK();
    }
    case TYPE_VARIANT:
        return append_variant(cell, builder);
    default:
        return Status::NotSupported("Unsupported native Paimon write cell");
    }
}

StatusOr<std::shared_ptr<arrow::RecordBatch>> columns_to_arrow(size_t rows, const Columns& columns,
                                                               const std::vector<TypeDescriptor>& types,
                                                               const std::shared_ptr<arrow::Schema>& target_schema,
                                                               const std::string& time_zone, arrow::MemoryPool* pool) {
    if (target_schema == nullptr || pool == nullptr || columns.size() != types.size() ||
        columns.size() != target_schema->num_fields() || rows > std::numeric_limits<int64_t>::max()) {
        return Status::InvalidArgument("Invalid Paimon write columns/types/schema/pool");
    }
    std::vector<std::shared_ptr<arrow::Array>> arrays;
    arrays.reserve(columns.size());
    for (size_t i = 0; i < columns.size(); ++i) {
        ASSIGN_OR_RETURN(const size_t size, logical_size(columns[i].get()));
        if (size != rows) return Status::InvalidArgument("Paimon output expression row count mismatch");
        auto conversion = make_conversion(types[i], target_schema->field(i), time_zone, pool);
        if (!conversion.ok()) {
            return conversion.status().clone_and_prepend("Paimon field " + target_schema->field(i)->name());
        }
        RETURN_IF_ERROR(from_arrow(conversion->builder->Reserve(rows)));
        for (size_t row = 0; row < rows; ++row) {
            const auto status = append_cell(columns[i].get(), row, *conversion, conversion->builder.get());
            if (!status.ok()) {
                // Correlate errors without logging sensitive string/VARIANT payloads here. The
                // sink's existing write timer and Status boundary retain ownership of reporting.
                return status.clone_and_prepend("Paimon field " + target_schema->field(i)->name() + " row " +
                                                std::to_string(row));
            }
        }
        std::shared_ptr<arrow::Array> array;
        RETURN_IF_ERROR(from_arrow(conversion->builder->Finish(&array)));
        arrays.emplace_back(std::move(array));
    }
    auto result = arrow::RecordBatch::Make(target_schema, rows, std::move(arrays));
    RETURN_IF_ERROR(from_arrow(result->ValidateFull()));
    return result;
}

} // namespace

StatusOr<std::shared_ptr<arrow::RecordBatch>> paimon_write_columns_to_arrow(
        const Columns& columns, const std::vector<TypeDescriptor>& types,
        const std::shared_ptr<arrow::Schema>& target_schema, const std::string& time_zone, arrow::MemoryPool* pool) {
    size_t rows = 0;
    if (!columns.empty()) {
        ASSIGN_OR_RETURN(rows, logical_size(columns[0].get()));
    }
    return columns_to_arrow(rows, columns, types, target_schema, time_zone, pool);
}

StatusOr<std::shared_ptr<arrow::RecordBatch>> paimon_write_chunk_to_arrow(
        const ChunkPtr& chunk, const std::vector<ExprContext*>& output_exprs,
        const std::shared_ptr<arrow::Schema>& target_schema, const std::string& time_zone, arrow::MemoryPool* pool) {
    if (chunk == nullptr || target_schema == nullptr || output_exprs.size() != target_schema->num_fields()) {
        return Status::InvalidArgument("Invalid Paimon write chunk/output expressions/schema");
    }
    Columns columns;
    std::vector<TypeDescriptor> types;
    columns.reserve(output_exprs.size());
    types.reserve(output_exprs.size());
    for (auto* expression : output_exprs) {
        if (expression == nullptr || expression->root() == nullptr) {
            return Status::InvalidArgument("Missing Paimon output expression");
        }
        ASSIGN_OR_RETURN(auto column, expression->evaluate(chunk.get()));
        columns.emplace_back(std::move(column));
        types.emplace_back(expression->root()->type());
    }
    return columns_to_arrow(chunk->num_rows(), columns, types, target_schema, time_zone, pool);
}

} // namespace starrocks
