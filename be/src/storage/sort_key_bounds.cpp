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

#include "storage/sort_key_bounds.h"

#include <algorithm>
#include <cstdint>
#include <cstring>
#include <limits>
#include <utility>
#include <vector>

#include "column/datum_convert.h"
#include "common/logging.h"
#include "common/statusor.h"
#include "fmt/format.h"
#include "gen_cpp/lake_types.pb.h"
#include "storage/key_coder.h"
#include "storage/primary_key_encoder.h"
#include "storage/type_traits.h"
#include "storage/types.h"
#include "types/logical_type.h"
#include "util/decimal_types.h"

namespace starrocks {

namespace {

static_assert(TYPE_MAX_VALUE < 256, "a LogicalType must fit in the one-byte type tag");

constexpr uint8_t kFormatVersion = 1;
constexpr uint8_t kNullMarker = 0x00;
constexpr uint8_t kValueMarker = 0x01;
// Version byte plus the big-endian uint16 column count.
constexpr size_t kFixedHeaderSize = 3;
constexpr size_t kMaxColumns = std::numeric_limits<uint16_t>::max();

struct ColumnHeader {
    LogicalType type;
    int precision;
    int scale;
};

bool is_supported_type(LogicalType type) {
    switch (type) {
    case TYPE_BOOLEAN:
    case TYPE_TINYINT:
    case TYPE_SMALLINT:
    case TYPE_INT:
    case TYPE_BIGINT:
    case TYPE_LARGEINT:
    case TYPE_DATE:
    case TYPE_DATETIME:
    case TYPE_DECIMALV2:
    case TYPE_DECIMAL32:
    case TYPE_DECIMAL64:
    case TYPE_DECIMAL128:
    case TYPE_DECIMAL256:
    case TYPE_CHAR:
    case TYPE_VARCHAR:
    case TYPE_VARBINARY:
        return true;
    default:
        return false;
    }
}

int max_decimal_precision(LogicalType type) {
    switch (type) {
    case TYPE_DECIMAL32:
        return decimal_precision_limit<int32_t>;
    case TYPE_DECIMAL64:
        return decimal_precision_limit<int64_t>;
    case TYPE_DECIMAL128:
        return decimal_precision_limit<int128_t>;
    case TYPE_DECIMAL256:
        return decimal_precision_limit<int256_t>;
    default:
        return -1;
    }
}

bool is_valid_decimal(LogicalType type, int precision, int scale) {
    return precision >= 1 && precision <= max_decimal_precision(type) && scale >= 0 && scale <= precision;
}

// This branch's KeyCoder has no full encoding for a 256-bit integer and decodes every BOOLEAN as true, so the codec
// handles those two types itself. The bytes are the KeyCoder full ascending encoding of later versions, which read
// these bounds as well: a BOOLEAN is one byte, 0 or 1; a 256-bit integer is 32 bytes, big-endian, sign bit flipped.
constexpr size_t kInt256Size = 32;

void encode_int256(const int256_t& v, std::string* out) {
    const uint128_t hi = static_cast<uint128_t>(v.high) ^ (static_cast<uint128_t>(1) << 127);
    const uint128_t lo = v.low;
    char buf[kInt256Size];
    for (int i = 0; i < 16; i++) buf[i] = static_cast<char>(hi >> (8 * (15 - i)));
    for (int i = 0; i < 16; i++) buf[16 + i] = static_cast<char>(lo >> (8 * (15 - i)));
    out->append(buf, kInt256Size);
}

Status decode_int256(Slice* in, Datum* out) {
    if (in->size < kInt256Size) {
        return Status::Corruption(fmt::format("sort key bound: truncated {} value", type_to_string(TYPE_DECIMAL256)));
    }
    const auto* p = reinterpret_cast<const uint8_t*>(in->data);
    uint128_t hi = 0;
    uint128_t lo = 0;
    for (int i = 0; i < 16; i++) hi = (hi << 8) | p[i];
    for (int i = 0; i < 16; i++) lo = (lo << 8) | p[16 + i];
    hi ^= (static_cast<uint128_t>(1) << 127);
    out->set_int256(int256_t(static_cast<int128_t>(hi), lo));
    in->remove_prefix(kInt256Size);
    return Status::OK();
}

Status decode_boolean(Slice* in, Datum* out) {
    if (in->empty()) {
        return Status::Corruption(fmt::format("sort key bound: truncated {} value", type_to_string(TYPE_BOOLEAN)));
    }
    out->set(static_cast<uint8_t>((*in)[0]) != 0);
    in->remove_prefix(1);
    return Status::OK();
}

Status encode_value(const DatumVariant& value, std::string* out) {
    TypeInfo* type_info = value.type().get();
    const Datum& datum = value.value();
    const LogicalType type = type_info->type();
    switch (type) {
    case TYPE_CHAR: {
        const Slice& s = datum.get_slice();
        encoding_utils::encode_slice(Slice(s.data, strnlen(s.data, s.size)), out, /*is_last=*/false);
        return Status::OK();
    }
    case TYPE_VARCHAR:
    case TYPE_VARBINARY:
        encoding_utils::encode_slice(datum.get_slice(), out, /*is_last=*/false);
        return Status::OK();
    case TYPE_DATE:
    case TYPE_DATETIME: {
        Datum normalized;
        RETURN_IF_ERROR(datum_from_string(type_info, &normalized, datum_to_string(type_info, datum), nullptr));
        get_key_coder(type)->full_encode_ascending(normalized, out);
        return Status::OK();
    }
    case TYPE_DECIMAL256:
        encode_int256(datum.get_int256(), out);
        return Status::OK();
    default:
        get_key_coder(type)->full_encode_ascending(datum, out);
        return Status::OK();
    }
}

// The big-endian column count at bytes 1-2 of a header of at least kFixedHeaderSize bytes.
size_t read_column_count(const char* header) {
    const auto* p = reinterpret_cast<const uint8_t*>(header);
    return (static_cast<size_t>(p[1]) << 8) | p[2];
}

// Parses and validates the whole header, leaving *in at the first value, and returns the column count. Only the
// descriptors of the first max_collect columns are stored in *columns, so a prefix decode does not collect the rest.
StatusOr<size_t> parse_header(Slice* in, size_t max_collect, std::vector<ColumnHeader>* columns) {
    if (in->size < kFixedHeaderSize) {
        return Status::Corruption(fmt::format("sort key bound: header of {} bytes is too short", in->size));
    }
    const auto version = static_cast<uint8_t>((*in)[0]);
    if (version != kFormatVersion) {
        return Status::Corruption(fmt::format("sort key bound: unknown format version {}", version));
    }
    const size_t count = read_column_count(in->data);
    in->remove_prefix(kFixedHeaderSize);
    // Each column takes at least its type byte, which also bounds the reservation below.
    if (count == 0 || count > in->size) {
        return Status::Corruption(
                fmt::format("sort key bound: column count {} with {} bytes remaining", count, in->size));
    }
    columns->clear();
    columns->reserve(std::min(count, max_collect));
    for (size_t i = 0; i < count; ++i) {
        if (in->empty()) {
            return Status::Corruption(fmt::format("sort key bound: header ends at column {}", i));
        }
        const auto tag = static_cast<uint8_t>((*in)[0]);
        in->remove_prefix(1);
        // Range-check the byte before casting: not every byte value is a LogicalType.
        if (tag >= TYPE_MAX_VALUE || !is_supported_type(static_cast<LogicalType>(tag))) {
            return Status::Corruption(fmt::format("sort key bound: unsupported type {} at column {}", tag, i));
        }
        ColumnHeader column{static_cast<LogicalType>(tag), -1, -1};
        if (is_decimalv3_field_type(column.type)) {
            if (in->size < 2) {
                return Status::Corruption(
                        fmt::format("sort key bound: header ends inside the precision and scale of column {}", i));
            }
            column.precision = static_cast<uint8_t>((*in)[0]);
            column.scale = static_cast<uint8_t>((*in)[1]);
            in->remove_prefix(2);
            if (!is_valid_decimal(column.type, column.precision, column.scale)) {
                return Status::Corruption(fmt::format("sort key bound: invalid {}({}, {}) at column {}",
                                                      type_to_string(column.type), column.precision, column.scale, i));
            }
        }
        if (columns->size() < max_collect) {
            columns->push_back(column);
        }
    }
    return count;
}

// Reverses encoding_utils::encode_slice(..., /*is_last=*/false), rejecting any byte sequence it
// cannot produce. encoding_utils::decode_slice is not reused: its non-fast path drops a 0x01 after
// any 0x00 and keeps any other byte after a 0x00, so a damaged value would decode to a different
// key instead of failing. The bounds decide which child tablet owns a segment, so a value that
// encode_slice cannot produce must be rejected.
Status unescape_string(Slice* in, std::string* out) {
    out->clear();
    while (true) {
        const auto* zero = static_cast<const char*>(memchr(in->data, 0, in->size));
        if (zero == nullptr) {
            return Status::Corruption("sort key bound: missing string terminator");
        }
        const size_t len = zero - in->data;
        if (len + 1 == in->size) {
            return Status::Corruption("sort key bound: missing string terminator");
        }
        out->append(in->data, len);
        const auto escaped = static_cast<uint8_t>((*in)[len + 1]);
        in->remove_prefix(len + 2);
        if (escaped == 0x00) {
            return Status::OK();
        }
        if (escaped != 0x01) {
            return Status::Corruption(fmt::format("sort key bound: invalid string escape 0x00 0x{:02x}", escaped));
        }
        out->push_back('\0');
    }
}

// Decodes into the storage C++ type of the LOGICAL type so that the Datum holds what the writer
// put there (for example, DATE as its int32 julian day).
template <LogicalType LT>
Status decode_fixed(Slice* in, Datum* out) {
    using CppType = typename CppTypeTraits<LT>::CppType;
    if (in->size < sizeof(CppType)) {
        return Status::Corruption(fmt::format("sort key bound: truncated {} value", type_to_string(LT)));
    }
    CppType raw{};
    // Fixed-width KeyCoder decoders never touch the pool.
    RETURN_IF_ERROR(
            get_key_coder(LT)->decode_ascending(in, sizeof(CppType), reinterpret_cast<uint8_t*>(&raw), nullptr));
    out->set(raw);
    return Status::OK();
}

// Decodes a non-NULL payload. A string Datum points into *scratch.
Status decode_value(LogicalType type, Slice* in, std::string* scratch, Datum* out) {
    switch (type) {
    case TYPE_CHAR:
    case TYPE_VARCHAR:
    case TYPE_VARBINARY:
        RETURN_IF_ERROR(unescape_string(in, scratch));
        out->set_slice(scratch->empty() ? Slice() : Slice(*scratch));
        return Status::OK();
    case TYPE_BOOLEAN:
        if (!in->empty() && static_cast<uint8_t>((*in)[0]) > 1) {
            return Status::Corruption(
                    fmt::format("sort key bound: invalid BOOLEAN value {}", static_cast<uint8_t>((*in)[0])));
        }
        return decode_boolean(in, out);
#define M(LT) \
    case LT:  \
        return decode_fixed<LT>(in, out);
        M(TYPE_TINYINT)
        M(TYPE_SMALLINT)
        M(TYPE_INT)
        M(TYPE_BIGINT)
        M(TYPE_LARGEINT)
        M(TYPE_DATE)
        M(TYPE_DATETIME)
        M(TYPE_DECIMALV2)
        M(TYPE_DECIMAL32)
        M(TYPE_DECIMAL64)
        M(TYPE_DECIMAL128)
#undef M
    case TYPE_DECIMAL256:
        return decode_int256(in, out);
    default:
        // parse_header admits only the types handled above.
        return Status::Corruption(fmt::format("sort key bound: unsupported type {}", type_to_string(type)));
    }
}

} // namespace

Status encode_sort_key_bound(const VariantTuple& tuple, std::string* out) {
    if (tuple.empty()) {
        return Status::InvalidArgument("sort key bound: cannot encode an empty tuple");
    }
    if (tuple.size() > kMaxColumns) {
        return Status::NotSupported(
                fmt::format("sort key bound: {} columns exceed the limit of {}", tuple.size(), kMaxColumns));
    }
    std::string buf;
    buf.push_back(static_cast<char>(kFormatVersion));
    buf.push_back(static_cast<char>(tuple.size() >> 8));
    buf.push_back(static_cast<char>(tuple.size() & 0xff));
    for (const DatumVariant& value : tuple.values()) {
        const LogicalType type = value.type()->type();
        if (!is_supported_type(type)) {
            return Status::NotSupported(fmt::format("sort key bound: unsupported type {}", type_to_string(type)));
        }
        buf.push_back(static_cast<char>(type));
        if (is_decimalv3_field_type(type)) {
            const int precision = value.type()->precision();
            const int scale = value.type()->scale();
            // The decoder would reject this header, so do not write it.
            if (!is_valid_decimal(type, precision, scale)) {
                return Status::NotSupported(
                        fmt::format("sort key bound: unsupported {}({}, {})", type_to_string(type), precision, scale));
            }
            buf.push_back(static_cast<char>(precision));
            buf.push_back(static_cast<char>(scale));
        }
    }
    for (const DatumVariant& value : tuple.values()) {
        if (value.value().is_null()) {
            buf.push_back(static_cast<char>(kNullMarker));
            continue;
        }
        buf.push_back(static_cast<char>(kValueMarker));
        RETURN_IF_ERROR(encode_value(value, &buf));
    }
    *out = std::move(buf);
    return Status::OK();
}

namespace {

// Validates the whole header, then decodes the first min(max_columns, column count) values into *out.
// All columns are decoded when max_columns is the column count or larger. The trailing-bytes check
// only applies when every column was decoded; a prefix decode stops before the remaining values.
Status decode_prefix(const Slice& encoded, size_t max_columns, VariantTuple* out) {
    Slice in = encoded;
    std::vector<ColumnHeader> columns;
    ASSIGN_OR_RETURN(const size_t count, parse_header(&in, max_columns, &columns));
    const size_t decode_count = columns.size();
    VariantTuple tuple;
    tuple.reserve(decode_count);
    std::string scratch;
    for (size_t i = 0; i < decode_count; ++i) {
        const ColumnHeader& column = columns[i];
        if (in.empty()) {
            return Status::Corruption(fmt::format("sort key bound: missing value of column {}", i));
        }
        const auto marker = static_cast<uint8_t>(in[0]);
        in.remove_prefix(1);
        Datum datum;
        if (marker == kValueMarker) {
            RETURN_IF_ERROR(decode_value(column.type, &in, &scratch, &datum));
        } else if (marker != kNullMarker) {
            return Status::Corruption(fmt::format("sort key bound: invalid value marker {} at column {}", marker, i));
        }
        TypeInfoPtr type_info = is_decimalv3_field_type(column.type)
                                        ? get_type_info(column.type, column.precision, column.scale)
                                        : get_type_info(column.type);
        // DatumVariant deep-copies string data, so `scratch` can be reused for the next column.
        tuple.emplace(std::move(type_info), datum);
    }
    if (decode_count == count && !in.empty()) {
        return Status::Corruption(fmt::format("sort key bound: {} trailing bytes", in.size));
    }
    *out = std::move(tuple);
    return Status::OK();
}

} // namespace

Status decode_sort_key_bound(const Slice& encoded, VariantTuple* out) {
    return decode_prefix(encoded, std::numeric_limits<size_t>::max(), out);
}

namespace {

enum class BoundsSource { kNone, kDeprecated, kEncoded };

bool has_known_format_version(const std::string& encoded) {
    return !encoded.empty() && static_cast<uint8_t>(encoded[0]) == kFormatVersion;
}

BoundsSource select_bounds_source(const SegmentMetadataPB& meta) {
    if (meta.has_deprecated_sort_key_min() && meta.has_deprecated_sort_key_max()) {
        return meta.deprecated_sort_key_min().values_size() > 0 && meta.deprecated_sort_key_max().values_size() > 0
                       ? BoundsSource::kDeprecated
                       : BoundsSource::kNone;
    }
    // Only the version byte is read, so selection stays O(1). An encoded pair in a format version this binary does
    // not know counts as no bounds; damage anywhere else is left to the decoders, which return Corruption.
    if (meta.has_sort_key_min_encoded() && meta.has_sort_key_max_encoded() &&
        has_known_format_version(meta.sort_key_min_encoded()) &&
        has_known_format_version(meta.sort_key_max_encoded())) {
        return BoundsSource::kEncoded;
    }
    return BoundsSource::kNone;
}

// The column count in the header of an encoded bound, without validating anything else. A bound too short to hold
// the count yields 0, which no valid bound has.
size_t encoded_column_count(const std::string& encoded) {
    return encoded.size() < kFixedHeaderSize ? 0 : read_column_count(encoded.data());
}

} // namespace

void set_segment_sort_key_bounds(const VariantTuple& min, const VariantTuple& max, SegmentMetadataPB* meta) {
    meta->clear_deprecated_sort_key_min();
    meta->clear_deprecated_sort_key_max();
    meta->clear_sort_key_min_encoded();
    meta->clear_sort_key_max_encoded();
    if (min.empty() || max.empty()) {
        return;
    }
    std::string encoded_min;
    std::string encoded_max;
    // An empty tuple is excluded above, so any failure here means an unsupported tuple.
    Status st = encode_sort_key_bound(min, &encoded_min);
    if (st.ok()) {
        st = encode_sort_key_bound(max, &encoded_max);
    }
    if (st.ok()) {
        meta->set_sort_key_min_encoded(std::move(encoded_min));
        meta->set_sort_key_max_encoded(std::move(encoded_max));
        return;
    }
    LOG_EVERY_N(WARNING, 1000) << "Storing segment sort key bounds in the deprecated form: " << st;
    min.to_proto(meta->mutable_deprecated_sort_key_min());
    max.to_proto(meta->mutable_deprecated_sort_key_max());
}

bool has_segment_sort_key_bounds(const SegmentMetadataPB& meta) {
    return select_bounds_source(meta) != BoundsSource::kNone;
}

bool segment_sort_key_bounds_have_arity(const SegmentMetadataPB& meta, size_t arity) {
    switch (select_bounds_source(meta)) {
    case BoundsSource::kDeprecated:
        return static_cast<size_t>(meta.deprecated_sort_key_min().values_size()) == arity &&
               static_cast<size_t>(meta.deprecated_sort_key_max().values_size()) == arity;
    case BoundsSource::kEncoded:
        return encoded_column_count(meta.sort_key_min_encoded()) == arity &&
               encoded_column_count(meta.sort_key_max_encoded()) == arity;
    case BoundsSource::kNone:
        break;
    }
    return false;
}

Status decode_segment_sort_key_bounds(const SegmentMetadataPB& meta, VariantTuple* min, VariantTuple* max) {
    VariantTuple decoded_min;
    VariantTuple decoded_max;
    switch (select_bounds_source(meta)) {
    case BoundsSource::kDeprecated:
        RETURN_IF_ERROR(decoded_min.from_proto(meta.deprecated_sort_key_min()));
        RETURN_IF_ERROR(decoded_max.from_proto(meta.deprecated_sort_key_max()));
        break;
    case BoundsSource::kEncoded:
        RETURN_IF_ERROR(decode_sort_key_bound(Slice(meta.sort_key_min_encoded()), &decoded_min));
        RETURN_IF_ERROR(decode_sort_key_bound(Slice(meta.sort_key_max_encoded()), &decoded_max));
        break;
    case BoundsSource::kNone:
        break;
    }
    *min = std::move(decoded_min);
    *max = std::move(decoded_max);
    return Status::OK();
}

Status decode_segment_leading_sort_key_bounds(const SegmentMetadataPB& meta, DatumVariant* min, DatumVariant* max) {
    DatumVariant leading_min;
    DatumVariant leading_max;
    switch (select_bounds_source(meta)) {
    case BoundsSource::kDeprecated:
        RETURN_IF_ERROR(leading_min.from_proto(meta.deprecated_sort_key_min().values(0)));
        RETURN_IF_ERROR(leading_max.from_proto(meta.deprecated_sort_key_max().values(0)));
        break;
    case BoundsSource::kEncoded: {
        VariantTuple decoded_min;
        VariantTuple decoded_max;
        RETURN_IF_ERROR(decode_prefix(Slice(meta.sort_key_min_encoded()), 1, &decoded_min));
        RETURN_IF_ERROR(decode_prefix(Slice(meta.sort_key_max_encoded()), 1, &decoded_max));
        leading_min = std::move(decoded_min[0]);
        leading_max = std::move(decoded_max[0]);
        break;
    }
    case BoundsSource::kNone:
        return Status::NotFound("segment has no sort key bounds");
    }
    *min = std::move(leading_min);
    *max = std::move(leading_max);
    return Status::OK();
}

} // namespace starrocks
