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
#include <optional>
#include <utility>
#include <vector>

#include "base/decimal_types.h"
#include "base/string/utf8.h"
#include "base/string/utf8_check.h"
#include "column/datum_convert.h"
#include "common/logging.h"
#include "common/statusor.h"
#include "fmt/format.h"
#include "gen_cpp/lake_types.pb.h"
#include "gutil/utf/utf.h"
#include "storage_primitive/key_coder.h"
#include "storage_primitive/primary_key_encoder.h"
#include "types/date_value.h"
#include "types/decimalv2_value.h"
#include "types/logical_type.h"
#include "types/storage_type_traits.h"
#include "types/time_types.h"
#include "types/timestamp_value.h"
#include "types/type_info.h"

namespace starrocks {

namespace {

static_assert(TYPE_MAX_VALUE < 256, "a LogicalType must fit in the one-byte type tag");

constexpr uint8_t kFormatVersion1 = 1;
constexpr uint8_t kFormatVersion2 = 2;
constexpr uint8_t kNullMarker = 0x00;
constexpr uint8_t kValueMarker = 0x01;
// Format version 1: version byte plus the big-endian uint16 column count.
constexpr size_t kFixedHeaderSize = 3;
// Format version 2: version byte, big-endian uint16 arity and count, flags byte.
constexpr size_t kFixedHeaderSizeV2 = 6;
// Format version 2 flag: the last present value is not the bound's own value.
constexpr uint8_t kLastValueReplaced = 0x01;
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

// The DATE or DATETIME value the TuplePB text round trip yields.
Status normalize_date_or_datetime(TypeInfo* type_info, const Datum& datum, Datum* normalized) {
    return datum_from_string(type_info, normalized, datum_to_string(type_info, datum), nullptr);
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
        RETURN_IF_ERROR(normalize_date_or_datetime(type_info, datum, &normalized));
        get_key_coder(type)->full_encode_ascending(normalized, out);
        return Status::OK();
    }
    default:
        get_key_coder(type)->full_encode_ascending(datum, out);
        return Status::OK();
    }
}

// The big-endian uint16 at p[0..1].
size_t read_u16(const char* p) {
    const auto* u = reinterpret_cast<const uint8_t*>(p);
    return (static_cast<size_t>(u[0]) << 8) | u[1];
}

void append_u16(size_t value, std::string* out) {
    out->push_back(static_cast<char>(value >> 8));
    out->push_back(static_cast<char>(value & 0xff));
}

struct BoundHeader {
    uint8_t version = 0;
    // Number of values present.
    size_t count = 0;
    // Format version 2 flag bit 0.
    bool last_value_replaced = false;
};

// Parses and validates the whole header, leaving *in at the first value. Only the descriptors of the first
// max_collect columns are stored in *columns, so a prefix decode does not collect the rest.
StatusOr<BoundHeader> parse_header(Slice* in, size_t max_collect, std::vector<ColumnHeader>* columns) {
    if (in->empty()) {
        return Status::Corruption("sort key bound: header of 0 bytes is too short");
    }
    BoundHeader header;
    header.version = static_cast<uint8_t>((*in)[0]);
    if (header.version != kFormatVersion1 && header.version != kFormatVersion2) {
        return Status::Corruption(fmt::format("sort key bound: unknown format version {}", header.version));
    }
    const size_t fixed_size = header.version == kFormatVersion1 ? kFixedHeaderSize : kFixedHeaderSizeV2;
    if (in->size < fixed_size) {
        return Status::Corruption(fmt::format("sort key bound: header of {} bytes is too short", in->size));
    }
    if (header.version == kFormatVersion1) {
        header.count = read_u16(in->data + 1);
    } else {
        const size_t arity = read_u16(in->data + 1);
        header.count = read_u16(in->data + 3);
        const auto flags = static_cast<uint8_t>((*in)[5]);
        if ((flags & ~kLastValueReplaced) != 0) {
            return Status::Corruption(fmt::format("sort key bound: invalid flags 0x{:02x}", flags));
        }
        header.last_value_replaced = (flags & kLastValueReplaced) != 0;
        // A bound that keeps all its own values is written in format version 1.
        if (header.count > arity || (header.count == arity && !header.last_value_replaced)) {
            return Status::Corruption(fmt::format("sort key bound: column count {} with arity {} and flags 0x{:02x}",
                                                  header.count, arity, flags));
        }
    }
    const size_t count = header.count;
    in->remove_prefix(fixed_size);
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
    return header;
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
    using CppType = StorageCppType<LT>;
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
        return decode_fixed<TYPE_BOOLEAN>(in, out);
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
        M(TYPE_DECIMAL256)
#undef M
    default:
        // parse_header admits only the types handled above.
        return Status::Corruption(fmt::format("sort key bound: unsupported type {}", type_to_string(type)));
    }
}

// Appends column{n} value{n} for the values of `tuple`, laid out as in format version 1.
Status append_columns_and_values(const VariantTuple& tuple, std::string* buf) {
    for (const DatumVariant& value : tuple.values()) {
        const LogicalType type = value.type()->type();
        if (!is_supported_type(type)) {
            return Status::NotSupported(fmt::format("sort key bound: unsupported type {}", type_to_string(type)));
        }
        buf->push_back(static_cast<char>(type));
        if (is_decimalv3_field_type(type)) {
            const int precision = value.type()->precision();
            const int scale = value.type()->scale();
            // The decoder would reject this header, so do not write it.
            if (!is_valid_decimal(type, precision, scale)) {
                return Status::NotSupported(
                        fmt::format("sort key bound: unsupported {}({}, {})", type_to_string(type), precision, scale));
            }
            buf->push_back(static_cast<char>(precision));
            buf->push_back(static_cast<char>(scale));
        }
    }
    for (const DatumVariant& value : tuple.values()) {
        if (value.value().is_null()) {
            buf->push_back(static_cast<char>(kNullMarker));
            continue;
        }
        buf->push_back(static_cast<char>(kValueMarker));
        RETURN_IF_ERROR(encode_value(value, buf));
    }
    return Status::OK();
}

bool is_string_or_binary(LogicalType type) {
    return type == TYPE_CHAR || type == TYPE_VARCHAR || type == TYPE_VARBINARY;
}

// The bytes encode_value writes for a non-NULL string value, before escaping.
Slice string_payload(const DatumVariant& value) {
    const Slice& s = value.value().get_slice();
    return value.type()->type() == TYPE_CHAR ? Slice(s.data, strnlen(s.data, s.size)) : s;
}

// The cost of a value against kSortKeyBoundPayloadBudget.
size_t payload_size(const DatumVariant& value) {
    if (value.value().is_null()) {
        return 0;
    }
    switch (value.type()->type()) {
    case TYPE_CHAR:
    case TYPE_VARCHAR:
    case TYPE_VARBINARY:
        return string_payload(value).size;
    default:
        // encode_sort_key_bound has rejected every type that is neither a string nor fixed-width.
        return value.type()->size();
    }
}

// The widest fixed-width value fits the budget alone, so a fixed-width column that does not fit is never column 0.
static_assert(sizeof(StorageCppType<TYPE_DECIMAL256>) <= kSortKeyBoundPayloadBudget);

// Strings are cut and incremented in units, taken from the start of the string: a unit is a valid UTF-8 code point,
// or else a single byte. A valid code point is never split, whether or not the rest of the string is valid UTF-8.
// Returns the byte length of the unit that starts at p, where n > 0 bytes remain.
size_t unit_size(const char* p, size_t n) {
    const size_t width = UTF8_BYTE_LENGTH_TABLE[static_cast<uint8_t>(*p)];
    return width <= n && validate_utf8(p, width) ? width : 1;
}

// The longest prefix of `s` that is made of whole units and is at most `room` bytes.
std::string cut_string(const Slice& s, size_t room) {
    size_t len = 0;
    while (len < s.size) {
        const size_t next = len + unit_size(s.data + len, s.size - len);
        if (next > room) {
            break;
        }
        len = next;
    }
    return {s.data, len};
}

// Replaces *s with a string greater than every string that starts with *s and no longer than *s, or returns false
// if there is none. The last unit is incremented: a code point to the next one, skipping the surrogates, a byte that
// is not a code point to the next byte. A last unit without an increment of the same length (U+10FFFF, a code point
// whose increment needs a longer encoding, the byte 0xFF) is dropped and the unit before it is incremented instead.
bool to_string_successor(std::string* s) {
    std::vector<size_t> starts;
    for (size_t pos = 0; pos < s->size(); pos += unit_size(s->data() + pos, s->size() - pos)) {
        starts.push_back(pos);
    }
    for (; !starts.empty(); starts.pop_back()) {
        const size_t start = starts.back();
        const size_t width = s->size() - start;
        const auto lead = static_cast<uint8_t>((*s)[start]);
        if (width == 1 && lead >= Runeself) {
            if (lead < 0xFF) {
                s->back() = static_cast<char>(lead + 1);
                return true;
            }
        } else {
            Rune cp;
            chartorune(&cp, s->data() + start);
            const Rune next = cp + 1 == 0xD800 ? 0xE000 : cp + 1;
            if (cp < Runemax && static_cast<size_t>(runelen(next)) == width) {
                char encoded[UTFmax];
                s->resize(start);
                s->append(encoded, runetochar(encoded, &next));
                return true;
            }
        }
        s->resize(start);
    }
    return false;
}

template <typename T>
std::optional<Datum> increment_below(const T& value, const T& max) {
    if (!(value < max)) {
        return std::nullopt;
    }
    Datum next;
    next.set(static_cast<T>(value + 1));
    return next;
}

// A valid value of the column type greater than `value`, and for a string also greater than every string that
// starts with it, or nullopt if there is none.
std::optional<DatumVariant> successor(const DatumVariant& value) {
    const Datum& datum = value.value();
    if (datum.is_null()) {
        return std::nullopt;
    }
    TypeInfo* type_info = value.type().get();
    const LogicalType type = type_info->type();
    std::optional<Datum> next;
    switch (type) {
    case TYPE_CHAR:
    case TYPE_VARCHAR:
    case TYPE_VARBINARY: {
        std::string s = string_payload(value).to_string();
        if (!to_string_successor(&s)) {
            return std::nullopt;
        }
        return DatumVariant(value.type(), Datum(Slice(s)));
    }
    case TYPE_BOOLEAN:
        next = increment_below<int8_t>(datum.get_int8(), 1);
        break;
    case TYPE_TINYINT:
        next = increment_below(datum.get_int8(), std::numeric_limits<int8_t>::max());
        break;
    case TYPE_SMALLINT:
        next = increment_below(datum.get_int16(), std::numeric_limits<int16_t>::max());
        break;
    case TYPE_INT:
        next = increment_below(datum.get_int32(), std::numeric_limits<int32_t>::max());
        break;
    case TYPE_BIGINT:
        next = increment_below(datum.get_int64(), std::numeric_limits<int64_t>::max());
        break;
    case TYPE_LARGEINT:
        next = increment_below(datum.get_int128(), std::numeric_limits<int128_t>::max());
        break;
    case TYPE_DECIMAL32:
        next = increment_below(datum.get_int32(), get_max_decimal<int32_t>(type_info->precision()));
        break;
    case TYPE_DECIMAL64:
        next = increment_below(datum.get_int64(), get_max_decimal<int64_t>(type_info->precision()));
        break;
    case TYPE_DECIMAL128:
        next = increment_below(datum.get_int128(), get_max_decimal<int128_t>(type_info->precision()));
        break;
    case TYPE_DECIMAL256:
        next = increment_below(datum.get_int256(), get_max_decimal<int256_t>(type_info->precision()));
        break;
    case TYPE_DECIMALV2: {
        const DecimalV2Value& v = datum.get_decimal();
        if (v < DecimalV2Value::get_max_decimal()) {
            next = Datum(DecimalV2Value(v.value() + 1));
        }
        break;
    }
    case TYPE_DATE: {
        const DateValue v = datum.get_date();
        if (v < DateValue::MAX_DATE_VALUE) {
            next = Datum(v.add<TimeUnit::DAY>(1));
        }
        break;
    }
    case TYPE_DATETIME: {
        const TimestampValue v = datum.get_timestamp();
        if (v < TimestampValue::MAX_TIMESTAMP_VALUE) {
            next = Datum(v.add<TimeUnit::MICROSECOND>(1));
        }
        break;
    }
    default:
        break;
    }
    if (!next.has_value()) {
        return std::nullopt;
    }
    if (type == TYPE_DATE || type == TYPE_DATETIME) {
        // encode_value stores the text round trip of these types. Keep the successor only if, after that round
        // trip, it is still above the value, both as given and as encode_value stores it.
        Datum normalized_next;
        Datum normalized_value;
        if (!normalize_date_or_datetime(type_info, *next, &normalized_next).ok() ||
            !normalize_date_or_datetime(type_info, datum, &normalized_value).ok() ||
            type_info->cmp(normalized_next, datum) <= 0 || type_info->cmp(normalized_next, normalized_value) <= 0) {
            return std::nullopt;
        }
        next = normalized_next;
    }
    return DatumVariant(value.type(), *next);
}

// The values of a format version 2 bound and its flag bit 0.
struct TruncatedBound {
    VariantTuple values;
    bool last_value_replaced = false;
};

// Columns [0, n) of `tuple`, followed by `last` if it is given.
TruncatedBound make_truncated(const VariantTuple& tuple, size_t n, std::optional<DatumVariant> last) {
    TruncatedBound bound;
    bound.values.reserve(n + 1);
    for (size_t i = 0; i < n; ++i) {
        bound.values.append(tuple[i]);
    }
    if (last.has_value()) {
        bound.values.append(std::move(*last));
        bound.last_value_replaced = true;
    }
    return bound;
}

// The format version 2 form of `tuple` described at encode_stored_sort_key_bound, or nullopt if `tuple` fits the
// budget or has no such form.
std::optional<TruncatedBound> truncate_bound(const VariantTuple& tuple, SortKeyBoundSide side) {
    size_t used = 0;
    size_t stop = tuple.size();
    for (size_t i = 0; i < tuple.size(); ++i) {
        const DatumVariant& value = tuple[i];
        const size_t size = payload_size(value);
        if (used + size <= kSortKeyBoundPayloadBudget) {
            used += size;
            continue;
        }
        // A NULL costs nothing, so `value` is not NULL.
        if (is_string_or_binary(value.type()->type())) {
            DatumVariant cut(value.type(),
                             Datum(Slice(cut_string(string_payload(value), kSortKeyBoundPayloadBudget - used))));
            if (side == SortKeyBoundSide::kMin) {
                return make_truncated(tuple, i, std::move(cut));
            }
            if (std::optional<DatumVariant> next = successor(cut); next.has_value()) {
                return make_truncated(tuple, i, std::move(next));
            }
        } else if (side == SortKeyBoundSide::kMin) {
            return make_truncated(tuple, i, std::nullopt);
        }
        stop = i;
        break;
    }
    if (stop == tuple.size()) {
        return std::nullopt;
    }
    for (size_t j = stop; j-- > 0;) {
        if (std::optional<DatumVariant> next = successor(tuple[j]); next.has_value()) {
            return make_truncated(tuple, j, std::move(next));
        }
    }
    return std::nullopt;
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
    buf.push_back(static_cast<char>(kFormatVersion1));
    append_u16(tuple.size(), &buf);
    RETURN_IF_ERROR(append_columns_and_values(tuple, &buf));
    *out = std::move(buf);
    return Status::OK();
}

Status encode_stored_sort_key_bound(const VariantTuple& tuple, SortKeyBoundSide side, std::string* out) {
    std::string full;
    RETURN_IF_ERROR(encode_sort_key_bound(tuple, &full));
    std::optional<TruncatedBound> truncated = truncate_bound(tuple, side);
    if (!truncated.has_value()) {
        *out = std::move(full);
        return Status::OK();
    }
    std::string buf;
    buf.push_back(static_cast<char>(kFormatVersion2));
    append_u16(tuple.size(), &buf);
    append_u16(truncated->values.size(), &buf);
    buf.push_back(static_cast<char>(truncated->last_value_replaced ? kLastValueReplaced : 0));
    RETURN_IF_ERROR(append_columns_and_values(truncated->values, &buf));
    *out = std::move(buf);
    return Status::OK();
}

namespace {

// Validates the whole header, then decodes the first min(max_columns, column count) values into *out and, if
// `header` is not null, stores the parsed header there. All columns are decoded when max_columns is the column
// count or larger. The trailing-bytes check only applies when every column was decoded; a prefix decode stops
// before the remaining values. *out and *header are unchanged on error.
Status decode_prefix(const Slice& encoded, size_t max_columns, VariantTuple* out, BoundHeader* header = nullptr) {
    Slice in = encoded;
    std::vector<ColumnHeader> columns;
    ASSIGN_OR_RETURN(const BoundHeader parsed, parse_header(&in, max_columns, &columns));
    const size_t count = parsed.count;
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
    if (header != nullptr) {
        *header = parsed;
    }
    return Status::OK();
}

} // namespace

Status decode_sort_key_bound(const Slice& encoded, VariantTuple* out) {
    return decode_prefix(encoded, std::numeric_limits<size_t>::max(), out);
}

namespace {

enum class BoundsSource { kNone, kDeprecated, kEncoded };

bool has_known_format_version(const std::string& encoded) {
    if (encoded.empty()) {
        return false;
    }
    const auto version = static_cast<uint8_t>(encoded[0]);
    return version == kFormatVersion1 || version == kFormatVersion2;
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

// The column count (format version 1) or arity (format version 2) in the header of an encoded bound, without
// validating anything else: both are the big-endian uint16 at bytes 1-2. A bound too short to hold it yields 0,
// which no valid bound has.
size_t encoded_arity(const std::string& encoded) {
    return encoded.size() < kFixedHeaderSize ? 0 : read_u16(encoded.data() + 1);
}

// Decodes up to max_columns values of the encoded `side` bound of a segment. A format version 2 maximum must have
// flag bit 0 set and, when its last value is among those decoded, a last value that is not NULL: a bare prefix of the
// maximum is below it, and so is that prefix followed by NULL. *out and *header (if given) are unchanged on error.
Status decode_segment_bound(const std::string& encoded, SortKeyBoundSide side, size_t max_columns, VariantTuple* out,
                            BoundHeader* header = nullptr) {
    VariantTuple decoded;
    BoundHeader parsed;
    RETURN_IF_ERROR(decode_prefix(Slice(encoded), max_columns, &decoded, &parsed));
    if (side == SortKeyBoundSide::kMax && parsed.version == kFormatVersion2) {
        if (!parsed.last_value_replaced) {
            return Status::Corruption("sort key bound: format version 2 maximum without a successor");
        }
        if (decoded.size() == parsed.count && decoded[decoded.size() - 1].value().is_null()) {
            return Status::Corruption("sort key bound: format version 2 maximum whose successor is NULL");
        }
    }
    *out = std::move(decoded);
    if (header != nullptr) *header = parsed;
    return Status::OK();
}

// The number of leading values of a decoded encoded bound that are the segment's own key values.
size_t exact_columns(const BoundHeader& header) {
    return header.count - (header.last_value_replaced ? 1 : 0);
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
    Status st = encode_stored_sort_key_bound(min, SortKeyBoundSide::kMin, &encoded_min);
    if (st.ok()) {
        st = encode_stored_sort_key_bound(max, SortKeyBoundSide::kMax, &encoded_max);
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
        return encoded_arity(meta.sort_key_min_encoded()) == arity &&
               encoded_arity(meta.sort_key_max_encoded()) == arity;
    case BoundsSource::kNone:
        break;
    }
    return false;
}

Status decode_segment_sort_key_bounds(const SegmentMetadataPB& meta, VariantTuple* min, VariantTuple* max,
                                      SortKeyBoundsExactness* exactness) {
    VariantTuple decoded_min;
    VariantTuple decoded_max;
    SortKeyBoundsExactness decoded_exactness;
    switch (select_bounds_source(meta)) {
    case BoundsSource::kDeprecated:
        RETURN_IF_ERROR(decoded_min.from_proto(meta.deprecated_sort_key_min()));
        RETURN_IF_ERROR(decoded_max.from_proto(meta.deprecated_sort_key_max()));
        decoded_exactness.min_exact_columns = decoded_min.size();
        decoded_exactness.max_exact_columns = decoded_max.size();
        break;
    case BoundsSource::kEncoded: {
        constexpr size_t kAllColumns = std::numeric_limits<size_t>::max();
        BoundHeader min_header;
        BoundHeader max_header;
        RETURN_IF_ERROR(decode_segment_bound(meta.sort_key_min_encoded(), SortKeyBoundSide::kMin, kAllColumns,
                                             &decoded_min, &min_header));
        RETURN_IF_ERROR(decode_segment_bound(meta.sort_key_max_encoded(), SortKeyBoundSide::kMax, kAllColumns,
                                             &decoded_max, &max_header));
        decoded_exactness.min_truncated = min_header.version == kFormatVersion2;
        decoded_exactness.max_truncated = max_header.version == kFormatVersion2;
        decoded_exactness.min_exact_columns = exact_columns(min_header);
        decoded_exactness.max_exact_columns = exact_columns(max_header);
        break;
    }
    case BoundsSource::kNone:
        break;
    }
    *min = std::move(decoded_min);
    *max = std::move(decoded_max);
    if (exactness != nullptr) {
        *exactness = decoded_exactness;
    }
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
        RETURN_IF_ERROR(decode_segment_bound(meta.sort_key_min_encoded(), SortKeyBoundSide::kMin, 1, &decoded_min));
        RETURN_IF_ERROR(decode_segment_bound(meta.sort_key_max_encoded(), SortKeyBoundSide::kMax, 1, &decoded_max));
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
