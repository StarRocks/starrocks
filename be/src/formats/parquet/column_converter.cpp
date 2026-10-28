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

#include "formats/parquet/column_converter.h"

#include <cctz/time_zone.h>
#include <glog/logging.h>

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <cstring>
#include <limits>
#include <memory>
#include <sstream>
#include <type_traits>
#include <utility>
#include <vector>

#include "base/bit/bit_util.h"
#include "base/decimal_types.h"
#include "base/time/timezone_utils.h"
#include "base/time/tz_offset_cache.h"
#include "base/types/int96.h"
#include "column/binary_column.h"
#include "column/column.h"
#include "column/column_helper.h"
#include "column/fixed_length_column.h"
#include "column/geo_column.h"
#include "column/nullable_column.h"
#include "column/runtime_type_traits.h"
#include "column/vectorized_fwd.h"
#include "common/compiler_util.h"
#include "common/config_scan_io_fwd.h"
#include "formats/parquet/schema.h"
#include "formats/parquet/types.h"
#include "gutil/casts.h"
#include "gutil/integral_types.h"
#include "gutil/strings/substitute.h"
#include "types/date_value.h"
#include "types/integer_overflow_arithmetics.h"
#include "types/logical_type.h"
#include "types/time_types.h"
#include "types/timestamp_value.h"
#include "types/type_descriptor.h"

namespace starrocks::parquet {

// When doing decimal convert, source scale may not equal with destination scale,
// when destination scale is greater than source, source unscaled data will be scaled up
// to match destination scale.
enum class DecimalScaleType { kNoScale, kScaleUp, kScaleDown };

class Int32ToDateConverter final : public ColumnConverter {
public:
    Int32ToDateConverter() = default;
    ~Int32ToDateConverter() override = default;

    Status convert(const Column* src, Column* dst) override;
};

class Int32ToTimeConverter final : public ColumnConverter {
public:
    Int32ToTimeConverter() = default;
    ~Int32ToTimeConverter() override = default;

    Status convert(const Column* src, Column* dst) override;
};

class Int32ToDateTimeConverter final : public ColumnConverter {
public:
    Int32ToDateTimeConverter() = default;
    ~Int32ToDateTimeConverter() override = default;

    Status convert(const Column* src, Column* dst) override;
};

class Int64ToTimeConverter final : public ColumnConverter {
public:
    Int64ToTimeConverter() = default;
    ~Int64ToTimeConverter() override = default;

    Status convert(const Column* src, Column* dst) override;
};

class Int96ToDateTimeConverter final : public ColumnConverter {
public:
    Int96ToDateTimeConverter() = default;
    ~Int96ToDateTimeConverter() override = default;

    Status init(const std::string& timezone, bool as_timestamp_ntz);
    // convert column from int96 to timestamp
    Status convert(const Column* src, Column* dst) override;

private:
    int _offset = 0;
    cctz::time_zone _ctz;
    bool _as_timestamp_ntz = false;
    TzOffsetCache _offset_cache;
    std::vector<int64_t> _seconds;
    std::vector<int64_t> _offsets;
};

class FixedLenByteArrayToUUIDConverter final : public ColumnConverter {
public:
    FixedLenByteArrayToUUIDConverter() = default;
    ~FixedLenByteArrayToUUIDConverter() override = default;

    Status convert(const Column* src, Column* dst) override;
};

class Int64ToDateTimeConverter final : public ColumnConverter {
public:
    Int64ToDateTimeConverter() = default;
    ~Int64ToDateTimeConverter() override = default;

    Status init(const std::string& timezone, const tparquet::SchemaElement& schema_element);
    Status convert(const Column* src, Column* dst) override;
    Status convert_min_max(const Column* min_src, const Column* max_src, Column* min_dst, Column* max_dst) override;

private:
    bool _is_adjusted_to_utc = false;
    int _offset = 0;
    cctz::time_zone _ctz;
    int64_t _second_mask = 0;
    TzOffsetCache _offset_cache;
    std::vector<int64_t> _seconds;
    std::vector<int64_t> _offsets;
};

template <typename SourceType, typename DestType, bool kSourceUnsigned = false>
void convert_int_to_int(const SourceType* __restrict__ src, DestType* __restrict__ dst, size_t size) {
    for (size_t i = 0; i < size; i++) {
        if constexpr (kSourceUnsigned && std::is_integral_v<SourceType>) {
            // Reinterpret the source through its unsigned bit pattern before widening so a
            // high-bit unsigned value is zero-extended instead of sign-extended.
            dst[i] = DestType(static_cast<std::make_unsigned_t<SourceType>>(src[i]));
        } else {
            dst[i] = DestType(src[i]);
        }
    }
}

// Support int => int and float => double
template <typename SourceType, typename DestType, bool kSourceUnsigned = false>
class NumericToNumericConverter final : public ColumnConverter {
public:
    NumericToNumericConverter() = default;
    ~NumericToNumericConverter() override = default;

    Status convert(const Column* src, Column* dst) override {
        auto* src_nullable_column = ColumnHelper::as_raw_column<NullableColumn>(src);
        // hive only support null column
        // TODO: support not null
        auto* dst_nullable_column = down_cast<NullableColumn*>(dst);
        dst_nullable_column->resize_uninitialized(src_nullable_column->size());

        auto* src_column =
                ColumnHelper::as_raw_column<FixedLengthColumn<SourceType>>(src_nullable_column->data_column());
        auto* dst_column =
                ColumnHelper::as_raw_column<FixedLengthColumn<DestType>>(dst_nullable_column->data_column_raw_ptr());

        auto& src_data = src_column->get_data();
        auto& dst_data = dst_column->get_data();
        auto& src_null_data = src_nullable_column->null_column()->get_data();
        auto& dst_null_data = dst_nullable_column->null_column_raw_ptr()->get_data();

        size_t size = dst_null_data.size();
        memcpy(dst_null_data.data(), src_null_data.data(), size);
        convert_int_to_int<SourceType, DestType, kSourceUnsigned>(src_data.data(), dst_data.data(), size);
        dst_nullable_column->set_has_null(src_nullable_column->has_null());
        return Status::OK();
    }
};

template <typename SourceType, LogicalType DestType, bool kSourceUnsigned = false>
class PrimitiveToDecimalConverter final : public ColumnConverter {
public:
    using DestDecimalType = typename RunTimeTypeTraits<DestType>::CppType;
    using DestColumnType = typename RunTimeTypeTraits<DestType>::ColumnType;
    // Keep DECIMAL32/64 arithmetic in int64 when the scale factor fits. Wider factors are handled
    // before the loop, and multiplication/narrowing must be checked before storing each value.
    using DestPrimitiveType = std::conditional_t<(sizeof(DestDecimalType) <= sizeof(int64_t)), int64_t, int128_t>;

    PrimitiveToDecimalConverter(int32_t src_scale, int32_t dst_scale) {
        if (src_scale < dst_scale) {
            _scale_type = DecimalScaleType::kScaleUp;
            _scale_factor = get_scale_factor<int128_t>(dst_scale - src_scale);
        } else if (src_scale > dst_scale) {
            _scale_type = DecimalScaleType::kScaleDown;
            _scale_factor = get_scale_factor<int128_t>(src_scale - dst_scale);
        }
    }

    // _scale_type is loop-invariant but was previously re-checked on every element, which both adds a
    // per-row branch and defeats vectorization/ILP. Dispatch on it once via a template parameter instead,
    // matching the pattern already used by BinaryToDecimalConverter::t_convert below.
    template <DecimalScaleType ST>
    Status t_convert(size_t size, DestDecimalType* dst_data, const SourceType* src_data, const uint8_t* src_null_data) {
        if constexpr (sizeof(DestPrimitiveType) == sizeof(int64_t) && ST != DecimalScaleType::kNoScale) {
            if (_scale_factor > std::numeric_limits<int64_t>::max()) {
                // A larger factor divides every int32/int64 source to zero; multiplying a nonzero
                // source by it cannot fit DECIMAL32/64 storage.
                for (size_t i = 0; i < size; ++i) {
                    if (src_null_data[i]) continue;
                    if constexpr (ST == DecimalScaleType::kScaleUp) {
                        if (src_data[i] != 0) {
                            return Status::InvalidArgument(
                                    strings::Substitute("Decimal storage overflow at row $0", i));
                        }
                    }
                    dst_data[i] = DestDecimalType(0);
                }
                return Status::OK();
            }
        }
        const auto scale_factor = static_cast<DestPrimitiveType>(_scale_factor);
        for (size_t i = 0; i < size; i++) {
            // A null row's source payload may be uninitialized.
            if (src_null_data[i]) continue;
            DestPrimitiveType value;
            if constexpr (kSourceUnsigned && std::is_integral_v<SourceType>) {
                // Reinterpret the source through its unsigned bit pattern so a high-bit
                // unsigned value is zero-extended instead of sign-extended.
                value = static_cast<std::make_unsigned_t<SourceType>>(src_data[i]);
            } else {
                value = src_data[i];
            }
            if constexpr (ST == DecimalScaleType::kScaleUp) {
                if (__builtin_mul_overflow(value, scale_factor, &value)) {
                    return Status::InvalidArgument(strings::Substitute("Decimal scale overflow at row $0", i));
                }
            } else if constexpr (ST == DecimalScaleType::kScaleDown) {
                value /= scale_factor;
            }
            if constexpr (DestType == TYPE_DECIMAL32 || DestType == TYPE_DECIMAL64) {
                if (value < std::numeric_limits<DestDecimalType>::min() ||
                    value > std::numeric_limits<DestDecimalType>::max()) {
                    return Status::InvalidArgument(strings::Substitute("Decimal storage overflow at row $0", i));
                }
            }
            dst_data[i] = DestDecimalType(value);
        }
        return Status::OK();
    }

    Status convert(const Column* src, Column* dst) override {
        auto* src_nullable_column = ColumnHelper::as_raw_column<NullableColumn>(src);
        // hive only support null column
        // TODO: support not null
        auto* dst_nullable_column = down_cast<NullableColumn*>(dst);
        dst_nullable_column->resize_uninitialized(src_nullable_column->size());

        auto* src_column =
                ColumnHelper::as_raw_column<FixedLengthColumn<SourceType>>(src_nullable_column->data_column());
        auto* dst_column = ColumnHelper::as_raw_column<DestColumnType>(dst_nullable_column->data_column_raw_ptr());

        auto& src_data = src_column->get_data();
        auto& dst_data = dst_column->get_data();
        auto& src_null_data = src_nullable_column->null_column()->get_data();
        auto& dst_null_data = dst_nullable_column->null_column_raw_ptr()->get_data();

        size_t size = src_column->size();
        memcpy(dst_null_data.data(), src_null_data.data(), size);

        switch (_scale_type) {
        case DecimalScaleType::kScaleUp:
            RETURN_IF_ERROR(t_convert<DecimalScaleType::kScaleUp>(size, dst_data.data(), src_data.data(),
                                                                  src_null_data.data()));
            break;
        case DecimalScaleType::kScaleDown:
            RETURN_IF_ERROR(t_convert<DecimalScaleType::kScaleDown>(size, dst_data.data(), src_data.data(),
                                                                    src_null_data.data()));
            break;
        default:
            RETURN_IF_ERROR(t_convert<DecimalScaleType::kNoScale>(size, dst_data.data(), src_data.data(),
                                                                  src_null_data.data()));
            break;
        }

        dst_nullable_column->set_has_null(src_nullable_column->has_null());
        return Status::OK();
    }

private:
    DecimalScaleType _scale_type = DecimalScaleType::kNoScale;
    int128_t _scale_factor = 1;
};

/*
Convert Parquet binary decimals from big-endian, two's-complement bytes:
- FIXED_LEN_BYTE_ARRAY with widths 1-16:
    - non-null values have one width and are packed together.
    - t_convert advances through the byte buffer by that width without building slices.
- BYTE_ARRAY and FIXED_LEN_BYTE_ARRAY wider than 16 bytes:
    - row lengths can differ or include redundant leading sign bytes.
    - convert_slices gets each value's start and length from its slice and decodes only those bytes.
Both paths adjust the unscaled integer from the source scale to the destination scale.
*/
template <LogicalType DestType>
class BinaryToDecimalConverter final : public ColumnConverter {
public:
    using DecimalType = typename RunTimeTypeTraits<DestType>::CppType;
    using ColumnType = typename RunTimeTypeTraits<DestType>::ColumnType;
    using DestPrimitiveType = typename RunTimeTypeTraits<TYPE_DECIMAL128>::CppType;
    BinaryToDecimalConverter(int32_t src_scale, int32_t dst_scale, int32_t type_length) {
        if (src_scale < dst_scale) {
            _scale_type = DecimalScaleType::kScaleUp;
            _scale_factor = get_scale_factor<DestPrimitiveType>(dst_scale - src_scale);
        } else if (src_scale > dst_scale) {
            _scale_type = DecimalScaleType::kScaleDown;
            _scale_factor = get_scale_factor<DestPrimitiveType>(src_scale - dst_scale);
        }
        _type_length = type_length;
    }

    template <int BINSZ, DecimalScaleType scale_type, typename T, bool has_null>
    Status t_convert(size_t size, uint8_t* dst_null_data, uint8_t* src_null_data, DecimalType* dst_data,
                     const uint8* src_data) {
        if constexpr (!has_null) {
            memset(dst_null_data, 0x0, size);
        } else {
            memcpy(dst_null_data, src_null_data, size);
        }

        for (size_t i = 0; i < size; i++) {
            if constexpr (has_null) {
                // Null rows occupy no bytes, so do not advance src_data for them.
                if (dst_null_data[i]) continue;
            }

            DestPrimitiveType unscale = 0;
            T value = 0;
            static_assert(BINSZ <= sizeof(value));

            /*
            Decode BINSZ bytes with a fixed-size load:
            - T is int64_t for widths 1-8 and int128_t for widths 9-16.
            - memcpy has a compile-time byte count. The compiler can replace it with
              register loads and keep the byte-order conversion and shift in registers.
            - This path relies on the Parquet decoders' RawVectorPad16 storage:
              non-null values are contiguous, and the allocation includes 16 extra bytes.
              Reading sizeof(T) bytes is safe even when BINSZ is smaller, including
              for the last value in the buffer.
            - Convert from big-endian byte order, then shift away the extra bytes.
              T is signed, so the right shift fills the high bits with the sign.
            */
            memcpy(reinterpret_cast<char*>(&value), src_data, sizeof(value));
            value = BitUtil::big_endian_to_host(value);
            value = value >> ((sizeof(value) - BINSZ) * 8);
            unscale = value;

            src_data += BINSZ;
            // hardware branch predicator works well here.
            if constexpr (scale_type == DecimalScaleType::kScaleUp) {
                if (__builtin_mul_overflow(unscale, _scale_factor, &unscale)) {
                    return Status::InvalidArgument(strings::Substitute("Binary decimal scale overflow at row $0", i));
                }
            }
            if constexpr (scale_type == DecimalScaleType::kScaleDown) {
                unscale /= _scale_factor;
            }
            if constexpr (DestType == TYPE_DECIMAL32 || DestType == TYPE_DECIMAL64) {
                if (unscale < std::numeric_limits<DecimalType>::min() ||
                    unscale > std::numeric_limits<DecimalType>::max()) {
                    return Status::InvalidArgument(strings::Substitute("Binary decimal storage overflow at row $0", i));
                }
            }
            dst_data[i] = DecimalType(unscale);
        }
        return Status::OK();
    }

    Status convert_slices(const BinaryColumn* src_column, const NullableColumn* src_nullable_column,
                          NullableColumn* dst_nullable_column, ColumnType* dst_column) {
        /*
        Steps to convert binary decimals to the destination decimal type:
        1. Preserve null rows and treat empty slices as null:
            - Copy the source null flag and skip null rows.
            - Filtered rows can leave empty placeholders, so treat empty slices as null.

        2. Ensure the encoded integer fits in 128 bits:
            - For values longer than 16 bytes, remove redundant leading sign bytes:
                - Drop 0x00 only if the next byte's sign bit is zero.
                - Drop 0xff only if the next byte's sign bit is one.
            - Return an error if the remaining value still exceeds 128 bits.

        3. Decode the bytes as a signed integer:
            - Start an unsigned 128-bit word with zeros (for a non-negative value) or ones (for a negative value).
            - Shift in each byte, starting with the most significant byte.
            - Reconstruct the signed integer (unused high bytes retain the sign).

        4. Adjust the integer to the destination scale:
            - Larger destination scale: multiply and check for overflow
            - Smaller destination scale: divide
            - Matching scales: do nothing

        5. Check storage bounds:
            - For DECIMAL32 and DECIMAL64, destination integer's storage range may be insufficient.
                - Check to make sure it fits, error if not

        6. Empty slices might've added null rows, so update destination's has_null flag after the batch.
        */

        auto& dst_data = dst_column->get_data();
        const auto& src_nulls = src_nullable_column->null_column()->get_data();
        auto& dst_nulls = dst_nullable_column->null_column_raw_ptr()->get_data();

        for (size_t i = 0; i < src_column->size(); ++i) {
            // Step 1: handle null and empty rows.
            dst_nulls[i] = src_nulls[i];
            if (dst_nulls[i]) continue;
            Slice value = src_column->get_slice(i);

            if (value.empty()) {
                /*
                    (shortcut) It's not possible to distinguish between:
                        - persisted empty decimals
                        - filtered row defaults

                    So reject them when the decoder exposes selection.
                */
                dst_nulls[i] = 1;
                continue;
            }

            // Step 2: remove redundant sign bytes and check the 128-bit limit.
            const auto* bytes = reinterpret_cast<const uint8_t*>(value.data);
            size_t length = value.size;

            while (length > sizeof(int128_t) &&
                   ((bytes[0] == 0 && bytes[1] < 0x80) || (bytes[0] == 0xff && bytes[1] >= 0x80))) {
                ++bytes;
                --length;
            }

            if (length > sizeof(int128_t)) {
                return Status::InvalidArgument(strings::Substitute("Binary decimal exceeds 128 bits at row $0", i));
            }

            // Step 3: decode and sign-extend the integer.
            const bool negative = bytes[0] >= 0x80;
            uint128_t word = negative ? ~uint128_t{0} : uint128_t{0};
            for (size_t j = 0; j < length; ++j) {
                word = (word << 8) | bytes[j];
            }

            int128_t unscaled = negative ? -static_cast<int128_t>(~word) - 1 : static_cast<int128_t>(word);

            // Step 4: adjust the scale.
            if (_scale_type == DecimalScaleType::kScaleUp) {
                int128_t scaled;
                if (__builtin_mul_overflow(unscaled, _scale_factor, &scaled)) {
                    return Status::InvalidArgument(strings::Substitute("Binary decimal scale overflow at row $0", i));
                }
                unscaled = scaled;
            } else if (_scale_type == DecimalScaleType::kScaleDown) {
                unscaled /= _scale_factor;
            }

            // Step 5: check DECIMAL32/64 storage bounds, then store the value.
            if constexpr (DestType == TYPE_DECIMAL32 || DestType == TYPE_DECIMAL64) {
                if (unscaled < std::numeric_limits<DecimalType>::min() ||
                    unscaled > std::numeric_limits<DecimalType>::max()) {
                    return Status::InvalidArgument(strings::Substitute("Binary decimal storage overflow at row $0", i));
                }
            }

            dst_data[i] = DecimalType(unscaled);
        }

        // Step 6: update the column's has_null flag.
        dst_nullable_column->update_has_null();
        return Status::OK();
    }

    Status convert(const Column* src, Column* dst) override {
        auto* src_nullable_column = ColumnHelper::as_raw_column<NullableColumn>(src);
        // hive only support null column
        // TODO: support not null
        auto* dst_nullable_column = down_cast<NullableColumn*>(dst);
        dst_nullable_column->resize_uninitialized(src_nullable_column->size());

        auto* src_column = ColumnHelper::as_raw_column<BinaryColumn>(src_nullable_column->data_column());
        auto* dst_column = ColumnHelper::as_raw_column<ColumnType>(dst_nullable_column->data_column_raw_ptr());

        auto src_data = src_column->get_immutable_bytes();
        auto& dst_data = dst_column->get_data();
        auto& src_null_data = src_nullable_column->null_column()->get_data();
        auto& dst_null_data = dst_nullable_column->null_column_raw_ptr()->get_data();

        size_t size = src_column->size();
        if (parquet_type == tparquet::Type::BYTE_ARRAY || _type_length > sizeof(DestPrimitiveType)) {
            return convert_slices(src_column, src_nullable_column, dst_nullable_column, dst_column);
        }

        bool has_null = src_nullable_column->has_null();

#define M(SZ, K, T)                                                                                                    \
    case SZ:                                                                                                           \
        if (has_null) {                                                                                                \
            RETURN_IF_ERROR(                                                                                           \
                    (t_convert<SZ, K, T, true>(size, dst_null_data.data(), const_cast<uint8_t*>(src_null_data.data()), \
                                               dst_data.data(), src_data.data())));                                    \
        } else {                                                                                                       \
            RETURN_IF_ERROR((t_convert<SZ, K, T, false>(size, dst_null_data.data(),                                    \
                                                        const_cast<uint8_t*>(src_null_data.data()), dst_data.data(),   \
                                                        src_data.data())));                                            \
        }                                                                                                              \
        break;

#define MX(T)          \
    M(1, T, int64_t)   \
    M(2, T, int64_t)   \
    M(3, T, int64_t)   \
    M(4, T, int64_t)   \
    M(5, T, int64_t)   \
    M(6, T, int64_t)   \
    M(7, T, int64_t)   \
    M(8, T, int64_t)   \
    M(9, T, int128_t)  \
    M(10, T, int128_t) \
    M(11, T, int128_t) M(12, T, int128_t) M(13, T, int128_t) M(14, T, int128_t) M(15, T, int128_t) M(16, T, int128_t)

        if (_scale_type == DecimalScaleType::kScaleUp) {
            switch (_type_length) {
                MX(DecimalScaleType::kScaleUp);
            default:
                __builtin_unreachable();
            }
        } else if (_scale_type == DecimalScaleType::kScaleDown) {
            switch (_type_length) {
                MX(DecimalScaleType::kScaleDown);
            default:
                __builtin_unreachable();
            }
        } else {
            switch (_type_length) {
                MX(DecimalScaleType::kNoScale);
            default:
                __builtin_unreachable();
            }
        }
        dst_nullable_column->set_has_null(has_null);
        return Status::OK();
    }

private:
    DecimalScaleType _scale_type = DecimalScaleType::kNoScale;
    DestPrimitiveType _scale_factor = 1;
    int32_t _type_length = 0;
};

template <LogicalType DestType>
class WideDecimalConverter final : public ColumnConverter {
public:
    using DecimalType = typename RunTimeTypeTraits<DestType>::CppType;
    using ColumnType = typename RunTimeTypeTraits<DestType>::ColumnType;

    WideDecimalConverter(int32_t src_scale, int32_t dst_scale, bool src_unsigned)
            : _scale_type(src_scale < dst_scale
                                  ? DecimalScaleType::kScaleUp
                                  : src_scale > dst_scale ? DecimalScaleType::kScaleDown : DecimalScaleType::kNoScale),
              _scale_factor(get_scale_factor<int256_t>(src_scale < dst_scale ? dst_scale - src_scale
                                                                             : src_scale - dst_scale)),
              _src_unsigned(src_unsigned) {}

    Status convert(const Column* src, Column* dst) override {
        auto* source = ColumnHelper::as_raw_column<NullableColumn>(src);
        auto* dest = down_cast<NullableColumn*>(dst);
        dest->resize_uninitialized(source->size());
        auto* dest_column = ColumnHelper::as_raw_column<ColumnType>(dest->data_column_raw_ptr());
        auto& values = dest_column->get_data();
        const auto& source_nulls = source->null_column()->get_data();
        auto& dest_nulls = dest->null_column_raw_ptr()->get_data();
        for (size_t i = 0; i < source->size(); ++i) {
            dest_nulls[i] = source_nulls[i];
            if (dest_nulls[i]) continue;
            int256_t value;
            if (parquet_type == tparquet::Type::INT32) {
                auto* column = ColumnHelper::as_raw_column<FixedLengthColumn<int32_t>>(source->data_column());
                int32_t raw = column->get_data()[i];
                value = _src_unsigned ? int256_t(static_cast<uint32_t>(raw)) : int256_t(raw);
            } else if (parquet_type == tparquet::Type::INT64) {
                auto* column = ColumnHelper::as_raw_column<FixedLengthColumn<int64_t>>(source->data_column());
                value = int256_t(column->get_data()[i]);
            } else {
                auto* column = ColumnHelper::as_raw_column<BinaryColumn>(source->data_column());
                Slice slice = column->get_slice(i);
                if (slice.empty()) {
                    dest_nulls[i] = 1;
                    continue;
                }
                const auto* bytes = reinterpret_cast<const uint8_t*>(slice.data);
                size_t length = slice.size;
                while (length > 32 && ((bytes[0] == 0 && bytes[1] < 0x80) || (bytes[0] == 0xff && bytes[1] >= 0x80))) {
                    ++bytes;
                    --length;
                }
                if (length > 32) {
                    return Status::InvalidArgument(strings::Substitute("Binary decimal exceeds 256 bits at row $0", i));
                }
                value = bytes[0] >= 0x80 ? int256_t(-1) : int256_t(0);
                for (size_t j = 0; j < length; ++j) {
                    value = (value << 8) | int256_t(bytes[j]);
                }
            }
            if (_scale_type == DecimalScaleType::kScaleUp) {
                int256_t scaled;
                if (mul_overflow(value, _scale_factor, &scaled)) {
                    return Status::InvalidArgument(strings::Substitute("Decimal scale overflow at row $0", i));
                }
                value = scaled;
            } else if (_scale_type == DecimalScaleType::kScaleDown) {
                if (value == INT256_MIN) {
                    return Status::InvalidArgument(
                            strings::Substitute("Decimal exceeds supported precision at row $0", i));
                }
                value = value / _scale_factor;
            }
            if constexpr (DestType != TYPE_DECIMAL256) {
                using LimitType = std::conditional_t<DestType == TYPE_DECIMALV2, int128_t, DecimalType>;
                if (value < int256_t(std::numeric_limits<LimitType>::min()) ||
                    value > int256_t(std::numeric_limits<LimitType>::max())) {
                    return Status::InvalidArgument(strings::Substitute("Decimal storage overflow at row $0", i));
                }
                values[i] = DecimalType(static_cast<int128_t>(value));
            } else {
                values[i] = value;
            }
        }
        dest->update_has_null();
        return Status::OK();
    }

private:
    DecimalScaleType _scale_type;
    int256_t _scale_factor;
    bool _src_unsigned;
};

// UUID is stored as 16 raw bytes; the canonical string form is 36 characters:
// xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx
static constexpr int kUUIDByteLength = 16;
static constexpr int kUUIDStringLength = 36;

Status FixedLenByteArrayToUUIDConverter::convert(const Column* src, Column* dst) {
    auto* src_nullable = ColumnHelper::as_raw_column<NullableColumn>(src);
    auto* dst_nullable = down_cast<NullableColumn*>(dst);
    auto* src_col = ColumnHelper::as_raw_column<BinaryColumn>(src_nullable->data_column());
    auto* dst_col = ColumnHelper::as_raw_column<BinaryColumn>(dst_nullable->data_column_raw_ptr());

    const auto& src_null = src_nullable->null_column()->get_data();
    auto& dst_null = dst_nullable->null_column_raw_ptr()->get_data();

    size_t n = src_col->size();
    dst_null.resize(n);
    memcpy(dst_null.data(), src_null.data(), n);

    static constexpr char HEX[] = "0123456789abcdef";
    char buf[kUUIDStringLength];

    for (size_t i = 0; i < n; i++) {
        if (src_null[i]) {
            dst_col->append_default();
            continue;
        }
        Slice s = src_col->get_slice(i);
        if (UNLIKELY(s.size != kUUIDByteLength)) {
            return Status::Corruption(strings::Substitute(
                    "parquet UUID column has unexpected byte length $0, expected $1", s.size, kUUIDByteLength));
        }
        const auto* bytes = reinterpret_cast<const uint8_t*>(s.data);
        int pos = 0;
        for (int j = 0; j < 4; j++) {
            buf[pos++] = HEX[bytes[j] >> 4];
            buf[pos++] = HEX[bytes[j] & 0xf];
        }
        buf[pos++] = '-';
        for (int j = 4; j < 6; j++) {
            buf[pos++] = HEX[bytes[j] >> 4];
            buf[pos++] = HEX[bytes[j] & 0xf];
        }
        buf[pos++] = '-';
        for (int j = 6; j < 8; j++) {
            buf[pos++] = HEX[bytes[j] >> 4];
            buf[pos++] = HEX[bytes[j] & 0xf];
        }
        buf[pos++] = '-';
        for (int j = 8; j < 10; j++) {
            buf[pos++] = HEX[bytes[j] >> 4];
            buf[pos++] = HEX[bytes[j] & 0xf];
        }
        buf[pos++] = '-';
        for (int j = 10; j < kUUIDByteLength; j++) {
            buf[pos++] = HEX[bytes[j] >> 4];
            buf[pos++] = HEX[bytes[j] & 0xf];
        }
        dst_col->append(Slice(buf, kUUIDStringLength));
    }
    dst_nullable->set_has_null(src_nullable->has_null());
    return Status::OK();
}

template <typename DestType>
static std::unique_ptr<ColumnConverter> make_int32_numeric_converter(bool src_unsigned) {
    if (src_unsigned) {
        return std::make_unique<NumericToNumericConverter<int32_t, DestType, true>>();
    }
    return std::make_unique<NumericToNumericConverter<int32_t, DestType, false>>();
}

template <LogicalType DestType>
static std::unique_ptr<ColumnConverter> make_int32_decimal_converter(bool src_unsigned, int32_t src_scale,
                                                                     int32_t dst_scale) {
    if (src_unsigned) {
        return std::make_unique<PrimitiveToDecimalConverter<int32_t, DestType, true>>(src_scale, dst_scale);
    }
    return std::make_unique<PrimitiveToDecimalConverter<int32_t, DestType, false>>(src_scale, dst_scale);
}

class BinaryToGeographyConverter final : public ColumnConverter {
public:
    Status convert(const Column* src, Column* dst) override;
};

Status ColumnConverterFactory::create_converter(const ParquetField& field, const TypeDescriptor& typeDescriptor,
                                                const std::string& timezone,
                                                std::unique_ptr<ColumnConverter>* converter) {
    LogicalType col_type = typeDescriptor.type;
    bool need_convert = false;
    tparquet::Type::type parquet_type = field.physical_type;
    const auto& schema_element = field.schema_element;

    if ((parquet_type == tparquet::Type::INT32 || parquet_type == tparquet::Type::INT64 ||
         parquet_type == tparquet::Type::BYTE_ARRAY || parquet_type == tparquet::Type::FIXED_LEN_BYTE_ARRAY) &&
        (col_type == TYPE_DECIMALV2 || col_type == TYPE_DECIMAL32 || col_type == TYPE_DECIMAL64 ||
         col_type == TYPE_DECIMAL128 || col_type == TYPE_DECIMAL256)) {
        const int target_scale = col_type == TYPE_DECIMALV2 ? 9 : typeDescriptor.scale;
        if (field.precision > 76 || typeDescriptor.precision > 76 || field.scale < 0 || field.scale > 76 ||
            target_scale < 0 || target_scale > 76 || (field.precision > 0 && field.scale > field.precision) ||
            (col_type != TYPE_DECIMALV2 && typeDescriptor.precision > 0 && target_scale > typeDescriptor.precision)) {
            return Status::InvalidArgument("Invalid Parquet decimal precision or scale");
        }
        if (col_type == TYPE_DECIMAL256 || field.precision > 38 || field.scale > 38 || target_scale > 38) {
            const bool src_unsigned = parquet_type == tparquet::Type::INT32 && is_unsigned_integer(schema_element);
            switch (col_type) {
            case TYPE_DECIMALV2:
                *converter =
                        std::make_unique<WideDecimalConverter<TYPE_DECIMALV2>>(field.scale, target_scale, src_unsigned);
                break;
            case TYPE_DECIMAL32:
                *converter =
                        std::make_unique<WideDecimalConverter<TYPE_DECIMAL32>>(field.scale, target_scale, src_unsigned);
                break;
            case TYPE_DECIMAL64:
                *converter =
                        std::make_unique<WideDecimalConverter<TYPE_DECIMAL64>>(field.scale, target_scale, src_unsigned);
                break;
            case TYPE_DECIMAL128:
                *converter = std::make_unique<WideDecimalConverter<TYPE_DECIMAL128>>(field.scale, target_scale,
                                                                                     src_unsigned);
                break;
            case TYPE_DECIMAL256:
                *converter = std::make_unique<WideDecimalConverter<TYPE_DECIMAL256>>(field.scale, target_scale,
                                                                                     src_unsigned);
                break;
            default:
                break;
            }
            (*converter)->init_info(true, parquet_type);
            return Status::OK();
        }
    }

    // the reason why there is down conversion of integer type is
    // assume we create a hive column called `col0` whose type is `tinyint`
    // but when we insert value into `col0`, the physical type in parquet file is actually `INT32`
    // so when we read `col0` from parquet file, we have to do a type conversion from int32_t to int8_t.
    switch (parquet_type) {
    case tparquet::Type::type::BOOLEAN: {
        if (col_type != LogicalType::TYPE_BOOLEAN) {
            need_convert = true;
        }
        break;
    }
    case tparquet::Type::type::INT32: {
        const bool src_unsigned = is_unsigned_integer(schema_element);
        if (col_type != LogicalType::TYPE_INT) {
            need_convert = true;
        }
        switch (col_type) {
        case LogicalType::TYPE_TINYINT:
            *converter = make_int32_numeric_converter<int8_t>(src_unsigned);
            break;
        case LogicalType::TYPE_SMALLINT:
            *converter = make_int32_numeric_converter<int16_t>(src_unsigned);
            break;
        case LogicalType::TYPE_BIGINT:
            *converter = make_int32_numeric_converter<int64_t>(src_unsigned);
            break;
        case LogicalType::TYPE_DOUBLE:
            *converter = make_int32_numeric_converter<double>(src_unsigned);
            break;
        case LogicalType::TYPE_TIME:
            *converter = std::make_unique<Int32ToTimeConverter>();
            break;
        case LogicalType::TYPE_DATE:
            *converter = std::make_unique<Int32ToDateConverter>();
            break;
        case LogicalType::TYPE_DATETIME:
            *converter = std::make_unique<Int32ToDateTimeConverter>();
            break;
            // when decimal precision is greater than 27, precision may be lost in the following
            // process. However to handle most enviroment, we also make progress other than
            // rejection
        case LogicalType::TYPE_DECIMALV2:
            // All DecimalV2 use scale 9 as scale
            *converter = make_int32_decimal_converter<TYPE_DECIMALV2>(src_unsigned, field.scale, 9);
            break;
        case LogicalType::TYPE_DECIMAL32:
            *converter = make_int32_decimal_converter<TYPE_DECIMAL32>(src_unsigned, field.scale, typeDescriptor.scale);
            break;
        case LogicalType::TYPE_DECIMAL64:
            *converter = make_int32_decimal_converter<TYPE_DECIMAL64>(src_unsigned, field.scale, typeDescriptor.scale);
            break;
        case LogicalType::TYPE_DECIMAL128:
            *converter = make_int32_decimal_converter<TYPE_DECIMAL128>(src_unsigned, field.scale, typeDescriptor.scale);
            break;
        default:
            break;
        }
        break;
    }
    case tparquet::Type::type::INT64: {
        if (col_type != LogicalType::TYPE_BIGINT) {
            need_convert = true;
        }
        switch (col_type) {
        case LogicalType::TYPE_TINYINT:
            *converter = std::make_unique<NumericToNumericConverter<int64_t, int8_t>>();
            break;
        case LogicalType::TYPE_SMALLINT:
            *converter = std::make_unique<NumericToNumericConverter<int64_t, int16_t>>();
            break;
        case LogicalType::TYPE_INT:
            *converter = std::make_unique<NumericToNumericConverter<int64_t, int32_t>>();
            break;
            // when decimal precision is greater than 27, precision may be lost in the following
            // process. However to handle most enviroment, we also make progress other than
            // rejection
        case LogicalType::TYPE_DECIMALV2:
            // All DecimalV2 use scale 9 as scale
            *converter = std::make_unique<PrimitiveToDecimalConverter<int64_t, TYPE_DECIMALV2>>(field.scale, 9);
            break;
        case LogicalType::TYPE_DECIMAL32:
            *converter = std::make_unique<PrimitiveToDecimalConverter<int64_t, TYPE_DECIMAL32>>(field.scale,
                                                                                                typeDescriptor.scale);
            break;
        case LogicalType::TYPE_DECIMAL64:
            *converter = std::make_unique<PrimitiveToDecimalConverter<int64_t, TYPE_DECIMAL64>>(field.scale,
                                                                                                typeDescriptor.scale);
            break;
        case LogicalType::TYPE_DECIMAL128:
            *converter = std::make_unique<PrimitiveToDecimalConverter<int64_t, TYPE_DECIMAL128>>(field.scale,
                                                                                                 typeDescriptor.scale);
            break;

        case LogicalType::TYPE_DATETIME: {
            auto _converter = std::make_unique<Int64ToDateTimeConverter>();
            RETURN_IF_ERROR(_converter->init(timezone, schema_element));
            *converter = std::move(_converter);
            break;
        }
        case LogicalType::TYPE_TIME: {
            auto _converter = std::make_unique<Int64ToTimeConverter>();
            *converter = std::move(_converter);
            break;
        }
        default:
            break;
        }
        break;
    }
    case tparquet::Type::type::BYTE_ARRAY: {
        if (col_type != LogicalType::TYPE_VARCHAR && col_type != LogicalType::TYPE_CHAR &&
            col_type != LogicalType::TYPE_VARBINARY) {
            need_convert = true;
        }
        if (col_type == TYPE_GEOGRAPHY) {
            *converter = std::make_unique<BinaryToGeographyConverter>();
        } else if (col_type == TYPE_DECIMALV2 || col_type == TYPE_DECIMAL32 || col_type == TYPE_DECIMAL64 ||
                   col_type == TYPE_DECIMAL128) {
            const int target_scale = col_type == TYPE_DECIMALV2 ? 9 : typeDescriptor.scale;
            switch (col_type) {
            case TYPE_DECIMALV2:
                *converter = std::make_unique<BinaryToDecimalConverter<TYPE_DECIMALV2>>(field.scale, target_scale, 0);
                break;
            case TYPE_DECIMAL32:
                *converter = std::make_unique<BinaryToDecimalConverter<TYPE_DECIMAL32>>(field.scale, target_scale, 0);
                break;
            case TYPE_DECIMAL64:
                *converter = std::make_unique<BinaryToDecimalConverter<TYPE_DECIMAL64>>(field.scale, target_scale, 0);
                break;
            case TYPE_DECIMAL128:
                *converter = std::make_unique<BinaryToDecimalConverter<TYPE_DECIMAL128>>(field.scale, target_scale, 0);
                break;
            default:
                break;
            }
        }
        break;
    }
    case tparquet::Type::type::FIXED_LEN_BYTE_ARRAY: {
        int32_t type_length = field.type_length;
        bool is_uuid = schema_element.__isset.logicalType && schema_element.logicalType.__isset.UUID;
        if (col_type != LogicalType::TYPE_VARCHAR && col_type != LogicalType::TYPE_CHAR &&
            col_type != LogicalType::TYPE_VARBINARY) {
            need_convert = true;
        }
        switch (col_type) {
        case LogicalType::TYPE_DECIMALV2:
            // All DecimalV2 use scale 9 as scale
            *converter = std::make_unique<BinaryToDecimalConverter<TYPE_DECIMALV2>>(field.scale, 9, type_length);
            break;
        case LogicalType::TYPE_DECIMAL32:
            *converter = std::make_unique<BinaryToDecimalConverter<TYPE_DECIMAL32>>(field.scale, typeDescriptor.scale,
                                                                                    type_length);
            break;
        case LogicalType::TYPE_DECIMAL64:
            *converter = std::make_unique<BinaryToDecimalConverter<TYPE_DECIMAL64>>(field.scale, typeDescriptor.scale,
                                                                                    type_length);
            break;
        case LogicalType::TYPE_DECIMAL128:
            *converter = std::make_unique<BinaryToDecimalConverter<TYPE_DECIMAL128>>(field.scale, typeDescriptor.scale,
                                                                                     type_length);
            break;
        case LogicalType::TYPE_VARCHAR:
        case LogicalType::TYPE_CHAR:
            if (is_uuid) {
                need_convert = true;
                *converter = std::make_unique<FixedLenByteArrayToUUIDConverter>();
            }
            break;
        default:
            break;
        }
        break;
    }
    case tparquet::Type::type::INT96: {
        need_convert = true;
        if (col_type == LogicalType::TYPE_DATETIME) {
            auto _converter = std::make_unique<Int96ToDateTimeConverter>();
            RETURN_IF_ERROR(_converter->init(timezone, typeDescriptor.datetime_is_ntz));
            *converter = std::move(_converter);
        }
        break;
    }
    case tparquet::Type::FLOAT: {
        if (col_type != LogicalType::TYPE_FLOAT) {
            need_convert = true;
        }
        if (col_type == LogicalType::TYPE_DOUBLE) {
            auto _converter = std::make_unique<NumericToNumericConverter<float, double>>();
            *converter = std::move(_converter);
            break;
        }
        break;
    }
    case tparquet::Type::DOUBLE: {
        if (col_type != LogicalType::TYPE_DOUBLE) {
            need_convert = true;
        }
        break;
    }
    default:
        need_convert = true;
        break;
    }

    if (need_convert && *converter == nullptr) {
        return Status::NotSupported(strings::Substitute(
                "parquet column reader: not supported convert from parquet `$0` to `$1`, field=$2",
                ::tparquet::to_string(parquet_type), type_to_string(col_type), field.debug_string()));
    }

    if (!need_convert) {
        *converter = std::make_unique<ColumnConverter>();
    }
    (*converter)->init_info(need_convert, parquet_type);
    return Status::OK();
}

MutableColumnPtr ColumnConverter::create_src_column() {
    MutableColumnPtr data_column = nullptr;
    switch (parquet_type) {
    case tparquet::Type::type::BOOLEAN:
        data_column = FixedLengthColumn<uint8_t>::create();
        break;
    case tparquet::Type::type::INT32:
        data_column = FixedLengthColumn<PhysicalTypeTraits<tparquet::Type::INT32>::CppType>::create();
        break;
    case tparquet::Type::type::INT64:
        data_column = FixedLengthColumn<PhysicalTypeTraits<tparquet::Type::INT64>::CppType>::create();
        break;
    case tparquet::Type::type::INT96:
        data_column = FixedLengthColumn<PhysicalTypeTraits<tparquet::Type::INT96>::CppType>::create();
        break;
    case tparquet::Type::type::FLOAT:
        data_column = FixedLengthColumn<PhysicalTypeTraits<tparquet::Type::FLOAT>::CppType>::create();
        break;
    case tparquet::Type::type::DOUBLE:
        data_column = FixedLengthColumn<PhysicalTypeTraits<tparquet::Type::DOUBLE>::CppType>::create();
        break;
    case tparquet::Type::type::BYTE_ARRAY:
    case tparquet::Type::type::FIXED_LEN_BYTE_ARRAY:
        data_column = BinaryColumn::create();
        break;
    }
    return NullableColumn::create(std::move(data_column), NullColumn::create());
}

Status parquet::Int32ToDateConverter::convert(const Column* src, Column* dst) {
    auto* src_nullable_column = ColumnHelper::as_raw_column<NullableColumn>(src);
    // hive only support null column
    // TODO: support not null
    auto* dst_nullable_column = down_cast<NullableColumn*>(dst);
    dst_nullable_column->resize_uninitialized(src_nullable_column->size());

    auto* src_column = ColumnHelper::as_raw_column<FixedLengthColumn<int32_t>>(src_nullable_column->data_column());
    auto* dst_column = ColumnHelper::as_raw_column<DateColumn>(dst_nullable_column->data_column_raw_ptr());

    auto& src_data = src_column->get_data();
    auto& dst_data = dst_column->get_data();
    auto& src_null_data = src_nullable_column->null_column()->get_data();
    auto& dst_null_data = dst_nullable_column->null_column_raw_ptr()->get_data();

    size_t size = dst_null_data.size();
    memcpy(dst_null_data.data(), src_null_data.data(), size);
    // DateValue's storage is a single int32_t (_julian), so we can stride through
    // dst_data as raw int32 and add the epoch with SIMD.
    const int32_t* src_ptr = src_data.data();
    int32_t* dst_ptr = reinterpret_cast<int32_t*>(dst_data.data());
    const int32_t epoch = date::UNIX_EPOCH_JULIAN;
    // Compiler auto-vectorises the broadcast-add.
    for (size_t i = 0; i < size; i++) {
        dst_ptr[i] = src_ptr[i] + epoch;
    }
    dst_nullable_column->set_has_null(src_nullable_column->has_null());
    return Status::OK();
}

Status Int32ToTimeConverter::convert(const Column* src, Column* dst) {
    auto* src_nullable_column = ColumnHelper::as_raw_column<NullableColumn>(src);
    // hive only support null column
    // TODO: support not null
    auto* dst_nullable_column = down_cast<NullableColumn*>(dst);
    dst_nullable_column->resize_uninitialized(src_nullable_column->size());

    auto* src_column = ColumnHelper::as_raw_column<FixedLengthColumn<int32_t>>(src_nullable_column->data_column());
    auto* dst_column = ColumnHelper::as_raw_column<DoubleColumn>(dst_nullable_column->data_column_raw_ptr());

    auto& src_data = src_column->get_data();
    auto& dst_data = dst_column->get_data();
    auto& src_null_data = src_nullable_column->null_column()->get_data();
    auto& dst_null_data = dst_nullable_column->null_column_raw_ptr()->get_data();

    size_t size = dst_null_data.size();

    for (size_t i = 0; i < size; i++) {
        dst_null_data[i] = src_null_data[i];
        if (!src_null_data[i]) {
            dst_data.data()[i] = src_data.data()[i] / 1000;
        }
    }
    dst_nullable_column->set_has_null(src_nullable_column->has_null());
    return Status::OK();
}

Status parquet::Int32ToDateTimeConverter::convert(const Column* src, Column* dst) {
    auto* src_nullable_column = ColumnHelper::as_raw_column<NullableColumn>(src);
    // hive only support null column
    // TODO: support not null
    auto* dst_nullable_column = down_cast<NullableColumn*>(dst);
    dst_nullable_column->resize_uninitialized(src_nullable_column->size());

    auto* src_column = ColumnHelper::as_raw_column<FixedLengthColumn<int32_t>>(src_nullable_column->data_column());
    auto* dst_column = ColumnHelper::as_raw_column<TimestampColumn>(dst_nullable_column->data_column_raw_ptr());

    auto& src_data = src_column->get_data();
    auto& dst_data = dst_column->get_data();
    auto& src_null_data = src_nullable_column->null_column()->get_data();
    auto& dst_null_data = dst_nullable_column->null_column_raw_ptr()->get_data();

    size_t size = dst_null_data.size();
    memcpy(dst_null_data.data(), src_null_data.data(), size);
    // from_unix_second(day * SECS_PER_DAY) divides the seconds back into a Julian day with no remainder
    // (UNIX_EPOCH_SECONDS is UNIX_EPOCH_JULIAN whole days), so build the midnight timestamp from the day
    // directly. Null rows hold arbitrary bytes, so they convert day 0 instead (the value is unused); the
    // loop has no division or branch and vectorizes.
    for (size_t i = 0; i < size; i++) {
        int64_t day = src_null_data[i] ? 0 : src_data[i];
        dst_data[i].set_timestamp(timestamp::from_julian_and_time(day + date::UNIX_EPOCH_JULIAN, 0));
    }
    dst_nullable_column->set_has_null(src_nullable_column->has_null());
    return Status::OK();
}

Status Int96ToDateTimeConverter::init(const std::string& timezone, bool as_timestamp_ntz) {
    _as_timestamp_ntz = as_timestamp_ntz;
    if (!TimezoneUtils::find_cctz_time_zone(timezone, _ctz)) {
        return Status::InternalError(strings::Substitute("can not find cctz time zone $0", timezone));
    }
    const auto tp = std::chrono::system_clock::now();
    const cctz::time_zone::absolute_lookup al = _ctz.lookup(tp);
    _offset = al.offset;
    return Status::OK();
}

Status Int96ToDateTimeConverter::convert(const Column* src, Column* dst) {
    auto* src_nullable_column = ColumnHelper::as_raw_column<NullableColumn>(src);
    // hive only support null column
    // TODO: support not null
    auto* dst_nullable_column = down_cast<NullableColumn*>(dst);
    dst_nullable_column->resize_uninitialized(src_nullable_column->size());

    auto* src_column = ColumnHelper::as_raw_column<FixedLengthColumn<int96_t>>(src_nullable_column->data_column());
    auto* dst_column = down_cast<TimestampColumn*>(dst_nullable_column->data_column_raw_ptr());

    auto& src_data = src_column->get_data();
    auto& dst_data = dst_column->get_data();
    auto& src_null_data = src_nullable_column->null_column()->get_data();
    auto& dst_null_data = dst_nullable_column->null_column_raw_ptr()->get_data();

    size_t size = dst_null_data.size();

    // The offset of a value is the one in force at 00:00 UTC of its day.
    auto day_seconds = [](Timestamp timestamp) -> int64_t {
        return (static_cast<int64_t>(timestamp::to_julian(timestamp)) - date::UNIX_EPOCH_JULIAN) * SECS_PER_DAY;
    };
    auto to_timestamp = [&](size_t i) -> Timestamp {
        return (static_cast<uint64_t>(src_data[i].hi) << TIMESTAMP_BITS) | (src_data[i].lo / 1000);
    };
    int64_t fixed_offset = _offset;
    const int64_t* offsets = nullptr;
    auto fill_dst_fn = [&]<bool FAST_TZ>() {
        for (size_t i = 0; i < size; i++) {
            dst_null_data[i] = src_null_data[i];
            if (!src_null_data[i]) {
                Timestamp timestamp = to_timestamp(i);
                if (_as_timestamp_ntz) {
                    // Paimon TIMESTAMP (NTZ): the INT96 stores the wall clock, so keep it as-is
                    // instead of shifting by the session timezone the way Hive/Spark INT96 needs.
                    dst_data[i].set_timestamp(timestamp);
                    continue;
                }
                int64_t offset = FAST_TZ ? fixed_offset : offsets[i];
                dst_data[i].set_timestamp(timestamp::add<TimeUnit::SECOND>(timestamp, offset));
            }
        }
    };
    // use fixed offset to adjust to local timezone(potentially could get wrong result but faster)
    if (config::parquet_fast_timezone_conversion || _as_timestamp_ntz) {
        fill_dst_fn.operator()<true>();
    } else {
        // One offset for the whole batch when its days lie within one offset window, else one per value.
        int64_t lo = std::numeric_limits<int64_t>::max();
        int64_t hi = std::numeric_limits<int64_t>::min();
        _seconds.resize(size);
        for (size_t i = 0; i < size; i++) {
            if (!src_null_data[i]) {
                _seconds[i] = day_seconds(to_timestamp(i));
                lo = std::min(lo, _seconds[i]);
                hi = std::max(hi, _seconds[i]);
            }
        }
        if (lo > hi || _offset_cache.uniform_offset_for_unix(lo, hi, _ctz, &fixed_offset)) {
            fill_dst_fn.operator()<true>();
        } else {
            _offsets.resize(size);
            _offset_cache.offsets_for_unix(_seconds.data(), src_null_data.data(), size, _ctz, _offsets.data());
            offsets = _offsets.data();
            fill_dst_fn.operator()<false>();
        }
    }
    dst_nullable_column->set_has_null(src_nullable_column->has_null());
    return Status::OK();
}

Status Int64ToDateTimeConverter::init(const std::string& timezone, const tparquet::SchemaElement& schema_element) {
    DCHECK_EQ(schema_element.type, tparquet::Type::INT64);
    if (schema_element.__isset.logicalType) {
        if (!schema_element.logicalType.__isset.TIMESTAMP) {
            std::stringstream ss;
            schema_element.logicalType.printTo(ss);
            return Status::InternalError(
                    strings::Substitute("expect parquet logical type is TIMESTAMP, actual is $0", ss.str()));
        }

        _is_adjusted_to_utc = schema_element.logicalType.TIMESTAMP.isAdjustedToUTC;

        const auto& time_unit = schema_element.logicalType.TIMESTAMP.unit;

        if (time_unit.__isset.MILLIS) {
            _second_mask = 1000;
        } else if (time_unit.__isset.MICROS) {
            _second_mask = 1000000;
        } else if (time_unit.__isset.NANOS) {
            _second_mask = 1000000000;
        } else {
            std::stringstream ss;
            time_unit.printTo(ss);
            return Status::InternalError(strings::Substitute("unexpected time unit $0", ss.str()));
        }
    } else if (schema_element.__isset.converted_type) {
        _is_adjusted_to_utc = true;

        const auto& converted_type = schema_element.converted_type;
        if (converted_type == tparquet::ConvertedType::TIMESTAMP_MILLIS) {
            _second_mask = 1000;
        } else if (converted_type == tparquet::ConvertedType::TIMESTAMP_MICROS) {
            _second_mask = 1000000;
        } else {
            return Status::InternalError(
                    strings::Substitute("unexpected converted type $0", tparquet::to_string(converted_type)));
        }
    } else {
        return Status::InternalError(strings::Substitute("can not convert parquet type $0 to date time",
                                                         tparquet::to_string(schema_element.type)));
    }

    if (_is_adjusted_to_utc) {
        if (!TimezoneUtils::find_cctz_time_zone(timezone, _ctz)) {
            return Status::InternalError(strings::Substitute("can not find cctz time zone $0", timezone));
        }
        const auto tp = std::chrono::system_clock::now();
        const cctz::time_zone::absolute_lookup al = _ctz.lookup(tp);
        _offset = al.offset;
    }

    return Status::OK();
}

Status Int64ToDateTimeConverter::convert_min_max(const Column* min_src, const Column* max_src, Column* min_dst,
                                                 Column* max_dst) {
    RETURN_IF_ERROR(convert(min_src, min_dst));
    RETURN_IF_ERROR(convert(max_src, max_dst));
    // A fixed offset (or none) keeps the order; offsets at each instant do not across a backward shift, so widen
    // each pair by the least and greatest offsets in force between its bounds.
    if (!_is_adjusted_to_utc || config::parquet_fast_timezone_conversion) {
        return Status::OK();
    }
    const auto* min_nullable = ColumnHelper::as_raw_column<NullableColumn>(min_src);
    const auto* max_nullable = ColumnHelper::as_raw_column<NullableColumn>(max_src);
    const auto& min_ticks =
            ColumnHelper::as_raw_column<FixedLengthColumn<int64_t>>(min_nullable->data_column())->get_data();
    const auto& max_ticks =
            ColumnHelper::as_raw_column<FixedLengthColumn<int64_t>>(max_nullable->data_column())->get_data();
    auto& min_values =
            ColumnHelper::as_raw_column<TimestampColumn>(down_cast<NullableColumn*>(min_dst)->data_column_raw_ptr())
                    ->get_data();
    auto& max_values =
            ColumnHelper::as_raw_column<TimestampColumn>(down_cast<NullableColumn*>(max_dst)->data_column_raw_ptr())
                    ->get_data();
    // Seconds (floored) and nanoseconds of a tick, as convert() splits it.
    auto split = [&](int64_t ticks, int64_t* seconds, int64_t* nanoseconds) {
        *seconds = ticks / _second_mask;
        *nanoseconds = (ticks % _second_mask) * (NANOSECS_PER_SEC / _second_mask);
        if (*nanoseconds < 0) {
            *seconds -= 1;
            *nanoseconds += NANOSECS_PER_SEC;
        }
    };
    for (size_t i = 0; i < min_ticks.size(); i++) {
        if (min_nullable->is_null(i) || max_nullable->is_null(i)) {
            continue;
        }
        int64_t lo, lo_nanos, hi, hi_nanos;
        split(min_ticks[i], &lo, &lo_nanos);
        split(max_ticks[i], &hi, &hi_nanos);
        int64_t min_offset, max_offset;
        if (!_offset_cache.offset_range_for_unix(lo, hi, _ctz, &min_offset, &max_offset)) {
            return Status::NotSupported("no time zone offset bounds for the min/max statistics");
        }
        min_values[i].set_timestamp(timestamp::of_epoch_second(lo + min_offset, lo_nanos));
        max_values[i].set_timestamp(timestamp::of_epoch_second(hi + max_offset, hi_nanos));
    }
    return Status::OK();
}

Status Int64ToDateTimeConverter::convert(const Column* src, Column* dst) {
    auto* src_nullable_column = ColumnHelper::as_raw_column<NullableColumn>(src);
    // hive only support null column
    // TODO: support not null
    auto* dst_nullable_column = down_cast<NullableColumn*>(dst);
    dst_nullable_column->resize_uninitialized(src_nullable_column->size());

    auto* src_column = ColumnHelper::as_raw_column<FixedLengthColumn<int64_t>>(src_nullable_column->data_column());
    auto* dst_column = ColumnHelper::as_raw_column<TimestampColumn>(dst_nullable_column->data_column_raw_ptr());

    auto& src_data = src_column->get_data();
    auto& dst_data = dst_column->get_data();
    auto& src_null_data = src_nullable_column->null_column()->get_data();
    auto& dst_null_data = dst_nullable_column->null_column_raw_ptr()->get_data();

    size_t size = dst_null_data.size();
    memcpy(dst_null_data.data(), src_null_data.data(), size);
    int64_t fixed_offset = _offset;
    const int64_t* offsets = nullptr;
    // SECOND_MASK is the tick count per second (1e3 / 1e6 / 1e9), known from the schema; as a template
    // argument the division and modulo below compile to multiplications instead of idiv.
    auto fill_dst_fn = [&]<int64_t SECOND_MASK, bool UTC_TO_TZ, bool FAST_TZ>() {
        constexpr int64_t kScaleToNano = NANOSECS_PER_SEC / SECOND_MASK;
        for (size_t i = 0; i < size; i++) {
            // With a fixed offset every row costs the same, so null rows are converted too, from tick 0 since
            // they hold arbitrary bytes (the value is unused), and the loop has no branch; a null row has no
            // per-row offset.
            int64_t ticks;
            if constexpr (FAST_TZ) {
                ticks = src_null_data[i] ? 0 : src_data[i];
            } else {
                if (src_null_data[i]) {
                    continue;
                }
                ticks = src_data[i];
            }
            int64_t seconds = ticks / SECOND_MASK;
            int64_t nanoseconds = (ticks % SECOND_MASK) * kScaleToNano;
            // Truncating division leaves a negative sub-second remainder for a pre-1970 tick;
            // borrow a whole second so nanoseconds stays in [0, NANOSECS_PER_SEC), matching the
            // floor split the FE boundary computation uses. Without this, of_epoch_second packs
            // a negative microsecond into the timestamp and corrupts the value.
            if (nanoseconds < 0) {
                seconds -= 1;
                nanoseconds += NANOSECS_PER_SEC;
            }

            if constexpr (UTC_TO_TZ) {
                seconds += FAST_TZ ? fixed_offset : offsets[i];
            }

            dst_data[i].set_timestamp(timestamp::of_epoch_second(seconds, nanoseconds));
        }
    };
    auto fill_dst_by_unit = [&]<bool UTC_TO_TZ, bool FAST_TZ>() {
        switch (_second_mask) {
        case 1000:
            fill_dst_fn.operator()<1000, UTC_TO_TZ, FAST_TZ>();
            break;
        case 1000000:
            fill_dst_fn.operator()<1000000, UTC_TO_TZ, FAST_TZ>();
            break;
        default:
            DCHECK_EQ(_second_mask, 1000000000);
            fill_dst_fn.operator()<1000000000, UTC_TO_TZ, FAST_TZ>();
            break;
        }
    };

    if (_is_adjusted_to_utc) {
        // use fixed offset to adjust to local timezone(potentially could get wrong result but faster)
        if (config::parquet_fast_timezone_conversion) {
            fill_dst_by_unit.operator()<true, true>();
        } else {
            // One offset for the whole batch when it lies within one offset window (always for a zone without
            // daylight saving time), else one per value. Floor division keeps the order, so the seconds of the
            // min and max ticks bound the seconds of every value.
            auto floor_seconds = [&](int64_t ticks) {
                return ticks / _second_mask - (ticks % _second_mask < 0 ? 1 : 0);
            };
            int64_t lo = std::numeric_limits<int64_t>::max();
            int64_t hi = std::numeric_limits<int64_t>::min();
            for (size_t i = 0; i < size; i++) {
                if (!src_null_data[i]) {
                    lo = std::min(lo, src_data[i]);
                    hi = std::max(hi, src_data[i]);
                }
            }
            if (lo > hi ||
                _offset_cache.uniform_offset_for_unix(floor_seconds(lo), floor_seconds(hi), _ctz, &fixed_offset)) {
                fill_dst_by_unit.operator()<true, true>();
            } else {
                _seconds.resize(size);
                for (size_t i = 0; i < size; i++) {
                    _seconds[i] = src_null_data[i] ? 0 : floor_seconds(src_data[i]);
                }
                _offsets.resize(size);
                _offset_cache.offsets_for_unix(_seconds.data(), src_null_data.data(), size, _ctz, _offsets.data());
                offsets = _offsets.data();
                fill_dst_by_unit.operator()<true, false>();
            }
        }
    } else {
        fill_dst_by_unit.operator()<false, true>();
    }

    dst_nullable_column->set_has_null(src_nullable_column->has_null());
    return Status::OK();
}

Status Int64ToTimeConverter::convert(const Column* src, Column* dst) {
    auto* src_nullable_column = ColumnHelper::as_raw_column<NullableColumn>(src);
    // hive only support null column
    // TODO: support not null
    auto* dst_nullable_column = down_cast<NullableColumn*>(dst);
    dst_nullable_column->resize_uninitialized(src_nullable_column->size());

    auto* src_column = ColumnHelper::as_raw_column<FixedLengthColumn<int64_t>>(src_nullable_column->data_column());
    auto* dst_column = ColumnHelper::as_raw_column<DoubleColumn>(dst_nullable_column->data_column_raw_ptr());

    auto& src_data = src_column->get_data();
    auto& dst_data = dst_column->get_data();
    auto& src_null_data = src_nullable_column->null_column()->get_data();
    auto& dst_null_data = dst_nullable_column->null_column_raw_ptr()->get_data();

    size_t size = dst_null_data.size();

    for (size_t i = 0; i < size; i++) {
        dst_null_data[i] = src_null_data[i];
        if (!src_null_data[i]) {
            dst_data.data()[i] = src_data.data()[i] / 1000000;
        }
    }
    dst_nullable_column->set_has_null(src_nullable_column->has_null());
    return Status::OK();
}

Status BinaryToGeographyConverter::convert(const Column* src, Column* dst) {
    const auto* input = down_cast<const NullableColumn*>(src);
    const auto* binary = down_cast<const BinaryColumn*>(input->data_column().get());
    auto* geo = down_cast<GeoColumn*>(ColumnHelper::get_data_column(dst));
    if (!dst->is_nullable() && input->has_null()) {
        return Status::InvalidArgument("Cannot write NULL Parquet GEOGRAPHY values to a non-nullable column");
    }
    geo->reset_column();
    geo->append_wkb_column(*binary);
    if (!dst->is_nullable()) {
        return Status::OK();
    }
    auto* output = down_cast<NullableColumn*>(dst);
    auto* nulls = output->null_column_raw_ptr();
    nulls->reset_column();
    nulls->append(*input->null_column(), 0, input->size());
    output->set_has_null(input->has_null());
    return Status::OK();
}

} // namespace starrocks::parquet
