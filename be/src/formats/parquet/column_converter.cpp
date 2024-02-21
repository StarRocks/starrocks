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

#include <memory>
#include <utility>

#include "column/array_column.h"
#include "column/binary_column.h"
#include "column/column_helper.h"
#include "column/fixed_length_column.h"
#include "column/type_traits.h"
#include "formats/parquet/schema.h"
#include "formats/parquet/stored_column_reader.h"
#include "gutil/casts.h"
#include "gutil/strings/substitute.h"
#include "runtime/decimalv2_value.h"
#include "types/logical_type.h"
#include "util/bit_util.h"
#include "util/logging.h"
#include "util/runtime_profile.h"
#include "util/timezone_utils.h"

namespace starrocks::parquet {

// When doing decimal convert, source scale may not equal with destination scale,
// when destination scale is greater than source, source unscaled data will be scaled up
// to match destination scale.
enum class DecimalScaleType { kNoScale, kScaleUp, kScaleDown };

class Int32ToDateConverter final : public ColumnConverter {
public:
    Int32ToDateConverter() = default;
    ~Int32ToDateConverter() override = default;

    Status convert(const ColumnPtr& src, Column* dst) override;
};

class Int32ToDateTimeConverter final : public ColumnConverter {
public:
    Int32ToDateTimeConverter() = default;
    ~Int32ToDateTimeConverter() override = default;

    Status convert(const ColumnPtr& src, Column* dst) override;
};

class Int64ToTimeConverter final : public ColumnConverter {
public:
    Int64ToTimeConverter() = default;
    ~Int64ToTimeConverter() override = default;

    Status convert(const ColumnPtr& src, Column* dst) override;
};

class Int96ToDateTimeConverter final : public ColumnConverter {
public:
    Int96ToDateTimeConverter() = default;
    ~Int96ToDateTimeConverter() override = default;

    Status init(const std::string& timezone);
    // convert column from int96 to timestamp
    Status convert(const ColumnPtr& src, Column* dst) override;

private:
    // When Hive stores a timestamp value into Parquet format, it converts local time
    // into UTC time, and when it reads data out, it should be converted to the time
    // according to session variable "time_zone".
    [[nodiscard]] Timestamp _utc_to_local(Timestamp timestamp) const {
        return timestamp::add<TimeUnit::SECOND>(timestamp, _offset);
    }

private:
    int _offset = 0;
};

class Int64ToDateTimeConverter final : public ColumnConverter {
public:
    Int64ToDateTimeConverter() = default;
    ~Int64ToDateTimeConverter() override = default;

    Status init(const std::string& timezone, const tparquet::SchemaElement& schema_element);
    Status convert(const ColumnPtr& src, Column* dst) override;

private:
    bool _is_adjusted_to_utc = false;
    cctz::time_zone _ctz;
    int64_t _second_mask = 0;
    int64_t _scale_to_nano_factor = 0;
};

template <typename SourceType, typename DestType>
void convert_int_to_int(SourceType* __restrict__ src, DestType* __restrict__ dst, size_t size) {
    for (size_t i = 0; i < size; i++) {
        dst[i] = DestType(src[i]);
    }
}

// Support int => int and float => double
template <typename SourceType, typename DestType>
class NumericToNumericConverter final : public ColumnConverter {
public:
    NumericToNumericConverter() = default;
    ~NumericToNumericConverter() override = default;

    Status convert(const ColumnPtr& src, Column* dst) override {
        auto* src_nullable_column = ColumnHelper::as_raw_column<NullableColumn>(src);
        // hive only support null column
        // TODO: support not null
        auto* dst_nullable_column = down_cast<NullableColumn*>(dst);
        dst_nullable_column->resize_uninitialized(src_nullable_column->size());

        auto* src_column =
                ColumnHelper::as_raw_column<FixedLengthColumn<SourceType>>(src_nullable_column->data_column());
        auto* dst_column = ColumnHelper::as_raw_column<FixedLengthColumn<DestType>>(dst_nullable_column->data_column());

        auto& src_data = src_column->get_data();
        auto& dst_data = dst_column->get_data();
        auto& src_null_data = src_nullable_column->null_column()->get_data();
        auto& dst_null_data = dst_nullable_column->null_column()->get_data();

        size_t size = src_column->size();
        memcpy(dst_null_data.data(), src_null_data.data(), size);
        convert_int_to_int<SourceType, DestType>(src_data.data(), dst_data.data(), size);
        dst_nullable_column->set_has_null(src_nullable_column->has_null());
        return Status::OK();
    }
};

template <typename SourceType, LogicalType DestType>
class PrimitiveToDecimalConverter final : public ColumnConverter {
public:
    using DestDecimalType = typename RunTimeTypeTraits<DestType>::CppType;
    using DestColumnType = typename RunTimeTypeTraits<DestType>::ColumnType;
    using DestPrimitiveType = typename RunTimeTypeTraits<TYPE_DECIMAL128>::CppType;

    PrimitiveToDecimalConverter(int32_t src_scale, int32_t dst_scale) {
        if (src_scale < dst_scale) {
            _scale_type = DecimalScaleType::kScaleUp;
            _scale_factor = get_scale_factor<DestPrimitiveType>(dst_scale - src_scale);
        } else if (src_scale > dst_scale) {
            _scale_type = DecimalScaleType::kScaleDown;
            _scale_factor = get_scale_factor<DestPrimitiveType>(src_scale - dst_scale);
        }
    }

    Status convert(const ColumnPtr& src, Column* dst) override {
        auto* src_nullable_column = ColumnHelper::as_raw_column<NullableColumn>(src);
        // hive only support null column
        // TODO: support not null
        auto* dst_nullable_column = down_cast<NullableColumn*>(dst);
        dst_nullable_column->resize_uninitialized(src_nullable_column->size());

        auto* src_column =
                ColumnHelper::as_raw_column<FixedLengthColumn<SourceType>>(src_nullable_column->data_column());
        auto* dst_column = ColumnHelper::as_raw_column<DestColumnType>(dst_nullable_column->data_column());

        auto& src_data = src_column->get_data();
        auto& dst_data = dst_column->get_data();
        auto& src_null_data = src_nullable_column->null_column()->get_data();
        auto& dst_null_data = dst_nullable_column->null_column()->get_data();

        bool has_null = false;
        size_t size = src_column->size();
        for (size_t i = 0; i < size; i++) {
            dst_null_data[i] = src_null_data[i];
            if (dst_null_data[i]) {
                has_null = true;
                continue;
            }
            DestPrimitiveType value = src_data[i];
            if (_scale_type == DecimalScaleType::kScaleUp) {
                value *= _scale_factor;
            } else if (_scale_type == DecimalScaleType::kScaleDown) {
                value /= _scale_factor;
            }
            dst_data[i] = DestDecimalType(value);
        }
        dst_nullable_column->set_has_null(has_null);
        return Status::OK();
    }

private:
    DecimalScaleType _scale_type = DecimalScaleType::kNoScale;
    DestPrimitiveType _scale_factor = 1;
};

// This class is to convert *fixed length* binary to decimal
// and for fixed length binary in parquet, string data is contiguous,
// and that's why we can do memcpy 8 bytes without accessing invalid address.
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
    void t_convert(size_t size, uint8_t* dst_null_data, uint8_t* src_null_data, DecimalType* dst_data,
                   const uint8* src_data) {
        if constexpr (!has_null) {
            memset(dst_null_data, 0x0, size);
        } else {
            memcpy(dst_null_data, src_null_data, size);
        }

        for (size_t i = 0; i < size; i++) {
            if constexpr (has_null) {
                if (dst_null_data[i]) continue;
            }
            // When Decimal in parquet is stored in byte arrays, binary and fixed,
            // the unscaled number must be encoded as two's complement using big-endian byte order.

            DestPrimitiveType unscale = 0;
            T value = 0;
            static_assert(BINSZ <= sizeof(value));

            // NOTE(yan): since fixed length binary data is contiguous, with condition check (i+8) < size
            // we can assure that there is no illegal memory access
            // UPDATE: bytes are allocated by `RawVectorPad16`, so there will be extra bytes at tail.

            // mempcy 8 bytes to compiler, it's instruction to move 8 bytes from memory to register
            // and follow actions are all in-register instructions.
            memcpy(reinterpret_cast<char*>(&value), src_data, sizeof(value));
            value = BitUtil::big_endian_to_host(value);
            value = value >> ((sizeof(value) - BINSZ) * 8);
            unscale = value;

            src_data += BINSZ;
            // hardware branch predicator works well here.
            if constexpr (scale_type == DecimalScaleType::kScaleUp) {
                unscale *= _scale_factor;
            }
            if constexpr (scale_type == DecimalScaleType::kScaleDown) {
                unscale /= _scale_factor;
            }
            dst_data[i] = DecimalType(unscale);
        }
    }

    Status convert(const ColumnPtr& src, Column* dst) override {
        auto* src_nullable_column = ColumnHelper::as_raw_column<NullableColumn>(src);
        // hive only support null column
        // TODO: support not null
        auto* dst_nullable_column = down_cast<NullableColumn*>(dst);
        dst_nullable_column->resize_uninitialized(src_nullable_column->size());

        auto* src_column = ColumnHelper::as_raw_column<BinaryColumn>(src_nullable_column->data_column());
        auto* dst_column = ColumnHelper::as_raw_column<ColumnType>(dst_nullable_column->data_column());

        const BinaryColumn::Bytes& src_data = src_column->get_bytes();
        auto& dst_data = dst_column->get_data();
        auto& src_null_data = src_nullable_column->null_column()->get_data();
        auto& dst_null_data = dst_nullable_column->null_column()->get_data();

        size_t size = src_column->size();
        if (_type_length > sizeof(DestPrimitiveType)) {
            memset(dst_null_data.data(), 0x1, size);
            dst_nullable_column->set_has_null(true);
            return Status::OK();
        }

        bool has_null = src_nullable_column->has_null();

        // For calling `src_data.get_bytes().data()` , we don't need to call `build_slices` underneath.
        // And notice bytes are allocated by `RawVectorPad16`, there will be extra 16 bytes.
#define M(SZ, K, T)                                                                                       \
    case SZ:                                                                                              \
        if (has_null) {                                                                                   \
            t_convert<SZ, K, T, true>(size, dst_null_data.data(), src_null_data.data(), dst_data.data(),  \
                                      src_data.data());                                                   \
        } else {                                                                                          \
            t_convert<SZ, K, T, false>(size, dst_null_data.data(), src_null_data.data(), dst_data.data(), \
                                       src_data.data());                                                  \
        }                                                                                                 \
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

Status ColumnConverterFactory::create_converter(const ParquetField& field, const TypeDescriptor& typeDescriptor,
                                                const std::string& timezone,
                                                std::unique_ptr<ColumnConverter>* converter) {
    LogicalType col_type = typeDescriptor.type;
    bool need_convert = false;
    tparquet::Type::type parquet_type = field.physical_type;
    const auto& schema_element = field.schema_element;

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
        if (col_type != LogicalType::TYPE_INT) {
            need_convert = true;
        }
        switch (col_type) {
        case LogicalType::TYPE_TINYINT:
            *converter = std::make_unique<NumericToNumericConverter<int32_t, int8_t>>();
            break;
        case LogicalType::TYPE_SMALLINT:
            *converter = std::make_unique<NumericToNumericConverter<int32_t, int16_t>>();
            break;
        case LogicalType::TYPE_BIGINT:
            *converter = std::make_unique<NumericToNumericConverter<int32_t, int64_t>>();
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
            *converter = std::make_unique<PrimitiveToDecimalConverter<int32_t, TYPE_DECIMALV2>>(field.scale, 9);
            break;
        case LogicalType::TYPE_DECIMAL32:
            *converter = std::make_unique<PrimitiveToDecimalConverter<int32_t, TYPE_DECIMAL32>>(field.scale,
                                                                                                typeDescriptor.scale);
            break;
        case LogicalType::TYPE_DECIMAL64:
            *converter = std::make_unique<PrimitiveToDecimalConverter<int32_t, TYPE_DECIMAL64>>(field.scale,
                                                                                                typeDescriptor.scale);
            break;
        case LogicalType::TYPE_DECIMAL128:
            *converter = std::make_unique<PrimitiveToDecimalConverter<int32_t, TYPE_DECIMAL128>>(field.scale,
                                                                                                 typeDescriptor.scale);
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
        // TODO don't support converter byte_array to decimal
        if (col_type != LogicalType::TYPE_VARCHAR && col_type != LogicalType::TYPE_CHAR &&
            col_type != LogicalType::TYPE_VARBINARY) {
            need_convert = true;
        }
        break;
    }
    case tparquet::Type::type::FIXED_LEN_BYTE_ARRAY: {
        int32_t type_length = field.type_length;
        if (col_type != LogicalType::TYPE_VARCHAR && col_type != LogicalType::TYPE_CHAR) {
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
        default:
            break;
        }
        break;
    }
    case tparquet::Type::type::INT96: {
        need_convert = true;
        if (col_type == LogicalType::TYPE_DATETIME) {
            auto _converter = std::make_unique<Int96ToDateTimeConverter>();
            RETURN_IF_ERROR(_converter->init(timezone));
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
        return Status::NotSupported(
                strings::Substitute("parquet column reader: not supported convert from parquet `$0` to `$1`",
                                    ::tparquet::to_string(parquet_type), type_to_string(col_type)));
    }

    if (!need_convert) {
        *converter = std::make_unique<ColumnConverter>();
    }
    (*converter)->init_info(need_convert, parquet_type);
    return Status::OK();
}

ColumnPtr ColumnConverter::create_src_column() {
    ColumnPtr data_column = nullptr;
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
    return NullableColumn::create(data_column, NullColumn::create());
}

Status parquet::Int32ToDateConverter::convert(const ColumnPtr& src, Column* dst) {
    auto* src_nullable_column = ColumnHelper::as_raw_column<NullableColumn>(src);
    // hive only support null column
    // TODO: support not null
    auto* dst_nullable_column = down_cast<NullableColumn*>(dst);
    dst_nullable_column->resize_uninitialized(src_nullable_column->size());

    auto* src_column = ColumnHelper::as_raw_column<FixedLengthColumn<int32_t>>(src_nullable_column->data_column());
    auto* dst_column = ColumnHelper::as_raw_column<DateColumn>(dst_nullable_column->data_column());

    auto& src_data = src_column->get_data();
    auto& dst_data = dst_column->get_data();
    auto& src_null_data = src_nullable_column->null_column()->get_data();
    auto& dst_null_data = dst_nullable_column->null_column()->get_data();

    size_t size = src_column->size();
    memcpy(dst_null_data.data(), src_null_data.data(), size);
    for (size_t i = 0; i < size; i++) {
        dst_data[i]._julian = src_data[i] + date::UNIX_EPOCH_JULIAN;
    }
    dst_nullable_column->set_has_null(src_nullable_column->has_null());
    return Status::OK();
}

Status parquet::Int32ToDateTimeConverter::convert(const ColumnPtr& src, Column* dst) {
    auto* src_nullable_column = ColumnHelper::as_raw_column<NullableColumn>(src);
    // hive only support null column
    // TODO: support not null
    auto* dst_nullable_column = down_cast<NullableColumn*>(dst);
    dst_nullable_column->resize_uninitialized(src_nullable_column->size());

    auto* src_column = ColumnHelper::as_raw_column<FixedLengthColumn<int32_t>>(src_nullable_column->data_column());
    auto* dst_column = ColumnHelper::as_raw_column<TimestampColumn>(dst_nullable_column->data_column());

    auto& src_data = src_column->get_data();
    auto& dst_data = dst_column->get_data();
    auto& src_null_data = src_nullable_column->null_column()->get_data();
    auto& dst_null_data = dst_nullable_column->null_column()->get_data();

    size_t size = src_column->size();
    for (size_t i = 0; i < size; i++) {
        dst_null_data[i] = src_null_data[i];
        if (!src_null_data[i]) {
            int64_t day = src_data[i];
            TimestampValue ep;
            ep.from_unix_second(day * 24 * 60 * 60, 0);
            dst_data[i].set_timestamp(ep.timestamp());
        }
    }
    dst_nullable_column->set_has_null(src_nullable_column->has_null());
    return Status::OK();
}

Status Int96ToDateTimeConverter::init(const std::string& timezone) {
    cctz::time_zone ctz;
    if (!TimezoneUtils::find_cctz_time_zone(timezone, ctz)) {
        return Status::InternalError(strings::Substitute("can not find cctz time zone $0", timezone));
    }

    const auto tp = std::chrono::system_clock::now();
    const cctz::time_zone::absolute_lookup al = ctz.lookup(tp);
    _offset = al.offset;

    return Status::OK();
}

Status Int96ToDateTimeConverter::convert(const ColumnPtr& src, Column* dst) {
    auto* src_nullable_column = ColumnHelper::as_raw_column<NullableColumn>(src);
    // hive only support null column
    // TODO: support not null
    auto* dst_nullable_column = down_cast<NullableColumn*>(dst);
    dst_nullable_column->resize_uninitialized(src_nullable_column->size());

    auto* src_column = ColumnHelper::as_raw_column<FixedLengthColumn<int96_t>>(src_nullable_column->data_column());
    auto* dst_column = ColumnHelper::as_raw_column<TimestampColumn>(dst_nullable_column->data_column());

    auto& src_data = src_column->get_data();
    auto& dst_data = dst_column->get_data();
    auto& src_null_data = src_nullable_column->null_column()->get_data();
    auto& dst_null_data = dst_nullable_column->null_column()->get_data();

    size_t size = src_column->size();
    for (size_t i = 0; i < size; i++) {
        dst_null_data[i] = src_null_data[i];
        if (!src_null_data[i]) {
            Timestamp timestamp = (static_cast<uint64_t>(src_data[i].hi) << TIMESTAMP_BITS) | (src_data[i].lo / 1000);
            dst_data[i].set_timestamp(_utc_to_local(timestamp));
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
            _scale_to_nano_factor = 1000000;
        } else if (time_unit.__isset.MICROS) {
            _second_mask = 1000000;
            _scale_to_nano_factor = 1000;
        } else if (time_unit.__isset.NANOS) {
            _second_mask = 1000000000;
            _scale_to_nano_factor = 1;
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
            _scale_to_nano_factor = 1000000;
        } else if (converted_type == tparquet::ConvertedType::TIMESTAMP_MICROS) {
            _second_mask = 1000000;
            _scale_to_nano_factor = 1000;
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
    }

    return Status::OK();
}

Status Int64ToDateTimeConverter::convert(const ColumnPtr& src, Column* dst) {
    auto* src_nullable_column = ColumnHelper::as_raw_column<NullableColumn>(src);
    // hive only support null column
    // TODO: support not null
    auto* dst_nullable_column = down_cast<NullableColumn*>(dst);
    dst_nullable_column->resize_uninitialized(src_nullable_column->size());

    auto* src_column = ColumnHelper::as_raw_column<FixedLengthColumn<int64_t>>(src_nullable_column->data_column());
    auto* dst_column = ColumnHelper::as_raw_column<TimestampColumn>(dst_nullable_column->data_column());

    auto& src_data = src_column->get_data();
    auto& dst_data = dst_column->get_data();
    auto& src_null_data = src_nullable_column->null_column()->get_data();
    auto& dst_null_data = dst_nullable_column->null_column()->get_data();

    size_t size = src_column->size();
    for (size_t i = 0; i < size; i++) {
        dst_null_data[i] = src_null_data[i];
        if (!src_null_data[i]) {
            int64_t seconds = src_data[i] / _second_mask;
            int64_t nanoseconds = (src_data[i] % _second_mask) * _scale_to_nano_factor;
            TimestampValue ep;
            ep.from_unixtime(seconds, nanoseconds / 1000, _ctz);
            dst_data[i].set_timestamp(ep.timestamp());
        }
    }
    dst_nullable_column->set_has_null(src_nullable_column->has_null());
    return Status::OK();
}

Status Int64ToTimeConverter::convert(const ColumnPtr& src, Column* dst) {
    auto* src_nullable_column = ColumnHelper::as_raw_column<NullableColumn>(src);
    // hive only support null column
    // TODO: support not null
    auto* dst_nullable_column = down_cast<NullableColumn*>(dst);
    dst_nullable_column->resize_uninitialized(src_nullable_column->size());

    auto* src_column = ColumnHelper::as_raw_column<FixedLengthColumn<int64_t>>(src_nullable_column->data_column());
    auto* dst_column = ColumnHelper::as_raw_column<DoubleColumn>(dst_nullable_column->data_column());

    auto& src_data = src_column->get_data();
    auto& dst_data = dst_column->get_data();
    auto& src_null_data = src_nullable_column->null_column()->get_data();
    auto& dst_null_data = dst_nullable_column->null_column()->get_data();

    size_t size = src_column->size();

    for (size_t i = 0; i < size; i++) {
        dst_null_data[i] = src_null_data[i];
        if (!src_null_data[i]) {
            dst_data.data()[i] = src_data.data()[i] / 1000000;
        }
    }
    dst_nullable_column->set_has_null(src_nullable_column->has_null());
    return Status::OK();
}

} // namespace starrocks::parquet
