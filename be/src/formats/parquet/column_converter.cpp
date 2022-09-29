// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

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
#include "gen_cpp/parquet_types.h"
#include "gutil/casts.h"
#include "gutil/strings/substitute.h"
#include "runtime/decimalv2_value.h"
#include "runtime/primitive_type.h"
#include "util/bit_util.h"
#include "util/logging.h"
#include "util/runtime_profile.h"
#include "util/timezone_utils.h"

namespace starrocks::parquet {

// When doing decimal convert, source scale may not equal with destination scale,
// when destination scale is greater than source, source unscaled data will be scaled up
// to match destination scale.
enum class DecimalScaleType { kNoScale, kScaleUp, kScaleDown };

class Int32ToDateConverter : public ColumnConverter {
public:
    Int32ToDateConverter() = default;
    ~Int32ToDateConverter() override = default;

    Status convert(const vectorized::ColumnPtr& src, vectorized::Column* dst) override;
};

class Int96ToDateTimeConverter : public ColumnConverter {
public:
    Int96ToDateTimeConverter() = default;
    ~Int96ToDateTimeConverter() override = default;

    Status init(const std::string& timezone);
    // convert column from int96 to timestamp
    Status convert(const vectorized::ColumnPtr& src, vectorized::Column* dst) override;

private:
    // When Hive stores a timestamp value into Parquet format, it converts local time
    // into UTC time, and when it reads data out, it should be converted to the time
    // according to session variable "time_zone".
    [[nodiscard]] vectorized::Timestamp _utc_to_local(vectorized::Timestamp timestamp) const {
        return vectorized::timestamp::add<vectorized::TimeUnit::SECOND>(timestamp, _offset);
    }

private:
    int _offset = 0;
};

class Int64ToDateTimeConverter : public ColumnConverter {
public:
    Int64ToDateTimeConverter() = default;
    ~Int64ToDateTimeConverter() override = default;

    Status init(const std::string& timezone, const tparquet::SchemaElement& schema_element);
    Status convert(const vectorized::ColumnPtr& src, vectorized::Column* dst) override {
        return _convert_to_timestamp_column(src, dst);
    }

private:
    // convert column from int64 to timestamp
    Status _convert_to_timestamp_column(const vectorized::ColumnPtr& src, vectorized::Column* dst);
    // When Hive stores a timestamp value into Parquet format, it converts local time
    // into UTC time, and when it reads data out, it should be converted to the time
    // according to session variable "time_zone".
    [[nodiscard]] vectorized::Timestamp _utc_to_local(vectorized::Timestamp timestamp) const {
        return vectorized::timestamp::add<vectorized::TimeUnit::SECOND>(timestamp, _offset);
    }

private:
    bool _is_adjusted_to_utc = false;
    int _offset = 0;

    int64_t _second_mask = 0;
    int64_t _scale_to_nano_factor = 0;
};

template <typename SourceType, typename DestType>
class IntToIntConverter : public ColumnConverter {
public:
    IntToIntConverter() = default;
    ~IntToIntConverter() override = default;

    Status convert(const vectorized::ColumnPtr& src, vectorized::Column* dst) override {
        auto* src_nullable_column = vectorized::ColumnHelper::as_raw_column<vectorized::NullableColumn>(src);
        // hive only support null column
        // TODO: support not null
        auto* dst_nullable_column = down_cast<vectorized::NullableColumn*>(dst);
        dst_nullable_column->resize(src_nullable_column->size());

        auto* src_column = vectorized::ColumnHelper::as_raw_column<vectorized::FixedLengthColumn<SourceType>>(
                src_nullable_column->data_column());
        auto* dst_column = vectorized::ColumnHelper::as_raw_column<vectorized::FixedLengthColumn<DestType>>(
                dst_nullable_column->data_column());

        auto& src_data = src_column->get_data();
        auto& dst_data = dst_column->get_data();
        auto& src_null_data = src_nullable_column->null_column()->get_data();
        auto& dst_null_data = dst_nullable_column->null_column()->get_data();

        size_t size = src_column->size();

        for (size_t i = 0; i < size; i++) {
            dst_null_data[i] = src_null_data[i];
        }
        for (size_t i = 0; i < size; i++) {
            dst_data[i] = DestType(src_data[i]);
        }

        dst_nullable_column->set_has_null(src_nullable_column->has_null());
        return Status::OK();
    }
};

template <typename SourceType, PrimitiveType DestType>
class PrimitiveToDecimalConverter : public ColumnConverter {
public:
    using DestDecimalType = typename vectorized::RunTimeTypeTraits<DestType>::CppType;
    using DestColumnType = typename vectorized::RunTimeTypeTraits<DestType>::ColumnType;
    using DestPrimitiveType = typename vectorized::RunTimeTypeTraits<TYPE_DECIMAL128>::CppType;

    PrimitiveToDecimalConverter(int32_t src_scale, int32_t dst_scale) {
        if (src_scale < dst_scale) {
            _scale_type = DecimalScaleType::kScaleUp;
            _scale_factor = get_scale_factor<DestPrimitiveType>(dst_scale - src_scale);
        } else if (src_scale > dst_scale) {
            _scale_type = DecimalScaleType::kScaleDown;
            _scale_factor = get_scale_factor<DestPrimitiveType>(src_scale - dst_scale);
        }
    }

    Status convert(const vectorized::ColumnPtr& src, vectorized::Column* dst) override {
        auto* src_nullable_column = vectorized::ColumnHelper::as_raw_column<vectorized::NullableColumn>(src);
        // hive only support null column
        // TODO: support not null
        auto* dst_nullable_column = down_cast<vectorized::NullableColumn*>(dst);
        dst_nullable_column->resize(src_nullable_column->size());

        auto* src_column = vectorized::ColumnHelper::as_raw_column<vectorized::FixedLengthColumn<SourceType>>(
                src_nullable_column->data_column());
        auto* dst_column = vectorized::ColumnHelper::as_raw_column<DestColumnType>(dst_nullable_column->data_column());

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

template <PrimitiveType DestType>
class BinaryToDecimalConverter : public ColumnConverter {
public:
    using DecimalType = typename vectorized::RunTimeTypeTraits<DestType>::CppType;
    using ColumnType = typename vectorized::RunTimeTypeTraits<DestType>::ColumnType;
    using DestPrimitiveType = typename vectorized::RunTimeTypeTraits<TYPE_DECIMAL128>::CppType;

    BinaryToDecimalConverter(int32_t src_scale, int32_t dst_scale) {
        if (src_scale < dst_scale) {
            _scale_type = DecimalScaleType::kScaleUp;
            _scale_factor = get_scale_factor<DestPrimitiveType>(dst_scale - src_scale);
        } else if (src_scale > dst_scale) {
            _scale_type = DecimalScaleType::kScaleDown;
            _scale_factor = get_scale_factor<DestPrimitiveType>(src_scale - dst_scale);
        }
    }

    Status convert(const vectorized::ColumnPtr& src, vectorized::Column* dst) override {
        auto* src_nullable_column = vectorized::ColumnHelper::as_raw_column<vectorized::NullableColumn>(src);
        // hive only support null column
        // TODO: support not null
        auto* dst_nullable_column = down_cast<vectorized::NullableColumn*>(dst);
        dst_nullable_column->resize(src_nullable_column->size());

        auto* src_column =
                vectorized::ColumnHelper::as_raw_column<vectorized::BinaryColumn>(src_nullable_column->data_column());
        auto* dst_column = vectorized::ColumnHelper::as_raw_column<ColumnType>(dst_nullable_column->data_column());

        auto& src_data = src_column->get_data();
        auto& dst_data = dst_column->get_data();
        auto& src_null_data = src_nullable_column->null_column()->get_data();
        auto& dst_null_data = dst_nullable_column->null_column()->get_data();

        size_t size = src_column->size();
        bool has_null = false;
        for (size_t i = 0; i < size; i++) {
            // If src_data[i].size > DestPrimitiveType, this means  we treat as null.
            dst_null_data[i] = src_null_data[i] | (src_data[i].size > sizeof(DestPrimitiveType));
            if (dst_null_data[i]) {
                has_null = true;
                continue;
            }

            // When Decimal in parquet is stored in byte arrays, binary and fixed,
            // the unscaled number must be encoded as two's complement using big-endian byte order.
            DestPrimitiveType value = src_data[i].data[0] & 0x80 ? -1 : 0;
            memcpy(reinterpret_cast<char*>(&value) + sizeof(DestPrimitiveType) - src_data[i].size, src_data[i].data,
                   src_data[i].size);
            value = BitUtil::big_endian_to_host(value);

            // scale up/down
            if (_scale_type == DecimalScaleType::kScaleUp) {
                value *= _scale_factor;
            } else if (_scale_type == DecimalScaleType::kScaleDown) {
                value /= _scale_factor;
            }
            dst_data[i] = DecimalType(value);
        }
        dst_nullable_column->set_has_null(has_null);
        return Status::OK();
    }

private:
    DecimalScaleType _scale_type = DecimalScaleType::kNoScale;
    DestPrimitiveType _scale_factor = 1;
};

Status ColumnConverterFactory::create_converter(const ParquetField& field, const TypeDescriptor& typeDescriptor,
                                                const std::string& timezone,
                                                std::unique_ptr<ColumnConverter>* converter) {
    PrimitiveType col_type = typeDescriptor.type;
    bool need_convert = false;
    tparquet::Type::type parquet_type = field.physical_type;
    const auto& schema_element = field.schema_element;

    // the reason why there is down conversion of integer type is
    // assume we create a hive column called `col0` whose type is `tinyint`
    // but when we insert value into `col0`, the physical type in parquet file is actually `INT32`
    // so when we read `col0` from parquet file, we have to do a type conversion from int32_t to int8_t.
    switch (parquet_type) {
    case tparquet::Type::type::BOOLEAN: {
        if (col_type != PrimitiveType::TYPE_BOOLEAN) {
            need_convert = true;
        }
        break;
    }
    case tparquet::Type::type::INT32: {
        if (col_type != PrimitiveType::TYPE_INT) {
            need_convert = true;
        }
        switch (col_type) {
        case PrimitiveType::TYPE_TINYINT:
            *converter = std::make_unique<IntToIntConverter<int32_t, int8_t>>();
            break;
        case PrimitiveType::TYPE_SMALLINT:
            *converter = std::make_unique<IntToIntConverter<int32_t, int16_t>>();
            break;
        case PrimitiveType::TYPE_BIGINT:
            *converter = std::make_unique<IntToIntConverter<int32_t, int64_t>>();
            break;
        case PrimitiveType::TYPE_DATE:
            *converter = std::make_unique<Int32ToDateConverter>();
            break;
            // when decimal precision is greater than 27, precision may be lost in the following
            // process. However to handle most enviroment, we also make progress other than
            // rejection
        case PrimitiveType::TYPE_DECIMALV2:
            // All DecimalV2 use scale 9 as scale
            *converter = std::make_unique<PrimitiveToDecimalConverter<int32_t, TYPE_DECIMALV2>>(field.scale, 9);
            break;
        case PrimitiveType::TYPE_DECIMAL32:
            *converter = std::make_unique<PrimitiveToDecimalConverter<int32_t, TYPE_DECIMAL32>>(field.scale,
                                                                                                typeDescriptor.scale);
            break;
        case PrimitiveType::TYPE_DECIMAL64:
            *converter = std::make_unique<PrimitiveToDecimalConverter<int32_t, TYPE_DECIMAL64>>(field.scale,
                                                                                                typeDescriptor.scale);
            break;
        case PrimitiveType::TYPE_DECIMAL128:
            *converter = std::make_unique<PrimitiveToDecimalConverter<int32_t, TYPE_DECIMAL128>>(field.scale,
                                                                                                 typeDescriptor.scale);
            break;
        default:
            break;
        }
        break;
    }
    case tparquet::Type::type::INT64: {
        if (col_type != PrimitiveType::TYPE_BIGINT) {
            need_convert = true;
        }
        switch (col_type) {
        case PrimitiveType::TYPE_TINYINT:
            *converter = std::make_unique<IntToIntConverter<int64_t, int8_t>>();
            break;
        case PrimitiveType::TYPE_SMALLINT:
            *converter = std::make_unique<IntToIntConverter<int64_t, int16_t>>();
            break;
        case PrimitiveType::TYPE_INT:
            *converter = std::make_unique<IntToIntConverter<int64_t, int32_t>>();
            break;
            // when decimal precision is greater than 27, precision may be lost in the following
            // process. However to handle most enviroment, we also make progress other than
            // rejection
        case PrimitiveType::TYPE_DECIMALV2:
            // All DecimalV2 use scale 9 as scale
            *converter = std::make_unique<PrimitiveToDecimalConverter<int64_t, TYPE_DECIMALV2>>(field.scale, 9);
            break;
        case PrimitiveType::TYPE_DECIMAL32:
            *converter = std::make_unique<PrimitiveToDecimalConverter<int64_t, TYPE_DECIMAL32>>(field.scale,
                                                                                                typeDescriptor.scale);
            break;
        case PrimitiveType::TYPE_DECIMAL64:
            *converter = std::make_unique<PrimitiveToDecimalConverter<int64_t, TYPE_DECIMAL64>>(field.scale,
                                                                                                typeDescriptor.scale);
            break;
        case PrimitiveType::TYPE_DECIMAL128:
            *converter = std::make_unique<PrimitiveToDecimalConverter<int64_t, TYPE_DECIMAL128>>(field.scale,
                                                                                                 typeDescriptor.scale);
            break;

        case PrimitiveType::TYPE_DATETIME: {
            auto _converter = std::make_unique<Int64ToDateTimeConverter>();
            RETURN_IF_ERROR(_converter->init(timezone, schema_element));
            *converter = std::move(_converter);
            break;
        }
        default:
            break;
        }
        break;
    }
    case tparquet::Type::type::BYTE_ARRAY: {
        if (col_type != PrimitiveType::TYPE_VARCHAR && col_type != PrimitiveType::TYPE_CHAR) {
            need_convert = true;
        }
        break;
    }
    case tparquet::Type::type::FIXED_LEN_BYTE_ARRAY: {
        if (col_type != PrimitiveType::TYPE_VARCHAR && col_type != PrimitiveType::TYPE_CHAR) {
            need_convert = true;
        }
        switch (col_type) {
        case PrimitiveType::TYPE_DECIMALV2:
            // All DecimalV2 use scale 9 as scale
            *converter = std::make_unique<BinaryToDecimalConverter<TYPE_DECIMALV2>>(field.scale, 9);
            break;
        case PrimitiveType::TYPE_DECIMAL32:
            *converter = std::make_unique<BinaryToDecimalConverter<TYPE_DECIMAL32>>(field.scale, typeDescriptor.scale);
            break;
        case PrimitiveType::TYPE_DECIMAL64:
            *converter = std::make_unique<BinaryToDecimalConverter<TYPE_DECIMAL64>>(field.scale, typeDescriptor.scale);
            break;
        case PrimitiveType::TYPE_DECIMAL128:
            *converter = std::make_unique<BinaryToDecimalConverter<TYPE_DECIMAL128>>(field.scale, typeDescriptor.scale);
            break;
        default:
            break;
        }
        break;
    }
    case tparquet::Type::type::INT96: {
        need_convert = true;
        if (col_type == PrimitiveType::TYPE_DATETIME) {
            auto _converter = std::make_unique<Int96ToDateTimeConverter>();
            RETURN_IF_ERROR(_converter->init(timezone));
            *converter = std::move(_converter);
        }
        break;
    }
    case tparquet::Type::FLOAT: {
        if (col_type != PrimitiveType::TYPE_FLOAT) {
            need_convert = true;
        }
        break;
    }
    case tparquet::Type::DOUBLE: {
        if (col_type != PrimitiveType::TYPE_DOUBLE) {
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

vectorized::ColumnPtr ColumnConverter::create_src_column() {
    vectorized::ColumnPtr data_column = nullptr;
    switch (parquet_type) {
    case tparquet::Type::type::BOOLEAN:
        data_column = vectorized::FixedLengthColumn<uint8_t>::create();
        break;
    case tparquet::Type::type::INT32:
        data_column = vectorized::FixedLengthColumn<PhysicalTypeTraits<tparquet::Type::INT32>::CppType>::create();
        break;
    case tparquet::Type::type::INT64:
        data_column = vectorized::FixedLengthColumn<PhysicalTypeTraits<tparquet::Type::INT64>::CppType>::create();
        break;
    case tparquet::Type::type::INT96:
        data_column = vectorized::FixedLengthColumn<PhysicalTypeTraits<tparquet::Type::INT96>::CppType>::create();
        break;
    case tparquet::Type::type::FLOAT:
        data_column = vectorized::FixedLengthColumn<PhysicalTypeTraits<tparquet::Type::FLOAT>::CppType>::create();
        break;
    case tparquet::Type::type::DOUBLE:
        data_column = vectorized::FixedLengthColumn<PhysicalTypeTraits<tparquet::Type::DOUBLE>::CppType>::create();
        break;
    case tparquet::Type::type::BYTE_ARRAY:
    case tparquet::Type::type::FIXED_LEN_BYTE_ARRAY:
        data_column = vectorized::BinaryColumn::create();
        break;
    }
    return vectorized::NullableColumn::create(data_column, vectorized::NullColumn::create());
}

Status parquet::Int32ToDateConverter::convert(const vectorized::ColumnPtr& src, vectorized::Column* dst) {
    auto* src_nullable_column = vectorized::ColumnHelper::as_raw_column<vectorized::NullableColumn>(src);
    // hive only support null column
    // TODO: support not null
    auto* dst_nullable_column = down_cast<vectorized::NullableColumn*>(dst);
    dst_nullable_column->resize(src_nullable_column->size());

    auto* src_column = vectorized::ColumnHelper::as_raw_column<vectorized::FixedLengthColumn<int32_t>>(
            src_nullable_column->data_column());
    auto* dst_column =
            vectorized::ColumnHelper::as_raw_column<vectorized::DateColumn>(dst_nullable_column->data_column());

    auto& src_data = src_column->get_data();
    auto& dst_data = dst_column->get_data();
    auto& src_null_data = src_nullable_column->null_column()->get_data();
    auto& dst_null_data = dst_nullable_column->null_column()->get_data();

    size_t size = src_column->size();
    for (size_t i = 0; i < size; i++) {
        dst_null_data[i] = src_null_data[i];
        if (!src_null_data[i]) {
            dst_data[i]._julian = src_data[i] + vectorized::date::UNIX_EPOCH_JULIAN;
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

Status Int96ToDateTimeConverter::convert(const vectorized::ColumnPtr& src, vectorized::Column* dst) {
    auto* src_nullable_column = vectorized::ColumnHelper::as_raw_column<vectorized::NullableColumn>(src);
    // hive only support null column
    // TODO: support not null
    auto* dst_nullable_column = down_cast<vectorized::NullableColumn*>(dst);
    dst_nullable_column->resize(src_nullable_column->size());

    auto* src_column = vectorized::ColumnHelper::as_raw_column<vectorized::FixedLengthColumn<int96_t>>(
            src_nullable_column->data_column());
    auto* dst_column =
            vectorized::ColumnHelper::as_raw_column<vectorized::TimestampColumn>(dst_nullable_column->data_column());

    auto& src_data = src_column->get_data();
    auto& dst_data = dst_column->get_data();
    auto& src_null_data = src_nullable_column->null_column()->get_data();
    auto& dst_null_data = dst_nullable_column->null_column()->get_data();

    size_t size = src_column->size();
    for (size_t i = 0; i < size; i++) {
        dst_null_data[i] = src_null_data[i];
        if (!src_null_data[i]) {
            vectorized::Timestamp timestamp = (static_cast<uint64_t>(src_data[i].hi) << 40u) | (src_data[i].lo / 1000);
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
        cctz::time_zone ctz;
        if (!TimezoneUtils::find_cctz_time_zone(timezone, ctz)) {
            return Status::InternalError(strings::Substitute("can not find cctz time zone $0", timezone));
        }

        const auto tp = std::chrono::system_clock::now();
        const cctz::time_zone::absolute_lookup al = ctz.lookup(tp);
        _offset = al.offset;
    }

    return Status::OK();
}

Status Int64ToDateTimeConverter::_convert_to_timestamp_column(const vectorized::ColumnPtr& src,
                                                              vectorized::Column* dst) {
    auto* src_nullable_column = vectorized::ColumnHelper::as_raw_column<vectorized::NullableColumn>(src);
    // hive only support null column
    // TODO: support not null
    auto* dst_nullable_column = down_cast<vectorized::NullableColumn*>(dst);
    dst_nullable_column->resize(src_nullable_column->size());

    auto* src_column = vectorized::ColumnHelper::as_raw_column<vectorized::FixedLengthColumn<int64_t>>(
            src_nullable_column->data_column());
    auto* dst_column =
            vectorized::ColumnHelper::as_raw_column<vectorized::TimestampColumn>(dst_nullable_column->data_column());

    auto& src_data = src_column->get_data();
    auto& dst_data = dst_column->get_data();
    auto& src_null_data = src_nullable_column->null_column()->get_data();
    auto& dst_null_data = dst_nullable_column->null_column()->get_data();

    size_t size = src_column->size();
    for (size_t i = 0; i < size; i++) {
        dst_null_data[i] = src_null_data[i];
        if (!src_null_data[i]) {
            vectorized::Timestamp timestamp = vectorized::timestamp::of_epoch_second(
                    static_cast<int>(src_data[i] / _second_mask),
                    static_cast<int>((src_data[i] % _second_mask) * _scale_to_nano_factor));
            dst_data[i].set_timestamp(_utc_to_local(timestamp));
        }
    }
    dst_nullable_column->set_has_null(src_nullable_column->has_null());
    return Status::OK();
}

} // namespace starrocks::parquet
