// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#pragma once

#include "column/column_helper.h"
#include "column/type_traits.h"
#include "common/status.h"
#include "formats/parquet/types.h"
#include "gen_cpp/parquet_types.h"
#include "runtime/types.h"
#include "util/bit_util.h"
#include "utils.h"

namespace starrocks {
class RandomAccessFile;
namespace vectorized {
class Column;
struct HdfsScanStats;
} // namespace vectorized
} // namespace starrocks

namespace starrocks::parquet {

class ParquetField;

struct ColumnReaderOptions {
    vectorized::HdfsScanStats* stats = nullptr;
    std::string timezone;
};

// When doing decimal convert, source scale may not equal with destination scale,
// when destination scale is greater than source, source unscaled data will be scaled up
// to match destination scale.
enum class DecimalScaleType { kNoScale, kScaleUp, kScaleDown };

class ColumnConverter {
public:
    ColumnConverter() = default;
    virtual ~ColumnConverter() = default;

    void init_info(bool _need_convert, tparquet::Type::type _parquet_type) {
        need_convert = _need_convert;
        parquet_type = _parquet_type;
    }

    // create column according parquet data type
    vectorized::ColumnPtr create_src_column();

    virtual Status convert(const vectorized::ColumnPtr& src, vectorized::Column* dst) { return Status::OK(); };

public:
    bool need_convert = false;
    tparquet::Type::type parquet_type;
};

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

class ColumnConverterFactory {
public:
    static Status create_converter(const ParquetField& field, const TypeDescriptor& typeDescriptor,
                                   const std::string& timezone, std::unique_ptr<ColumnConverter>* converter);
};

} // namespace starrocks::parquet