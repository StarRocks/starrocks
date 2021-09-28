// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#include "exec/parquet/column_reader.h"

#include <memory>
#include <utility>

#include "column/binary_column.h"
#include "column/column_helper.h"
#include "column/fixed_length_column.h"
#include "column/type_traits.h"
#include "exec/parquet/schema.h"
#include "exec/parquet/stored_column_reader.h"
#include "gen_cpp/parquet_types.h"
#include "gutil/casts.h"
#include "gutil/strings/substitute.h"
#include "runtime/decimalv2_value.h"
#include "runtime/primitive_type.h"
#include "storage/types.h"
#include "util/bit_util.h"
#include "util/debug_util.h"
#include "util/runtime_profile.h"

namespace starrocks {
class RandomAccessFile;
}

namespace starrocks::parquet {

class ColumnConverter {
public:
    ColumnConverter() = default;
    virtual ~ColumnConverter() = default;

    virtual Status convert(const vectorized::ColumnPtr& src, vectorized::Column* dst) = 0;
};

class Int96ToDateTimeConverter : public ColumnConverter {
public:
    Int96ToDateTimeConverter() = default;
    ~Int96ToDateTimeConverter() override = default;

    Status init(const std::string& timezone);
    Status convert(const vectorized::ColumnPtr& src, vectorized::Column* dst) override {
        return _convert_to_timestamp_column(src, dst);
    }

private:
    // convert column from int96 to timestamp
    Status _convert_to_timestamp_column(const vectorized::ColumnPtr& src, vectorized::Column* dst);
    // When Hive stores a timestamp value into Parquet format, it converts local time
    // into UTC time, and when it reads data out, it should be converted to the time
    // according to session variable "time_zone".
    vectorized::Timestamp _utc_to_local(vectorized::Timestamp timestamp) {
        return vectorized::timestamp::add<vectorized::TimeUnit::SECOND>(timestamp, _offset);
    }

private:
    int _offset = 0;
};

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

Status Int96ToDateTimeConverter::_convert_to_timestamp_column(const vectorized::ColumnPtr& src,
                                                              vectorized::Column* dst) {
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

template <typename FROM, typename TO>
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

        auto* src_column = vectorized::ColumnHelper::as_raw_column<vectorized::FixedLengthColumn<FROM>>(
                src_nullable_column->data_column());
        auto* dst_column = vectorized::ColumnHelper::as_raw_column<vectorized::FixedLengthColumn<TO>>(
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
            dst_data[i] = TO(src_data[i]);
        }

        dst_nullable_column->set_has_null(src_nullable_column->has_null());
        return Status::OK();
    }
};

class Int32ToDateConverter : public ColumnConverter {
public:
    Int32ToDateConverter() = default;
    ~Int32ToDateConverter() override = default;

    Status convert(const vectorized::ColumnPtr& src, vectorized::Column* dst) override {
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
};

// When doing decimal convert, source scale may not equal with destination scale,
// when destination scale is greater than source, source unscaled data will be scaled up
// to match destination scale.
enum DecimalScaleType {
    NO_SCALE,
    SCALE_UP,
    SCALE_DOWN,
};

template <typename T>
struct DecimalTypeTraits {
    using PrimitiveType = T;
};

template <>
struct DecimalTypeTraits<DecimalV2Value> {
    using PrimitiveType = int128_t;
};

template <typename SourceType, PrimitiveType dst_type>
class PrimitiveToDecimalConverter : public ColumnConverter {
public:
    using DestDecimalType = typename vectorized::RunTimeTypeTraits<dst_type>::CppType;
    using DestPrimitiveType = typename DecimalTypeTraits<DestDecimalType>::PrimitiveType;
    using DestColumnType = typename vectorized::RunTimeTypeTraits<dst_type>::ColumnType;

    PrimitiveToDecimalConverter(int32_t src_scale, int32_t dst_scale) {
        if (src_scale < dst_scale) {
            _scale_type = SCALE_UP;
            _scale_factor = get_scale_factor<DestPrimitiveType>(dst_scale - src_scale);
        } else if (src_scale > dst_scale) {
            _scale_type = SCALE_DOWN;
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
            if (_scale_type == SCALE_UP) {
                value *= _scale_factor;
            } else if (_scale_type == SCALE_DOWN) {
                value /= _scale_factor;
            }
            dst_data[i] = DestDecimalType(value);
        }
        dst_nullable_column->set_has_null(has_null);
        return Status::OK();
    }

private:
    DecimalScaleType _scale_type = NO_SCALE;
    DestPrimitiveType _scale_factor = 1;
};

template <PrimitiveType dst_type>
class BinaryToDecimalConverter : public ColumnConverter {
public:
    using DecimalType = typename vectorized::RunTimeTypeTraits<dst_type>::CppType;
    using DestPrimitiveType = typename DecimalTypeTraits<DecimalType>::PrimitiveType;
    using ColumnType = typename vectorized::RunTimeTypeTraits<dst_type>::ColumnType;

    BinaryToDecimalConverter(int32_t src_scale, int32_t dst_scale) {
        if (src_scale < dst_scale) {
            _scale_type = SCALE_UP;
            _scale_factor = get_scale_factor<DestPrimitiveType>(dst_scale - src_scale);
        } else if (src_scale > dst_scale) {
            _scale_type = SCALE_DOWN;
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
            if (_scale_type == SCALE_UP) {
                value *= _scale_factor;
            } else if (_scale_type == SCALE_DOWN) {
                value /= _scale_factor;
            }
            dst_data[i] = DecimalType(value);
        }
        dst_nullable_column->set_has_null(has_null);
        return Status::OK();
    }

private:
    DecimalScaleType _scale_type = NO_SCALE;
    DestPrimitiveType _scale_factor = 1;
};

class ScalarColumnReader : public ColumnReader {
public:
    ScalarColumnReader(ColumnReaderOptions opts) : _opts(std::move(opts)) {}
    ~ScalarColumnReader() override = default;

    Status init(RandomAccessFile* file, const ParquetField* field, const tparquet::ColumnChunk* chunk_metadata,
                const TypeDescriptor& col_type) {
        StoredColumnReaderOptions opts;
        opts.stats = _opts.stats;
        _field = field;
        _col_type = col_type;

        RETURN_IF_ERROR(_init_convert_info());

        return StoredColumnReader::create(file, field, chunk_metadata, opts, &_reader);
    }

    Status prepare_batch(size_t* num_records, ColumnContentType content_type, vectorized::Column* dst) override {
        if (!_need_convert) {
            return _reader->read_records(num_records, content_type, dst);
        } else {
            SCOPED_RAW_TIMER(&_opts.stats->column_convert_ns);
            auto data_column = _create_column(_field->physical_type);
            vectorized::ColumnPtr column =
                    vectorized::NullableColumn::create(data_column, vectorized::NullColumn::create());

            Status status = _reader->read_records(num_records, content_type, column.get());
            if (!status.ok() && !status.is_end_of_file()) {
                return status;
            }

            RETURN_IF_ERROR(_converter->convert(column, dst));

            return Status::OK();
        }
    }

    Status finish_batch() override { return Status::OK(); }

    void get_levels(level_t** def_levels, level_t** rep_levels, size_t* num_levels) override {
        _reader->get_levels(def_levels, rep_levels, num_levels);
    }

    Status get_dict_values(vectorized::Column* column) override { return _reader->get_dict_values(column); }

    Status get_dict_values(const std::vector<int32_t>& dict_codes, vectorized::Column* column) override {
        return _reader->get_dict_values(dict_codes, column);
    }

    Status get_dict_codes(const std::vector<Slice>& dict_values, std::vector<int32_t>* dict_codes) override {
        return _reader->get_dict_codes(dict_values, dict_codes);
    }

private:
    Status _init_convert_info();

    // create column according parquet data type
    static vectorized::ColumnPtr _create_column(tparquet::Type::type type);

private:
    ColumnReaderOptions _opts;

    const ParquetField* _field = nullptr;
    TypeDescriptor _col_type;
    bool _need_convert = false;
    std::unique_ptr<ColumnConverter> _converter;

    std::unique_ptr<StoredColumnReader> _reader;
};

// TODO(zc): Use the registration mechanism instead
Status ScalarColumnReader::_init_convert_info() {
    tparquet::Type::type parquet_type = _field->physical_type;
    PrimitiveType col_type = _col_type.type;

    _need_convert = false;

    // the reason why there is down conversion of integer type is
    // assume we create a hive column called `col0` whose type is `tinyint`
    // but when we insert value into `col0`, the physical type in parquet file is actually `INT32`
    // so when we read `col0` from parquet file, we have to do a type conversion from int32_t to int8_t.
    switch (parquet_type) {
    case tparquet::Type::type::INT32: {
        if (col_type != PrimitiveType::TYPE_INT) {
            _need_convert = true;
        }
        switch (col_type) {
        case PrimitiveType::TYPE_TINYINT:
            _converter = std::make_unique<IntToIntConverter<int32_t, int8_t>>();
            break;
        case PrimitiveType::TYPE_SMALLINT:
            _converter = std::make_unique<IntToIntConverter<int32_t, int16_t>>();
            break;
        case PrimitiveType::TYPE_BIGINT:
            _converter = std::make_unique<IntToIntConverter<int32_t, int64_t>>();
            break;
        case PrimitiveType::TYPE_DATE:
            _converter = std::make_unique<Int32ToDateConverter>();
            break;
            // when decimal precision is greater than 27, precision may be lost in the following
            // process. However to handle most enviroment, we also make progress other than
            // rejection
        case PrimitiveType::TYPE_DECIMALV2:
            // All DecimalV2 use scale 9 as scale
            _converter = std::make_unique<PrimitiveToDecimalConverter<int32_t, TYPE_DECIMALV2>>(_field->scale, 9);
            break;
        case PrimitiveType::TYPE_DECIMAL32:
            _converter = std::make_unique<PrimitiveToDecimalConverter<int32_t, TYPE_DECIMAL32>>(_field->scale,
                                                                                                _col_type.scale);
            break;
        case PrimitiveType::TYPE_DECIMAL64:
            _converter = std::make_unique<PrimitiveToDecimalConverter<int32_t, TYPE_DECIMAL64>>(_field->scale,
                                                                                                _col_type.scale);
            break;
        case PrimitiveType::TYPE_DECIMAL128:
            _converter = std::make_unique<PrimitiveToDecimalConverter<int32_t, TYPE_DECIMAL128>>(_field->scale,
                                                                                                 _col_type.scale);
            break;
        default:
            break;
        }
        break;
    }
    case tparquet::Type::type::INT64: {
        if (col_type != PrimitiveType::TYPE_BIGINT) {
            _need_convert = true;
        }
        switch (col_type) {
        case PrimitiveType::TYPE_TINYINT:
            _converter = std::make_unique<IntToIntConverter<int64_t, int8_t>>();
            break;
        case PrimitiveType::TYPE_SMALLINT:
            _converter = std::make_unique<IntToIntConverter<int64_t, int16_t>>();
            break;
        case PrimitiveType::TYPE_INT:
            _converter = std::make_unique<IntToIntConverter<int64_t, int32_t>>();
            break;
            // when decimal precision is greater than 27, precision may be lost in the following
            // process. However to handle most enviroment, we also make progress other than
            // rejection
        case PrimitiveType::TYPE_DECIMALV2:
            // All DecimalV2 use scale 9 as scale
            _converter = std::make_unique<PrimitiveToDecimalConverter<int64_t, TYPE_DECIMALV2>>(_field->scale, 9);
            break;
        case PrimitiveType::TYPE_DECIMAL32:
            _converter = std::make_unique<PrimitiveToDecimalConverter<int64_t, TYPE_DECIMAL32>>(_field->scale,
                                                                                                _col_type.scale);
            break;
        case PrimitiveType::TYPE_DECIMAL64:
            _converter = std::make_unique<PrimitiveToDecimalConverter<int64_t, TYPE_DECIMAL64>>(_field->scale,
                                                                                                _col_type.scale);
            break;
        case PrimitiveType::TYPE_DECIMAL128:
            _converter = std::make_unique<PrimitiveToDecimalConverter<int64_t, TYPE_DECIMAL128>>(_field->scale,
                                                                                                 _col_type.scale);
            break;
        default:
            break;
        }
        break;
    }
    case tparquet::Type::type::BYTE_ARRAY: {
        if (col_type != PrimitiveType::TYPE_VARCHAR && col_type != PrimitiveType::TYPE_CHAR) {
            _need_convert = true;
        }
        break;
    }
    case tparquet::Type::type::FIXED_LEN_BYTE_ARRAY: {
        if (col_type != PrimitiveType::TYPE_VARCHAR && col_type != PrimitiveType::TYPE_CHAR) {
            _need_convert = true;
        }
        switch (col_type) {
        case PrimitiveType::TYPE_DECIMALV2:
            // All DecimalV2 use scale 9 as scale
            _converter = std::make_unique<BinaryToDecimalConverter<TYPE_DECIMALV2>>(_field->scale, 9);
            break;
        case PrimitiveType::TYPE_DECIMAL32:
            _converter = std::make_unique<BinaryToDecimalConverter<TYPE_DECIMAL32>>(_field->scale, _col_type.scale);
            break;
        case PrimitiveType::TYPE_DECIMAL64:
            _converter = std::make_unique<BinaryToDecimalConverter<TYPE_DECIMAL64>>(_field->scale, _col_type.scale);
            break;
        case PrimitiveType::TYPE_DECIMAL128:
            _converter = std::make_unique<BinaryToDecimalConverter<TYPE_DECIMAL128>>(_field->scale, _col_type.scale);
            break;
        default:
            break;
        }
        break;
    }
    case tparquet::Type::type::INT96: {
        if (col_type == PrimitiveType::TYPE_DATETIME) {
            _need_convert = true;
            std::unique_ptr<Int96ToDateTimeConverter> converter(new Int96ToDateTimeConverter());
            RETURN_IF_ERROR(converter->init(_opts.timezone));
            _converter = std::move(converter);
        }
        break;
    }
    case tparquet::Type::FLOAT: {
        if (col_type != PrimitiveType::TYPE_FLOAT) {
            _need_convert = true;
        }
        break;
    }
    case tparquet::Type::DOUBLE: {
        if (col_type != PrimitiveType::TYPE_DOUBLE) {
            _need_convert = true;
        }
        break;
    }
    default:
        _need_convert = true;
        break;
    }

    if (_need_convert && _converter == nullptr) {
        return Status::NotSupported(
                strings::Substitute("parquet column reader: not supported convert from parquet `$0` to `$1`",
                                    ::tparquet::to_string(parquet_type), type_to_string(col_type)));
    }

    return Status::OK();
}

vectorized::ColumnPtr ScalarColumnReader::_create_column(tparquet::Type::type type) {
    switch (type) {
    case tparquet::Type::type::BOOLEAN:
        return vectorized::FixedLengthColumn<uint8_t>::create();
    case tparquet::Type::type::INT32:
        return vectorized::FixedLengthColumn<TypeTraits<OLAP_FIELD_TYPE_INT>::CppType>::create();
    case tparquet::Type::type::INT64:
        return vectorized::FixedLengthColumn<TypeTraits<OLAP_FIELD_TYPE_BIGINT>::CppType>::create();
    case tparquet::Type::type::INT96:
        return vectorized::FixedLengthColumn<int96_t>::create();
    case tparquet::Type::type::FLOAT:
        return vectorized::FixedLengthColumn<TypeTraits<OLAP_FIELD_TYPE_FLOAT>::CppType>::create();
    case tparquet::Type::type::DOUBLE:
        return vectorized::FixedLengthColumn<TypeTraits<OLAP_FIELD_TYPE_DOUBLE>::CppType>::create();
    case tparquet::Type::type::BYTE_ARRAY:
    case tparquet::Type::type::FIXED_LEN_BYTE_ARRAY:
        return vectorized::BinaryColumn::create();
    default:
        return nullptr;
    }
}

static void def_rep_to_offset(const LevelInfo& level_info, const level_t* def_levels, const level_t* rep_levels,
                              size_t num_levels, int32_t* offsets, int8_t* is_nulls, size_t* num_offsets) {
    size_t offset_pos = 0;
    for (int i = 0; i < num_levels; ++i) {
        // when dev_level is less than immediate_repeated_ancestor_def_level, it means that level
        // will affect its ancestor.
        // when rep_level is greater than max_rep_level, this means that level affects its
        // descendants.
        // So we can skip this levels
        if (def_levels[i] < level_info.immediate_repeated_ancestor_def_level ||
            rep_levels[i] > level_info.max_rep_level) {
            continue;
        }
        if (rep_levels[i] == level_info.max_rep_level) {
            offsets[offset_pos]++;
            continue;
        }

        offset_pos++;
        offsets[offset_pos] = offsets[offset_pos - 1];
        if (def_levels[i] >= level_info.max_def_level) {
            offsets[offset_pos]++;
        }

        // when del_level equals with max_def_level, this is a null element or a required element
        // when del_level equals with (max_def_level - 1), this indicates a empty array
        // when del_level less than (max_def_level - 1) it means this array is null
        if (def_levels[i] >= level_info.max_def_level - 1) {
            is_nulls[offset_pos - 1] = 0;
        } else {
            is_nulls[offset_pos - 1] = 1;
        }
    }
    *num_offsets = offset_pos;
}

class ListColumnReader : public ColumnReader {
public:
    ListColumnReader(ColumnReaderOptions opts) : _opts(std::move(opts)) {}
    ~ListColumnReader() override = default;

    Status init(const ParquetField* field, std::unique_ptr<ColumnReader> element_reader) {
        _field = field;
        _element_reader = std::move(element_reader);
        return Status::OK();
    }

    Status prepare_batch(size_t* num_records, ColumnContentType content_type, vectorized::Column* dst) override {
        return _element_reader->prepare_batch(num_records, content_type, dst);
    }

    Status finish_batch() override {
        level_t* def_levels = nullptr;
        level_t* rep_levels = nullptr;
        size_t num_levels = 0;

        _element_reader->get_levels(&def_levels, &rep_levels, &num_levels);
        std::vector<int32_t> offsets(num_levels + 1);
        std::vector<int8_t> is_nulls(num_levels);
        size_t num_offsets = 0;

        offsets[0] = 0;
        def_rep_to_offset(_field->level_info, def_levels, rep_levels, num_levels, &offsets[0], &is_nulls[0],
                          &num_offsets);
        return Status::OK();
    }

    void get_levels(level_t** def_levels, level_t** rep_levels, size_t* num_levels) override {
        _element_reader->get_levels(def_levels, rep_levels, num_levels);
    }

private:
    ColumnReaderOptions _opts;

    const ParquetField* _field = nullptr;
    std::unique_ptr<ColumnReader> _element_reader;
};

Status ColumnReader::create(RandomAccessFile* file, const ParquetField* field, const tparquet::RowGroup& row_group,
                            const TypeDescriptor& col_type, const ColumnReaderOptions& opts,
                            std::unique_ptr<ColumnReader>* output) {
    if (field->type.type == TYPE_MAP || field->type.type == TYPE_STRUCT) {
        return Status::InternalError("not supported type");
    }
    if (field->type.type == TYPE_ARRAY) {
        std::unique_ptr<ColumnReader> child_reader;
        RETURN_IF_ERROR(ColumnReader::create(file, &field->children[0], row_group, col_type, opts, &child_reader));
        std::unique_ptr<ListColumnReader> reader(new ListColumnReader(opts));
        RETURN_IF_ERROR(reader->init(field, std::move(child_reader)));
        *output = std::move(reader);
    } else {
        std::unique_ptr<ScalarColumnReader> reader(new ScalarColumnReader(opts));
        RETURN_IF_ERROR(reader->init(file, field, &row_group.columns[field->physical_column_index], col_type));
        *output = std::move(reader);
    }
    return Status::OK();
}

} // namespace starrocks::parquet
