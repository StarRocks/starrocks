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

#include "convert_helper.h"

#include <utility>

#include "base/hash/hash_util.hpp"
#include "base/hash/unaligned_access.h"
#include "base/utility/pred_guard.h"
#include "column/chunk.h"
#include "column/chunk_factory.h"
#include "column/nullable_column.h"
#include "column/schema.h"
#include "column/type_converter_detail.h"
#include "storage/chunk_helper.h"
#include "storage/tablet_schema.h"
#include "types/bitmap_value.h"
#include "types/datetime_value.h"
#include "types/decimalv2_value.h"
#include "types/decimalv3.h"
#include "types/hll.h"
#include "types/olap_type_infra.h"
#include "types/percentile_value.h"
#include "types/storage_type_traits.h"
#include "types/timestamp_value.h"

namespace starrocks {

template <typename SrcType>
class BitMapTypeConverter : public MaterializeTypeConverter {
public:
    BitMapTypeConverter() = default;
    ~BitMapTypeConverter() override = default;

    Status convert_materialized(ColumnPtr src_col, Column* dst_col, TypeInfo* src_type) const override {
        for (size_t row_index = 0; row_index < src_col->size(); ++row_index) {
            Datum src_datum = src_col->get(row_index);
            Datum dst_datum;
            if (src_datum.is_null()) {
                dst_datum.set_null();
                dst_col->append_datum(dst_datum);
                continue;
            }
            uint64_t origin_value = src_datum.get<SrcType>();
            if (origin_value < 0) {
                LOG(WARNING) << "The input which less than zero "
                             << " is not valid, to_bitmap only support bigint value from 0 to "
                                "18446744073709551615 currently";
                return Status::InternalError("input is invalid");
            }
            BitmapValue bitmap;
            bitmap.add(origin_value);
            dst_datum.set_bitmap(&bitmap);
            dst_col->append_datum(dst_datum);
        }
        return Status::OK();
    }
};

template <typename SrcType>
class HLLTypeConverter : public MaterializeTypeConverter {
public:
    HLLTypeConverter() = default;
    ~HLLTypeConverter() override = default;

    Status convert_materialized(ColumnPtr src_col, Column* dst_col, TypeInfo* src_type) const override {
        for (size_t row_index = 0; row_index < src_col->size(); ++row_index) {
            Datum src_datum = src_col->get(row_index);
            Datum dst_datum;
            if (src_datum.is_null()) {
                dst_datum.set_null();
                dst_col->append_datum(dst_datum);
                continue;
            }
            auto value = src_datum.template get<SrcType>();
            std::string src = src_type->to_string(&value);
            uint64_t hash_value =
                    HashUtil::murmur_hash64A(src.data(), static_cast<int32_t>(src.size()), HashUtil::MURMUR_SEED);
            HyperLogLog hll;
            hll.update(hash_value);
            dst_datum.set_hyperloglog(&hll);
            dst_col->append_datum(dst_datum);
        }
        return Status::OK();
    }
};

template <typename SrcType>
class PercentileTypeConverter : public MaterializeTypeConverter {
public:
    PercentileTypeConverter() = default;
    ~PercentileTypeConverter() override = default;

    Status convert_materialized(ColumnPtr src_col, Column* dst_col, TypeInfo* src_type) const override {
        for (size_t row_index = 0; row_index < src_col->size(); ++row_index) {
            Datum src_datum = src_col->get(row_index);
            Datum dst_datum;
            if (src_datum.is_null()) {
                dst_datum.set_null();
                dst_col->append_datum(dst_datum);
                continue;
            }
            double origin_value = src_datum.get<SrcType>();
            PercentileValue percentile;
            percentile.add(origin_value);
            dst_datum.set_percentile(&percentile);
            dst_col->append_datum(dst_datum);
        }
        return Status::OK();
    }
};

template <LogicalType SrcType>
class DecimalToPercentileTypeConverter : public MaterializeTypeConverter {
public:
    using CppType = StorageCppType<SrcType>;
    DecimalToPercentileTypeConverter() = default;
    ~DecimalToPercentileTypeConverter() override = default;

    Status convert_materialized(ColumnPtr src_col, Column* dst_col, TypeInfo* src_type) const override {
        for (size_t row_index = 0; row_index < src_col->size(); ++row_index) {
            Datum src_datum = src_col->get(row_index);
            Datum dst_datum;
            if (src_datum.is_null()) {
                dst_datum.set_null();
                dst_col->append_datum(dst_datum);
                continue;
            }
            double origin_value;
            auto v = src_datum.get<CppType>();
            auto scale_factor = get_scale_factor<CppType>(src_type->scale());
            DecimalV3Cast::to_float<CppType, double>(v, scale_factor, &origin_value);
            PercentileValue percentile;
            percentile.add(static_cast<float>(origin_value));
            dst_datum.set_percentile(&percentile);
            dst_col->append_datum(dst_datum);
        }
        return Status::OK();
    }
};

class CountTypeConverter : public MaterializeTypeConverter {
public:
    CountTypeConverter() = default;
    ~CountTypeConverter() override = default;

    Status convert_materialized(ColumnPtr src_col, Column* dst_col, TypeInfo* src_type) const override {
        for (size_t row_index = 0; row_index < src_col->size(); ++row_index) {
            Datum src_datum = src_col->get(row_index);
            Datum dst_datum;
            int64_t count = src_datum.is_null() ? 0 : 1;
            dst_datum.set_int64(count);
            dst_col->append_datum(dst_datum);
        }
        return Status::OK();
    }
};

#define GET_PERENTILE_CONVERTER(from_type)                     \
    {                                                          \
        static PercentileTypeConverter<from_type> s_converter; \
        return &s_converter;                                   \
    }

#define GET_DECIMAL_PERENTILE_CONVERTER(from_type)                      \
    {                                                                   \
        static DecimalToPercentileTypeConverter<from_type> s_converter; \
        return &s_converter;                                            \
    }

#define GET_HLL_CONVERTER(from_type)                    \
    {                                                   \
        static HLLTypeConverter<from_type> s_converter; \
        return &s_converter;                            \
    }

#define GET_BTIMAP_CONVERTER(from_type)                    \
    {                                                      \
        static BitMapTypeConverter<from_type> s_converter; \
        return &s_converter;                               \
    }

const MaterializeTypeConverter* get_perentile_converter(LogicalType from_type, MaterializeType to_type) {
    switch (from_type) {
    case TYPE_TINYINT:
        GET_PERENTILE_CONVERTER(int8_t);
    case TYPE_UNSIGNED_TINYINT:
        GET_PERENTILE_CONVERTER(uint8_t);
    case TYPE_SMALLINT:
        GET_PERENTILE_CONVERTER(int16_t);
    case TYPE_UNSIGNED_SMALLINT:
        GET_PERENTILE_CONVERTER(uint16_t);
    case TYPE_INT:
        GET_PERENTILE_CONVERTER(int32_t);
    case TYPE_UNSIGNED_INT:
        GET_PERENTILE_CONVERTER(uint32_t);
    case TYPE_BIGINT:
        GET_PERENTILE_CONVERTER(int64_t);
    case TYPE_UNSIGNED_BIGINT:
        GET_PERENTILE_CONVERTER(uint64_t);
    case TYPE_LARGEINT:
        GET_PERENTILE_CONVERTER(int128_t);
    case TYPE_FLOAT:
        GET_PERENTILE_CONVERTER(float);
    case TYPE_DOUBLE:
        GET_PERENTILE_CONVERTER(double);
    case TYPE_DECIMALV2:
        GET_PERENTILE_CONVERTER(DecimalV2Value);
    case TYPE_DECIMAL32:
        GET_DECIMAL_PERENTILE_CONVERTER(TYPE_DECIMAL32);
    case TYPE_DECIMAL64:
        GET_DECIMAL_PERENTILE_CONVERTER(TYPE_DECIMAL64);
    case TYPE_DECIMAL128:
        GET_DECIMAL_PERENTILE_CONVERTER(TYPE_DECIMAL128);
    default:
        LOG(WARNING) << "the column type which was altered from was unsupported."
                     << " from_type=" << from_type;
        return nullptr;
    }
}

const MaterializeTypeConverter* get_hll_converter(LogicalType from_type, MaterializeType to_type) {
    switch (from_type) {
    case TYPE_CHAR:
    case TYPE_VARCHAR:
        GET_HLL_CONVERTER(Slice);
    case TYPE_BOOLEAN:
    case TYPE_TINYINT:
        GET_HLL_CONVERTER(int8_t);
    case TYPE_UNSIGNED_TINYINT:
        GET_HLL_CONVERTER(uint8_t);
    case TYPE_SMALLINT:
        GET_HLL_CONVERTER(int16_t);
    case TYPE_UNSIGNED_SMALLINT:
        GET_HLL_CONVERTER(uint16_t);
    case TYPE_DATE:
    case TYPE_INT:
        GET_HLL_CONVERTER(int32_t);
    case TYPE_UNSIGNED_INT:
        GET_HLL_CONVERTER(uint32_t);
    case TYPE_DATETIME:
    case TYPE_DATETIME_V1:
    case TYPE_BIGINT:
        GET_HLL_CONVERTER(int64_t);
    case TYPE_UNSIGNED_BIGINT:
        GET_HLL_CONVERTER(uint64_t);
    case TYPE_LARGEINT:
        GET_HLL_CONVERTER(int128_t);
    case TYPE_FLOAT:
        GET_HLL_CONVERTER(float);
    case TYPE_DOUBLE:
        GET_HLL_CONVERTER(double);
    case TYPE_DATE_V1:
        GET_HLL_CONVERTER(uint24_t);
    default:
        LOG(WARNING) << "fail to hll hash type : " << from_type;
        return nullptr;
    }
}

const MaterializeTypeConverter* get_bitmap_converter(LogicalType from_type, MaterializeType to_type) {
    switch (from_type) {
    case TYPE_TINYINT:
        GET_BTIMAP_CONVERTER(int8_t);
    case TYPE_UNSIGNED_TINYINT:
        GET_BTIMAP_CONVERTER(uint8_t);
    case TYPE_SMALLINT:
        GET_BTIMAP_CONVERTER(int16_t);
    case TYPE_UNSIGNED_SMALLINT:
        GET_BTIMAP_CONVERTER(uint16_t);
    case TYPE_INT:
        GET_BTIMAP_CONVERTER(int32_t);
    case TYPE_UNSIGNED_INT:
        GET_BTIMAP_CONVERTER(uint32_t);
    case TYPE_BIGINT:
        GET_BTIMAP_CONVERTER(int64_t);
    case TYPE_UNSIGNED_BIGINT:
        GET_BTIMAP_CONVERTER(uint64_t);
    default:
        LOG(WARNING) << "the column type which was altered from was unsupported."
                     << " from_type=" << from_type;
        return nullptr;
    }
}

#undef GET_PERENTILE_CONVERTER
#undef GET_DECIMAL_PERENTILE_CONVERTER
#undef GET_HLL_CONVERTER
#undef GET_BITMAP_CONVERTER

const MaterializeTypeConverter* get_materialized_converter(LogicalType from_type, MaterializeType to_type) {
    switch (to_type) {
    case OLAP_MATERIALIZE_TYPE_PERCENTILE: {
        return get_perentile_converter(from_type, to_type);
    }
    case OLAP_MATERIALIZE_TYPE_HLL: {
        return get_hll_converter(from_type, to_type);
    }
    case OLAP_MATERIALIZE_TYPE_BITMAP: {
        return get_bitmap_converter(from_type, to_type);
    }
    case OLAP_MATERIALIZE_TYPE_COUNT: {
        static CountTypeConverter s_converter;
        return &s_converter;
    }
    default:
        LOG(WARNING) << "unknown materialized type";
        break;
    }
    return nullptr;
}

class SameFieldConverter : public FieldConverter {
public:
    explicit SameFieldConverter(TypeInfoPtr type_info) : _type_info(std::move(type_info)) {}

    ~SameFieldConverter() override = default;

    void convert(void* dst, const void* src) const override { _type_info->shallow_copy(dst, src); }

    void convert(Datum* dst, const Datum& src) const override { *dst = src; }

    ColumnPtr copy_convert(const Column& src) const override { return src.clone(); }

    ColumnPtr move_convert(Column* src) const override {
        auto ret = src->clone_empty();
        ret->swap_column(*src);
        return ret;
    }

private:
    TypeInfoPtr _type_info;
};

class DateToDateV2FieldConverter : public FieldConverter {
public:
    DateToDateV2FieldConverter() = default;
    ~DateToDateV2FieldConverter() override = default;

    void convert(void* dst, const void* src) const override {
        DateValue tmp;
        tmp.from_mysql_date(unaligned_load<uint24_t>(src));
        unaligned_store<DateValue>(dst, tmp);
    }

    void convert(Datum* dst, const Datum& src) const override {
        if (src.is_null()) {
            return;
        }
        DateValue date_v2{0};
        date_v2.from_mysql_date(src.get_uint24());
        dst->set_date(date_v2);
    }

    ColumnPtr copy_convert(const Column& src) const override {
        auto nullable = src.is_nullable();
        auto dst = ChunkFactory::column_from_field_type(TYPE_DATE, nullable);
        int num_items = static_cast<int>(src.size());
        dst->reserve(num_items);
        for (int i = 0; i < num_items; ++i) {
            Datum dst_datum;
            Datum src_datum = src.get(i);
            convert(&dst_datum, src_datum);
            dst->append_datum(dst_datum);
        }
        return dst;
    }

private:
};

class DateV2ToDateFieldConverter : public FieldConverter {
public:
    DateV2ToDateFieldConverter() = default;
    ~DateV2ToDateFieldConverter() override = default;

    void convert(void* dst, const void* src) const override {
        unaligned_store<uint24_t>(dst, unaligned_load<DateValue>(src).to_mysql_date());
    }

    void convert(Datum* dst, const Datum& src) const override {
        if (src.is_null()) {
            dst->set_null();
            return;
        }
        dst->set_uint24(src.get_date().to_mysql_date());
    }

    ColumnPtr copy_convert(const Column& src) const override {
        auto nullable = src.is_nullable();
        auto dst = ChunkFactory::column_from_field_type(TYPE_DATE_V1, nullable);
        int num_items = static_cast<int>(src.size());
        dst->reserve(num_items);
        for (int i = 0; i < num_items; ++i) {
            Datum dst_datum;
            Datum src_datum = src.get(i);
            convert(&dst_datum, src_datum);
            dst->append_datum(dst_datum);
        }
        return dst;
    }

private:
};

class DatetimeToTimestampFieldConverter : public FieldConverter {
public:
    DatetimeToTimestampFieldConverter() = default;
    ~DatetimeToTimestampFieldConverter() override = default;

    void convert(void* dst, const void* src) const override {
        TimestampValue tmp;
        tmp.from_timestamp_literal(unaligned_load<int64_t>(src));
        unaligned_store<TimestampValue>(dst, tmp);
    }

    void convert(Datum* dst, const Datum& src) const override {
        if (src.is_null()) {
            dst->set_null();
            return;
        }
        TimestampValue timestamp{0};
        timestamp.from_timestamp_literal(src.get_int64());
        dst->set_timestamp(timestamp);
    }

    ColumnPtr copy_convert(const Column& src) const override {
        auto nullable = src.is_nullable();
        auto dst = ChunkFactory::column_from_field_type(TYPE_DATETIME, nullable);
        int num_items = static_cast<int>(src.size());
        dst->reserve(num_items);
        for (int i = 0; i < num_items; ++i) {
            Datum dst_datum;
            Datum src_datum = src.get(i);
            convert(&dst_datum, src_datum);
            dst->append_datum(dst_datum);
        }
        return dst;
    }

private:
};

class TimestampToDatetimeFieldConverter : public FieldConverter {
public:
    TimestampToDatetimeFieldConverter() = default;
    ~TimestampToDatetimeFieldConverter() override = default;

    void convert(void* dst, const void* src) const override {
        LOG(INFO) << "convert from datetime to timestamp";
        unaligned_store<int64_t>(dst, unaligned_load<TimestampValue>(src).to_timestamp_literal());
    }

    void convert(Datum* dst, const Datum& src) const override {
        if (src.is_null()) {
            dst->set_null();
            return;
        }
        dst->set_int64(src.get_timestamp().to_timestamp_literal());
    }

    ColumnPtr copy_convert(const Column& src) const override {
        auto nullable = src.is_nullable();
        auto dst = ChunkFactory::column_from_field_type(TYPE_DATETIME_V1, nullable);
        int num_items = static_cast<int>(src.size());
        dst->reserve(num_items);
        for (int i = 0; i < num_items; ++i) {
            Datum dst_datum;
            Datum src_datum = src.get(i);
            convert(&dst_datum, src_datum);
            dst->append_datum(dst_datum);
        }
        return dst;
    }

private:
};

class Decimal12ToDecimalFieldConverter : public FieldConverter {
public:
    Decimal12ToDecimalFieldConverter() = default;
    ~Decimal12ToDecimalFieldConverter() override = default;

    void convert(void* dst, const void* src) const override {
        auto src_val = unaligned_load<decimal12_t>(src);
        DecimalV2Value dst_val;
        dst_val.from_olap_decimal(src_val.integer, src_val.fraction);
        unaligned_store<DecimalV2Value>(dst, dst_val);
    }

    void convert(Datum* dst, const Datum& src) const override {
        if (src.is_null()) {
            dst->set_null();
            return;
        }
        DecimalV2Value dst_val;
        dst_val.from_olap_decimal(src.get_decimal12().integer, src.get_decimal12().fraction);
        dst->set_decimal(dst_val);
    }

    ColumnPtr copy_convert(const Column& src) const override {
        auto nullable = src.is_nullable();
        auto dst = ChunkFactory::column_from_field_type(TYPE_DECIMALV2, nullable);
        int num_items = static_cast<int>(src.size());
        dst->reserve(num_items);
        for (int i = 0; i < num_items; ++i) {
            Datum dst_datum;
            Datum src_datum = src.get(i);
            convert(&dst_datum, src_datum);
            dst->append_datum(dst_datum);
        }
        return dst;
    }

private:
};

class DecimalToDecimal12FieldConverter : public FieldConverter {
public:
    DecimalToDecimal12FieldConverter() = default;
    ~DecimalToDecimal12FieldConverter() override = default;

    void convert(void* dst, const void* src) const override {
        auto src_val = unaligned_load<DecimalV2Value>(src);
        decimal12_t dst_val;
        dst_val.integer = src_val.int_value();
        dst_val.fraction = src_val.frac_value();
        unaligned_store<decimal12_t>(dst, dst_val);
    }

    void convert(Datum* dst, const Datum& src) const override {
        if (src.is_null()) {
            dst->set_null();
            return;
        }
        decimal12_t dst_val(src.get_decimal().int_value(), src.get_decimal().frac_value());
        dst->set_decimal12(dst_val);
    }

    ColumnPtr copy_convert(const Column& src) const override {
        auto nullable = src.is_nullable();
        auto dst = ChunkFactory::column_from_field_type(TYPE_DECIMAL, nullable);
        int num_items = static_cast<int>(src.size());
        dst->reserve(num_items);
        for (int i = 0; i < num_items; ++i) {
            Datum dst_datum;
            Datum src_datum = src.get(i);
            convert(&dst_datum, src_datum);
            dst->append_datum(dst_datum);
        }
        return dst;
    }

private:
};

DEF_PRED_GUARD(DirectlyCopybleGuard, is_directly_copyable, typename, SrcType, typename, DstType)
#define IS_DIRECTLY_COPYABLE_CTOR(SrcType, DstType) DEF_PRED_CASE_CTOR(is_directly_copyable, SrcType, DstType)
#define IS_DIRECTLY_COPYABLE(LT, ...) DEF_BINARY_RELATION_ENTRY_SEP_NONE(IS_DIRECTLY_COPYABLE_CTOR, LT, ##__VA_ARGS__)

IS_DIRECTLY_COPYABLE(DecimalV2Value, int128_t)
IS_DIRECTLY_COPYABLE(int128_t, DecimalV2Value)

template <typename T>
struct ColumnTypeTraits {};
template <>
struct ColumnTypeTraits<DecimalV2Value> {
    using type = DecimalColumn;
};

template <>
struct ColumnTypeTraits<int128_t> {
    using type = Decimal128Column;
};

template <>
struct ColumnTypeTraits<decimal12_t> {
    using type = FixedLengthColumn<decimal12_t>;
};

template <typename T>
using ColumnType = typename ColumnTypeTraits<T>::type;

template <typename SrcType, typename DstType>
class DecimalFieldConverter : public FieldConverter {
public:
    DecimalFieldConverter() = default;
    ~DecimalFieldConverter() override = default;
    void convert(void* dst, const void* src) const override {
        const auto* src_val = reinterpret_cast<const SrcType*>(src);
        auto* dst_val = reinterpret_cast<DstType*>(dst);
        ConvFunction<SrcType, DstType>::apply(src_val, dst_val);
    }

    void convert(Datum* dst, const Datum& src) const override {
        if (src.is_null()) {
            dst->set_null();
            return;
        }
        const auto& src_val = src.get<SrcType>();
        DstType dst_val;
        ConvFunction<SrcType, DstType>::apply(&src_val, &dst_val);
        dst->set<DstType>(dst_val);
    }

    ColumnPtr copy_convert(const Column& src) const override {
        using SrcColumnType = ColumnType<SrcType>;
        using DstColumnType = ColumnType<DstType>;
        // FIXME: precision and scale are lost.
        MutableColumnPtr dst = DstColumnType::create();
        dst->reserve(src.size());
        if constexpr (is_directly_copyable<SrcType, DstType>) {
            if (!src.is_nullable() && !src.is_constant()) {
                auto* dst_column = down_cast<DstColumnType*>(dst.get());
                auto* src_column = down_cast<SrcColumnType*>(const_cast<Column*>(&src));
                // TODO (by satanson): unsafe abstraction leak
                //  swap std::vector<DecimalV2Value> and std::vector<int128_t>,
                //  raw memory copy is more sound.
                std::swap(dst_column->get_data(), (typename DstColumnType::Container&)(src_column->get_data()));
                return dst;
            } else if (src.is_nullable() && !dst->only_null()) {
                dst = NullableColumn::create(std::move(dst), NullColumn::create());
                auto* nullable_dst_column = down_cast<NullableColumn*>(dst.get());
                auto* nullable_src_column = down_cast<NullableColumn*>(const_cast<Column*>(&src));
                auto* dst_column = down_cast<DstColumnType*>(nullable_dst_column->data_column_raw_ptr());
                auto* src_column = down_cast<SrcColumnType*>(nullable_src_column->data_column_raw_ptr());
                auto* null_dst_column = nullable_dst_column->null_column_raw_ptr();
                auto* null_src_column = nullable_src_column->null_column_raw_ptr();
                // TODO (by satanson): unsafe abstraction leak
                //  swap std::vector<DecimalV2Value> and std::vector<int128_t>
                //  raw memory copy is more sound.
                std::swap(dst_column->get_data(), (typename DstColumnType::Container&)(src_column->get_data()));
                std::swap(null_dst_column->get_data(), null_src_column->get_data());
                return dst;
            }
        }
        int num_items = static_cast<int>(src.size());
        for (int i = 0; i < num_items; ++i) {
            Datum dst_datum;
            Datum src_datum = src.get(i);
            convert(&dst_datum, src_datum);
            dst->append_datum(dst_datum);
        }
        return dst;
    }

private:
};

const FieldConverter* get_field_converter(LogicalType from_type, LogicalType to_type) {
#define TYPE_CASE_CLAUSE(type)                                      \
    case type: {                                                    \
        static SameFieldConverter s_converter(get_type_info(type)); \
        return &s_converter;                                        \
    }

    if (from_type == to_type) {
        switch (from_type) {
            TYPE_CASE_CLAUSE(TYPE_BOOLEAN)
            TYPE_CASE_CLAUSE(TYPE_TINYINT)
            TYPE_CASE_CLAUSE(TYPE_SMALLINT)
            TYPE_CASE_CLAUSE(TYPE_INT)
            TYPE_CASE_CLAUSE(TYPE_BIGINT)
            TYPE_CASE_CLAUSE(TYPE_LARGEINT)
            TYPE_CASE_CLAUSE(TYPE_FLOAT)
            TYPE_CASE_CLAUSE(TYPE_DOUBLE)
            TYPE_CASE_CLAUSE(TYPE_DECIMAL)
            TYPE_CASE_CLAUSE(TYPE_DECIMALV2)
            TYPE_CASE_CLAUSE(TYPE_CHAR)
            TYPE_CASE_CLAUSE(TYPE_VARCHAR)
            TYPE_CASE_CLAUSE(TYPE_DATE_V1)
            TYPE_CASE_CLAUSE(TYPE_DATE)
            TYPE_CASE_CLAUSE(TYPE_DATETIME_V1)
            TYPE_CASE_CLAUSE(TYPE_DATETIME)
            TYPE_CASE_CLAUSE(TYPE_HLL)
            TYPE_CASE_CLAUSE(TYPE_OBJECT)
            TYPE_CASE_CLAUSE(TYPE_PERCENTILE)
            TYPE_CASE_CLAUSE(TYPE_JSON)
            TYPE_CASE_CLAUSE(TYPE_VARIANT)
            TYPE_CASE_CLAUSE(TYPE_VARBINARY)
        case TYPE_DECIMAL32:
        case TYPE_DECIMAL64:
        case TYPE_DECIMAL128:
        case TYPE_DECIMAL256:
        case TYPE_INT256:
        case TYPE_ARRAY:
        case TYPE_UNSIGNED_TINYINT:
        case TYPE_UNSIGNED_SMALLINT:
        case TYPE_UNSIGNED_INT:
        case TYPE_UNSIGNED_BIGINT:
        case TYPE_UNKNOWN:
        case TYPE_MAP:
        case TYPE_STRUCT:
        case TYPE_DISCRETE_DOUBLE:
        case TYPE_NONE:
        case TYPE_NULL:
        case TYPE_FUNCTION:
        case TYPE_TIME:
        case TYPE_BINARY:
        case TYPE_MAX_VALUE:
            return nullptr;
        }
        DCHECK(false) << "unreachable path";
        return nullptr;
    }
#undef TYPE_CASE_CLAUSE

    if (to_type == TYPE_DATE && from_type == TYPE_DATE_V1) {
        static DateToDateV2FieldConverter s_converter;
        return &s_converter;
    } else if (to_type == TYPE_DATE_V1 && from_type == TYPE_DATE) {
        static DateV2ToDateFieldConverter s_converter;
        return &s_converter;
    } else if (to_type == TYPE_DATETIME && from_type == TYPE_DATETIME_V1) {
        static DatetimeToTimestampFieldConverter s_converter;
        return &s_converter;
    } else if (to_type == TYPE_DATETIME_V1 && from_type == TYPE_DATETIME) {
        static TimestampToDatetimeFieldConverter s_converter;
        return &s_converter;
    } else if (to_type == TYPE_DECIMALV2 && from_type == TYPE_DECIMAL) {
        static Decimal12ToDecimalFieldConverter s_converter;
        return &s_converter;
    } else if (to_type == TYPE_DECIMAL && from_type == TYPE_DECIMALV2) {
        static DecimalToDecimal12FieldConverter s_converter;
        return &s_converter;
    } else if (to_type == TYPE_DECIMAL128 && from_type == TYPE_DECIMAL) {
        static DecimalFieldConverter<decimal12_t, int128_t> s_converter;
        return &s_converter;
    } else if (to_type == TYPE_DECIMAL && from_type == TYPE_DECIMAL128) {
        static DecimalFieldConverter<int128_t, decimal12_t> s_converter;
        return &s_converter;
    } else if (to_type == TYPE_DECIMAL128 && from_type == TYPE_DECIMALV2) {
        static DecimalFieldConverter<DecimalV2Value, int128_t> s_converter;
        return &s_converter;
    } else if (to_type == TYPE_DECIMALV2 && from_type == TYPE_DECIMAL128) {
        static DecimalFieldConverter<int128_t, DecimalV2Value> s_converter;
        return &s_converter;
    }
    return nullptr;
}

Status RowConverter::init(const TabletSchema& in_schema, const TabletSchema& out_schema) {
    auto num_columns = in_schema.num_columns();
    _converters.resize(num_columns);
    _cids.resize(num_columns, 0);
    for (int i = 0; i < in_schema.num_columns(); ++i) {
        _cids[i] = i;
        _converters[i] = get_field_converter(in_schema.column(i).type(), out_schema.column(i).type());
        if (_converters[i] == nullptr) {
            return Status::NotSupported("Cannot get field converter");
        }
    }
    return Status::OK();
}

Status RowConverter::init(const Schema& in_schema, const Schema& out_schema) {
    auto num_columns = in_schema.num_fields();
    _converters.resize(num_columns);
    for (int i = 0; i < num_columns; ++i) {
        _converters[i] = get_field_converter(in_schema.field(i)->type()->type(), out_schema.field(i)->type()->type());
        if (_converters[i] == nullptr) {
            return Status::NotSupported("Cannot get field converter");
        }
    }
    return Status::OK();
}

void RowConverter::convert(std::vector<Datum>* dst, const std::vector<Datum>& src) const {
    size_t num_datums = src.size();
    dst->resize(num_datums);
    for (size_t i = 0; i < num_datums; ++i) {
        _converters[i]->convert(&(*dst)[i], src[i]);
    }
}

} // namespace starrocks
