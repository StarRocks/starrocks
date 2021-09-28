// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#include "storage/vectorized/convert_helper.h"

#include <utility>

#include "column/chunk.h"
#include "column/decimalv3_column.h"
#include "column/nullable_column.h"
#include "column/schema.h"
#include "gutil/strings/substitute.h"
#include "runtime/decimalv2_value.h"
#include "runtime/timestamp_value.h"
#include "storage/row.h"
#include "storage/row_block2.h"
#include "storage/row_cursor.h"
#include "storage/schema.h"
#include "storage/tablet_schema.h"
#include "storage/vectorized/chunk_helper.h"
#include "util/pred_guard.h"
#include "util/stack_util.h"
#include "util/unaligned_access.h"

namespace starrocks::vectorized {

using strings::Substitute;

// Used for schema change
class DateToTimestampTypeConverter : public TypeConverter {
public:
    DateToTimestampTypeConverter() = default;
    ~DateToTimestampTypeConverter() = default;

    Status convert(void* dst, const void* src, MemPool* memPool) const override {
        uint32_t src_value = unaligned_load<uint24_t>(src);
        int day = implicit_cast<int>(src_value & 31u);
        int month = implicit_cast<int>((src_value >> 5u) & 15u);
        int year = implicit_cast<int>(src_value >> 9u);
        unaligned_store<TimestampValue>(dst, TimestampValue::create(year, month, day, 0, 0, 0));
        return Status::OK();
    }
};

class DateV2ToTimestampTypeConverter : public TypeConverter {
public:
    DateV2ToTimestampTypeConverter() = default;
    ~DateV2ToTimestampTypeConverter() = default;

    Status convert(void* dst, const void* src, MemPool* memPool) const override {
        auto src_value = unaligned_load<DateValue>(src);
        int year = 0;
        int month = 0;
        int day = 0;
        src_value.to_date(&year, &month, &day);
        unaligned_store<TimestampValue>(dst, TimestampValue::create(year, month, day, 0, 0, 0));
        return Status::OK();
    }
};

class DatetimeToDateV2TypeConverter : public TypeConverter {
public:
    DatetimeToDateV2TypeConverter() = default;
    ~DatetimeToDateV2TypeConverter() = default;

    Status convert(void* dst, const void* src, MemPool* memPool) const override {
        TimestampValue timestamp{0};
        timestamp.from_timestamp_literal(unaligned_load<int64_t>(src));
        unaligned_store<DateValue>(dst, timestamp);
        return Status::OK();
    }
};

class TimestampToDateV2TypeConverter : public TypeConverter {
public:
    TimestampToDateV2TypeConverter() = default;
    ~TimestampToDateV2TypeConverter() = default;

    Status convert(void* dst, const void* src, MemPool* memPool) const override {
        unaligned_store<DateValue>(dst, unaligned_load<TimestampValue>(src));
        return Status::OK();
    }
};

template <FieldType int_type>
class IntegerToDateV2TypeConverter : public TypeConverter {
public:
    using CppType = typename CppTypeTraits<int_type>::CppType;

    IntegerToDateV2TypeConverter() = default;
    ~IntegerToDateV2TypeConverter() = default;

    Status convert(void* dst, const void* src, MemPool* memPool) const override {
        auto src_value = unaligned_load<CppType>(src);
        DateValue dst_val;
        if (dst_val.from_date_literal_with_check(src_value)) {
            unaligned_store<DateValue>(dst, dst_val);
            return Status::OK();
        }
        return Status::InvalidArgument(Substitute("Can not convert $0 to Date", src_value));
    }
};

class StringToDateV2TypeConverter : public TypeConverter {
public:
    StringToDateV2TypeConverter() = default;
    ~StringToDateV2TypeConverter() = default;

    Status convert(void* dst, const void* src, MemPool* memPool) const override {
        auto str = unaligned_load<Slice>(src);
        DateValue tmp;
        if (tmp.from_string((const char*)str.data, str.size)) {
            unaligned_store<DateValue>(dst, tmp);
            return Status::OK();
        }
        return Status::InvalidArgument(Substitute("Can not convert $0 to Date", str.to_string()));
    }
};

const TypeConverter* get_type_converter(FieldType from_type, FieldType to_type) {
    if (to_type == OLAP_FIELD_TYPE_TIMESTAMP) {
        switch (from_type) {
        case OLAP_FIELD_TYPE_DATE: {
            static DateToTimestampTypeConverter s_converter;
            return &s_converter;
        }
        case OLAP_FIELD_TYPE_DATE_V2: {
            static DateV2ToTimestampTypeConverter s_converter;
            return &s_converter;
        }
        default:
            break;
        }
    } else if (to_type == OLAP_FIELD_TYPE_DATE_V2) {
        switch (from_type) {
        case OLAP_FIELD_TYPE_TIMESTAMP: {
            static TimestampToDateV2TypeConverter s_converter;
            return &s_converter;
        }
        case OLAP_FIELD_TYPE_DATETIME: {
            static DatetimeToDateV2TypeConverter s_converter;
            return &s_converter;
        }
        case OLAP_FIELD_TYPE_INT: {
            static IntegerToDateV2TypeConverter<OLAP_FIELD_TYPE_INT> s_converter;
            return &s_converter;
        }
        case OLAP_FIELD_TYPE_VARCHAR: {
            static StringToDateV2TypeConverter s_converter;
            return &s_converter;
        }
        default:
            break;
        }
    }
    return nullptr;
}

class SameFieldConverter : public FieldConverter {
public:
    explicit SameFieldConverter(TypeInfoPtr type_info) : _type_info(std::move(type_info)) {}

    ~SameFieldConverter() override = default;

    void convert(void* dst, const void* src) const override { _type_info->shallow_copy(dst, src); }

    void convert(Datum* dst, const Datum& src) const override { *dst = src; }

    ColumnPtr copy_convert(const Column& src) const override { return src.clone_shared(); }

    ColumnPtr move_convert(Column* src) const override {
        auto ret = src->clone_empty();
        ret->swap_column(*src);
        return ret;
    }

    void convert(ColumnVectorBatch* dst, ColumnVectorBatch* src, const uint16_t* selection,
                 uint16_t selected_size) const override {
        src->swap(dst);
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
        auto dst = ChunkHelper::column_from_field_type(OLAP_FIELD_TYPE_DATE_V2, nullable);
        uint16_t num_items = src.size();
        dst->reserve(num_items);
        for (int i = 0; i < num_items; ++i) {
            Datum dst_datum;
            Datum src_datum = src.get(i);
            convert(&dst_datum, src_datum);
            dst->append_datum(dst_datum);
        }
        return dst;
    }

    void convert(ColumnVectorBatch* dst, ColumnVectorBatch* src, const uint16_t* selection,
                 uint16_t selected_size) const override {
        static const size_t SRC_FIELD_SIZE = sizeof(uint24_t);
        static const size_t DST_FIELD_SIZE = sizeof(int32_t);

        const uint8_t* src_data = src->data();
        uint8_t* dst_data = dst->data();
        if (!src->is_nullable()) {
            for (uint16_t i = 0; i < selected_size; ++i) {
                uint16_t row_id = selection[i];
                convert(dst_data + DST_FIELD_SIZE * row_id, src_data + SRC_FIELD_SIZE * row_id);
            }
        } else {
            const uint8_t* src_null_map = src->null_signs();
            uint8_t* dst_null_map = dst->null_signs();
            for (uint16_t i = 0; i < selected_size; ++i) {
                uint16_t row_id = selection[i];
                dst_null_map[row_id] = src_null_map[row_id];
                if (src_null_map[row_id]) {
                    continue;
                }
                convert(dst_data + DST_FIELD_SIZE * row_id, src_data + SRC_FIELD_SIZE * row_id);
            }
        }
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
        auto dst = ChunkHelper::column_from_field_type(OLAP_FIELD_TYPE_DATE, nullable);
        uint16_t num_items = src.size();
        dst->reserve(num_items);
        for (int i = 0; i < num_items; ++i) {
            Datum dst_datum;
            Datum src_datum = src.get(i);
            convert(&dst_datum, src_datum);
            dst->append_datum(dst_datum);
        }
        return dst;
    }

    void convert(ColumnVectorBatch* dst, ColumnVectorBatch* src, const uint16_t* selection,
                 uint16_t selected_size) const override {
        static const size_t SRC_FIELD_SIZE = sizeof(int32_t);
        static const size_t DST_FIELD_SIZE = sizeof(uint24_t);

        const uint8_t* src_data = src->data();
        uint8_t* dst_data = dst->data();
        if (!src->is_nullable()) {
            for (uint16_t i = 0; i < selected_size; ++i) {
                uint16_t row_id = selection[i];
                convert(dst_data + DST_FIELD_SIZE * row_id, src_data + SRC_FIELD_SIZE * row_id);
            }
        } else {
            const uint8_t* src_null_map = src->null_signs();
            uint8_t* dst_null_map = dst->null_signs();
            for (uint16_t i = 0; i < selected_size; ++i) {
                uint16_t row_id = selection[i];
                dst_null_map[row_id] = src_null_map[row_id];
                if (src_null_map[row_id]) {
                    continue;
                }
                convert(dst_data + DST_FIELD_SIZE * row_id, src_data + SRC_FIELD_SIZE * row_id);
            }
        }
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
        auto dst = ChunkHelper::column_from_field_type(OLAP_FIELD_TYPE_TIMESTAMP, nullable);
        uint16_t num_items = src.size();
        dst->reserve(num_items);
        for (int i = 0; i < num_items; ++i) {
            Datum dst_datum;
            Datum src_datum = src.get(i);
            convert(&dst_datum, src_datum);
            dst->append_datum(dst_datum);
        }
        return dst;
    }

    void convert(ColumnVectorBatch* dst, ColumnVectorBatch* src, const uint16_t* selection,
                 uint16_t selected_size) const override {
        static const size_t SRC_FIELD_SIZE = sizeof(int64_t);
        static const size_t DST_FIELD_SIZE = sizeof(int64_t);
        uint8_t* dst_data = dst->data();
        const uint8_t* src_data = src->data();
        if (src->is_nullable()) {
            const uint8_t* src_null_map = src->null_signs();
            uint8_t* dst_null_map = dst->null_signs();
            for (uint16_t i = 0; i < selected_size; ++i) {
                uint16_t row_id = selection[i];
                dst_null_map[row_id] = src_null_map[row_id];
                if (src_null_map[row_id]) {
                    continue;
                }
                convert(dst_data + DST_FIELD_SIZE * row_id, src_data + SRC_FIELD_SIZE * row_id);
            }
        } else {
            for (uint16_t i = 0; i < selected_size; ++i) {
                uint16_t row_id = selection[i];
                convert(dst_data + DST_FIELD_SIZE * row_id, src_data + SRC_FIELD_SIZE * row_id);
            }
        }
    }

private:
};

class TimestampToDatetimeFieldConverter : public FieldConverter {
public:
    TimestampToDatetimeFieldConverter() = default;
    ~TimestampToDatetimeFieldConverter() override = default;

    void convert(void* dst, const void* src) const override {
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
        auto dst = ChunkHelper::column_from_field_type(OLAP_FIELD_TYPE_DATETIME, nullable);
        uint16_t num_items = src.size();
        dst->reserve(num_items);
        for (int i = 0; i < num_items; ++i) {
            Datum dst_datum;
            Datum src_datum = src.get(i);
            convert(&dst_datum, src_datum);
            dst->append_datum(dst_datum);
        }
        return dst;
    }

    void convert(ColumnVectorBatch* dst, ColumnVectorBatch* src, const uint16_t* selection,
                 uint16_t selected_size) const override {
        static const size_t SRC_FIELD_SIZE = sizeof(int64_t);
        static const size_t DST_FIELD_SIZE = sizeof(int64_t);

        const uint8_t* src_data = src->data();
        uint8_t* dst_data = dst->data();
        if (!src->is_nullable()) {
            for (uint16_t i = 0; i < selected_size; ++i) {
                uint16_t row_id = selection[i];
                convert(dst_data + DST_FIELD_SIZE * row_id, src_data + SRC_FIELD_SIZE * row_id);
            }
        } else {
            const uint8_t* src_null_map = src->null_signs();
            uint8_t* dst_null_map = dst->null_signs();
            for (uint16_t i = 0; i < selected_size; ++i) {
                uint16_t row_id = selection[i];
                dst_null_map[row_id] = src_null_map[row_id];
                if (src_null_map[row_id]) {
                    continue;
                }
                convert(dst_data + DST_FIELD_SIZE * row_id, src_data + SRC_FIELD_SIZE * row_id);
            }
        }
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
        auto dst = ChunkHelper::column_from_field_type(OLAP_FIELD_TYPE_DECIMAL_V2, nullable);
        uint16_t num_items = src.size();
        dst->reserve(num_items);
        for (int i = 0; i < num_items; ++i) {
            Datum dst_datum;
            Datum src_datum = src.get(i);
            convert(&dst_datum, src_datum);
            dst->append_datum(dst_datum);
        }
        return dst;
    }

    // NOTE: This function should not be used.
    void convert(ColumnVectorBatch* dst, ColumnVectorBatch* src, const uint16_t* selection,
                 uint16_t selected_size) const override {
        static const size_t SRC_FIELD_SIZE = sizeof(decimal12_t);
        static const size_t DST_FIELD_SIZE = sizeof(DecimalV2Value);

        const uint8_t* src_data = src->data();
        uint8_t* dst_data = dst->data();
        if (!src->is_nullable()) {
            for (uint16_t i = 0; i < selected_size; ++i) {
                uint16_t row_id = selection[i];
                convert(dst_data + DST_FIELD_SIZE * row_id, src_data + SRC_FIELD_SIZE * row_id);
            }
        } else {
            const uint8_t* src_null_map = src->null_signs();
            uint8_t* dst_null_map = dst->null_signs();
            for (uint16_t i = 0; i < selected_size; ++i) {
                uint16_t row_id = selection[i];
                dst_null_map[row_id] = src_null_map[row_id];
                if (src_null_map[row_id]) {
                    continue;
                }
                convert(dst_data + DST_FIELD_SIZE * row_id, src_data + SRC_FIELD_SIZE * row_id);
            }
        }
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
        auto dst = ChunkHelper::column_from_field_type(OLAP_FIELD_TYPE_DECIMAL, nullable);
        uint16_t num_items = src.size();
        dst->reserve(num_items);
        for (int i = 0; i < num_items; ++i) {
            Datum dst_datum;
            Datum src_datum = src.get(i);
            convert(&dst_datum, src_datum);
            dst->append_datum(dst_datum);
        }
        return dst;
    }

    void convert(ColumnVectorBatch* dst, ColumnVectorBatch* src, const uint16_t* selection,
                 uint16_t selected_size) const override {
        static const size_t SRC_FIELD_SIZE = sizeof(DecimalV2Value);
        static const size_t DST_FIELD_SIZE = sizeof(decimal12_t);

        const uint8_t* src_data = src->data();
        uint8_t* dst_data = dst->data();
        if (!src->is_nullable()) {
            for (uint16_t i = 0; i < selected_size; ++i) {
                uint16_t row_id = selection[i];
                convert(dst_data + DST_FIELD_SIZE * row_id, src_data + SRC_FIELD_SIZE * row_id);
            }
        } else {
            const uint8_t* src_null_map = src->null_signs();
            uint8_t* dst_null_map = dst->null_signs();
            for (uint16_t i = 0; i < selected_size; ++i) {
                uint16_t row_id = selection[i];
                dst_null_map[row_id] = src_null_map[row_id];
                if (src_null_map[row_id]) {
                    continue;
                }
                convert(dst_data + DST_FIELD_SIZE * row_id, src_data + SRC_FIELD_SIZE * row_id);
            }
        }
    }

private:
};

template <typename SrcType, typename DstType>
struct ConvFunction {};
static inline constexpr int128_t TEN_POWER_9 = get_scale_factor<int128_t>(9);
template <>
struct ConvFunction<decimal12_t, int128_t> {
    static inline void apply(const decimal12_t* src, int128_t* dst) {
        auto src_val = unaligned_load<decimal12_t>(src);
        DecimalV2Value tmp;
        tmp.from_olap_decimal(src_val.integer, src_val.fraction);
        unaligned_store<DecimalV2Value>(dst, tmp);
    }
};

template <>
struct ConvFunction<int128_t, decimal12_t> {
    static inline void apply(const int128_t* src, decimal12_t* dst) {
        auto src_val = unaligned_load<DecimalV2Value>(src);
        decimal12_t tmp;
        tmp.integer = src_val.int_value();
        tmp.fraction = src_val.frac_value();
        unaligned_store<decimal12_t>(dst, tmp);
    }
};

template <>
struct ConvFunction<DecimalV2Value, int128_t> {
    static inline void apply(const DecimalV2Value* src, int128_t* dst) {
        strings::memcpy_inlined(dst, src, sizeof(int128_t));
    }
};

template <>
struct ConvFunction<int128_t, DecimalV2Value> {
    static inline void apply(const int128_t* src, DecimalV2Value* dst) {
        strings::memcpy_inlined(dst, src, sizeof(int128_t));
    }
};

DEF_PRED_GUARD(DirectlyCopybleGuard, is_directly_copyable, typename, SrcType, typename, DstType)
#define IS_DIRECTLY_COPYABLE_CTOR(SrcType, DstType) DEF_PRED_CASE_CTOR(is_directly_copyable, SrcType, DstType)
#define IS_DIRECTLY_COPYABLE(PT, ...) DEF_BINARY_RELATION_ENTRY_SEP_NONE(IS_DIRECTLY_COPYABLE_CTOR, PT, ##__VA_ARGS__)

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
        ColumnPtr dst = DstColumnType::create();
        dst->reserve(src.size());
        if constexpr (is_directly_copyable<SrcType, DstType>) {
            if (!src.is_nullable() && !src.is_constant()) {
                auto* dst_column = down_cast<DstColumnType*>(dst.get());
                auto* src_column = down_cast<const SrcColumnType*>(&src);
                // TODO (by satanson): unsafe abstraction leak
                //  swap std::vector<DecimalV2Value> and std::vector<int128_t>,
                //  raw memory copy is more sound.
                std::swap(dst_column->get_data(), (typename DstColumnType::Container&)(src_column->get_data()));
                return dst;
            } else if (src.is_nullable() && !dst->only_null()) {
                dst = NullableColumn::create(dst, NullColumn::create());
                auto* nullable_dst_column = down_cast<NullableColumn*>(dst.get());
                auto* nullable_src_column = down_cast<const NullableColumn*>(&src);
                auto* dst_column = down_cast<DstColumnType*>(nullable_dst_column->data_column().get());
                auto* src_column = down_cast<SrcColumnType*>(nullable_src_column->data_column().get());
                auto& null_dst_column = nullable_dst_column->null_column();
                auto& null_src_column = nullable_src_column->null_column();
                // TODO (by satanson): unsafe abstraction leak
                //  swap std::vector<DecimalV2Value> and std::vector<int128_t>
                //  raw memory copy is more sound.
                std::swap(dst_column->get_data(), (typename DstColumnType::Container&)(src_column->get_data()));
                std::swap(null_dst_column->get_data(), null_src_column->get_data());
                return dst;
            }
        }
        uint16_t num_items = src.size();
        for (int i = 0; i < num_items; ++i) {
            Datum dst_datum;
            Datum src_datum = src.get(i);
            convert(&dst_datum, src_datum);
            dst->append_datum(dst_datum);
        }
        return dst;
    }

    void convert(ColumnVectorBatch* dst, ColumnVectorBatch* src, const uint16_t* selection,
                 uint16_t selected_size) const override {
        static const size_t SRC_FIELD_SIZE = sizeof(SrcType);
        static const size_t DST_FIELD_SIZE = sizeof(DstType);

        const uint8_t* src_data = src->data();
        uint8_t* dst_data = dst->data();
        if (!src->is_nullable()) {
            for (uint16_t i = 0; i < selected_size; ++i) {
                uint16_t row_id = selection[i];
                convert(dst_data + DST_FIELD_SIZE * row_id, src_data + SRC_FIELD_SIZE * row_id);
            }
        } else {
            const uint8_t* src_null_map = src->null_signs();
            uint8_t* dst_null_map = dst->null_signs();
            for (uint16_t i = 0; i < selected_size; ++i) {
                uint16_t row_id = selection[i];
                dst_null_map[row_id] = src_null_map[row_id];
                if (src_null_map[row_id]) {
                    continue;
                }
                convert(dst_data + DST_FIELD_SIZE * row_id, src_data + SRC_FIELD_SIZE * row_id);
            }
        }
    }

private:
};

const FieldConverter* get_field_converter(FieldType from_type, FieldType to_type) {
#define TYPE_CASE_CLAUSE(type)                                      \
    case type: {                                                    \
        static SameFieldConverter s_converter(get_type_info(type)); \
        return &s_converter;                                        \
    }

    if (from_type == to_type) {
        switch (from_type) {
            TYPE_CASE_CLAUSE(OLAP_FIELD_TYPE_BOOL)
            TYPE_CASE_CLAUSE(OLAP_FIELD_TYPE_TINYINT)
            TYPE_CASE_CLAUSE(OLAP_FIELD_TYPE_SMALLINT)
            TYPE_CASE_CLAUSE(OLAP_FIELD_TYPE_INT)
            TYPE_CASE_CLAUSE(OLAP_FIELD_TYPE_BIGINT)
            TYPE_CASE_CLAUSE(OLAP_FIELD_TYPE_LARGEINT)
            TYPE_CASE_CLAUSE(OLAP_FIELD_TYPE_FLOAT)
            TYPE_CASE_CLAUSE(OLAP_FIELD_TYPE_DOUBLE)
            TYPE_CASE_CLAUSE(OLAP_FIELD_TYPE_DECIMAL)
            TYPE_CASE_CLAUSE(OLAP_FIELD_TYPE_DECIMAL_V2)
            TYPE_CASE_CLAUSE(OLAP_FIELD_TYPE_CHAR)
            TYPE_CASE_CLAUSE(OLAP_FIELD_TYPE_VARCHAR)
            TYPE_CASE_CLAUSE(OLAP_FIELD_TYPE_DATE)
            TYPE_CASE_CLAUSE(OLAP_FIELD_TYPE_DATE_V2)
            TYPE_CASE_CLAUSE(OLAP_FIELD_TYPE_DATETIME)
            TYPE_CASE_CLAUSE(OLAP_FIELD_TYPE_TIMESTAMP)
            TYPE_CASE_CLAUSE(OLAP_FIELD_TYPE_HLL)
            TYPE_CASE_CLAUSE(OLAP_FIELD_TYPE_OBJECT)
            TYPE_CASE_CLAUSE(OLAP_FIELD_TYPE_PERCENTILE)
        case OLAP_FIELD_TYPE_DECIMAL32:
        case OLAP_FIELD_TYPE_DECIMAL64:
        case OLAP_FIELD_TYPE_DECIMAL128:
        case OLAP_FIELD_TYPE_ARRAY:
        case OLAP_FIELD_TYPE_UNSIGNED_TINYINT:
        case OLAP_FIELD_TYPE_UNSIGNED_SMALLINT:
        case OLAP_FIELD_TYPE_UNSIGNED_INT:
        case OLAP_FIELD_TYPE_UNSIGNED_BIGINT:
        case OLAP_FIELD_TYPE_UNKNOWN:
        case OLAP_FIELD_TYPE_MAP:
        case OLAP_FIELD_TYPE_STRUCT:
        case OLAP_FIELD_TYPE_DISCRETE_DOUBLE:
        case OLAP_FIELD_TYPE_NONE:
        case OLAP_FIELD_TYPE_MAX_VALUE:
            return nullptr;
        }
        DCHECK(false) << "unreachable path";
        return nullptr;
    }
#undef TYPE_CASE_CLAUSE

    if (to_type == OLAP_FIELD_TYPE_DATE_V2 && from_type == OLAP_FIELD_TYPE_DATE) {
        static DateToDateV2FieldConverter s_converter;
        return &s_converter;
    } else if (to_type == OLAP_FIELD_TYPE_DATE && from_type == OLAP_FIELD_TYPE_DATE_V2) {
        static DateV2ToDateFieldConverter s_converter;
        return &s_converter;
    } else if (to_type == OLAP_FIELD_TYPE_TIMESTAMP && from_type == OLAP_FIELD_TYPE_DATETIME) {
        static DatetimeToTimestampFieldConverter s_converter;
        return &s_converter;
    } else if (to_type == OLAP_FIELD_TYPE_DATETIME && from_type == OLAP_FIELD_TYPE_TIMESTAMP) {
        static TimestampToDatetimeFieldConverter s_converter;
        return &s_converter;
    } else if (to_type == OLAP_FIELD_TYPE_DECIMAL_V2 && from_type == OLAP_FIELD_TYPE_DECIMAL) {
        static Decimal12ToDecimalFieldConverter s_converter;
        return &s_converter;
    } else if (to_type == OLAP_FIELD_TYPE_DECIMAL && from_type == OLAP_FIELD_TYPE_DECIMAL_V2) {
        static DecimalToDecimal12FieldConverter s_converter;
        return &s_converter;
    } else if (to_type == OLAP_FIELD_TYPE_DECIMAL128 && from_type == OLAP_FIELD_TYPE_DECIMAL) {
        static DecimalFieldConverter<decimal12_t, int128_t> s_converter;
        return &s_converter;
    } else if (to_type == OLAP_FIELD_TYPE_DECIMAL && from_type == OLAP_FIELD_TYPE_DECIMAL128) {
        static DecimalFieldConverter<int128_t, decimal12_t> s_converter;
        return &s_converter;
    } else if (to_type == OLAP_FIELD_TYPE_DECIMAL128 && from_type == OLAP_FIELD_TYPE_DECIMAL_V2) {
        static DecimalFieldConverter<DecimalV2Value, int128_t> s_converter;
        return &s_converter;
    } else if (to_type == OLAP_FIELD_TYPE_DECIMAL_V2 && from_type == OLAP_FIELD_TYPE_DECIMAL128) {
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

Status RowConverter::init(const ::starrocks::Schema& in_schema, const ::starrocks::Schema& out_schema) {
    auto num_columns = in_schema.num_column_ids();
    _converters.resize(num_columns);
    _cids.resize(num_columns, 0);
    for (int i = 0; i < num_columns; ++i) {
        auto cid = in_schema.column_ids()[i];
        _cids[i] = cid;
        _converters[i] = get_field_converter(in_schema.column(i)->type(), out_schema.column(i)->type());
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

template <typename RowType>
void RowConverter::convert(RowCursor* dst, const RowType& src) const {
    for (int i = 0; i < _converters.size(); ++i) {
        auto cid = _cids[i];

        auto src_cell = src.cell(cid);
        auto dst_cell = dst->cell(cid);
        bool is_null = src_cell.is_null();
        dst_cell.set_is_null(is_null);
        if (is_null) {
            continue;
        }
        _converters[i]->convert(dst_cell.mutable_cell_ptr(), src_cell.cell_ptr());
    }
}

template void RowConverter::convert<RowCursor>(RowCursor* dst, const RowCursor& src) const;
template void RowConverter::convert<ContiguousRow>(RowCursor* dst, const ContiguousRow& src) const;

void RowConverter::convert(std::vector<Datum>* dst, const std::vector<Datum>& src) const {
    int num_datums = src.size();
    dst->resize(num_datums);
    for (int i = 0; i < num_datums; ++i) {
        _converters[i]->convert(&(*dst)[i], src[i]);
    }
}

Status ChunkConverter::init(const Schema& in_schema, const Schema& out_schema) {
    DCHECK_EQ(in_schema.num_fields(), out_schema.num_fields());
    DCHECK_EQ(in_schema.num_key_fields(), out_schema.num_key_fields());
    auto num_columns = in_schema.num_fields();
    _converters.resize(num_columns, nullptr);
    for (int i = 0; i < num_columns; ++i) {
        auto& f1 = in_schema.field(i);
        auto& f2 = out_schema.field(i);
        DCHECK_EQ(f1->id(), f2->id());
        _converters[i] = get_field_converter(f1->type()->type(), f2->type()->type());
        if (_converters[i] == nullptr) {
            return Status::NotSupported("Cannot get field converter");
        }
    }
    _out_schema = std::make_shared<Schema>(out_schema);
    return Status::OK();
}

std::unique_ptr<Chunk> ChunkConverter::copy_convert(const Chunk& from) const {
    auto dest = std::make_unique<Chunk>(Columns{}, std::make_shared<Schema>());
    auto num_columns = _converters.size();
    DCHECK_EQ(num_columns, from.num_columns());
    for (int i = 0; i < num_columns; ++i) {
        auto f = _out_schema->field(i);
        auto c = _converters[i]->copy_convert(*from.get_column_by_id(f->id()));
        dest->append_column(std::move(c), f);
    }
    return dest;
}

std::unique_ptr<Chunk> ChunkConverter::move_convert(Chunk* from) const {
    auto dest = std::make_unique<Chunk>(Columns{}, std::make_shared<Schema>());
    auto num_columns = _converters.size();
    DCHECK_EQ(num_columns, from->num_columns());
    for (int i = 0; i < num_columns; ++i) {
        auto f = _out_schema->field(i);
        auto c = _converters[i]->move_convert(from->get_column_by_id(f->id()).get());
        dest->append_column(std::move(c), f);
    }
    return dest;
}

Status BlockConverter::init(const ::starrocks::Schema& in_schema, const ::starrocks::Schema& out_schema) {
    auto num_columns = in_schema.num_column_ids();
    _converters.resize(num_columns, nullptr);
    _cids.resize(num_columns, 0);
    for (int i = 0; i < num_columns; ++i) {
        auto cid = in_schema.column_ids()[i];
        _cids[i] = cid;
        _converters[i] = get_field_converter(in_schema.column(cid)->type(), out_schema.column(cid)->type());
        if (_converters[i] == nullptr) {
            return Status::NotSupported("Cannot get field converter");
        }
    }
    return Status::OK();
}

Status BlockConverter::convert(::starrocks::RowBlockV2* dst, ::starrocks::RowBlockV2* src) const {
    DCHECK_EQ(dst->_capacity, src->_capacity);

    auto num_columns = _converters.size();
    for (int i = 0; i < num_columns; ++i) {
        auto cid = _cids[i];
        _converters[i]->convert(dst->_column_vector_batches[cid].get(), src->_column_vector_batches[cid].get(),
                                src->_selection_vector, src->_selected_size);
    }
    std::swap(dst->_num_rows, src->_num_rows);
    std::swap(dst->_tracker, src->_tracker);
    std::swap(dst->_pool, src->_pool);
    std::swap(dst->_selection_vector, src->_selection_vector);
    std::swap(dst->_selected_size, src->_selected_size);
    std::swap(dst->_delete_state, src->_delete_state);
    return Status::OK();
}

} // namespace starrocks::vectorized
