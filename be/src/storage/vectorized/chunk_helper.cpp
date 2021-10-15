// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#include "storage/vectorized/chunk_helper.h"

#include "column/chunk.h"
#include "column/column_pool.h"
#include "column/schema.h"
#include "storage/tablet_schema.h"
#include "storage/vectorized/type_utils.h"
#include "util/metrics.h"

namespace starrocks::vectorized {

vectorized::Field ChunkHelper::convert_field(ColumnId id, const TabletColumn& c) {
    TypeInfoPtr type_info = get_type_info(c);
    starrocks::vectorized::Field f(id, std::string(c.name()), type_info, c.is_nullable());
    f.set_is_key(c.is_key());
    f.set_short_key_length(c.index_length());
    f.set_aggregate_method(c.aggregation());
    return f;
}

vectorized::Schema ChunkHelper::convert_schema(const starrocks::TabletSchema& schema) {
    starrocks::vectorized::Fields fields;
    for (ColumnId cid = 0; cid < schema.num_columns(); ++cid) {
        auto f = convert_field(cid, schema.column(cid));
        fields.emplace_back(std::make_shared<starrocks::vectorized::Field>(std::move(f)));
    }
    return starrocks::vectorized::Schema(std::move(fields));
}

starrocks::vectorized::Field ChunkHelper::convert_field_to_format_v2(ColumnId id, const TabletColumn& c) {
    FieldType type = TypeUtils::to_storage_format_v2(c.type());

    TypeInfoPtr type_info = nullptr;
    if (type == OLAP_FIELD_TYPE_ARRAY || type == OLAP_FIELD_TYPE_DECIMAL32 || type == OLAP_FIELD_TYPE_DECIMAL64 ||
        type == OLAP_FIELD_TYPE_DECIMAL128) {
        // ARRAY and DECIMAL should be handled specially
        // Array is nested type, the message is stored in TabletColumn
        // Decimal has precision and scale, the message is stored in TabletColumn
        type_info = get_type_info(c);
    } else {
        type_info = get_type_info(type);
    }
    starrocks::vectorized::Field f(id, std::string(c.name()), type_info, c.is_nullable());
    f.set_is_key(c.is_key());

    if (type == OLAP_FIELD_TYPE_ARRAY) {
        const TabletColumn& sub_column = c.subcolumn(0);
        auto sub_field = convert_field_to_format_v2(id, sub_column);
        f.add_sub_field(sub_field);
    }

    // If origin type needs to be converted format v2, we should change its short key length
    if (TypeUtils::specific_type_of_format_v1(c.type())) {
        // Get TypeInfo with new type
        TypeInfoPtr type_info = get_type_info(type);
        f.set_short_key_length(type_info->size());
    } else {
        f.set_short_key_length(c.index_length());
    }

    f.set_aggregate_method(c.aggregation());
    return f;
}

starrocks::vectorized::Schema ChunkHelper::convert_schema_to_format_v2(const starrocks::TabletSchema& schema) {
    starrocks::vectorized::Fields fields;
    for (ColumnId cid = 0; cid < schema.num_columns(); ++cid) {
        auto f = convert_field_to_format_v2(cid, schema.column(cid));
        fields.emplace_back(std::make_shared<starrocks::vectorized::Field>(std::move(f)));
    }
    return starrocks::vectorized::Schema(std::move(fields));
}

starrocks::vectorized::Schema ChunkHelper::convert_schema_to_format_v2(const starrocks::TabletSchema& schema,
                                                                       const std::vector<ColumnId>& cids) {
    starrocks::vectorized::Fields fields;
    for (ColumnId cid : cids) {
        auto f = convert_field_to_format_v2(cid, schema.column(cid));
        fields.emplace_back(std::make_shared<starrocks::vectorized::Field>(std::move(f)));
    }
    return starrocks::vectorized::Schema(std::move(fields));
}

ColumnId ChunkHelper::max_column_id(const starrocks::vectorized::Schema& schema) {
    ColumnId id = 0;
    for (const auto& field : schema.fields()) {
        id = std::max(id, field->id());
    }
    return id;
}

template <typename T>
struct ColumnDeleter {
    void operator()(Column* ptr) const { return_column<T>(down_cast<T*>(ptr)); }
};

template <typename T, bool force>
inline std::shared_ptr<T> get_column_ptr() {
    if constexpr (std::negation_v<HasColumnPool<T>>) {
        return std::make_shared<T>();
    } else {
        T* ptr = get_column<T, force>();
        if (LIKELY(ptr != nullptr)) {
            return std::shared_ptr<T>(ptr, ColumnDeleter<T>());
        } else {
            return std::make_shared<T>();
        }
    }
}

template <typename T, bool force>
inline std::shared_ptr<DecimalColumnType<T>> get_decimal_column_ptr(int precision, int scale) {
    auto column = get_column_ptr<T, force>();
    column->set_precision(precision);
    column->set_scale(scale);
    return column;
}

template <bool force>
ColumnPtr column_from_pool(const Field& field) {
    auto Nullable = [&](ColumnPtr c) -> ColumnPtr {
        return field.is_nullable() ? NullableColumn::create(std::move(c), get_column_ptr<NullColumn, force>()) : c;
    };

    auto precision = field.type()->precision();
    auto scale = field.type()->scale();
    switch (field.type()->type()) {
    case OLAP_FIELD_TYPE_HLL:
        return Nullable(get_column_ptr<HyperLogLogColumn, force>());
    case OLAP_FIELD_TYPE_OBJECT:
        return Nullable(get_column_ptr<BitmapColumn, force>());
    case OLAP_FIELD_TYPE_PERCENTILE:
        return Nullable(get_column_ptr<PercentileColumn, force>());
    case OLAP_FIELD_TYPE_CHAR:
    case OLAP_FIELD_TYPE_VARCHAR:
        return Nullable(get_column_ptr<BinaryColumn, force>());
    case OLAP_FIELD_TYPE_BOOL:
        return Nullable(get_column_ptr<UInt8Column, force>());
    case OLAP_FIELD_TYPE_TINYINT:
        return Nullable(get_column_ptr<Int8Column, force>());
    case OLAP_FIELD_TYPE_SMALLINT:
        return Nullable(get_column_ptr<Int16Column, force>());
    case OLAP_FIELD_TYPE_INT:
        return Nullable(get_column_ptr<Int32Column, force>());
    case OLAP_FIELD_TYPE_UNSIGNED_INT:
        return Nullable(get_column_ptr<FixedLengthColumn<uint32_t>, force>());
    case OLAP_FIELD_TYPE_BIGINT:
        return Nullable(get_column_ptr<Int64Column, force>());
    case OLAP_FIELD_TYPE_UNSIGNED_BIGINT:
        return Nullable(get_column_ptr<FixedLengthColumn<uint64_t>, force>());
    case OLAP_FIELD_TYPE_LARGEINT:
        return Nullable(get_column_ptr<Int128Column, force>());
    case OLAP_FIELD_TYPE_FLOAT:
        return Nullable(get_column_ptr<FloatColumn, force>());
    case OLAP_FIELD_TYPE_DOUBLE:
        return Nullable(get_column_ptr<DoubleColumn, force>());
    case OLAP_FIELD_TYPE_DATE:
        return Nullable(get_column_ptr<FixedLengthColumn<uint24_t>, force>());
    case OLAP_FIELD_TYPE_DATE_V2:
        return Nullable(get_column_ptr<DateColumn, force>());
    case OLAP_FIELD_TYPE_DATETIME:
        return Nullable(get_column_ptr<FixedLengthColumn<int64_t>, force>());
    case OLAP_FIELD_TYPE_TIMESTAMP:
        return Nullable(get_column_ptr<TimestampColumn, force>());
    case OLAP_FIELD_TYPE_DECIMAL:
        return Nullable(get_column_ptr<FixedLengthColumn<decimal12_t>, force>());
    case OLAP_FIELD_TYPE_DECIMAL_V2:
        return Nullable(get_column_ptr<DecimalColumn, force>());
    case OLAP_FIELD_TYPE_DECIMAL32:
        return Nullable(get_decimal_column_ptr<Decimal32Column, force>(precision, scale));
    case OLAP_FIELD_TYPE_DECIMAL64:
        return Nullable(get_decimal_column_ptr<Decimal64Column, force>(precision, scale));
    case OLAP_FIELD_TYPE_DECIMAL128:
        return Nullable(get_decimal_column_ptr<Decimal128Column, force>(precision, scale));
    case OLAP_FIELD_TYPE_ARRAY: {
        // Never allocate array element columns from column-pool, because its max size is unknown.
        auto elements = field.get_sub_field(0).create_column();
        auto offsets = get_column_ptr<UInt32Column, force>();
        auto array = ArrayColumn::create(std::move(elements), offsets);
        return Nullable(array);
    }
    case OLAP_FIELD_TYPE_UNSIGNED_TINYINT:
    case OLAP_FIELD_TYPE_UNSIGNED_SMALLINT:
    case OLAP_FIELD_TYPE_DISCRETE_DOUBLE:
    case OLAP_FIELD_TYPE_STRUCT:
    case OLAP_FIELD_TYPE_MAP:
    case OLAP_FIELD_TYPE_UNKNOWN:
    case OLAP_FIELD_TYPE_NONE:
    case OLAP_FIELD_TYPE_MAX_VALUE:
        CHECK(false) << "unsupported column type " << field.type()->type();
        return nullptr;
    }
    return nullptr;
}

Chunk* ChunkHelper::new_chunk_pooled(const vectorized::Schema& schema, size_t n, bool force) {
    Columns columns;
    columns.reserve(schema.num_fields());
    for (size_t i = 0; i < schema.num_fields(); i++) {
        const vectorized::FieldPtr& f = schema.field(i);
        auto column =
                (force && !config::disable_column_pool) ? column_from_pool<true>(*f) : column_from_pool<false>(*f);
        column->reserve(n);
        columns.emplace_back(std::move(column));
    }
    return new Chunk(std::move(columns), std::make_shared<vectorized::Schema>(schema));
}

size_t ChunkHelper::approximate_sizeof_type(FieldType type) {
    switch (type) {
    case OLAP_FIELD_TYPE_HLL:
        return sizeof(HyperLogLog);
    case OLAP_FIELD_TYPE_OBJECT:
        return sizeof(BitmapValue);
    case OLAP_FIELD_TYPE_PERCENTILE:
        return sizeof(PercentileValue);
    case OLAP_FIELD_TYPE_CHAR:
    case OLAP_FIELD_TYPE_VARCHAR:
        return sizeof(Slice);
    case OLAP_FIELD_TYPE_BOOL:
        return sizeof(uint8_t);
    case OLAP_FIELD_TYPE_TINYINT:
        return sizeof(int8_t);
    case OLAP_FIELD_TYPE_SMALLINT:
        return sizeof(int16_t);
    case OLAP_FIELD_TYPE_DECIMAL32:
    case OLAP_FIELD_TYPE_INT:
    case OLAP_FIELD_TYPE_UNSIGNED_INT:
        return sizeof(uint32_t);
    case OLAP_FIELD_TYPE_DECIMAL64:
    case OLAP_FIELD_TYPE_BIGINT:
    case OLAP_FIELD_TYPE_UNSIGNED_BIGINT:
        return sizeof(uint64_t);
    case OLAP_FIELD_TYPE_DECIMAL128:
    case OLAP_FIELD_TYPE_LARGEINT:
        return sizeof(int128_t);
    case OLAP_FIELD_TYPE_FLOAT:
        return sizeof(float);
    case OLAP_FIELD_TYPE_DOUBLE:
        return sizeof(double);
    case OLAP_FIELD_TYPE_DATE:
    case OLAP_FIELD_TYPE_DATE_V2:
        return sizeof(DateValue);
    case OLAP_FIELD_TYPE_DATETIME:
    case OLAP_FIELD_TYPE_TIMESTAMP:
        return sizeof(TimestampValue);
    case OLAP_FIELD_TYPE_DECIMAL:
    case OLAP_FIELD_TYPE_DECIMAL_V2:
        return sizeof(DecimalV2Value);
    case OLAP_FIELD_TYPE_UNSIGNED_TINYINT:
    case OLAP_FIELD_TYPE_UNSIGNED_SMALLINT:
    case OLAP_FIELD_TYPE_DISCRETE_DOUBLE:
    case OLAP_FIELD_TYPE_STRUCT:
    case OLAP_FIELD_TYPE_ARRAY:
    case OLAP_FIELD_TYPE_MAP:
    case OLAP_FIELD_TYPE_UNKNOWN:
    case OLAP_FIELD_TYPE_NONE:
    case OLAP_FIELD_TYPE_MAX_VALUE:
        return 4;
    }
    return 4;
}

std::vector<size_t> ChunkHelper::get_char_field_indexes(const vectorized::Schema& schema) {
    std::vector<size_t> char_field_indexes;
    for (size_t i = 0; i < schema.num_fields(); ++i) {
        const auto& field = schema.field(i);
        if (field->type()->type() == OLAP_FIELD_TYPE_CHAR) {
            char_field_indexes.push_back(i);
        }
    }
    return char_field_indexes;
}

void ChunkHelper::padding_char_columns(const std::vector<size_t>& char_column_indexes, const vectorized::Schema& schema,
                                       const starrocks::TabletSchema& tschema, vectorized::Chunk* chunk) {
    size_t num_rows = chunk->num_rows();
    for (auto field_index : char_column_indexes) {
        vectorized::Column* column = chunk->get_column_by_index(field_index).get();
        vectorized::Column* data_column = vectorized::ColumnHelper::get_data_column(column);
        vectorized::BinaryColumn* binary = down_cast<vectorized::BinaryColumn*>(data_column);

        vectorized::Offsets& offset = binary->get_offset();
        vectorized::Bytes& bytes = binary->get_bytes();

        // Padding 0 to CHAR field, the storage bitmap index and zone map need it.
        // TODO(kks): we could improve this if there are many null valus
        auto new_binary = vectorized::BinaryColumn::create();
        vectorized::Offsets& new_offset = new_binary->get_offset();
        vectorized::Bytes& new_bytes = new_binary->get_bytes();

        uint32_t len = tschema.column(field_index).length();

        new_offset.resize(num_rows + 1);
        new_bytes.assign(num_rows * len, 0); // padding 0

        uint32_t from = 0;
        for (size_t j = 0; j < num_rows; ++j) {
            uint32_t copy_data_len = std::min(len, offset[j + 1] - offset[j]);
            strings::memcpy_inlined(new_bytes.data() + from, bytes.data() + offset[j], copy_data_len);
            from += len; // no copy data will be 0
        }

        for (size_t j = 1; j <= num_rows; ++j) {
            new_offset[j] = len * j;
        }

        const auto& field = schema.field(field_index);

        if (field->is_nullable()) {
            auto* nullable_column = down_cast<vectorized::NullableColumn*>(column);
            ColumnPtr new_column = vectorized::NullableColumn::create(new_binary, nullable_column->null_column());
            new_column->swap_column(*column);
        } else {
            new_binary->swap_column(*column);
        }
    }
}

} // namespace starrocks::vectorized
