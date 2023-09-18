// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#include "storage/chunk_helper.h"

#include "column/chunk.h"
#include "column/column_pool.h"
#include "column/schema.h"
#include "column/type_traits.h"
#include "storage/olap_type_infra.h"
#include "storage/tablet_schema.h"
#include "storage/type_utils.h"
#include "storage/types.h"
#include "util/metrics.h"

namespace starrocks::vectorized {

vectorized::Field ChunkHelper::convert_field(ColumnId id, const TabletColumn& c) {
    TypeInfoPtr type_info = get_type_info(c);
    starrocks::vectorized::Field f(id, std::string(c.name()), type_info, c.is_nullable());
    f.set_is_key(c.is_key());
    f.set_short_key_length(c.index_length());
    f.set_aggregate_method(c.aggregation());
    f.set_length(c.length());
    return f;
}

vectorized::Schema ChunkHelper::convert_schema(const starrocks::TabletSchema& schema) {
    starrocks::vectorized::Fields fields;
    for (ColumnId cid = 0; cid < schema.num_columns(); ++cid) {
        auto f = convert_field(cid, schema.column(cid));
        fields.emplace_back(std::make_shared<starrocks::vectorized::Field>(std::move(f)));
    }
    return starrocks::vectorized::Schema(std::move(fields), schema.keys_type());
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
    f.set_length(c.length());

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
    return starrocks::vectorized::Schema(std::move(fields), schema.keys_type());
}

starrocks::vectorized::Schema ChunkHelper::convert_schema_to_format_v2(const starrocks::TabletSchema& schema,
                                                                       const std::vector<ColumnId>& cids) {
    starrocks::vectorized::Fields fields;
    for (ColumnId cid : cids) {
        auto f = convert_field_to_format_v2(cid, schema.column(cid));
        fields.emplace_back(std::make_shared<starrocks::vectorized::Field>(std::move(f)));
    }
    return starrocks::vectorized::Schema(std::move(fields), schema.keys_type());
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
    ColumnDeleter(uint32_t chunk_size) : chunk_size(chunk_size) {}
    void operator()(Column* ptr) const { return_column<T>(down_cast<T*>(ptr), chunk_size); }
    uint32_t chunk_size;
};

template <typename T, bool force>
inline std::shared_ptr<T> get_column_ptr(size_t chunk_size) {
    if constexpr (std::negation_v<HasColumnPool<T>>) {
        return std::make_shared<T>();
    } else {
        T* ptr = get_column<T, force>();
        if (LIKELY(ptr != nullptr)) {
            return std::shared_ptr<T>(ptr, ColumnDeleter<T>(chunk_size));
        } else {
            return std::make_shared<T>();
        }
    }
}

template <typename T, bool force>
inline std::shared_ptr<DecimalColumnType<T>> get_decimal_column_ptr(int precision, int scale, size_t chunk_size) {
    auto column = get_column_ptr<T, force>(chunk_size);
    column->set_precision(precision);
    column->set_scale(scale);
    return column;
}

template <bool force>
struct ColumnPtrBuilder {
    template <FieldType ftype>
    ColumnPtr operator()(size_t chunk_size, const Field& field, int precision, int scale) {
        auto nullable = [&](ColumnPtr c) -> ColumnPtr {
            return field.is_nullable()
                           ? NullableColumn::create(std::move(c), get_column_ptr<NullColumn, force>(chunk_size))
                           : c;
        };

        if constexpr (ftype == OLAP_FIELD_TYPE_ARRAY) {
            auto elements = field.sub_field(0).create_column();
            auto offsets = get_column_ptr<UInt32Column, force>(chunk_size);
            auto array = ArrayColumn::create(std::move(elements), offsets);
            return nullable(array);
        } else {
            switch (ftype) {
            case OLAP_FIELD_TYPE_DECIMAL32:
                return nullable(get_decimal_column_ptr<Decimal32Column, force>(precision, scale, chunk_size));
            case OLAP_FIELD_TYPE_DECIMAL64:
                return nullable(get_decimal_column_ptr<Decimal64Column, force>(precision, scale, chunk_size));
            case OLAP_FIELD_TYPE_DECIMAL128:
                return nullable(get_decimal_column_ptr<Decimal128Column, force>(precision, scale, chunk_size));
            default: {
                return nullable(get_column_ptr<typename CppColumnTraits<ftype>::ColumnType, force>(chunk_size));
            }
            }
        }
    }
};

template <bool force>
ColumnPtr column_from_pool(const Field& field, size_t chunk_size) {
    auto precision = field.type()->precision();
    auto scale = field.type()->scale();
    return field_type_dispatch_column(field.type()->type(), ColumnPtrBuilder<force>(), chunk_size, field, precision,
                                      scale);
}

Chunk* ChunkHelper::new_chunk_pooled(const vectorized::Schema& schema, size_t chunk_size, bool force) {
    Columns columns;
    columns.reserve(schema.num_fields());
    for (size_t i = 0; i < schema.num_fields(); i++) {
        const vectorized::FieldPtr& f = schema.field(i);
        auto column = force ? column_from_pool<true>(*f, chunk_size) : column_from_pool<false>(*f, chunk_size);
        column->reserve(chunk_size);
        columns.emplace_back(std::move(column));
    }
    return new Chunk(std::move(columns), std::make_shared<vectorized::Schema>(schema));
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

        // |schema| maybe partial columns in vertical compaction, so get char column length by name.
        uint32_t len = tschema.column(tschema.field_index(schema.field(field_index)->name())).length();

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

struct ColumnBuilder {
    template <FieldType ftype>
    ColumnPtr operator()(bool nullable) {
        [[maybe_unused]] auto NullableIfNeed = [&](ColumnPtr col) -> ColumnPtr {
            return nullable ? NullableColumn::create(std::move(col), NullColumn::create()) : col;
        };

        if constexpr (ftype == OLAP_FIELD_TYPE_ARRAY) {
            CHECK(false) << "array not supported";
        } else {
            return NullableIfNeed(CppColumnTraits<ftype>::ColumnType::create());
        }
    }
};

ColumnPtr ChunkHelper::column_from_field_type(FieldType type, bool nullable) {
    return field_type_dispatch_column(type, ColumnBuilder(), nullable);
}

ColumnPtr ChunkHelper::column_from_field(const Field& field) {
    auto NullableIfNeed = [&](ColumnPtr col) -> ColumnPtr {
        return field.is_nullable() ? NullableColumn::create(std::move(col), NullColumn::create()) : col;
    };

    auto type = field.type()->type();
    switch (type) {
    case OLAP_FIELD_TYPE_DECIMAL32:
        return NullableIfNeed(Decimal32Column::create(field.type()->precision(), field.type()->scale()));
    case OLAP_FIELD_TYPE_DECIMAL64:
        return NullableIfNeed(Decimal64Column::create(field.type()->precision(), field.type()->scale()));
    case OLAP_FIELD_TYPE_DECIMAL128:
        return NullableIfNeed(Decimal128Column::create(field.type()->precision(), field.type()->scale()));
    case OLAP_FIELD_TYPE_ARRAY: {
        return NullableIfNeed(ArrayColumn::create(column_from_field(field.sub_field(0)), UInt32Column::create()));
    }
    default:
        return NullableIfNeed(column_from_field_type(type, false));
    }
}

} // namespace starrocks::vectorized
