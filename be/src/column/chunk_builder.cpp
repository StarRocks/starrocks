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

#include "column/chunk_builder.h"

#include <algorithm>
#include <string>
#include <utility>

#include "column/array_column.h"
#include "column/chunk.h"
#include "column/json_column.h"
#include "column/map_column.h"
#include "column/runtime_type_traits.h"
#include "column/schema.h"
#include "column/storage_column_traits.h"
#include "column/struct_column.h"
#include "common/logging.h"

namespace starrocks {

#define CHUNK_BUILDER_TYPE_DISPATCH_CASE(type) \
    case type:                                 \
        return fun.template operator()<type>(std::forward<Args>(args)...);

template <class Functor, class... Args>
auto chunk_builder_type_dispatch_column(LogicalType ftype, Functor fun, Args&&... args) {
    switch (ftype) {
        CHUNK_BUILDER_TYPE_DISPATCH_CASE(TYPE_TINYINT)
        CHUNK_BUILDER_TYPE_DISPATCH_CASE(TYPE_SMALLINT)
        CHUNK_BUILDER_TYPE_DISPATCH_CASE(TYPE_BIGINT)
        CHUNK_BUILDER_TYPE_DISPATCH_CASE(TYPE_LARGEINT)
        CHUNK_BUILDER_TYPE_DISPATCH_CASE(TYPE_INT)
        CHUNK_BUILDER_TYPE_DISPATCH_CASE(TYPE_INT256)
        CHUNK_BUILDER_TYPE_DISPATCH_CASE(TYPE_DATE_V1)
        CHUNK_BUILDER_TYPE_DISPATCH_CASE(TYPE_DATE)
        CHUNK_BUILDER_TYPE_DISPATCH_CASE(TYPE_DATETIME_V1)
        CHUNK_BUILDER_TYPE_DISPATCH_CASE(TYPE_DATETIME)
        CHUNK_BUILDER_TYPE_DISPATCH_CASE(TYPE_UNSIGNED_INT)
        CHUNK_BUILDER_TYPE_DISPATCH_CASE(TYPE_FLOAT)
        CHUNK_BUILDER_TYPE_DISPATCH_CASE(TYPE_DOUBLE)
        CHUNK_BUILDER_TYPE_DISPATCH_CASE(TYPE_CHAR)
        CHUNK_BUILDER_TYPE_DISPATCH_CASE(TYPE_VARCHAR)
        CHUNK_BUILDER_TYPE_DISPATCH_CASE(TYPE_BOOLEAN)
        CHUNK_BUILDER_TYPE_DISPATCH_CASE(TYPE_DECIMAL)
        CHUNK_BUILDER_TYPE_DISPATCH_CASE(TYPE_DECIMALV2)
        CHUNK_BUILDER_TYPE_DISPATCH_CASE(TYPE_DECIMAL32)
        CHUNK_BUILDER_TYPE_DISPATCH_CASE(TYPE_DECIMAL64)
        CHUNK_BUILDER_TYPE_DISPATCH_CASE(TYPE_DECIMAL128)
        CHUNK_BUILDER_TYPE_DISPATCH_CASE(TYPE_DECIMAL256)
        CHUNK_BUILDER_TYPE_DISPATCH_CASE(TYPE_JSON)
        CHUNK_BUILDER_TYPE_DISPATCH_CASE(TYPE_VARBINARY)
        CHUNK_BUILDER_TYPE_DISPATCH_CASE(TYPE_HLL)
        CHUNK_BUILDER_TYPE_DISPATCH_CASE(TYPE_OBJECT)
        CHUNK_BUILDER_TYPE_DISPATCH_CASE(TYPE_PERCENTILE)
        CHUNK_BUILDER_TYPE_DISPATCH_CASE(TYPE_ARRAY)
        CHUNK_BUILDER_TYPE_DISPATCH_CASE(TYPE_MAP)
        CHUNK_BUILDER_TYPE_DISPATCH_CASE(TYPE_STRUCT)
        CHUNK_BUILDER_TYPE_DISPATCH_CASE(TYPE_UNSIGNED_SMALLINT)
    default:
        CHECK(false) << "unknown type " << ftype;
        __builtin_unreachable();
    }
}

#undef CHUNK_BUILDER_TYPE_DISPATCH_CASE

starrocks::SchemaPtr ChunkBuilder::get_non_nullable_schema(const starrocks::SchemaPtr& schema,
                                                           const std::vector<int>* keys) {
    const auto& old_fields = schema->fields();
    Fields new_fields;
    new_fields.resize(old_fields.size());
    DCHECK(keys == nullptr || old_fields.size() == keys->size());

    int idx = 0;
    for (const auto& old_field : old_fields) {
        ColumnId id = old_field->id();
        std::string_view name = old_field->name();
        TypeInfoPtr type = old_field->type();
        starrocks::StorageAggregateType agg = old_field->aggregate_method();
        uint8_t short_key_length = old_field->short_key_length();
        bool is_key = old_field->is_key();
        bool nullable = false;

        auto new_field = std::make_shared<Field>(id, name, type, agg, short_key_length,
                                                 keys != nullptr ? static_cast<bool>((*keys)[idx]) : is_key, nullable);
        new_fields[idx] = new_field;
        ++idx;
    }

    return std::make_shared<starrocks::Schema>(new_fields, schema->keys_type(), schema->sort_key_idxes());
}

ColumnId ChunkBuilder::max_column_id(const starrocks::Schema& schema) {
    ColumnId id = 0;
    for (const auto& field : schema.fields()) {
        id = std::max(id, field->id());
    }
    return id;
}

template <typename T>
inline typename T::MutablePtr get_column_ptr() {
    return T::create();
}

template <typename T>
inline typename DecimalColumnType<T>::MutablePtr get_decimal_column_ptr(int precision, int scale) {
    auto column = get_column_ptr<T>();
    column->set_precision(precision);
    column->set_scale(scale);
    return column;
}

struct ColumnPtrBuilder {
    template <LogicalType ftype>
    MutableColumnPtr operator()(const Field& field, int precision, int scale) {
        auto NullableIfNeed = [&](MutableColumnPtr&& c) -> MutableColumnPtr {
            return field.is_nullable()
                           ? MutableColumnPtr(NullableColumn::create(std::move(c), get_column_ptr<NullColumn>()))
                           : std::move(c);
        };

        if constexpr (ftype == TYPE_ARRAY) {
            auto elements = NullableColumn::wrap_if_necessary(ChunkBuilder::column_from_field(field.sub_field(0)));
            auto offsets = UInt32Column::create();
            auto array = ArrayColumn::create(std::move(elements), std::move(offsets));
            return NullableIfNeed(std::move(array));
        } else if constexpr (ftype == TYPE_MAP) {
            auto keys = NullableColumn::wrap_if_necessary(ChunkBuilder::column_from_field(field.sub_field(0)));
            auto values = NullableColumn::wrap_if_necessary(ChunkBuilder::column_from_field(field.sub_field(1)));
            auto offsets = get_column_ptr<UInt32Column>();
            auto map = MapColumn::create(std::move(keys), std::move(values), std::move(offsets));
            return NullableIfNeed(std::move(map));
        } else if constexpr (ftype == TYPE_STRUCT) {
            std::vector<std::string> names;
            MutableColumns fields;
            for (auto& sub_field : field.sub_fields()) {
                names.emplace_back(sub_field.name());
                fields.emplace_back(ChunkBuilder::column_from_field(sub_field));
            }
            auto struct_column = StructColumn::create(std::move(fields), std::move(names));
            return NullableIfNeed(std::move(struct_column));
        } else {
            if constexpr (ftype == TYPE_DECIMAL32) {
                return NullableIfNeed(get_decimal_column_ptr<Decimal32Column>(precision, scale));
            } else if constexpr (ftype == TYPE_DECIMAL64) {
                return NullableIfNeed(get_decimal_column_ptr<Decimal64Column>(precision, scale));
            } else if constexpr (ftype == TYPE_DECIMAL128) {
                return NullableIfNeed(get_decimal_column_ptr<Decimal128Column>(precision, scale));
            } else if constexpr (ftype == TYPE_DECIMAL256) {
                return NullableIfNeed(get_decimal_column_ptr<Decimal256Column>(precision, scale));
            } else {
                return NullableIfNeed(get_column_ptr<StorageColumnType<ftype>>());
            }
        }
    }
};

MutableColumnPtr column_from_pool(const Field& field) {
    auto precision = field.type()->precision();
    auto scale = field.type()->scale();
    return chunk_builder_type_dispatch_column(field.type()->type(), ColumnPtrBuilder(), field, precision, scale);
}

Chunk* ChunkBuilder::new_chunk_pooled(const Schema& schema, size_t chunk_size) {
    Columns columns;
    columns.reserve(schema.num_fields());
    for (size_t i = 0; i < schema.num_fields(); i++) {
        const FieldPtr& f = schema.field(i);
        auto column = column_from_pool(*f);
        // TODO: call reserve in SegmentIterator::read
        column->reserve(chunk_size);
        columns.emplace_back(std::move(column));
    }
    return new Chunk(std::move(columns), std::make_shared<Schema>(schema));
}

struct ColumnBuilder {
    template <LogicalType ftype>
    MutableColumnPtr operator()(bool nullable) {
        [[maybe_unused]] auto NullableIfNeed = [&](MutableColumnPtr&& col) -> MutableColumnPtr {
            return nullable ? MutableColumnPtr(NullableColumn::create(std::move(col), NullColumn::create()))
                            : std::move(col);
        };

        if constexpr (ftype == TYPE_ARRAY) {
            CHECK(false) << "array not supported";
            return nullptr;
        } else if constexpr (ftype == TYPE_MAP) {
            CHECK(false) << "array not supported";
            return nullptr;
        } else if constexpr (ftype == TYPE_STRUCT) {
            CHECK(false) << "array not supported";
            return nullptr;
        } else {
            return NullableIfNeed(StorageColumnType<ftype>::create());
        }
    }
};

MutableColumnPtr ChunkBuilder::column_from_field_type(LogicalType type, bool nullable) {
    return chunk_builder_type_dispatch_column(type, ColumnBuilder(), nullable);
}

MutableColumnPtr ChunkBuilder::column_from_field(const Field& field) {
    [[maybe_unused]] auto NullableIfNeed = [&](MutableColumnPtr&& col) -> MutableColumnPtr {
        return field.is_nullable() ? MutableColumnPtr(NullableColumn::create(std::move(col), NullColumn::create()))
                                   : std::move(col);
    };
    auto type = field.type()->type();
    switch (type) {
    case TYPE_DECIMAL32:
        return NullableIfNeed(Decimal32Column::create(field.type()->precision(), field.type()->scale()));
    case TYPE_DECIMAL64:
        return NullableIfNeed(Decimal64Column::create(field.type()->precision(), field.type()->scale()));
    case TYPE_DECIMAL128:
        return NullableIfNeed(Decimal128Column::create(field.type()->precision(), field.type()->scale()));
    case TYPE_DECIMAL256:
        return NullableIfNeed(Decimal256Column::create(field.type()->precision(), field.type()->scale()));
    case TYPE_ARRAY: {
        return NullableIfNeed(ArrayColumn::create(column_from_field(field.sub_field(0)), UInt32Column::create()));
    }
    case TYPE_MAP:
        return NullableIfNeed(MapColumn::create(column_from_field(field.sub_field(0)),
                                                column_from_field(field.sub_field(1)), UInt32Column::create()));
    case TYPE_STRUCT: {
        std::vector<std::string> names;
        MutableColumns fields;
        for (auto& sub_field : field.sub_fields()) {
            names.emplace_back(sub_field.name());
            fields.emplace_back(ChunkBuilder::column_from_field(sub_field));
        }
        auto struct_column = StructColumn::create(std::move(fields), std::move(names));
        return NullableIfNeed(std::move(struct_column));
    }
    default:
        return NullableIfNeed(column_from_field_type(type, false));
    }
}

ChunkUniquePtr ChunkBuilder::new_chunk(const Schema& schema, size_t n) {
    size_t fields = schema.num_fields();
    Columns columns;
    columns.reserve(fields);
    for (size_t i = 0; i < fields; i++) {
        const FieldPtr& f = schema.field(i);
        auto col = column_from_field(*f);
        col->reserve(n);
        columns.emplace_back(std::move(col));
    }
    return std::make_unique<Chunk>(std::move(columns), std::make_shared<Schema>(schema));
}

MutableChunkPtr ChunkBuilder::new_mutable_chunk(const Schema& schema, size_t n) {
    size_t fields = schema.num_fields();
    MutableColumns columns;
    columns.reserve(fields);
    for (size_t i = 0; i < fields; i++) {
        const FieldPtr& f = schema.field(i);
        auto col = column_from_field(*f);
        col->reserve(n);
        columns.emplace_back(std::move(col));
    }
    return std::make_shared<MutableChunk>(std::move(columns), std::make_shared<Schema>(schema));
}

} // namespace starrocks
