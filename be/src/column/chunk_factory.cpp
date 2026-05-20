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

#include "column/chunk_factory.h"

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "column/array_column.h"
#include "column/binary_column.h"
#include "column/chunk.h"
#include "column/decimalv3_column.h"
#include "column/field.h"
#include "column/fixed_length_column.h"
#include "column/json_column.h"
#include "column/map_column.h"
#include "column/nullable_column.h"
#include "column/object_column.h"
#include "column/schema.h"
#include "column/storage_column_traits.h"
#include "column/struct_column.h"
#include "types/olap_type_infra.h"

namespace starrocks {

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
            auto elements = NullableColumn::wrap_if_necessary(ChunkFactory::column_from_field(field.sub_field(0)));
            auto offsets = UInt32Column::create();
            auto array = ArrayColumn::create(std::move(elements), std::move(offsets));
            return NullableIfNeed(std::move(array));
        } else if constexpr (ftype == TYPE_MAP) {
            auto keys = NullableColumn::wrap_if_necessary(ChunkFactory::column_from_field(field.sub_field(0)));
            auto values = NullableColumn::wrap_if_necessary(ChunkFactory::column_from_field(field.sub_field(1)));
            auto offsets = get_column_ptr<UInt32Column>();
            auto map = MapColumn::create(std::move(keys), std::move(values), std::move(offsets));
            return NullableIfNeed(std::move(map));
        } else if constexpr (ftype == TYPE_STRUCT) {
            std::vector<std::string> names;
            MutableColumns fields;
            for (auto& sub_field : field.sub_fields()) {
                names.emplace_back(sub_field.name());
                fields.emplace_back(ChunkFactory::column_from_field(sub_field));
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
    return field_type_dispatch_column(field.type()->type(), ColumnPtrBuilder(), field, precision, scale);
}

Chunk* ChunkFactory::new_chunk_pooled(const Schema& schema, size_t chunk_size) {
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

MutableColumnPtr ChunkFactory::column_from_field_type(LogicalType type, bool nullable) {
    return field_type_dispatch_column(type, ColumnBuilder(), nullable);
}

MutableColumnPtr ChunkFactory::column_from_field(const Field& field) {
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
        return NullableIfNeed(
                ArrayColumn::create(ChunkFactory::column_from_field(field.sub_field(0)), UInt32Column::create()));
    }
    case TYPE_MAP:
        return NullableIfNeed(MapColumn::create(ChunkFactory::column_from_field(field.sub_field(0)),
                                                ChunkFactory::column_from_field(field.sub_field(1)),
                                                UInt32Column::create()));
    case TYPE_STRUCT: {
        std::vector<std::string> names;
        MutableColumns fields;
        for (auto& sub_field : field.sub_fields()) {
            names.emplace_back(sub_field.name());
            fields.emplace_back(ChunkFactory::column_from_field(sub_field));
        }
        auto struct_column = StructColumn::create(std::move(fields), std::move(names));
        return NullableIfNeed(std::move(struct_column));
    }
    default:
        return NullableIfNeed(ChunkFactory::column_from_field_type(type, false));
    }
}

ChunkUniquePtr ChunkFactory::new_chunk(const Schema& schema, size_t n) {
    size_t fields = schema.num_fields();
    Columns columns;
    columns.reserve(fields);
    for (size_t i = 0; i < fields; i++) {
        const FieldPtr& f = schema.field(i);
        auto col = ChunkFactory::column_from_field(*f);
        col->reserve(n);
        columns.emplace_back(std::move(col));
    }
    return std::make_unique<Chunk>(std::move(columns), std::make_shared<Schema>(schema));
}

MutableChunkPtr ChunkFactory::new_mutable_chunk(const Schema& schema, size_t n) {
    size_t fields = schema.num_fields();
    MutableColumns columns;
    columns.reserve(fields);
    for (size_t i = 0; i < fields; i++) {
        const FieldPtr& f = schema.field(i);
        auto col = ChunkFactory::column_from_field(*f);
        col->reserve(n);
        columns.emplace_back(std::move(col));
    }
    return std::make_shared<MutableChunk>(std::move(columns), std::make_shared<Schema>(schema));
}

} // namespace starrocks
