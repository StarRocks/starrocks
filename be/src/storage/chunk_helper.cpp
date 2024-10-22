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

#include "storage/chunk_helper.h"

#include <numeric>
#include <utility>

#include "column/array_column.h"
#include "column/chunk.h"
#include "column/column_helper.h"
#include "column/json_column.h"
#include "column/map_column.h"
#include "column/schema.h"
#include "column/struct_column.h"
#include "column/type_traits.h"
#include "column/vectorized_fwd.h"
#include "gutil/strings/fastmem.h"
#include "runtime/current_thread.h"
#include "runtime/descriptors.h"
#include "simd/simd.h"
#include "storage/olap_type_infra.h"
#include "storage/tablet_schema.h"
#include "storage/type_traits.h"
#include "storage/type_utils.h"
#include "storage/types.h"
#include "util/metrics.h"
#include "util/percentile_value.h"

namespace starrocks {

// NOTE(zc): now CppColumnTraits is only used for this class, so I move it here.
// Someday if it is used by others, please move it into a single file.
// CppColumnTraits
// Infer ColumnType from LogicalType
template <LogicalType ftype>
struct CppColumnTraits {
    using CppType = typename CppTypeTraits<ftype>::CppType;
    using ColumnType = typename ColumnTraits<CppType>::ColumnType;
};

template <>
struct CppColumnTraits<TYPE_BOOLEAN> {
    using ColumnType = UInt8Column;
};

// deprecated
template <>
struct CppColumnTraits<TYPE_DATE_V1> {
    using ColumnType = FixedLengthColumn<uint24_t>;
};

template <>
struct CppColumnTraits<TYPE_DATE> {
    using ColumnType = DateColumn;
};

template <>
struct CppColumnTraits<TYPE_DATETIME> {
    using ColumnType = TimestampColumn;
};

// deprecated
template <>
struct CppColumnTraits<TYPE_DECIMAL> {
    using ColumnType = FixedLengthColumn<decimal12_t>;
};

template <>
struct CppColumnTraits<TYPE_HLL> {
    using ColumnType = HyperLogLogColumn;
};

template <>
struct CppColumnTraits<TYPE_PERCENTILE> {
    using ColumnType = PercentileColumn;
};

template <>
struct CppColumnTraits<TYPE_OBJECT> {
    using ColumnType = BitmapColumn;
};

template <>
struct CppColumnTraits<TYPE_UNSIGNED_INT> {
    using ColumnType = UInt32Column;
};

template <>
struct CppColumnTraits<TYPE_JSON> {
    using ColumnType = JsonColumn;
};

template <>
struct CppColumnTraits<TYPE_VARBINARY> {
    using ColumnType = BinaryColumn;
};

Field ChunkHelper::convert_field(ColumnId id, const TabletColumn& c) {
    LogicalType type = c.type();

    TypeInfoPtr type_info = nullptr;
    if (type == TYPE_ARRAY || type == TYPE_MAP || type == TYPE_STRUCT || type == TYPE_DECIMAL32 ||
        type == TYPE_DECIMAL64 || type == TYPE_DECIMAL128) {
        // ARRAY and DECIMAL should be handled specially
        // Array is nested type, the message is stored in TabletColumn
        // Decimal has precision and scale, the message is stored in TabletColumn
        type_info = get_type_info(c);
    } else {
        type_info = get_type_info(type);
    }
    starrocks::Field f(id, std::string(c.name()), type_info, c.is_nullable());
    f.set_is_key(c.is_key());
    f.set_length(c.length());
    f.set_uid(c.unique_id());

    if (type == TYPE_ARRAY) {
        const TabletColumn& sub_column = c.subcolumn(0);
        auto sub_field = convert_field(id, sub_column);
        f.add_sub_field(sub_field);
    } else if (type == TYPE_MAP) {
        for (int i = 0; i < 2; ++i) {
            const TabletColumn& sub_column = c.subcolumn(i);
            auto sub_field = convert_field(id, sub_column);
            f.add_sub_field(sub_field);
        }
    } else if (type == TYPE_STRUCT) {
        for (int i = 0; i < c.subcolumn_count(); ++i) {
            const TabletColumn& sub_column = c.subcolumn(i);
            auto sub_field = convert_field(id, sub_column);
            f.add_sub_field(sub_field);
        }
    }

    f.set_short_key_length(c.index_length());
    f.set_aggregate_method(c.aggregation());
    f.set_agg_state_desc(c.get_agg_state_desc());
    return f;
}

starrocks::Schema ChunkHelper::convert_schema(const starrocks::TabletSchemaCSPtr& schema) {
    return starrocks::Schema(schema->schema());
}

starrocks::Schema ChunkHelper::convert_schema(const starrocks::TabletSchemaCSPtr& schema,
                                              const std::vector<ColumnId>& cids) {
    return starrocks::Schema(schema->schema(), cids);
}

starrocks::SchemaPtr ChunkHelper::convert_schema(const std::vector<TabletColumn*>& columns,
                                                 const std::vector<std::string_view>& col_names) {
    SchemaPtr schema = std::make_shared<Schema>();
    // ordered by col_names
    int new_column_idx = 0;
    for (auto s : col_names) {
        for (int32_t idx = 0; idx < columns.size(); ++idx) {
            if (!s.compare(columns[idx]->name())) {
                auto f = std::make_shared<Field>(ChunkHelper::convert_field(new_column_idx++, *columns[idx]));
                schema->append(f);
            }
        }
    }
    return schema->fields().size() != 0 ? schema : nullptr;
}

starrocks::Schema ChunkHelper::get_short_key_schema(const starrocks::TabletSchemaCSPtr& schema) {
    std::vector<ColumnId> short_key_cids;
    const auto& sort_key_idxes = schema->sort_key_idxes();
    short_key_cids.reserve(schema->num_short_key_columns());
    for (auto i = 0; i < schema->num_short_key_columns(); ++i) {
        short_key_cids.push_back(sort_key_idxes[i]);
    }
    return starrocks::Schema(schema->schema(), short_key_cids);
}

starrocks::Schema ChunkHelper::get_sort_key_schema(const starrocks::TabletSchemaCSPtr& schema) {
    std::vector<ColumnId> sort_key_iota_idxes(schema->sort_key_idxes().size());
    std::iota(sort_key_iota_idxes.begin(), sort_key_iota_idxes.end(), 0);
    return starrocks::Schema(schema->schema(), schema->sort_key_idxes(), sort_key_iota_idxes);
}

starrocks::Schema ChunkHelper::get_sort_key_schema_by_primary_key(const starrocks::TabletSchemaCSPtr& tablet_schema) {
    std::vector<ColumnId> primary_key_iota_idxes(tablet_schema->num_key_columns());
    std::iota(primary_key_iota_idxes.begin(), primary_key_iota_idxes.end(), 0);
    std::vector<ColumnId> all_keys_iota_idxes(tablet_schema->num_columns());
    std::iota(all_keys_iota_idxes.begin(), all_keys_iota_idxes.end(), 0);
    return starrocks::Schema(tablet_schema->schema(), all_keys_iota_idxes, primary_key_iota_idxes);
}

starrocks::SchemaPtr ChunkHelper::get_non_nullable_schema(const starrocks::SchemaPtr& schema,
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

ColumnId ChunkHelper::max_column_id(const starrocks::Schema& schema) {
    ColumnId id = 0;
    for (const auto& field : schema.fields()) {
        id = std::max(id, field->id());
    }
    return id;
}

template <typename T>
inline std::shared_ptr<T> get_column_ptr() {
    return std::make_shared<T>();
}

template <typename T>
inline std::shared_ptr<DecimalColumnType<T>> get_decimal_column_ptr(int precision, int scale) {
    auto column = get_column_ptr<T>();
    column->set_precision(precision);
    column->set_scale(scale);
    return column;
}

struct ColumnPtrBuilder {
    template <LogicalType ftype>
    ColumnPtr operator()(const Field& field, int precision, int scale) {
        auto NullableIfNeed = [&](ColumnPtr c) -> ColumnPtr {
            return field.is_nullable() ? NullableColumn::create(std::move(c), get_column_ptr<NullColumn>()) : c;
        };

        if constexpr (ftype == TYPE_ARRAY) {
            auto elements = NullableColumn::wrap_if_necessary(field.sub_field(0).create_column());
            auto offsets = get_column_ptr<UInt32Column>();
            auto array = ArrayColumn::create(std::move(elements), offsets);
            return NullableIfNeed(array);
        } else if constexpr (ftype == TYPE_MAP) {
            auto keys = NullableColumn::wrap_if_necessary(field.sub_field(0).create_column());
            auto values = NullableColumn::wrap_if_necessary(field.sub_field(1).create_column());
            auto offsets = get_column_ptr<UInt32Column>();
            auto map = MapColumn::create(std::move(keys), std::move(values), offsets);
            return NullableIfNeed(map);
        } else if constexpr (ftype == TYPE_STRUCT) {
            std::vector<std::string> names;
            std::vector<ColumnPtr> fields;
            for (auto& sub_field : field.sub_fields()) {
                names.emplace_back(sub_field.name());
                fields.emplace_back(sub_field.create_column());
            }
            auto struct_column = StructColumn::create(std::move(fields), std::move(names));
            return NullableIfNeed(struct_column);
        } else {
            switch (ftype) {
            case TYPE_DECIMAL32:
                return NullableIfNeed(get_decimal_column_ptr<Decimal32Column>(precision, scale));
            case TYPE_DECIMAL64:
                return NullableIfNeed(get_decimal_column_ptr<Decimal64Column>(precision, scale));
            case TYPE_DECIMAL128:
                return NullableIfNeed(get_decimal_column_ptr<Decimal128Column>(precision, scale));
            default: {
                return NullableIfNeed(get_column_ptr<typename CppColumnTraits<ftype>::ColumnType>());
            }
            }
        }
    }
};

ColumnPtr column_from_pool(const Field& field) {
    auto precision = field.type()->precision();
    auto scale = field.type()->scale();
    return field_type_dispatch_column(field.type()->type(), ColumnPtrBuilder(), field, precision, scale);
}

Chunk* ChunkHelper::new_chunk_pooled(const Schema& schema, size_t chunk_size) {
    Columns columns;
    columns.reserve(schema.num_fields());
    for (size_t i = 0; i < schema.num_fields(); i++) {
        const FieldPtr& f = schema.field(i);
        auto column = column_from_pool(*f);
        column->reserve(chunk_size);
        columns.emplace_back(std::move(column));
    }
    return new Chunk(std::move(columns), std::make_shared<Schema>(schema));
}

std::vector<size_t> ChunkHelper::get_char_field_indexes(const Schema& schema) {
    std::vector<size_t> char_field_indexes;
    for (size_t i = 0; i < schema.num_fields(); ++i) {
        const auto& field = schema.field(i);
        if (field->type()->type() == TYPE_CHAR) {
            char_field_indexes.push_back(i);
        }
    }
    return char_field_indexes;
}

void ChunkHelper::padding_char_columns(const std::vector<size_t>& char_column_indexes, const Schema& schema,
                                       const starrocks::TabletSchemaCSPtr& tschema, Chunk* chunk) {
    size_t num_rows = chunk->num_rows();
    for (auto field_index : char_column_indexes) {
        Column* column = chunk->get_column_by_index(field_index).get();
        Column* data_column = ColumnHelper::get_data_column(column);
        auto* binary = down_cast<BinaryColumn*>(data_column);

        Offsets& offset = binary->get_offset();
        Bytes& bytes = binary->get_bytes();

        // Padding 0 to CHAR field, the storage bitmap index and zone map need it.
        // TODO(kks): we could improve this if there are many null valus
        auto new_binary = BinaryColumn::create();
        Offsets& new_offset = new_binary->get_offset();
        Bytes& new_bytes = new_binary->get_bytes();

        // |schema| maybe partial columns in vertical compaction, so get char column length by name.
        uint32_t len = tschema->column(tschema->field_index(schema.field(field_index)->name())).length();

        new_offset.resize(num_rows + 1);
        new_bytes.assign(num_rows * len, 0); // padding 0

        uint32_t from = 0;
        for (size_t j = 0; j < num_rows; ++j) {
            uint32_t copy_data_len = std::min(len, offset[j + 1] - offset[j]);
            strings::memcpy_inlined(new_bytes.data() + from, bytes.data() + offset[j], copy_data_len);
            from += len; // no copy data will be 0
        }

        for (size_t j = 1; j <= num_rows; ++j) {
            new_offset[j] = static_cast<uint32_t>(len * j);
        }

        const auto& field = schema.field(field_index);

        if (field->is_nullable()) {
            auto* nullable_column = down_cast<NullableColumn*>(column);
            auto new_column = NullableColumn::create(new_binary, nullable_column->null_column());
            new_column->swap_column(*column);
        } else {
            new_binary->swap_column(*column);
        }
    }
}

struct ColumnBuilder {
    template <LogicalType ftype>
    ColumnPtr operator()(bool nullable) {
        [[maybe_unused]] auto NullableIfNeed = [&](ColumnPtr col) -> ColumnPtr {
            return nullable ? NullableColumn::create(std::move(col), NullColumn::create()) : col;
        };

        if constexpr (ftype == TYPE_ARRAY) {
            CHECK(false) << "array not supported";
        } else if constexpr (ftype == TYPE_MAP) {
            CHECK(false) << "array not supported";
        } else if constexpr (ftype == TYPE_STRUCT) {
            CHECK(false) << "array not supported";
        } else {
            return NullableIfNeed(CppColumnTraits<ftype>::ColumnType::create());
        }
    }
};

ColumnPtr ChunkHelper::column_from_field_type(LogicalType type, bool nullable) {
    return field_type_dispatch_column(type, ColumnBuilder(), nullable);
}

ColumnPtr ChunkHelper::column_from_field(const Field& field) {
    auto NullableIfNeed = [&](ColumnPtr col) -> ColumnPtr {
        return field.is_nullable() ? NullableColumn::create(std::move(col), NullColumn::create()) : col;
    };

    auto type = field.type()->type();
    switch (type) {
    case TYPE_DECIMAL32:
        return NullableIfNeed(Decimal32Column::create(field.type()->precision(), field.type()->scale()));
    case TYPE_DECIMAL64:
        return NullableIfNeed(Decimal64Column::create(field.type()->precision(), field.type()->scale()));
    case TYPE_DECIMAL128:
        return NullableIfNeed(Decimal128Column::create(field.type()->precision(), field.type()->scale()));
    case TYPE_ARRAY: {
        return NullableIfNeed(ArrayColumn::create(column_from_field(field.sub_field(0)), UInt32Column::create()));
    }
    case TYPE_MAP:
        return NullableIfNeed(MapColumn::create(column_from_field(field.sub_field(0)),
                                                column_from_field(field.sub_field(1)), UInt32Column::create()));
    case TYPE_STRUCT: {
        std::vector<std::string> names;
        std::vector<ColumnPtr> fields;
        for (auto& sub_field : field.sub_fields()) {
            names.emplace_back(sub_field.name());
            fields.emplace_back(sub_field.create_column());
        }
        auto struct_column = StructColumn::create(std::move(fields), std::move(names));
        return NullableIfNeed(struct_column);
    }
    default:
        return NullableIfNeed(column_from_field_type(type, false));
    }
}

ChunkUniquePtr ChunkHelper::new_chunk(const Schema& schema, size_t n) {
    size_t fields = schema.num_fields();
    Columns columns;
    columns.reserve(fields);
    for (size_t i = 0; i < fields; i++) {
        const FieldPtr& f = schema.field(i);
        columns.emplace_back(column_from_field(*f));
        columns.back()->reserve(n);
    }
    return std::make_unique<Chunk>(std::move(columns), std::make_shared<Schema>(schema));
}

ChunkUniquePtr ChunkHelper::new_chunk(const TupleDescriptor& tuple_desc, size_t n) {
    return new_chunk(tuple_desc.slots(), n);
}

ChunkUniquePtr ChunkHelper::new_chunk(const std::vector<SlotDescriptor*>& slots, size_t n) {
    auto chunk = std::make_unique<Chunk>();
    for (const auto slot : slots) {
        auto column = ColumnHelper::create_column(slot->type(), slot->is_nullable());
        column->reserve(n);
        chunk->append_column(column, slot->id());
    }
    return chunk;
}

void ChunkHelper::reorder_chunk(const TupleDescriptor& tuple_desc, Chunk* chunk) {
    return reorder_chunk(tuple_desc.slots(), chunk);
}

void ChunkHelper::reorder_chunk(const std::vector<SlotDescriptor*>& slots, Chunk* chunk) {
    auto reordered_chunk = Chunk();
    auto& original_chunk = (*chunk);
    for (auto slot : slots) {
        auto slot_id = slot->id();
        reordered_chunk.append_column(original_chunk.get_column_by_slot_id(slot_id), slot_id);
    }
    original_chunk.swap_chunk(reordered_chunk);
}

ChunkPtr ChunkHelper::createDummyChunk() {
    ChunkPtr dummyChunk = std::make_shared<Chunk>();
    auto col = ColumnHelper::create_const_column<TYPE_INT>(1, 1);
    dummyChunk->append_column(std::move(col), 0);
    return dummyChunk;
}

} // namespace starrocks
