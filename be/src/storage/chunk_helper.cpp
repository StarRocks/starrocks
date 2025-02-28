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
            auto elements = NullableColumn::wrap_if_necessary(field.sub_field(0).create_column())->as_mutable_ptr();
            auto offsets = UInt32Column::create();
            auto array = ArrayColumn::create(std::move(elements), std::move(offsets));
            return NullableIfNeed(std::move(array));
        } else if constexpr (ftype == TYPE_MAP) {
            auto keys = NullableColumn::wrap_if_necessary(field.sub_field(0).create_column())->as_mutable_ptr();
            auto values = NullableColumn::wrap_if_necessary(field.sub_field(1).create_column())->as_mutable_ptr();
            auto offsets = get_column_ptr<UInt32Column>();
            auto map = MapColumn::create(std::move(keys), std::move(values), std::move(offsets));
            return NullableIfNeed(std::move(map));
        } else if constexpr (ftype == TYPE_STRUCT) {
            std::vector<std::string> names;
            MutableColumns fields;
            for (auto& sub_field : field.sub_fields()) {
                names.emplace_back(sub_field.name());
                fields.emplace_back(sub_field.create_column());
            }
            auto struct_column = StructColumn::create(std::move(fields), std::move(names));
            return NullableIfNeed(std::move(struct_column));
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

void ChunkHelper::padding_char_column(const starrocks::TabletSchemaCSPtr& tschema, const Field& field, Column* column) {
    size_t num_rows = column->size();
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
    uint32_t len = tschema->column(tschema->field_index(field.name())).length();

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

    if (field.is_nullable()) {
        auto* nullable_column = down_cast<NullableColumn*>(column);
        auto new_column = NullableColumn::create(std::move(new_binary), nullable_column->null_column());
        new_column->swap_column(*column);
    } else {
        new_binary->swap_column(*column);
    }
}

void ChunkHelper::padding_char_columns(const std::vector<size_t>& char_column_indexes, const Schema& schema,
                                       const starrocks::TabletSchemaCSPtr& tschema, Chunk* chunk) {
    for (auto field_index : char_column_indexes) {
        Column* column = chunk->get_column_by_index(field_index).get();
        padding_char_column(tschema, *schema.field(field_index), column);
    }
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
        } else if constexpr (ftype == TYPE_MAP) {
            CHECK(false) << "array not supported";
        } else if constexpr (ftype == TYPE_STRUCT) {
            CHECK(false) << "array not supported";
        } else {
            return NullableIfNeed(CppColumnTraits<ftype>::ColumnType::create());
        }
    }
};

MutableColumnPtr ChunkHelper::column_from_field_type(LogicalType type, bool nullable) {
    return field_type_dispatch_column(type, ColumnBuilder(), nullable);
}

MutableColumnPtr ChunkHelper::column_from_field(const Field& field) {
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
            fields.emplace_back(sub_field.create_column());
        }
        auto struct_column = StructColumn::create(std::move(fields), std::move(names));
        return NullableIfNeed(std::move(struct_column));
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
        chunk->append_column(std::move(column), slot->id());
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

ChunkAccumulator::ChunkAccumulator(size_t desired_size) : _desired_size(desired_size) {}

void ChunkAccumulator::set_desired_size(size_t desired_size) {
    _desired_size = desired_size;
}

void ChunkAccumulator::reset() {
    _output.clear();
    _tmp_chunk.reset();
    _accumulate_count = 0;
}

Status ChunkAccumulator::push(ChunkPtr&& chunk) {
    size_t input_rows = chunk->num_rows();
    // TODO: optimize for zero-copy scenario
    // Cut the input chunk into pieces if larger than desired
    for (size_t start = 0; start < input_rows;) {
        size_t remain_rows = input_rows - start;
        size_t need_rows = 0;
        if (_tmp_chunk) {
            need_rows = std::min(_desired_size - _tmp_chunk->num_rows(), remain_rows);
            TRY_CATCH_BAD_ALLOC(_tmp_chunk->append(*chunk, start, need_rows));
        } else {
            need_rows = std::min(_desired_size, remain_rows);
            _tmp_chunk = chunk->clone_empty(_desired_size);
            TRY_CATCH_BAD_ALLOC(_tmp_chunk->append(*chunk, start, need_rows));
        }

        if (_tmp_chunk->num_rows() >= _desired_size) {
            _output.emplace_back(std::move(_tmp_chunk));
        }
        start += need_rows;
    }
    _accumulate_count++;
    return Status::OK();
}

bool ChunkAccumulator::empty() const {
    return _output.empty();
}

bool ChunkAccumulator::reach_limit() const {
    return _accumulate_count >= kAccumulateLimit;
}

ChunkPtr ChunkAccumulator::pull() {
    if (!_output.empty()) {
        auto res = std::move(_output.front());
        _output.pop_front();
        _accumulate_count = 0;
        return res;
    }
    return nullptr;
}

void ChunkAccumulator::finalize() {
    if (_tmp_chunk) {
        _output.emplace_back(std::move(_tmp_chunk));
    }
    _accumulate_count = 0;
}

bool ChunkPipelineAccumulator::_check_json_schema_equallity(const Chunk* one, const Chunk* two) {
    if (one->num_columns() != two->num_columns()) {
        return false;
    }

    for (size_t i = 0; i < one->num_columns(); i++) {
        auto& c1 = one->get_column_by_index(i);
        auto& c2 = two->get_column_by_index(i);
        const auto* a1 = ColumnHelper::get_data_column(c1.get());
        const auto* a2 = ColumnHelper::get_data_column(c2.get());

        if (a1->is_json() && a2->is_json()) {
            auto json1 = down_cast<const JsonColumn*>(a1);
            if (!json1->is_equallity_schema(a2)) {
                return false;
            }
        } else if (a1->is_json() || a2->is_json()) {
            // never hit
            DCHECK_EQ(a1->is_json(), a2->is_json());
            return false;
        }
    }

    return true;
}

void ChunkPipelineAccumulator::push(const ChunkPtr& chunk) {
    chunk->check_or_die();
    DCHECK(_out_chunk == nullptr);
    if (_in_chunk == nullptr) {
        _in_chunk = chunk;
        _mem_usage = chunk->bytes_usage();
    } else if (_in_chunk->num_rows() + chunk->num_rows() > _max_size ||
               _in_chunk->owner_info() != chunk->owner_info() || _in_chunk->owner_info().is_last_chunk() ||
               !_check_json_schema_equallity(chunk.get(), _in_chunk.get())) {
        _out_chunk = std::move(_in_chunk);
        _in_chunk = chunk;
        _mem_usage = chunk->bytes_usage();
    } else {
        _in_chunk->append(*chunk);
        _mem_usage += chunk->bytes_usage();
    }

    if (_out_chunk == nullptr && (_in_chunk->num_rows() >= _max_size * LOW_WATERMARK_ROWS_RATE ||
                                  _mem_usage >= LOW_WATERMARK_BYTES || _in_chunk->owner_info().is_last_chunk())) {
        _out_chunk = std::move(_in_chunk);
        _mem_usage = 0;
    }
}

void ChunkPipelineAccumulator::reset() {
    _in_chunk.reset();
    _out_chunk.reset();
    _mem_usage = 0;
}

void ChunkPipelineAccumulator::finalize() {
    _finalized = true;
    _mem_usage = 0;
}

void ChunkPipelineAccumulator::reset_state() {
    reset();
    _finalized = false;
}

ChunkPtr& ChunkPipelineAccumulator::pull() {
    if (_finalized && _out_chunk == nullptr) {
        return _in_chunk;
    }
    return _out_chunk;
}

bool ChunkPipelineAccumulator::has_output() const {
    return _out_chunk != nullptr || (_finalized && _in_chunk != nullptr);
}

bool ChunkPipelineAccumulator::need_input() const {
    return !_finalized && _out_chunk == nullptr;
}

bool ChunkPipelineAccumulator::is_finished() const {
    return _finalized && _out_chunk == nullptr && _in_chunk == nullptr;
}

template <class ColumnT>
inline constexpr bool is_object = std::is_same_v<ColumnT, ArrayColumn> || std::is_same_v<ColumnT, StructColumn> ||
                                  std::is_same_v<ColumnT, MapColumn> || std::is_same_v<ColumnT, JsonColumn> ||
                                  std::is_same_v<ObjectColumn<typename ColumnT::ValueType>, ColumnT>;

// Selective-copy data from SegmentedColumn according to provided index
class SegmentedColumnSelectiveCopy final : public ColumnVisitorAdapter<SegmentedColumnSelectiveCopy> {
public:
    SegmentedColumnSelectiveCopy(SegmentedColumnPtr segment_column, const uint32_t* indexes, uint32_t from,
                                 uint32_t size)
            : ColumnVisitorAdapter(this),
              _segment_column(std::move(segment_column)),
              _indexes(indexes),
              _from(from),
              _size(size) {}

    template <class T>
    Status do_visit(const FixedLengthColumnBase<T>& column) {
        using ColumnT = FixedLengthColumn<T>;
        using ContainerT = typename ColumnT::Container;

        _result = column.clone_empty();
        auto output = ColumnHelper::as_column<ColumnT>(_result);
        const size_t segment_size = _segment_column->segment_size();

        std::vector<const ContainerT*> buffers;
        auto columns = _segment_column->columns();
        for (auto& seg_column : columns) {
            buffers.push_back(&(ColumnHelper::as_column<ColumnT>(seg_column)->get_data()));
        }

        ContainerT& output_items = output->get_data();
        output_items.resize(_size);
        size_t from = _from;
        for (size_t i = 0; i < _size; i++) {
            size_t idx = _indexes[from + i];
            auto [segment_id, segment_offset] = _segment_address(idx, segment_size);
            DCHECK_LT(segment_id, columns.size());
            DCHECK_LT(segment_offset, columns[segment_id]->size());

            output_items[i] = (*buffers[segment_id])[segment_offset];
        }
        return {};
    }

    // Implementation refers to BinaryColumn::append_selective
    template <class Offset>
    Status do_visit(const BinaryColumnBase<Offset>& column) {
        using ColumnT = BinaryColumnBase<Offset>;
        using ContainerT = typename ColumnT::Container*;
        using Bytes = typename ColumnT::Bytes;
        using Byte = typename ColumnT::Byte;
        using Offsets = typename ColumnT::Offsets;

        _result = column.clone_empty();
        auto output = ColumnHelper::as_column<ColumnT>(_result);
        auto& output_offsets = output->get_offset();
        auto& output_bytes = output->get_bytes();
        const size_t segment_size = _segment_column->segment_size();

        // input
        auto columns = _segment_column->columns();
        std::vector<Bytes*> input_bytes;
        std::vector<Offsets*> input_offsets;
        for (auto& seg_column : columns) {
            input_bytes.push_back(&ColumnHelper::as_column<ColumnT>(seg_column)->get_bytes());
            input_offsets.push_back(&ColumnHelper::as_column<ColumnT>(seg_column)->get_offset());
        }

#ifndef NDEBUG
        for (auto& src_col : columns) {
            src_col->check_or_die();
        }
#endif

        // assign offsets
        output_offsets.resize(_size + 1);
        size_t num_bytes = 0;
        size_t from = _from;
        for (size_t i = 0; i < _size; i++) {
            size_t idx = _indexes[from + i];
            auto [segment_id, segment_offset] = _segment_address(idx, segment_size);
            DCHECK_LT(segment_id, columns.size());
            DCHECK_LT(segment_offset, columns[segment_id]->size());

            Offsets& src_offsets = *input_offsets[segment_id];
            Offset str_size = src_offsets[segment_offset + 1] - src_offsets[segment_offset];

            output_offsets[i + 1] = output_offsets[i] + str_size;
            num_bytes += str_size;
        }
        output_bytes.resize(num_bytes);

        // copy bytes
        Byte* dest_bytes = output_bytes.data();
        for (size_t i = 0; i < _size; i++) {
            size_t idx = _indexes[from + i];
            auto [segment_id, segment_offset] = _segment_address(idx, segment_size);
            Bytes& src_bytes = *input_bytes[segment_id];
            Offsets& src_offsets = *input_offsets[segment_id];
            Offset str_size = src_offsets[segment_offset + 1] - src_offsets[segment_offset];
            Byte* str_data = src_bytes.data() + src_offsets[segment_offset];

            strings::memcpy_inlined(dest_bytes + output_offsets[i], str_data, str_size);
        }

#ifndef NDEBUG
        output->check_or_die();
#endif

        return {};
    }

    // Inefficient fallback implementation, it's usually used for Array/Struct/Map/Json
    template <class ColumnT>
    typename std::enable_if_t<is_object<ColumnT>, Status> do_visit(const ColumnT& column) {
        _result = column.clone_empty();
        auto output = ColumnHelper::as_column<ColumnT>(_result);
        const size_t segment_size = _segment_column->segment_size();
        output->reserve(_size);

        auto columns = _segment_column->columns();
        size_t from = _from;
        for (size_t i = 0; i < _size; i++) {
            size_t idx = _indexes[from + i];
            auto [segment_id, segment_offset] = _segment_address(idx, segment_size);
            output->append(*columns[segment_id], segment_offset, 1);
        }
        return {};
    }

    Status do_visit(const NullableColumn& column) {
        Columns data_columns, null_columns;
        for (auto& column : _segment_column->columns()) {
            NullableColumn::Ptr nullable = ColumnHelper::as_column<NullableColumn>(column);
            data_columns.push_back(nullable->data_column());
            null_columns.push_back(nullable->null_column());
        }

        auto segmented_data_column = std::make_shared<SegmentedColumn>(data_columns, _segment_column->segment_size());
        SegmentedColumnSelectiveCopy copy_data(segmented_data_column, _indexes, _from, _size);
        (void)data_columns[0]->accept(&copy_data);
        auto segmented_null_column = std::make_shared<SegmentedColumn>(null_columns, _segment_column->segment_size());
        SegmentedColumnSelectiveCopy copy_null(segmented_null_column, _indexes, _from, _size);
        (void)null_columns[0]->accept(&copy_null);
        _result = NullableColumn::create(copy_data.result(), ColumnHelper::as_column<NullColumn>(copy_null.result()));

        return {};
    }

    Status do_visit(const ConstColumn& column) { return Status::NotSupported("SegmentedColumnVisitor"); }

    ColumnPtr result() { return _result; }

private:
    __attribute__((always_inline)) std::pair<size_t, size_t> _segment_address(size_t idx, size_t segment_size) {
        size_t segment_id = idx / segment_size;
        size_t segment_offset = idx % segment_size;
        return {segment_id, segment_offset};
    }

    SegmentedColumnPtr _segment_column;
    ColumnPtr _result;
    const uint32_t* _indexes;
    uint32_t _from;
    uint32_t _size;
};

SegmentedColumn::SegmentedColumn(const SegmentedChunkPtr& chunk, size_t column_index)
        : _chunk(chunk), _column_index(column_index), _segment_size(chunk->segment_size()) {}

SegmentedColumn::SegmentedColumn(Columns columns, size_t segment_size)
        : _segment_size(segment_size), _cached_columns(std::move(columns)) {}

ColumnPtr SegmentedColumn::clone_selective(const uint32_t* indexes, uint32_t from, uint32_t size) {
    if (num_segments() == 1) {
        auto first = columns()[0];
        auto result = first->clone_empty();
        result->append_selective(*first, indexes, from, size);
        return result;
    } else {
        SegmentedColumnSelectiveCopy visitor(shared_from_this(), indexes, from, size);
        (void)columns()[0]->accept(&visitor);
        return visitor.result();
    }
}

ColumnPtr SegmentedColumn::materialize() const {
    auto actual_columns = columns();
    if (actual_columns.empty()) {
        return {};
    }
    ColumnPtr result = actual_columns[0]->clone_empty();
    for (size_t i = 0; i < actual_columns.size(); i++) {
        result->append(*actual_columns[i]);
    }
    return result;
}

size_t SegmentedColumn::segment_size() const {
    return _segment_size;
}

size_t SegmentedColumn::num_segments() const {
    return _chunk.lock()->num_segments();
}

size_t SegmentedChunk::segment_size() const {
    return _segment_size;
}

bool SegmentedColumn::is_nullable() const {
    return columns()[0]->is_nullable();
}

bool SegmentedColumn::has_null() const {
    for (auto& column : columns()) {
        RETURN_IF(column->has_null(), true);
    }
    return false;
}

size_t SegmentedColumn::size() const {
    size_t result = 0;
    for (auto& column : columns()) {
        result += column->size();
    }
    return result;
}

Columns SegmentedColumn::columns() const {
    if (!_cached_columns.empty()) {
        return _cached_columns;
    }
    Columns columns;
    for (auto& segment : _chunk.lock()->segments()) {
        columns.push_back(segment->get_column_by_index(_column_index));
    }
    return columns;
}

void SegmentedColumn::upgrade_to_nullable() {
    for (auto& segment : _chunk.lock()->segments()) {
        auto& column = segment->get_column_by_index(_column_index);
        column = NullableColumn::wrap_if_necessary(column);
    }
}

SegmentedChunk::SegmentedChunk(size_t segment_size) : _segment_size(segment_size) {
    // put at least one chunk there
    _segments.resize(1);
    _segments[0] = std::make_shared<Chunk>();
}

SegmentedChunkPtr SegmentedChunk::create(size_t segment_size) {
    return std::make_shared<SegmentedChunk>(segment_size);
}

void SegmentedChunk::append_column(ColumnPtr column, SlotId slot_id) {
    // It's only used when initializing the chunk, so append the column to first chunk is enough
    DCHECK_EQ(_segments.size(), 1);
    _segments[0]->append_column(std::move(column), slot_id);
}

void SegmentedChunk::append_chunk(const ChunkPtr& chunk, const std::vector<SlotId>& slots) {
    ChunkPtr open_segment = _segments.back();
    size_t append_rows = chunk->num_rows();
    size_t append_index = 0;
    while (append_rows > 0) {
        size_t open_segment_append_rows = std::min(_segment_size - open_segment->num_rows(), append_rows);
        for (int i = 0; i < slots.size(); i++) {
            SlotId slot = slots[i];
            ColumnPtr column = chunk->get_column_by_slot_id(slot);
            open_segment->columns()[i]->append(*column, append_index, open_segment_append_rows);
        }
        append_index += open_segment_append_rows;
        append_rows -= open_segment_append_rows;
        if (open_segment->num_rows() == _segment_size) {
            open_segment->check_or_die();
            open_segment = open_segment->clone_empty();
            _segments.emplace_back(open_segment);
        }
    }
}

void SegmentedChunk::append_chunk(const ChunkPtr& chunk) {
    ChunkPtr open_segment = _segments.back();
    size_t append_rows = chunk->num_rows();
    size_t append_index = 0;
    while (append_rows > 0) {
        size_t open_segment_append_rows = std::min(_segment_size - open_segment->num_rows(), append_rows);
        open_segment->append_safe(*chunk, append_index, open_segment_append_rows);
        append_index += open_segment_append_rows;
        append_rows -= open_segment_append_rows;
        if (open_segment->num_rows() == _segment_size) {
            open_segment->check_or_die();
            open_segment = open_segment->clone_empty();
            _segments.emplace_back(open_segment);
        }
    }
}

void SegmentedChunk::append(const SegmentedChunkPtr& chunk, size_t offset) {
    auto& input_segments = chunk->segments();
    size_t segment_index = offset / chunk->_segment_size;
    size_t segment_offset = offset % chunk->_segment_size;
    for (size_t i = segment_index; i < chunk->num_segments(); i++) {
        // The segment need to cutoff
        if (i == segment_index && segment_offset > 0) {
            auto cutoff = input_segments[i]->clone_empty();
            size_t count = input_segments[i]->num_rows() - segment_offset;
            cutoff->append(*input_segments[i], segment_offset, count);
            append_chunk(std::move(cutoff));
        } else {
            append_chunk(input_segments[i]);
        }
    }
    for (auto& segment : _segments) {
        segment->check_or_die();
    }
}

void SegmentedChunk::build_columns() {
    DCHECK(_segments.size() >= 1);
    size_t num_columns = _segments[0]->num_columns();
    for (int i = 0; i < num_columns; i++) {
        _columns.emplace_back(std::make_shared<SegmentedColumn>(shared_from_this(), i));
    }
}

size_t SegmentedChunk::memory_usage() const {
    size_t result = 0;
    for (auto& chunk : _segments) {
        result += chunk->memory_usage();
    }
    return result;
}

size_t SegmentedChunk::num_rows() const {
    size_t result = 0;
    for (auto& chunk : _segments) {
        result += chunk->num_rows();
    }
    return result;
}

SegmentedColumnPtr SegmentedChunk::get_column_by_slot_id(SlotId slot_id) {
    DCHECK(!!_segments[0]);
    auto& map = _segments[0]->get_slot_id_to_index_map();
    auto iter = map.find(slot_id);
    if (iter == map.end()) {
        return nullptr;
    }
    return _columns[iter->second];
}

const SegmentedColumns& SegmentedChunk::columns() const {
    return _columns;
}

SegmentedColumns& SegmentedChunk::columns() {
    return _columns;
}

Status SegmentedChunk::upgrade_if_overflow() {
    for (auto& chunk : _segments) {
        RETURN_IF_ERROR(chunk->upgrade_if_overflow());
    }
    return {};
}

Status SegmentedChunk::downgrade() {
    for (auto& chunk : _segments) {
        RETURN_IF_ERROR(chunk->downgrade());
    }
    return {};
}

bool SegmentedChunk::has_large_column() const {
    for (auto& chunk : _segments) {
        if (chunk->has_large_column()) {
            return true;
        }
    }
    return false;
}

size_t SegmentedChunk::num_segments() const {
    return _segments.size();
}

const std::vector<ChunkPtr>& SegmentedChunk::segments() const {
    return _segments;
}
std::vector<ChunkPtr>& SegmentedChunk::segments() {
    return _segments;
}

ChunkUniquePtr SegmentedChunk::clone_empty(size_t reserve) {
    return _segments[0]->clone_empty(reserve);
}

void SegmentedChunk::reset() {
    for (auto& chunk : _segments) {
        chunk->reset();
    }
}

void SegmentedChunk::check_or_die() {
    for (auto& chunk : _segments) {
        chunk->check_or_die();
    }
}

} // namespace starrocks
