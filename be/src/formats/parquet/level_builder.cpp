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

#include "formats/parquet/level_builder.h"

#include <parquet/arrow/writer.h>

#include <utility>

#include "column/array_column.h"
#include "column/column_helper.h"
#include "column/map_column.h"
#include "column/struct_column.h"
#include "column/vectorized_fwd.h"
#include "common/logging.h"
#include "exprs/expr.h"
#include "gutil/casts.h"
#include "gutil/endian.h"
#include "util/defer_op.h"

namespace starrocks::parquet {

inline uint8_t* get_raw_null_column(const ColumnPtr& col) {
    if (!col->has_null()) {
        return nullptr;
    }
    auto& null_column = down_cast<NullableColumn*>(col.get())->null_column();
    auto* raw_column = null_column->get_data().data();
    return raw_column;
}

template <LogicalType lt>
inline RunTimeCppType<lt>* get_raw_data_column(const ColumnPtr& col) {
    auto* data_column = ColumnHelper::get_data_column(col.get());
    auto* raw_column = down_cast<RunTimeColumnType<lt>*>(data_column)->get_data().data();
    return raw_column;
}

LevelBuilder::LevelBuilder(TypeDescriptor type_desc, ::parquet::schema::NodePtr root)
        : _type_desc(std::move(type_desc)), _root(std::move(root)) {}

void LevelBuilder::write(const LevelBuilderContext& ctx, const ColumnPtr& col,
                         const CallbackFunction& write_leaf_callback) {
    _write_column_chunk(ctx, _type_desc, _root, col, write_leaf_callback);
}

void LevelBuilder::_write_column_chunk(const LevelBuilderContext& ctx, const TypeDescriptor& type_desc,
                                       const ::parquet::schema::NodePtr& node, const ColumnPtr& col,
                                       const CallbackFunction& write_leaf_callback) {
    switch (type_desc.type) {
    case TYPE_BOOLEAN: {
        _write_boolean_column_chunk(ctx, type_desc, node, col, write_leaf_callback);
        break;
    }
    case TYPE_TINYINT: {
        _write_int_column_chunk<TYPE_TINYINT, ::parquet::Type::INT32>(ctx, type_desc, node, col, write_leaf_callback);
        break;
    }
    case TYPE_SMALLINT: {
        _write_int_column_chunk<TYPE_SMALLINT, ::parquet::Type::INT32>(ctx, type_desc, node, col, write_leaf_callback);
        break;
    }
    case TYPE_INT: {
        _write_int_column_chunk<TYPE_INT, ::parquet::Type::INT32>(ctx, type_desc, node, col, write_leaf_callback);
        break;
    }
    case TYPE_BIGINT: {
        _write_int_column_chunk<TYPE_BIGINT, ::parquet::Type::INT64>(ctx, type_desc, node, col, write_leaf_callback);
        break;
    }
    case TYPE_FLOAT: {
        _write_int_column_chunk<TYPE_FLOAT, ::parquet::Type::FLOAT>(ctx, type_desc, node, col, write_leaf_callback);
        break;
    }
    case TYPE_DOUBLE: {
        _write_int_column_chunk<TYPE_DOUBLE, ::parquet::Type::DOUBLE>(ctx, type_desc, node, col, write_leaf_callback);
        break;
    }
    case TYPE_DECIMAL32: {
        _write_int_column_chunk<TYPE_DECIMAL32, ::parquet::Type::INT32>(ctx, type_desc, node, col, write_leaf_callback);
        break;
    }
    case TYPE_DECIMAL64: {
        _write_int_column_chunk<TYPE_DECIMAL64, ::parquet::Type::INT64>(ctx, type_desc, node, col, write_leaf_callback);
        break;
    }
    case TYPE_DECIMAL128: {
        _write_decimal128_column_chunk(ctx, type_desc, node, col, write_leaf_callback);
        break;
    }
    case TYPE_DATE: {
        _write_date_column_chunk(ctx, type_desc, node, col, write_leaf_callback);
        break;
    }
    case TYPE_DATETIME: {
        _write_datetime_column_chunk(ctx, type_desc, node, col, write_leaf_callback);
        break;
    }
    case TYPE_CHAR:
    case TYPE_VARCHAR: {
        _write_byte_array_column_chunk<TYPE_VARCHAR>(ctx, type_desc, node, col, write_leaf_callback);
        break;
    }
    case TYPE_BINARY:
    case TYPE_VARBINARY: {
        _write_byte_array_column_chunk<TYPE_VARBINARY>(ctx, type_desc, node, col, write_leaf_callback);
        break;
    }
    case TYPE_ARRAY: {
        _write_array_column_chunk(ctx, type_desc, node, col, write_leaf_callback);
        break;
    }
    case TYPE_MAP: {
        _write_map_column_chunk(ctx, type_desc, node, col, write_leaf_callback);
        break;
    }
    case TYPE_STRUCT: {
        _write_struct_column_chunk(ctx, type_desc, node, col, write_leaf_callback);
        break;
    }
    default: {
    }
    }
}

void LevelBuilder::_write_boolean_column_chunk(const LevelBuilderContext& ctx, const TypeDescriptor& type_desc,
                                               const ::parquet::schema::NodePtr& node, const ColumnPtr& col,
                                               const CallbackFunction& write_leaf_callback) {
    const auto* data_col = get_raw_data_column<TYPE_BOOLEAN>(col);
    const auto* null_col = get_raw_null_column(col);

    // Use the rep_levels in the context from caller since node is primitive.
    auto& rep_levels = ctx._rep_levels;
    auto def_levels = _make_def_levels(ctx, node, null_col, col->size());
    auto null_bitset = _make_null_bitset(ctx, null_col, col->size());

    // sizeof(bool) depends on implementation, thus we cast values to ensure correctness
    auto values = new bool[col->size()];
    DeferOp defer([&] { delete[] values; });

    for (int i = 0; i < col->size(); i++) {
        values[i] = static_cast<bool>(data_col[i]);
    }

    write_leaf_callback(LevelBuilderResult{
            .num_levels = ctx._num_levels,
            .def_levels = def_levels ? def_levels->data() : nullptr,
            .rep_levels = rep_levels ? rep_levels->data() : nullptr,
            .values = reinterpret_cast<uint8_t*>(values),
            .null_bitset = null_bitset ? null_bitset->data() : nullptr,
    });
}

template <LogicalType lt, ::parquet::Type::type pt>
void LevelBuilder::_write_int_column_chunk(const LevelBuilderContext& ctx, const TypeDescriptor& type_desc,
                                           const ::parquet::schema::NodePtr& node, const ColumnPtr& col,
                                           const CallbackFunction& write_leaf_callback) {
    auto* data_col = get_raw_data_column<lt>(col);
    auto* null_col = get_raw_null_column(col);

    // Use the rep_levels in the context from caller since node is primitive.
    auto& rep_levels = ctx._rep_levels;
    auto def_levels = _make_def_levels(ctx, node, null_col, col->size());
    auto null_bitset = _make_null_bitset(ctx, null_col, col->size());

    using source_type = RunTimeCppType<lt>;
    using target_type = typename ::parquet::type_traits<pt>::value_type;

    if constexpr (std::is_same_v<source_type, target_type>) {
        // Zero-copy for identical source/target types
        // If leaf column has null entries, provide a bitset to denote not-null entries.
        write_leaf_callback(LevelBuilderResult{
                .num_levels = ctx._num_levels,
                .def_levels = def_levels ? def_levels->data() : nullptr,
                .rep_levels = rep_levels ? rep_levels->data() : nullptr,
                .values = reinterpret_cast<uint8_t*>(data_col),
                .null_bitset = null_bitset ? null_bitset->data() : nullptr,
        });
    } else {
        // If two types are different, cast values
        auto values = new target_type[col->size()];
        DeferOp defer([&] { delete[] values; });

        for (size_t i = 0; i < col->size(); i++) {
            values[i] = static_cast<target_type>(data_col[i]);
        }

        write_leaf_callback(LevelBuilderResult{
                .num_levels = ctx._num_levels,
                .def_levels = def_levels ? def_levels->data() : nullptr,
                .rep_levels = rep_levels ? rep_levels->data() : nullptr,
                .values = reinterpret_cast<uint8_t*>(values),
                .null_bitset = null_bitset ? null_bitset->data() : nullptr,
        });
    }
}

void LevelBuilder::_write_decimal128_column_chunk(const LevelBuilderContext& ctx, const TypeDescriptor& type_desc,
                                                  const ::parquet::schema::NodePtr& node, const ColumnPtr& col,
                                                  const CallbackFunction& write_leaf_callback) {
    const auto* data_col = get_raw_data_column<TYPE_DECIMAL128>(col);
    const auto* null_col = get_raw_null_column(col);

    // Use the rep_levels in the context from caller since node is primitive.
    auto& rep_levels = ctx._rep_levels;
    auto def_levels = _make_def_levels(ctx, node, null_col, col->size());
    auto null_bitset = _make_null_bitset(ctx, null_col, col->size());

    auto values = new unsigned __int128[col->size()];
    DeferOp defer([&] { delete[] values; });

    for (size_t i = 0; i < col->size(); i++) {
        // unscaled number must be encoded as two's complement using big-endian byte order (the most significant byte
        // is the zeroth element). See https://github.com/apache/parquet-format/blob/master/LogicalTypes.md#decimal
        values[i] = BigEndian::FromHost128(data_col[i]);
    }

    auto flba_values = new ::parquet::FixedLenByteArray[col->size()];
    DeferOp flba_defer([&] { delete[] flba_values; });

    for (size_t i = 0; i < col->size(); i++) {
        flba_values[i].ptr = reinterpret_cast<const uint8_t*>(values + i);
    }

    write_leaf_callback(LevelBuilderResult{
            .num_levels = ctx._num_levels,
            .def_levels = def_levels ? def_levels->data() : nullptr,
            .rep_levels = rep_levels ? rep_levels->data() : nullptr,
            .values = reinterpret_cast<uint8_t*>(flba_values),
            .null_bitset = null_bitset ? null_bitset->data() : nullptr,
    });
}

void LevelBuilder::_write_date_column_chunk(const LevelBuilderContext& ctx, const TypeDescriptor& type_desc,
                                            const ::parquet::schema::NodePtr& node, const ColumnPtr& col,
                                            const CallbackFunction& write_leaf_callback) {
    const auto* data_col = get_raw_data_column<TYPE_DATE>(col);
    const auto* null_col = get_raw_null_column(col);

    // Use the rep_levels in the context from caller since node is primitive.
    auto& rep_levels = ctx._rep_levels;
    auto def_levels = _make_def_levels(ctx, node, null_col, col->size());
    auto null_bitset = _make_null_bitset(ctx, null_col, col->size());

    auto unix_epoch_date = DateValue::create(1970, 1, 1); // base date to subtract

    auto values = new int32_t[col->size()];
    DeferOp defer([&] { delete[] values; });

    for (size_t i = 0; i < col->size(); i++) {
        values[i] = data_col[i]._julian - unix_epoch_date._julian;
    }

    write_leaf_callback(LevelBuilderResult{
            .num_levels = ctx._num_levels,
            .def_levels = def_levels ? def_levels->data() : nullptr,
            .rep_levels = rep_levels ? rep_levels->data() : nullptr,
            .values = reinterpret_cast<uint8_t*>(values),
            .null_bitset = null_bitset ? null_bitset->data() : nullptr,
    });
}

void LevelBuilder::_write_datetime_column_chunk(const LevelBuilderContext& ctx, const TypeDescriptor& type_desc,
                                                const ::parquet::schema::NodePtr& node, const ColumnPtr& col,
                                                const CallbackFunction& write_leaf_callback) {
    const auto data_col = get_raw_data_column<TYPE_DATETIME>(col);
    const auto null_col = get_raw_null_column(col);

    // Use the rep_levels in the context from caller since node is primitive.
    auto rep_levels = ctx._rep_levels;
    auto def_levels = _make_def_levels(ctx, node, null_col, col->size());
    auto null_bitset = _make_null_bitset(ctx, null_col, col->size());

    auto values = new int64_t[col->size()];
    DeferOp defer([&] { delete[] values; });

    for (size_t i = 0; i < col->size(); i++) {
        values[i] = data_col[i].to_unix_second() * 1000;
    }

    write_leaf_callback(LevelBuilderResult{
            .num_levels = ctx._num_levels,
            .def_levels = def_levels ? def_levels->data() : nullptr,
            .rep_levels = rep_levels ? rep_levels->data() : nullptr,
            .values = reinterpret_cast<uint8_t*>(values),
            .null_bitset = null_bitset ? null_bitset->data() : nullptr,
    });
}

template <LogicalType lt>
void LevelBuilder::_write_byte_array_column_chunk(const LevelBuilderContext& ctx, const TypeDescriptor& type_desc,
                                                  const ::parquet::schema::NodePtr& node, const ColumnPtr& col,
                                                  const CallbackFunction& write_leaf_callback) {
    const auto* data_col = down_cast<const RunTimeColumnType<lt>*>(ColumnHelper::get_data_column(col.get()));
    const auto* null_col = get_raw_null_column(col);
    auto& vo = data_col->get_offset();
    auto& vb = data_col->get_bytes();

    // Use the rep_levels in the context from caller since node is primitive.
    auto& rep_levels = ctx._rep_levels;
    auto def_levels = _make_def_levels(ctx, node, null_col, col->size());
    auto null_bitset = _make_null_bitset(ctx, null_col, col->size());

    auto values = new ::parquet::ByteArray[col->size()];
    DeferOp defer([&] { delete[] values; });

    for (size_t i = 0; i < col->size(); i++) {
        values[i].len = static_cast<uint32_t>(vo[i + 1] - vo[i]);
        values[i].ptr = reinterpret_cast<const uint8_t*>(vb.data() + vo[i]);
    }

    write_leaf_callback(LevelBuilderResult{
            .num_levels = ctx._num_levels,
            .def_levels = def_levels ? def_levels->data() : nullptr,
            .rep_levels = rep_levels ? rep_levels->data() : nullptr,
            .values = reinterpret_cast<uint8_t*>(values),
            .null_bitset = null_bitset ? null_bitset->data() : nullptr,
    });
}

void LevelBuilder::_write_array_column_chunk(const LevelBuilderContext& ctx, const TypeDescriptor& type_desc,
                                             const ::parquet::schema::NodePtr& node, const ColumnPtr& col,
                                             const CallbackFunction& write_leaf_callback) {
    // <list-repetition> group <name> (LIST) {
    //     repeated group list {
    //             <element-repetition> <element-type> element;
    //     }
    // }

    DCHECK(type_desc.type == TYPE_ARRAY);
    auto outer_node = std::static_pointer_cast<::parquet::schema::GroupNode>(node);
    auto mid_node = std::static_pointer_cast<::parquet::schema::GroupNode>(outer_node->field(0));
    auto inner_node = mid_node->field(0);

    auto* null_col = get_raw_null_column(col);
    auto* array_col = down_cast<ArrayColumn*>(ColumnHelper::get_data_column(col.get()));
    const auto& elements = array_col->elements_column();
    const auto& offsets = array_col->offsets_column()->get_data();

    size_t num_levels_upper_bound = ctx._num_levels + elements->size();
    auto def_levels = std::make_shared<std::vector<int16_t>>(num_levels_upper_bound,
                                                             ctx._max_def_level + node->is_optional() + 1);
    auto rep_levels = std::make_shared<std::vector<int16_t>>(num_levels_upper_bound, ctx._max_rep_level + 1);

    size_t num_levels = 0; // pointer to def/rep levels
    int offset = 0;        // pointer to current column
    for (auto i = 0; i < ctx._num_levels; i++) {
        auto def_level = ctx._def_levels ? (*ctx._def_levels)[i] : 0;
        auto rep_level = ctx._rep_levels ? (*ctx._rep_levels)[i] : 0;

        // already null in parent column
        if (def_level < ctx._repeated_ancestor_def_level) {
            (*def_levels)[num_levels] = def_level;
            (*rep_levels)[num_levels] = rep_level;

            num_levels++;
            continue;
        }

        // null in current array_column
        if (def_level < ctx._max_def_level || (null_col != nullptr && null_col[offset])) {
            (*def_levels)[num_levels] = def_level;
            (*rep_levels)[num_levels] = rep_level;

            num_levels++;
            offset++;
            continue;
        }

        auto array_size = offsets[offset + 1] - offsets[offset];
        // not null but empty array
        if (array_size == 0) {
            (*def_levels)[num_levels] = def_level + node->is_optional();
            (*rep_levels)[num_levels] = rep_level;

            num_levels++;
            offset++;
            continue;
        }

        // not null and non-empty array
        (*rep_levels)[num_levels] = rep_level;
        num_levels += array_size;
        offset++;
    }

    DCHECK(col->size() == offset);

    def_levels->resize(num_levels);
    rep_levels->resize(num_levels);
    LevelBuilderContext derived_ctx(def_levels->size(), def_levels, rep_levels,
                                    ctx._max_def_level + node->is_optional() + 1, ctx._max_rep_level + 1,
                                    ctx._max_def_level + node->is_optional() + 1);

    _write_column_chunk(derived_ctx, type_desc.children[0], inner_node, elements, write_leaf_callback);
}

void LevelBuilder::_write_map_column_chunk(const LevelBuilderContext& ctx, const TypeDescriptor& type_desc,
                                           const ::parquet::schema::NodePtr& node, const ColumnPtr& col,
                                           const CallbackFunction& write_leaf_callback) {
    // <map-repetition> group <name> (MAP) {
    //     repeated group key_value {
    //             required <key-type> key;
    //             <value-repetition> <value-type> value;
    //     }
    // }

    DCHECK(type_desc.type == TYPE_MAP);
    auto outer_node = std::static_pointer_cast<::parquet::schema::GroupNode>(node);
    auto mid_node = std::static_pointer_cast<::parquet::schema::GroupNode>(outer_node->field(0));
    auto key_node = mid_node->field(0);
    auto value_node = mid_node->field(1);

    auto* null_col = get_raw_null_column(col);
    auto* map_col = down_cast<MapColumn*>(ColumnHelper::get_data_column(col.get()));
    const auto& keys = map_col->keys_column();
    const auto& values = map_col->values_column();
    const auto& offsets = map_col->offsets_column()->get_data();

    size_t num_levels_upper_bound = ctx._num_levels + keys->size();
    auto def_levels = std::make_shared<std::vector<int16_t>>(num_levels_upper_bound,
                                                             ctx._max_def_level + node->is_optional() + 1);
    auto rep_levels = std::make_shared<std::vector<int16_t>>(num_levels_upper_bound, ctx._max_rep_level + 1);

    size_t num_levels = 0; // pointer to def/rep levels
    int offset = 0;        // pointer to current column
    for (auto i = 0; i < ctx._num_levels; i++) {
        auto def_level = ctx._def_levels ? (*ctx._def_levels)[i] : 0;
        auto rep_level = ctx._rep_levels ? (*ctx._rep_levels)[i] : 0;

        if (def_level < ctx._repeated_ancestor_def_level) {
            (*def_levels)[num_levels] = def_level;
            (*rep_levels)[num_levels] = rep_level;

            num_levels++;
            continue;
        }

        if (def_level < ctx._max_def_level || (null_col != nullptr && null_col[offset])) {
            (*def_levels)[num_levels] = def_level;
            (*rep_levels)[num_levels] = rep_level;

            num_levels++;
            offset++;
            continue;
        }

        auto array_size = offsets[offset + 1] - offsets[offset];
        if (array_size == 0) {
            (*def_levels)[num_levels] = def_level + node->is_optional();
            (*rep_levels)[num_levels] = rep_level;

            num_levels++;
            offset++;
            continue;
        }

        (*rep_levels)[num_levels] = rep_level;
        num_levels += array_size;
        offset++;
    }

    DCHECK(col->size() == offset);

    def_levels->resize(num_levels);
    rep_levels->resize(num_levels);
    LevelBuilderContext derived_ctx(def_levels->size(), def_levels, rep_levels,
                                    ctx._max_def_level + node->is_optional() + 1, ctx._max_rep_level + 1,
                                    ctx._max_def_level + node->is_optional() + 1);

    _write_column_chunk(derived_ctx, type_desc.children[0], key_node, keys, write_leaf_callback);
    _write_column_chunk(derived_ctx, type_desc.children[1], value_node, values, write_leaf_callback);
}

void LevelBuilder::_write_struct_column_chunk(const LevelBuilderContext& ctx, const TypeDescriptor& type_desc,
                                              const ::parquet::schema::NodePtr& node, const ColumnPtr& col,
                                              const CallbackFunction& write_leaf_callback) {
    DCHECK(type_desc.type == TYPE_STRUCT);
    auto struct_node = std::static_pointer_cast<::parquet::schema::GroupNode>(node);

    auto* null_col = get_raw_null_column(col);
    auto* data_col = ColumnHelper::get_data_column(col.get());
    auto* struct_col = down_cast<StructColumn*>(data_col);

    // Use the rep_levels in the context from caller since node is primitive.
    auto rep_levels = ctx._rep_levels;
    auto def_levels = _make_def_levels(ctx, node, null_col, col->size());

    LevelBuilderContext derived_ctx(def_levels->size(), def_levels, rep_levels,
                                    ctx._max_def_level + node->is_optional(), ctx._max_rep_level,
                                    ctx._repeated_ancestor_def_level);

    for (size_t i = 0; i < type_desc.children.size(); i++) {
        auto sub_col = struct_col->field_column(type_desc.field_names[i]);
        _write_column_chunk(derived_ctx, type_desc.children[i], struct_node->field(i), sub_col, write_leaf_callback);
    }
}

// Convert byte-addressable mask into a bit-addressable mask. Note the 0/1 values are flipped.
std::shared_ptr<std::vector<uint8_t>> LevelBuilder::_make_null_bitset(const LevelBuilderContext& ctx,
                                                                      const uint8_t* nulls,
                                                                      const size_t col_size) const {
    if (ctx._repeated_ancestor_def_level == ctx._max_def_level) {
        if (nulls == nullptr) {
            return nullptr;
        }

        auto bitset = std::make_shared<std::vector<uint8_t>>((col_size + 7) / 8);
        for (size_t i = 0; i < col_size; i++) {
            (*bitset)[i / 8] |= (1 - nulls[i]) << (i % 8);
        }
        return bitset;
    }

    // slow path
    auto bitset = std::make_shared<std::vector<uint8_t>>((col_size + 7) / 8);
    size_t col_offset = 0;
    for (size_t i = 0; i < ctx._num_levels; i++) {
        int16_t level = ctx._def_levels ? (*ctx._def_levels)[i] : 0;
        if (level < ctx._repeated_ancestor_def_level) {
            continue;
        }
        uint8_t is_null = nulls != nullptr ? nulls[col_offset] : 0;
        is_null |= (level < ctx._max_def_level); // undefined but having a slot in leaf column
        (*bitset)[col_offset / 8] |= (1 - is_null) << (col_offset % 8);
        col_offset++;
    }
    DCHECK(col_size == col_offset);
    return bitset;
}

// Make definition levels in terms of repetition and nullity.
// node could be primitive, or group node denoting struct.
std::shared_ptr<std::vector<int16_t>> LevelBuilder::_make_def_levels(const LevelBuilderContext& ctx,
                                                                     const ::parquet::schema::NodePtr& node,
                                                                     const uint8_t* nulls,
                                                                     const size_t col_size) const {
    DCHECK(!node->is_repeated());
    if (node->is_required()) {
        // For required node, use the def_levels in the context from caller.
        return ctx._def_levels;
    }

    if (ctx._max_def_level == 0) {
        auto def_levels = std::make_shared<std::vector<int16_t>>(ctx._num_levels, 1); // assume not-null first
        if (nulls == nullptr) {                                                       // column has no null
            return def_levels;
        }

        DCHECK(ctx._max_rep_level == 0);
        for (size_t i = 0; i < ctx._num_levels; i++) { // nulls.size() == ctx._num_levels
            // decrement def_levels for null entries
            (*def_levels)[i] -= nulls[i];
        }

        return def_levels;
    }

    DCHECK(ctx._def_levels != nullptr);
    auto def_levels = std::make_shared<std::vector<int16_t>>(*ctx._def_levels);

    int col_offset = 0;
    int level_offset = 0;
    if (nulls != nullptr) {
        while (level_offset < ctx._num_levels && col_offset < col_size) {
            auto& level = (*def_levels)[level_offset];
            uint8_t defined = (level == ctx._max_def_level);
            uint8_t not_null = defined & (1 - nulls[col_offset]);
            level += not_null;
            col_offset += (level >= ctx._repeated_ancestor_def_level);
            level_offset++;
        }
    } else {
        while (level_offset < ctx._num_levels && col_offset < col_size) {
            auto& level = (*def_levels)[level_offset];
            uint8_t defined = (level == ctx._max_def_level);
            uint8_t not_null = defined;
            level += not_null;
            col_offset += (level >= ctx._repeated_ancestor_def_level);
            level_offset++;
        }
    }
    DCHECK(col_offset == col_size);

    return def_levels;
}

} // namespace starrocks::parquet
