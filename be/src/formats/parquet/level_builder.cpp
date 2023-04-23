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
    auto null_column = down_cast<NullableColumn*>(col.get())->null_column();
    auto raw_column = null_column->get_data().data();
    return raw_column;
}

template <LogicalType lt>
inline RunTimeCppType<lt>* get_raw_data_column(const ColumnPtr& col) {
    auto data_column = ColumnHelper::get_data_column(col.get());
    auto raw_column = down_cast<RunTimeColumnType<lt>*>(data_column)->get_data().data();
    return raw_column;
}

LevelBuilder::LevelBuilder(TypeDescriptor type_desc, ::parquet::schema::NodePtr root)
        : _type_desc(std::move(type_desc)), _root(std::move(root)) {}

void LevelBuilder::write(const LevelBuilderContext& ctx, const ColumnPtr& col,
                         const CallbackFunction& write_leaf_callback) {
    _write_column_chunk(ctx, _type_desc, _root, col, write_leaf_callback);
}

Status LevelBuilder::_write_column_chunk(const LevelBuilderContext& ctx, const TypeDescriptor& type_desc,
                                         const ::parquet::schema::NodePtr& node, const ColumnPtr& col,
                                         const CallbackFunction& write_leaf_callback) {
    switch (type_desc.type) {
    case TYPE_BOOLEAN: {
        return _write_boolean_column_chunk(ctx, type_desc, node, col, write_leaf_callback);
    }
    case TYPE_TINYINT: {
        return _write_int_column_chunk<TYPE_TINYINT, ::parquet::Type::INT32>(ctx, type_desc, node, col,
                                                                             write_leaf_callback);
    }
    case TYPE_SMALLINT: {
        return _write_int_column_chunk<TYPE_SMALLINT, ::parquet::Type::INT32>(ctx, type_desc, node, col,
                                                                              write_leaf_callback);
    }
    case TYPE_INT: {
        return _write_int_column_chunk<TYPE_INT, ::parquet::Type::INT32>(ctx, type_desc, node, col,
                                                                         write_leaf_callback);
    }
    case TYPE_BIGINT: {
        return _write_int_column_chunk<TYPE_BIGINT, ::parquet::Type::INT64>(ctx, type_desc, node, col,
                                                                            write_leaf_callback);
    }
    case TYPE_FLOAT: {
        return _write_int_column_chunk<TYPE_FLOAT, ::parquet::Type::FLOAT>(ctx, type_desc, node, col,
                                                                           write_leaf_callback);
    }
    case TYPE_DOUBLE: {
        return _write_int_column_chunk<TYPE_DOUBLE, ::parquet::Type::DOUBLE>(ctx, type_desc, node, col,
                                                                             write_leaf_callback);
    }
    case TYPE_DECIMAL32: {
        return _write_int_column_chunk<TYPE_DECIMAL32, ::parquet::Type::INT32>(ctx, type_desc, node, col,
                                                                               write_leaf_callback);
    }
    case TYPE_DECIMAL64: {
        return _write_int_column_chunk<TYPE_DECIMAL64, ::parquet::Type::INT64>(ctx, type_desc, node, col,
                                                                               write_leaf_callback);
    }
    case TYPE_DECIMAL128: {
        return _write_decimal128_column_chunk(ctx, type_desc, node, col, write_leaf_callback);
    }
    case TYPE_DATE: {
        return _write_date_column_chunk(ctx, type_desc, node, col, write_leaf_callback);
    }
    case TYPE_DATETIME: {
        return _write_datetime_column_chunk(ctx, type_desc, node, col, write_leaf_callback);
    }
    case TYPE_CHAR:
    case TYPE_VARCHAR: {
        return _write_varchar_column_chunk(ctx, type_desc, node, col, write_leaf_callback);
    }
    case TYPE_ARRAY: {
        return _write_array_column_chunkV3(ctx, type_desc, node, col, write_leaf_callback);
    }
    case TYPE_MAP: {
        return _write_map_column_chunk(ctx, type_desc, node, col, write_leaf_callback);
    }
    case TYPE_STRUCT: {
        return _write_struct_column_chunk(ctx, type_desc, node, col, write_leaf_callback);
    }
    default: {
        return Status::NotSupported(fmt::format("Type {} is not supported", type_desc.debug_string()));
    }
    }
}

Status LevelBuilder::_write_boolean_column_chunk(const LevelBuilderContext& ctx, const TypeDescriptor& type_desc,
                                                 const ::parquet::schema::NodePtr& node, const ColumnPtr& col,
                                                 const CallbackFunction& write_leaf_callback) {
    const auto data_col = get_raw_data_column<TYPE_BOOLEAN>(col);
    const auto null_col = get_raw_null_column(col);

    // Use the rep_levels in the context from caller since node is primitive.
    auto rep_levels = ctx._rep_levels;
    auto def_levels = _make_def_levelsV3(ctx, node, null_col);

    // sizeof(bool) depends on implementation, thus we cast values to ensure correctness
    auto values = new bool[col->size()];
    DeferOp defer([&] { delete[] values; });

    int offset = 0;
    for (size_t i = 0; i < col->size(); i++) {
        if (null_col != nullptr && null_col[i] == 0) {
            values[offset++] = static_cast<bool>(data_col[i]);
        }
    }

    write_leaf_callback(LevelBuilderResult{
            .num_levels = ctx.num_levels(),
            .def_levels = def_levels->data(),
            .rep_levels = rep_levels->data(),
            .values = reinterpret_cast<uint8_t*>(values),
            .null_bitset = nullptr,
    });

    return Status::OK();
}

template <LogicalType lt, ::parquet::Type::type pt>
Status LevelBuilder::_write_int_column_chunk(const LevelBuilderContext& ctx, const TypeDescriptor& type_desc,
                                             const ::parquet::schema::NodePtr& node, const ColumnPtr& col,
                                             const CallbackFunction& write_leaf_callback) {
    const auto data_col = get_raw_data_column<lt>(col);
    const auto null_col = get_raw_null_column(col);

    // Use the rep_levels in the context from caller since node is primitive.
    auto rep_levels = ctx._rep_levels;
    auto def_levels = _make_def_levelsV3(ctx, node, null_col);

    using source_type = RunTimeCppType<lt>;
    using target_type = typename ::parquet::type_traits<pt>::value_type;

    if constexpr (std::is_same_v<source_type, target_type>) {
        // Zero-copy for identical source/target types
        // If leaf column has null entries, provide a bitset to denote not-null entries.
        write_leaf_callback(LevelBuilderResult{
                .num_levels = ctx.num_levels(),
                .def_levels = def_levels ? def_levels->data() : nullptr,
                .rep_levels = rep_levels ? rep_levels->data() : nullptr,
                .values = reinterpret_cast<uint8_t*>(data_col),
                // Make bitset to denote not-null values
                .null_bitset = col->has_null() ? _make_null_bitset(col->size(), null_col).data() : nullptr,
        });
    } else {
        // If two types are different, we have to cast values anyway
        std::vector<target_type> values;
        values.reserve(col->size());
        for (size_t i = 0; i < col->size(); i++) {
            if (null_col == nullptr || null_col[i] == 0) {
                values.push_back(static_cast<target_type>(data_col[i]));
            }
        }

        write_leaf_callback(LevelBuilderResult{
                .num_levels = ctx.num_levels(),
                .def_levels = def_levels ? def_levels->data() : nullptr,
                .rep_levels = rep_levels ? rep_levels->data() : nullptr,
                .values = reinterpret_cast<uint8_t*>(values.data()),
                .null_bitset = nullptr,
        });
    }
    return Status::OK();
}

Status LevelBuilder::_write_decimal128_column_chunk(const LevelBuilderContext& ctx, const TypeDescriptor& type_desc,
                                                    const ::parquet::schema::NodePtr& node, const ColumnPtr& col,
                                                    const CallbackFunction& write_leaf_callback) {
    const auto data_col = get_raw_data_column<TYPE_DECIMAL128>(col);
    const auto null_col = get_raw_null_column(col);

    // Use the rep_levels in the context from caller since node is primitive.
    auto rep_levels = ctx._rep_levels;
    auto def_levels = _make_def_levelsV3(ctx, node, null_col);

    std::vector<unsigned __int128> values;
    values.reserve(col->size());
    for (size_t i = 0; i < col->size(); i++) {
        if (null_col == nullptr || null_col[i] == 0) {
            // unscaled number must be encoded as two's complement using big-endian byte order (the most significant byte
            // is the zeroth element). See https://github.com/apache/parquet-format/blob/master/LogicalTypes.md#decimal
            auto big_endian_value = BigEndian::FromHost128(data_col[i]);
            values.push_back(big_endian_value);
        }
    }

    std::vector<::parquet::FixedLenByteArray> flba_values;
    flba_values.reserve(values.size());
    for (size_t i = 0; i < values.size(); i++) {
        auto ptr = reinterpret_cast<const uint8_t*>(values.data() + i);
        flba_values.emplace_back(ptr);
    }

    write_leaf_callback(LevelBuilderResult{
            .num_levels = ctx.num_levels(),
            .def_levels = def_levels ? def_levels->data() : nullptr,
            .rep_levels = rep_levels ? rep_levels->data() : nullptr,
            .values = reinterpret_cast<uint8_t*>(values.data()),
            .null_bitset = nullptr,
    });

    return Status::OK();
}

Status LevelBuilder::_write_date_column_chunk(const LevelBuilderContext& ctx, const TypeDescriptor& type_desc,
                                              const ::parquet::schema::NodePtr& node, const ColumnPtr& col,
                                              const CallbackFunction& write_leaf_callback) {
    const auto data_col = get_raw_data_column<TYPE_DATE>(col);
    const auto null_col = get_raw_null_column(col);

    // Use the rep_levels in the context from caller since node is primitive.
    auto rep_levels = ctx._rep_levels;
    auto def_levels = _make_def_levelsV3(ctx, node, null_col);

    std::vector<int32_t> values;
    values.reserve(col->size());

    auto unix_epoch_date = DateValue::create(1970, 1, 1);
    for (size_t i = 0; i < col->size(); i++) {
        if (null_col == nullptr || null_col[i] == 0) {
            int32_t unix_days = data_col[i]._julian - unix_epoch_date._julian;
            values.push_back(unix_days);
        }
    }

    write_leaf_callback(LevelBuilderResult{
            .num_levels = ctx.num_levels(),
            .def_levels = def_levels ? def_levels->data() : nullptr,
            .rep_levels = rep_levels ? rep_levels->data() : nullptr,
            .values = reinterpret_cast<uint8_t*>(values.data()),
            .null_bitset = nullptr,
    });

    return Status::OK();
}

Status LevelBuilder::_write_datetime_column_chunk(const LevelBuilderContext& ctx, const TypeDescriptor& type_desc,
                                                  const ::parquet::schema::NodePtr& node, const ColumnPtr& col,
                                                  const CallbackFunction& write_leaf_callback) {
    const auto data_col = get_raw_data_column<TYPE_DATETIME>(col);
    const auto null_col = get_raw_null_column(col);

    // Use the rep_levels in the context from caller since node is primitive.
    auto rep_levels = ctx._rep_levels;
    auto def_levels = _make_def_levelsV3(ctx, node, null_col);

    std::vector<int64_t> values;
    values.reserve(col->size());
    for (size_t i = 0; i < col->size(); i++) {
        if (null_col == nullptr || null_col[i] == 0) {
            int64_t milliseconds = data_col[i].to_unix_second() * 1000;
            values.push_back(milliseconds);
        }
    }

    write_leaf_callback(LevelBuilderResult{
            .num_levels = ctx.num_levels(),
            .def_levels = def_levels ? def_levels->data() : nullptr,
            .rep_levels = rep_levels ? rep_levels->data() : nullptr,
            .values = reinterpret_cast<uint8_t*>(values.data()),
            .null_bitset = nullptr,
    });

    return Status::OK();
}

Status LevelBuilder::_write_varchar_column_chunk(const LevelBuilderContext& ctx, const TypeDescriptor& type_desc,
                                                 const ::parquet::schema::NodePtr& node, const ColumnPtr& col,
                                                 const CallbackFunction& write_leaf_callback) {
    auto data_col = down_cast<const RunTimeColumnType<TYPE_VARCHAR>*>(ColumnHelper::get_data_column(col.get()));
    const auto null_col = get_raw_null_column(col);
    auto& vo = data_col->get_offset();
    auto& vb = data_col->get_bytes();

    // Use the rep_levels in the context from caller since node is primitive.
    auto rep_levels = ctx._rep_levels;
    auto def_levels = _make_def_levelsV3(ctx, node, null_col);

    std::vector<::parquet::ByteArray> values;
    values.reserve(col->size());
    for (size_t i = 0; i < col->size(); i++) {
        if (null_col == nullptr || null_col[i] == 0) {
            auto len = static_cast<uint32_t>(vo[i + 1] - vo[i]);
            auto ptr = reinterpret_cast<const uint8_t*>(vb.data() + vo[i]);
            values.emplace_back(len, ptr);
        }
    }

    write_leaf_callback(LevelBuilderResult{
            .num_levels = ctx.num_levels(),
            .def_levels = def_levels ? def_levels->data() : nullptr,
            .rep_levels = rep_levels ? rep_levels->data() : nullptr,
            .values = reinterpret_cast<uint8_t*>(values.data()),
            .null_bitset = nullptr,
    });
    return Status::OK();
}

Status LevelBuilder::_write_array_column_chunk(const LevelBuilderContext& ctx, const TypeDescriptor& type_desc,
                                               const ::parquet::schema::NodePtr& node, const ColumnPtr& col,
                                               const CallbackFunction& write_leaf_callback) {
    DCHECK(type_desc.type == TYPE_ARRAY);
    auto outer_node = std::static_pointer_cast<::parquet::schema::GroupNode>(node);
    auto mid_node = std::static_pointer_cast<::parquet::schema::GroupNode>(outer_node->field(0));
    auto inner_node = mid_node->field(0);

    const auto null_col = get_raw_null_column(col);
    const auto array_col = down_cast<ArrayColumn*>(ColumnHelper::get_data_column(col.get()));
    const auto& elements = array_col->elements_column();
    const auto& offsets = array_col->offsets_column()->get_data();

    auto def_levels = std::make_shared<std::vector<int16_t>>();
    auto rep_levels = std::make_shared<std::vector<int16_t>>();

    int offset = 0;
    for (auto i = 0; i < ctx.num_levels(); i++) {
        auto def_level = ctx._def_levels ? ctx._def_levels->at(i) : 0;
        auto rep_level = ctx._rep_levels ? ctx._rep_levels->at(i) : 0;
        if (def_level < ctx._max_def_level) {
            def_levels->push_back(def_level);
            rep_levels->push_back(rep_level);
            continue;
        }

        if (null_col != nullptr && null_col[offset]) {
            def_levels->push_back(def_level);
            rep_levels->push_back(rep_level);

            offset++;
            continue;
        }

        auto array_size = offsets[offset + 1] - offsets[offset];
        if (array_size == 0) {
            def_levels->push_back(def_level + node->is_optional());
            rep_levels->push_back(rep_level);
        } else {
            def_levels->push_back(def_level + node->is_optional() + 1);
            rep_levels->push_back(rep_level);
            for (auto j = 1; j < array_size; j++) {
                def_levels->push_back(def_level + node->is_optional() + 1);
                rep_levels->push_back(ctx._max_rep_level + 1);
            }
        }
        offset++;
    }

    LevelBuilderContext derived_ctx(def_levels->size(), def_levels, ctx._max_def_level + node->is_optional() + 1,
                                    rep_levels, ctx._max_rep_level + 1);

    DCHECK(elements->size() == offset);
    return _write_column_chunk(derived_ctx, type_desc.children[0], inner_node, elements, write_leaf_callback);
}

Status LevelBuilder::_write_array_column_chunkV2(const LevelBuilderContext& ctx, const TypeDescriptor& type_desc,
                                                 const ::parquet::schema::NodePtr& node, const ColumnPtr& col,
                                                 const CallbackFunction& write_leaf_callback) {
    DCHECK(type_desc.type == TYPE_ARRAY);
    auto outer_node = std::static_pointer_cast<::parquet::schema::GroupNode>(node);
    auto mid_node = std::static_pointer_cast<::parquet::schema::GroupNode>(outer_node->field(0));
    auto inner_node = mid_node->field(0);

    const auto null_col = get_raw_null_column(col);
    const auto array_col = down_cast<ArrayColumn*>(ColumnHelper::get_data_column(col.get()));
    const auto& elements = array_col->elements_column();
    const auto& offsets = array_col->offsets_column()->get_data();

    size_t num_levels_upper_bound = ctx.num_levels() + elements->size();
    auto def_levels = std::make_shared<std::vector<int16_t>>(num_levels_upper_bound,
                                                             ctx._max_def_level + node->is_optional() + 1);
    auto rep_levels = std::make_shared<std::vector<int16_t>>(num_levels_upper_bound, ctx._max_rep_level + 1);

    size_t num_levels = 0; // pointer to def/rep levels
    int offset = 0;        // pointer to current column
    for (auto i = 0; i < ctx.num_levels(); i++) {
        auto def_level = ctx._def_levels ? (*ctx._def_levels)[i] : 0;
        auto rep_level = ctx._rep_levels ? (*ctx._rep_levels)[i] : 0;

        // already null in parent column
        if (def_level < ctx._max_def_level) {
            (*def_levels)[num_levels] = def_level;
            (*rep_levels)[num_levels] = rep_level;

            num_levels++;
            continue;
        }

        // null in current array_column
        if (null_col != nullptr && null_col[offset]) {
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
    LevelBuilderContext derived_ctx(def_levels->size(), def_levels, ctx._max_def_level + node->is_optional() + 1,
                                    rep_levels, ctx._max_rep_level + 1);

    return _write_column_chunk(derived_ctx, type_desc.children[0], inner_node, elements, write_leaf_callback);
}

Status LevelBuilder::_write_array_column_chunkV3(const LevelBuilderContext& ctx, const TypeDescriptor& type_desc,
                                                 const ::parquet::schema::NodePtr& node, const ColumnPtr& col,
                                                 const CallbackFunction& write_leaf_callback) {
    DCHECK(type_desc.type == TYPE_ARRAY);
    auto outer_node = std::static_pointer_cast<::parquet::schema::GroupNode>(node);
    auto mid_node = std::static_pointer_cast<::parquet::schema::GroupNode>(outer_node->field(0));
    auto inner_node = mid_node->field(0);

    const auto null_col = get_raw_null_column(col);
    const auto array_col = down_cast<ArrayColumn*>(ColumnHelper::get_data_column(col.get()));
    const auto& elements = array_col->elements_column();
    const auto& offsets = array_col->offsets_column()->get_data();

    size_t num_levels_upper_bound = ctx.num_levels() + elements->size();
    auto def_levels = std::make_shared<std::vector<int16_t>>(num_levels_upper_bound,
                                                             ctx._max_def_level + node->is_optional() + 1);
    auto rep_levels = std::make_shared<std::vector<int16_t>>(num_levels_upper_bound, ctx._max_rep_level + 1);

    size_t num_levels = 0; // pointer to def/rep levels
    int offset = 0;        // pointer to current column
    int ctx_ptr = 0;       // pointer to levels in context

    if (null_col != nullptr) {
        while (ctx_ptr < ctx.num_levels() && offset < col->size()) {
            auto def_level = ctx._def_levels ? (*ctx._def_levels)[ctx_ptr] : 0;
            auto rep_level = ctx._rep_levels ? (*ctx._rep_levels)[ctx_ptr] : 0;

            uint8_t null_in_parent_column = def_level != ctx._max_def_level;
            uint8_t null_in_current_column = null_col[offset] == 1;
            uint8_t empty_array = offsets[offset + 1] == offsets[offset];
            uint8_t is_optional = node->is_optional();
            auto array_size = offsets[offset + 1] - offsets[offset];

            // set def_level back for null in parent column
            (*def_levels)[num_levels] += null_in_parent_column * (def_level - (*def_levels)[num_levels]);
            // decrement 1 for empty array
            (*def_levels)[num_levels] -= ((1 - null_in_parent_column) & (1 - null_in_current_column) & empty_array);
            // decrement node->is_optional for null in current column
            (*def_levels)[num_levels] -= ((1 - null_in_parent_column) & null_in_current_column & is_optional);
            (*rep_levels)[num_levels] = rep_level;

            // update pointers
            uint8_t advance_one_step = null_in_parent_column | null_in_current_column | empty_array;
            num_levels += advance_one_step + (1 - advance_one_step) * array_size;
            offset += (1 - null_in_parent_column);
            ctx_ptr++;
        }
    } else {
        while (ctx_ptr < ctx.num_levels() && offset < col->size()) {
            auto def_level = ctx._def_levels ? (*ctx._def_levels)[ctx_ptr] : 0;
            auto rep_level = ctx._rep_levels ? (*ctx._rep_levels)[ctx_ptr] : 0;

            uint8_t null_in_parent_column = def_level != ctx._max_def_level;
            uint8_t empty_array = offsets[offset + 1] == offsets[offset];
            auto array_size = offsets[offset + 1] - offsets[offset];

            // set def_level back for null in parent column
            (*def_levels)[num_levels] = null_in_parent_column * def_level + (1 - null_in_parent_column) * (*def_levels)[num_levels];
            // decrement 1 for empty array
            (*def_levels)[num_levels] -= ((1 - null_in_parent_column) & empty_array);
            (*rep_levels)[num_levels] = rep_level;

            // update pointers
            uint8_t advance_one_step = null_in_parent_column | empty_array;
            num_levels += advance_one_step + (1 - advance_one_step) * array_size;
            offset += (1 - null_in_parent_column);
            ctx_ptr++;
        }
    }
    DCHECK(col->size() == offset);

    // handle remaining undefined entries
    if (ctx_ptr < ctx.num_levels()) {
        if (ctx._def_levels == nullptr) {
            memset(def_levels->data() + num_levels, 0, ctx.num_levels() - ctx_ptr);
        } else {
            memcpy(def_levels->data() + num_levels, ctx._def_levels->data() + ctx_ptr, ctx.num_levels() - ctx_ptr);
        }

        if (ctx._rep_levels == nullptr) {
            memset(rep_levels->data() + num_levels, 0, ctx.num_levels() - ctx_ptr);
        } else {
            memcpy(rep_levels->data() + num_levels, ctx._rep_levels->data() + ctx_ptr, ctx.num_levels() - ctx_ptr);
        }
    }

    def_levels->resize(num_levels);
    rep_levels->resize(num_levels);
    LevelBuilderContext derived_ctx(def_levels->size(), def_levels, ctx._max_def_level + node->is_optional() + 1,
                                    rep_levels, ctx._max_rep_level + 1);

    return _write_column_chunk(derived_ctx, type_desc.children[0], inner_node, elements, write_leaf_callback);
}

Status LevelBuilder::_write_map_column_chunk(const LevelBuilderContext& ctx, const TypeDescriptor& type_desc,
                                             const ::parquet::schema::NodePtr& node, const ColumnPtr& col,
                                             const CallbackFunction& write_leaf_callback) {
    DCHECK(type_desc.type == TYPE_MAP);
    auto outer_node = std::static_pointer_cast<::parquet::schema::GroupNode>(node);
    auto mid_node = std::static_pointer_cast<::parquet::schema::GroupNode>(outer_node->field(0));
    auto key_node = mid_node->field(0);
    auto value_node = mid_node->field(1);

    const auto null_col = get_raw_null_column(col);
    const auto map_col = down_cast<MapColumn*>(ColumnHelper::get_data_column(col.get()));
    const auto& keys = map_col->keys_column();
    const auto& values = map_col->values_column();
    const auto& offsets = map_col->offsets_column()->get_data();

    size_t num_levels_upper_bound = ctx.num_levels() + keys->size();
    auto def_levels = std::make_shared<std::vector<int16_t>>(num_levels_upper_bound,
                                                             ctx._max_def_level + node->is_optional() + 1);
    auto rep_levels = std::make_shared<std::vector<int16_t>>(num_levels_upper_bound, ctx._max_rep_level + 1);

    size_t num_levels = 0; // pointer to def/rep levels
    int offset = 0;        // pointer to current column
    for (auto i = 0; i < ctx.num_levels(); i++) {
        auto def_level = ctx._def_levels ? (*ctx._def_levels)[i] : 0;
        auto rep_level = ctx._rep_levels ? (*ctx._rep_levels)[i] : 0;

        if (def_level < ctx._max_def_level) {
            (*def_levels)[num_levels] = def_level;
            (*rep_levels)[num_levels] = rep_level;

            num_levels++;
            continue;
        }

        if (null_col != nullptr && null_col[offset]) {
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
    LevelBuilderContext derived_ctx(def_levels->size(), def_levels, ctx._max_def_level + node->is_optional() + 1,
                                    rep_levels, ctx._max_rep_level + 1);

    auto ret = _write_column_chunk(derived_ctx, type_desc.children[0], key_node, keys, write_leaf_callback);
    if (!ret.ok()) {
        return ret;
    }
    return _write_column_chunk(derived_ctx, type_desc.children[1], value_node, values, write_leaf_callback);
}

Status LevelBuilder::_write_struct_column_chunk(const LevelBuilderContext& ctx, const TypeDescriptor& type_desc,
                                                const ::parquet::schema::NodePtr& node, const ColumnPtr& col,
                                                const CallbackFunction& write_leaf_callback) {
    DCHECK(type_desc.type == TYPE_STRUCT);
    auto struct_node = std::static_pointer_cast<::parquet::schema::GroupNode>(node);

    const auto null_col = get_raw_null_column(col);
    const auto data_col = ColumnHelper::get_data_column(col.get());
    const auto struct_col = down_cast<StructColumn*>(data_col);

    // Use the rep_levels in the context from caller since node is primitive.
    auto rep_levels = ctx._rep_levels;
    auto def_levels = _make_def_levelsV3(ctx, node, null_col);

    LevelBuilderContext derived_ctx(def_levels->size(), def_levels, ctx._max_def_level + node->is_optional(),
                                    rep_levels, ctx._max_rep_level);

    for (size_t i = 0; i < type_desc.children.size(); i++) {
        auto sub_col = struct_col->field_column(type_desc.field_names[i]);
        auto ret = _write_column_chunk(derived_ctx, type_desc.children[i], struct_node->field(i), sub_col,
                                       write_leaf_callback);
        if (!ret.ok()) {
            return ret;
        }
    }
    return Status::OK();
}

// Convert byte-addressable bitset into a bit-addressable bitset.
std::vector<uint8_t> LevelBuilder::_make_null_bitset(size_t n, const uint8_t* nulls) const {
    DCHECK(nulls != nullptr);
    std::vector<uint8_t> bitset((n + 7) / 8);
    size_t start = 0;

#ifndef __AVX2__
    constexpr size_t kBatchSize = /*width of AVX registers*/ 256 / 8;
    while (start + kBatchSize < n) {
        __m256i null_vec = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(nulls + start));
        uint32_t mask = _mm256_testz_si256(null_vec, _mm256_set1_epi8(0x01));
        memcpy(bitset.data() + kBatchSize / sizeof(uint8_t), &mask, sizeof(uint32_t));
        start += kBatchSize;
    }
#endif

    for (size_t i = start; i < n; i++) {
        if (!nulls[i]) {
            bitset[i / 8] |= 1 << (i % 8);
        }
    }
    return bitset;
}

// Make definition levels int terms of repetition type of primitive node.
// For required node, use the def_levels in the context from caller.
// For optional node, increment def_levels of these defined values.
std::shared_ptr<std::vector<int16_t>> LevelBuilder::_make_def_levelsV2(const LevelBuilderContext& ctx,
                                                                       const ::parquet::schema::NodePtr& node,
                                                                       const uint8_t* nulls) const {
    DCHECK(node->is_primitive());
    if (node->is_required()) {
        return ctx._def_levels;
    }

    auto def_levels = std::make_shared<std::vector<int16_t>>();
    def_levels->reserve(ctx.num_levels());

    int offset = 0;
    for (auto i = 0; i < ctx.num_levels(); i++) {
        int level = ctx._def_levels ? ctx._def_levels->at(i) : 0;
        if (level < ctx._max_def_level) {
            def_levels->push_back(level);
            continue;
        }

        // increment def_level for non-null entry
        if (nulls == nullptr || nulls[offset] == 0) {
            level++;
        }

        def_levels->push_back(level);
        offset++;
    }
    return def_levels;
}

std::shared_ptr<std::vector<int16_t>> LevelBuilder::_make_def_levelsV3(const LevelBuilderContext& ctx,
                                                                       const ::parquet::schema::NodePtr& node,
                                                                       const uint8_t* nulls) const {
    if (node->is_required()) {
        return ctx._def_levels;
    }

    if (ctx._def_levels == nullptr) {
        auto def_levels = std::make_shared<std::vector<int16_t>>(ctx.num_levels(), 1); // assume not-null first
        if (nulls == nullptr) {
            // column has no null
            return def_levels;
        }

        // nulls.size() == ctx.num_levels()
        for (size_t i = 0; i < ctx.num_levels(); i++) {
            // decrement def_levels for null entries
            if (nulls[i] != 0) {
                def_levels->at(i)--;
            }
        }

        return def_levels;
    }

    DCHECK(ctx._def_levels != nullptr);
    auto def_levels = std::make_shared<std::vector<int16_t>>(*ctx._def_levels);

    int offset = 0;
    for (auto& level : *def_levels) {
        if (level == ctx._max_def_level) {
            if (nulls == nullptr || nulls[offset] == 0) {
                // increment def_level for non-null entry
                level++;
            }
            offset++;
        }
    }
    return def_levels;
}

} // namespace starrocks::parquet
