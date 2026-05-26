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

#include <memory>
#include <numeric>
#include <type_traits>
#include <utility>

#include "column/adaptive_nullable_column.h"
#include "column/array_column.h"
#include "column/chunk.h"
#include "column/column_helper.h"
#include "column/column_visitor_adapter.h"
#include "column/json_column.h"
#include "column/map_column.h"
#include "column/runtime_type_traits.h"
#include "column/schema.h"
#include "column/storage_column_traits.h"
#include "column/struct_column.h"
#include "column/vectorized_fwd.h"
#include "exprs/expr_context.h"
#include "gutil/strings/fastmem.h"
#include "runtime/current_thread.h"
#include "storage/tablet_schema.h"
#include "storage/types.h"
#include "types/olap_type_infra.h"
#include "types/storage_type_traits.h"

namespace starrocks {

Field ChunkHelper::convert_field(ColumnId id, const TabletColumn& c) {
    LogicalType type = c.type();

    TypeInfoPtr type_info = nullptr;
    if (type == TYPE_ARRAY || type == TYPE_MAP || type == TYPE_STRUCT || type == TYPE_DECIMAL32 ||
        type == TYPE_DECIMAL64 || type == TYPE_DECIMAL128 || type == TYPE_DECIMAL256) {
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
    f.set_is_virtual(c.is_virtual_column());

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

    const uint64_t final_offset = static_cast<uint64_t>(len) * num_rows;
    new_offset.resize_uninitialized(num_rows + 1, final_offset);
    new_bytes.assign(num_rows * len, 0); // padding 0

    size_t from = 0;
    offset.visit_storage([&](const auto& offsets_buf) {
        const auto* __restrict offset_data = offsets_buf.data();
        for (size_t j = 0; j < num_rows; ++j) {
            size_t copy_data_len = std::min<size_t>(len, offset_data[j + 1] - offset_data[j]);
            strings::memcpy_inlined(new_bytes.data() + from, bytes.data() + offset_data[j], copy_data_len);
            from += len; // no copy data will be 0
        }
    });

    new_offset.visit_storage([&](auto& offsets_buf) {
        using OffsetValue = typename std::decay_t<decltype(offsets_buf)>::value_type;
        auto* __restrict offset_data = offsets_buf.data();
        for (size_t j = 0; j <= num_rows; ++j) {
            offset_data[j] = static_cast<OffsetValue>(static_cast<uint64_t>(len) * j);
        }
    });

    if (field.is_nullable()) {
        auto* nullable_column = down_cast<NullableColumn*>(column);
        auto null_column = NullColumn::static_pointer_cast(std::move(*nullable_column->null_column()).mutate());
        auto new_column = NullableColumn::create(std::move(new_binary), std::move(null_column));
        new_column->swap_column(*column);
    } else {
        new_binary->swap_column(*column);
    }
}

void ChunkHelper::padding_char_columns(const std::vector<size_t>& char_column_indexes, const Schema& schema,
                                       const starrocks::TabletSchemaCSPtr& tschema, Chunk* chunk) {
    for (auto field_index : char_column_indexes) {
        Column* column = chunk->get_column_raw_ptr_by_index(field_index);
        padding_char_column(tschema, *schema.field(field_index), column);
    }
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

namespace {
bool check_json_schema_compatibility(const Chunk* one, const Chunk* two) {
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
} // namespace

Status ChunkAccumulator::push(ChunkPtr&& chunk) {
    size_t input_rows = chunk->num_rows();
    // TODO: optimize for zero-copy scenario
    // Cut the input chunk into pieces if larger than desired
    for (size_t start = 0; start < input_rows;) {
        size_t remain_rows = input_rows - start;
        size_t need_rows = 0;
        if (_tmp_chunk) {
            need_rows = std::min(_desired_size - _tmp_chunk->num_rows(), remain_rows);
            // Check JSON schema compatibility before appending
            if (!check_json_schema_compatibility(_tmp_chunk.get(), chunk.get())) {
                // Schema mismatch, output current chunk and create a new one
                _output.emplace_back(std::move(_tmp_chunk));
                _tmp_chunk = chunk->clone_empty(_desired_size);
                TRY_CATCH_BAD_ALLOC(_tmp_chunk->append(*chunk, start, need_rows));
            } else {
                TRY_CATCH_BAD_ALLOC(_tmp_chunk->append(*chunk, start, need_rows));
            }
            RETURN_IF_ERROR(_tmp_chunk->capacity_limit_reached());
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
    return check_json_schema_compatibility(one, two);
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

CommonExprEvalScopeGuard::CommonExprEvalScopeGuard(const ChunkPtr& chunk,
                                                   const std::map<SlotId, ExprContext*>& common_expr_ctxs)
        : _chunk(chunk), _common_expr_ctxs(common_expr_ctxs) {}

CommonExprEvalScopeGuard::~CommonExprEvalScopeGuard() {
    for (const auto& [slot_id, _] : _common_expr_ctxs) {
        _chunk->remove_column_by_slot_id(slot_id);
    }
}

Status CommonExprEvalScopeGuard::evaluate() {
    for (const auto& [slot_id, ctx] : _common_expr_ctxs) {
        ASSIGN_OR_RETURN(auto column, ctx->evaluate(_chunk.get()));
        _chunk->append_column(std::move(column), slot_id);
    }
    return Status::OK();
}

} // namespace starrocks
