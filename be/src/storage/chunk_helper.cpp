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

#include "base/coding.h"
#include "base/simd/simd.h"
#include "column/adaptive_nullable_column.h"
#include "column/array_column.h"
#include "column/chunk.h"
#include "column/column_helper.h"
#include "column/column_visitor_adapter.h"
#include "column/map_column.h"
#include "column/runtime_type_traits.h"
#include "column/schema.h"
#include "column/storage_column_traits.h"
#include "column/struct_column.h"
#include "column/vectorized_fwd.h"
#include "exprs/expr_context.h"
#include "gutil/strings/fastmem.h"
#include "runtime/chunk_accumulator.h"
#include "storage/tablet_schema.h"
#include "types/olap_type_infra.h"
#include "types/storage_type_traits.h"

namespace starrocks {

starrocks::Schema ChunkHelper::convert_schema(const starrocks::TabletSchemaCSPtr& schema) {
    return starrocks::Schema(schema->schema());
}

starrocks::Schema ChunkHelper::convert_schema(const starrocks::TabletSchemaCSPtr& schema,
                                              const std::vector<ColumnId>& cids) {
    return starrocks::Schema(schema->schema(), cids);
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
    auto new_binary = BinaryColumn::create();
    Offsets& new_offset = new_binary->get_offset();
    Bytes& new_bytes = new_binary->get_bytes();

    // |schema| maybe partial columns in vertical compaction, so get char column length by name.
    uint32_t len = tschema->column(tschema->field_index(field.name())).length();

    const uint64_t final_offset = static_cast<uint64_t>(len) * num_rows;
    new_offset.resize_uninitialized(num_rows + 1, final_offset);
    new_bytes.assign(final_offset, 0); // padding 0

    size_t from = 0;
    offset.visit_storage([&](const auto& offsets_buf) {
        const auto* __restrict offset_data = offsets_buf.data();
        // Optimization: skip memcpy for null rows when there are many nulls.
        // The buffer is pre-zeroed, so null rows already have valid padding.
        if (field.is_nullable()) {
            auto* nullable_column = down_cast<NullableColumn*>(column);
            if (!nullable_column->has_null()) {
                for (size_t j = 0; j < num_rows; ++j) {
                    size_t copy_data_len = std::min<size_t>(len, offset_data[j + 1] - offset_data[j]);
                    strings::memcpy_inlined(new_bytes.data() + from, bytes.data() + offset_data[j], copy_data_len);
                    from += len;
                }
            } else {
                const uint8_t* null_data = nullable_column->null_column()->get_data().data();
                size_t null_count = SIMD::count_nonzero(null_data, num_rows);
                if (null_count > num_rows / 8) {
                    for (size_t j = 0; j < num_rows; ++j) {
                        if (!null_data[j]) {
                            size_t copy_data_len = std::min<size_t>(len, offset_data[j + 1] - offset_data[j]);
                            strings::memcpy_inlined(new_bytes.data() + from, bytes.data() + offset_data[j],
                                                    copy_data_len);
                        }
                        from += len;
                    }
                } else {
                    for (size_t j = 0; j < num_rows; ++j) {
                        size_t copy_data_len = std::min<size_t>(len, offset_data[j + 1] - offset_data[j]);
                        strings::memcpy_inlined(new_bytes.data() + from, bytes.data() + offset_data[j], copy_data_len);
                        from += len;
                    }
                }
            }
        } else {
            for (size_t j = 0; j < num_rows; ++j) {
                size_t copy_data_len = std::min<size_t>(len, offset_data[j + 1] - offset_data[j]);
                strings::memcpy_inlined(new_bytes.data() + from, bytes.data() + offset_data[j], copy_data_len);
                from += len;
            }
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
