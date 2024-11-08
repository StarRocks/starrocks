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

#include "exec/partition/chunks_partitioner.h"

#include <memory>
#include <utility>

#include "exec/limited_pipeline_chunk_buffer.h"
#include "exprs/expr.h"
#include "exprs/expr_context.h"
#include "types/logical_type.h"

namespace starrocks {

ChunksPartitioner::ChunksPartitioner(const bool has_nullable_partition_column,
                                     const std::vector<ExprContext*>& partition_exprs,
                                     std::vector<PartitionColumnType> partition_types, MemPool* mem_pool)
        : _has_nullable_partition_column(has_nullable_partition_column),
          _partition_exprs(partition_exprs),
          _partition_types(std::move(partition_types)),
          _mem_pool(mem_pool) {
    _partition_columns.resize(partition_exprs.size());
}

Status ChunksPartitioner::prepare(RuntimeState* state, RuntimeProfile* runtime_profile, bool enable_pre_agg) {
    _state = state;
    _obj_pool = std::make_unique<ObjectPool>();
    _init_hash_map_variant();

    _statistics.chunk_buffer_peak_memory = ADD_PEAK_COUNTER(runtime_profile, "ChunkBufferPeakMem", TUnit::BYTES);
    _statistics.chunk_buffer_peak_size = ADD_PEAK_COUNTER(runtime_profile, "ChunkBufferPeakSize", TUnit::BYTES);

    _limited_buffer = std::make_unique<LimitedPipelineChunkBuffer<ChunksPartitionStatistics>>(
            &_statistics, 1, config::local_exchange_buffer_mem_limit_per_driver,
            state->chunk_size() * config::streaming_agg_chunk_buffer_size);
    if (enable_pre_agg) {
        _hash_map_variant.set_enable_pre_agg();
    }
    return Status::OK();
}

ChunkPtr ChunksPartitioner::consume_from_passthrough_buffer() {
    if (_limited_buffer->is_empty()) {
        return nullptr;
    }
    ChunkPtr chunk = nullptr;
    return _limited_buffer->pull();
}

bool ChunksPartitioner::_is_partition_columns_fixed_size(const std::vector<ExprContext*>& partition_expr_ctxs,
                                                         const std::vector<PartitionColumnType>& partition_types,
                                                         size_t* max_size, bool* has_null) {
    size_t size = 0;
    *has_null = false;

    for (size_t i = 0; i < partition_expr_ctxs.size(); i++) {
        ExprContext* ctx = partition_expr_ctxs[i];
        if (partition_types[i].is_nullable) {
            *has_null = true;
            size += 1; // 1 bytes for  null flag.
        }
        LogicalType ltype = ctx->root()->type().type;
        size_t byte_size = get_size_of_fixed_length_type(ltype);
        if (byte_size == 0) return false;
        size += byte_size;
    }
    *max_size = size;
    return true;
}

void ChunksPartitioner::_init_hash_map_variant() {
    PartitionHashMapVariant::Type type = PartitionHashMapVariant::Type::phase1_slice;
    if (_has_nullable_partition_column) {
        switch (_partition_exprs.size()) {
        case 0:
            break;
        case 1: {
            auto partition_expr = _partition_exprs[0];
            switch (partition_expr->root()->type().type) {
#define M(TYPE, VALUE)                                             \
    case TYPE: {                                                   \
        type = PartitionHashMapVariant::Type::phase1_null_##VALUE; \
        break;                                                     \
    }
                M(TYPE_BOOLEAN, uint8);
                M(TYPE_TINYINT, int8);
                M(TYPE_SMALLINT, int16);
                M(TYPE_INT, int32);
                M(TYPE_DECIMAL32, decimal32);
                M(TYPE_BIGINT, int64);
                M(TYPE_DECIMAL64, decimal64);
                M(TYPE_DATE, date);
                M(TYPE_DATETIME, timestamp);
                M(TYPE_DECIMAL128, decimal128);
                M(TYPE_LARGEINT, int128);
                M(TYPE_CHAR, string);
                M(TYPE_VARCHAR, string);
#undef M
            default:
                break;
            }
        } break;
        default:
            break;
        }
    } else {
        switch (_partition_exprs.size()) {
        case 0:
            break;
        case 1: {
            auto partition_expr_ctx = _partition_exprs[0];
            switch (partition_expr_ctx->root()->type().type) {
#define M(TYPE, VALUE)                                        \
    case TYPE: {                                              \
        type = PartitionHashMapVariant::Type::phase1_##VALUE; \
        break;                                                \
    }
                M(TYPE_BOOLEAN, uint8);
                M(TYPE_TINYINT, int8);
                M(TYPE_SMALLINT, int16);
                M(TYPE_INT, int32);
                M(TYPE_DECIMAL32, decimal32);
                M(TYPE_BIGINT, int64);
                M(TYPE_DECIMAL64, decimal64);
                M(TYPE_DATE, date);
                M(TYPE_DATETIME, timestamp);
                M(TYPE_LARGEINT, int128);
                M(TYPE_DECIMAL128, decimal128);
                M(TYPE_CHAR, string);
                M(TYPE_VARCHAR, string);
#undef M
            default:
                break;
            }
        } break;
        default:
            break;
        }
    }

    bool has_null_column = false;
    int fixed_byte_size = 0;
    // this optimization don't need to be limited to multi-column partition.
    // single column like float/double/decimal/largeint could also be applied to.
    if (type == PartitionHashMapVariant::Type::phase1_slice) {
        size_t max_size = 0;
        if (_is_partition_columns_fixed_size(_partition_exprs, _partition_types, &max_size, &has_null_column)) {
            // we need reserve a byte for serialization length for nullable columns
            if (max_size < 4 || (!has_null_column && max_size == 4)) {
                type = PartitionHashMapVariant::Type::phase1_slice_fx4;
            } else if (max_size < 8 || (!has_null_column && max_size == 8)) {
                type = PartitionHashMapVariant::Type::phase1_slice_fx8;
            } else if (max_size < 16 || (!has_null_column && max_size == 16)) {
                type = PartitionHashMapVariant::Type::phase1_slice_fx16;
            }
            if (!has_null_column) {
                fixed_byte_size = max_size;
            }
        }
    }
    _hash_map_variant.init(_state, type);

    _hash_map_variant.visit([&](auto& hash_map_with_key) {
        if constexpr (std::decay_t<decltype(*hash_map_with_key)>::is_fixed_length_slice) {
            hash_map_with_key->has_null_column = has_null_column;
            hash_map_with_key->fixed_byte_size = fixed_byte_size;
        }
    });
}
} // namespace starrocks
