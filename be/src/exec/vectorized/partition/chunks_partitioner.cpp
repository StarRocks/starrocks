// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#include "exec/vectorized/partition/chunks_partitioner.h"

#include <utility>

#include "exprs/expr.h"
#include "exprs/expr_context.h"
#include "runtime/primitive_type.h"

namespace starrocks::vectorized {

ChunksPartitioner::ChunksPartitioner(const bool has_nullable_partition_column,
                                     const std::vector<ExprContext*>& partition_exprs,
                                     std::vector<PartitionColumnType> partition_types)
        : _has_nullable_partition_column(has_nullable_partition_column),
          _partition_exprs(partition_exprs),
          _partition_types(std::move(partition_types)) {
    _partition_columns.resize(partition_exprs.size());
}

Status ChunksPartitioner::prepare(RuntimeState* state) {
    _state = state;
    _mem_pool = std::make_unique<MemPool>();
    _obj_pool = std::make_unique<ObjectPool>();
    _init_hash_map_variant();
    return Status::OK();
}

ChunkPtr ChunksPartitioner::consume_from_passthrough_buffer() {
    vectorized::ChunkPtr chunk = nullptr;
    if (_passthrough_buffer.empty()) {
        return chunk;
    }
    {
        std::lock_guard<std::mutex> l(_buffer_lock);
        chunk = _passthrough_buffer.front();
        _passthrough_buffer.pop();
    }
    return chunk;
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
        PrimitiveType ptype = ctx->root()->type().type;
        size_t byte_size = get_size_of_fixed_length_type(ptype);
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
} // namespace starrocks::vectorized
