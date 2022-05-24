// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#include "exec/vectorized/partition/chunks_partitioner.h"

#include "exprs/expr.h"
#include "exprs/expr_context.h"

namespace starrocks::vectorized {

// TODO(hcf) move to utils
#define RETURN_PTYPE_BYTE_SIZE(TYPE, SIZE)                                           \
    case TYPE:                                                                       \
        static_assert(sizeof(vectorized::RunTimeTypeTraits<TYPE>::CppType) == SIZE); \
        return SIZE;

inline static int get_byte_size_of_primitive_type(PrimitiveType type) {
    switch (type) {
        RETURN_PTYPE_BYTE_SIZE(TYPE_NULL, 1);
        RETURN_PTYPE_BYTE_SIZE(TYPE_BOOLEAN, 1);
        RETURN_PTYPE_BYTE_SIZE(TYPE_TINYINT, 1);

        RETURN_PTYPE_BYTE_SIZE(TYPE_SMALLINT, 2);

        RETURN_PTYPE_BYTE_SIZE(TYPE_DECIMAL32, 4);
        RETURN_PTYPE_BYTE_SIZE(TYPE_DATE, 4);
        RETURN_PTYPE_BYTE_SIZE(TYPE_INT, 4);
        RETURN_PTYPE_BYTE_SIZE(TYPE_FLOAT, 4);

        RETURN_PTYPE_BYTE_SIZE(TYPE_DECIMAL64, 8);
        RETURN_PTYPE_BYTE_SIZE(TYPE_BIGINT, 8);
        RETURN_PTYPE_BYTE_SIZE(TYPE_TIME, 8);
        RETURN_PTYPE_BYTE_SIZE(TYPE_DATETIME, 8);
        RETURN_PTYPE_BYTE_SIZE(TYPE_DOUBLE, 8);

        RETURN_PTYPE_BYTE_SIZE(TYPE_DECIMAL128, 16);
        RETURN_PTYPE_BYTE_SIZE(TYPE_LARGEINT, 16);
        RETURN_PTYPE_BYTE_SIZE(TYPE_DECIMALV2, 16);
    default:
        return 0;
    }
}

ChunksPartitioner::ChunksPartitioner(const bool has_nullable_partition_column,
                                     const std::vector<ExprContext*>& partition_exprs,
                                     const std::vector<PartitionColumnType>& partition_types)
        : _has_nullable_partition_column(has_nullable_partition_column),
          _partition_exprs(partition_exprs),
          _partition_types(partition_types) {
    _partition_columns.resize(partition_exprs.size());
}

Status ChunksPartitioner::prepare(RuntimeState* state) {
    _state = state;
    _mem_pool = std::make_unique<MemPool>();
    _obj_pool = state->obj_pool();
    _init_hash_map_variant();
    return Status::OK();
}

Status ChunksPartitioner::offer(const ChunkPtr& chunk) {
    DCHECK(!_partition_it.has_value());

    for (size_t i = 0; i < _partition_exprs.size(); i++) {
        ASSIGN_OR_RETURN(_partition_columns[i], _partition_exprs[i]->evaluate(chunk.get()));
    }

    if (false) {
    }
#define HASH_MAP_METHOD(NAME)                                                                         \
    else if (_hash_map_variant.type == PartitionHashMapVariant::Type::NAME) {                         \
        TRY_CATCH_BAD_ALLOC(split_chunk_by_partition<decltype(_hash_map_variant.NAME)::element_type>( \
                *_hash_map_variant.NAME, chunk));                                                     \
    }
    APPLY_FOR_PARTITION_VARIANT_ALL(HASH_MAP_METHOD)
#undef HASH_MAP_METHOD

    return Status::OK();
}

int32_t ChunksPartitioner::num_partitions() {
    return _hash_map_variant.size();
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
        size_t byte_size = get_byte_size_of_primitive_type(ptype);
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

#define SET_FIXED_SLICE_HASH_MAP_FIELD(TYPE)                       \
    if (type == PartitionHashMapVariant::Type::TYPE) {             \
        _hash_map_variant.TYPE->has_null_column = has_null_column; \
        _hash_map_variant.TYPE->fixed_byte_size = fixed_byte_size; \
    }
    SET_FIXED_SLICE_HASH_MAP_FIELD(phase1_slice_fx4);
    SET_FIXED_SLICE_HASH_MAP_FIELD(phase1_slice_fx8);
    SET_FIXED_SLICE_HASH_MAP_FIELD(phase1_slice_fx16);
#undef SET_FIXED_SLICE_HASH_MAP_FIELD
}
} // namespace starrocks::vectorized
