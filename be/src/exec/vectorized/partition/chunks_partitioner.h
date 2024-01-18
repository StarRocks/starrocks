// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#pragma once

#include <any>
#include <queue>

#include "column/chunk.h"
#include "column/vectorized_fwd.h"
#include "common/statusor.h"
#include "exprs/expr_context.h"
#include "partition_hash_variant.h"
#include "runtime/current_thread.h"

namespace starrocks::vectorized {

struct PartitionColumnType {
    TypeDescriptor result_type;
    bool is_nullable;
};

class ChunksPartitioner;

using ChunksPartitionerPtr = std::shared_ptr<ChunksPartitioner>;

class ChunksPartitioner {
public:
    ChunksPartitioner(const bool has_nullable_partition_column, const std::vector<ExprContext*>& partition_exprs,
                      std::vector<PartitionColumnType> partition_types);

    Status prepare(RuntimeState* state);

    // Chunk is divided into multiple parts by partition columns,
    // and each partition corresponds to a key-value pair in the hash map.
    // Params:
    // @chunk:
    //      The chunk added to the hash map.
    // @new_partition_cb: void(size_t partition_idx)
    //      called when coming a new key not in the hash map.
    // @partition_chunk_consumer: void(size_t partition_idx, const ChunkPtr& chunk)
    //      called for each partition with enough num rows after adding chunk to the hash map.
    template <bool EnablePassthrough, typename NewPartitionCallback, typename PartitionChunkConsumer>
    Status offer(const ChunkPtr& chunk, NewPartitionCallback&& new_partition_cb,
                 PartitionChunkConsumer&& partition_chunk_consumer) {
        DCHECK(!_partition_it.has_value());

        if (!_is_passthrough) {
            for (size_t i = 0; i < _partition_exprs.size(); i++) {
                ASSIGN_OR_RETURN(_partition_columns[i], _partition_exprs[i]->evaluate(chunk.get()));
<<<<<<< HEAD

                if (false) {
                }
#define HASH_MAP_METHOD(NAME)                                                                                     \
    else if (_hash_map_variant.type == PartitionHashMapVariant::Type::NAME) {                                     \
        if (!_partition_columns[i]->is_nullable()) {                                                              \
            _partition_columns[i] = NullableColumn::create(_partition_columns[i],                                 \
                                                           NullColumn::create(_partition_columns[i]->size(), 0)); \
        }                                                                                                         \
    }
                APPLY_FOR_PARTITION_VARIANT_NULL(HASH_MAP_METHOD)
#undef HASH_MAP_METHOD
=======
                if (_hash_map_variant.is_nullable() && !_partition_columns[i]->is_nullable()) {
                    _partition_columns[i] = NullableColumn::create(
                            _partition_columns[i], NullColumn::create(_partition_columns[i]->size(), 0));
                }
>>>>>>> 2.5.18
            }
        }

        TRY_CATCH_BAD_ALLOC(_hash_map_variant.visit([&](auto& hash_map_with_key) {
            _split_chunk_by_partition<EnablePassthrough>(
                    *hash_map_with_key, chunk, std::forward<NewPartitionCallback>(new_partition_cb),
                    std::forward<PartitionChunkConsumer>(partition_chunk_consumer));
        }));

        return Status::OK();
    }

    // Number of partitions
    int32_t num_partitions() const { return _hash_map_variant.size(); }

    bool is_passthrough() const { return _is_passthrough; }

    bool is_passthrough_buffer_empty() const { return _passthrough_buffer.empty(); }

    bool is_hash_map_eos() const { return _hash_map_eos && (!_hash_map_variant.is_nullable() || _null_key_eos); }

    // Consumers consume from the hash map
    // @Params:
    // @consumer: bool consumer(int32_t partition_idx, const ChunkPtr& chunk)
    //      The return value of the consumer denote whether to continue or not
    template <typename Consumer>
    Status consume_from_hash_map(Consumer&& consumer) {
        if (is_hash_map_eos()) {
            return Status::OK();
        }

        TRY_CATCH_BAD_ALLOC(_hash_map_variant.visit([&](auto& hash_map_with_key) {
            // First, fetch chunks from hash map
            bool continue_consume;
            _fetch_chunks_from_hash_map(*hash_map_with_key, consumer, continue_consume);
            if (continue_consume && _hash_map_eos) {
                // Second, fetch chunks from null_key_value if any
                if constexpr (std::decay_t<decltype(*hash_map_with_key)>::is_nullable) {
                    _fetch_chunks_from_null_key_value(*hash_map_with_key, consumer);
                }
            }
        }));

        if (is_hash_map_eos()) {
            _hash_map_variant.reset();
            _mem_pool.reset();
            _obj_pool.reset();
        }

        return Status::OK();
    }

    // Fetch one chunk from passthrough buffer if any
    ChunkPtr consume_from_passthrough_buffer();

private:
    bool _is_partition_columns_fixed_size(const std::vector<ExprContext*>& partition_expr_ctxs,
                                          const std::vector<PartitionColumnType>& partition_types, size_t* max_size,
                                          bool* has_null);
    void _init_hash_map_variant();

    template <bool EnablePassthrough, typename HashMapWithKey, typename NewPartitionCallback,
              typename PartitionChunkConsumer>
    void _split_chunk_by_partition(HashMapWithKey& hash_map_with_key, const ChunkPtr& chunk,
                                   NewPartitionCallback&& new_partition_cb,
                                   PartitionChunkConsumer&& partition_chunk_consumer) {
        if (!_is_passthrough) {
            _is_passthrough = hash_map_with_key.template append_chunk<EnablePassthrough>(
                    chunk, _partition_columns, _mem_pool.get(), _obj_pool.get(),
                    std::forward<NewPartitionCallback>(new_partition_cb),
                    std::forward<PartitionChunkConsumer>(partition_chunk_consumer));
        }
        if (_is_passthrough) {
            std::lock_guard<std::mutex> l(_buffer_lock);
            _passthrough_buffer.push(chunk);
        }
    }

    // Fetch chunks from hash map, return true if reaches eos
    template <typename HashMapWithKey, typename Consumer>
    void _fetch_chunks_from_hash_map(HashMapWithKey& hash_map_with_key, Consumer&& consumer, bool& continue_consume) {
        continue_consume = true;
        if (_hash_map_eos) {
            return;
        }
        if (!_partition_it.has_value()) {
            _partition_it = hash_map_with_key.hash_map.begin();
        }

        using PartitionIterator = typename HashMapWithKey::Iterator;
        PartitionIterator partition_it = std::any_cast<PartitionIterator>(_partition_it);
        const PartitionIterator partition_end = hash_map_with_key.hash_map.end();

        using ChunkIterator = typename std::vector<ChunkPtr>::iterator;
        ChunkIterator chunk_it;
        DeferOp defer([&]() {
            if (partition_it == partition_end) {
                _hash_map_eos = true;
                _partition_it.reset();
                _chunk_it.reset();
            } else {
                _partition_it = partition_it;
                _chunk_it = chunk_it;
            }
        });

        while (partition_it != partition_end) {
            std::vector<ChunkPtr>& chunks = partition_it->second->chunks;
            const auto partition_idx = partition_it->second->partition_idx;
            if (!_chunk_it.has_value()) {
                _chunk_it = chunks.begin();
            }

            chunk_it = std::any_cast<ChunkIterator>(_chunk_it);
            ChunkIterator chunk_end = chunks.end();

            while (chunk_it != chunk_end) {
                if (!consumer(partition_idx, *chunk_it++)) {
                    // Fetch suspend, and it may proceed the next call.
                    continue_consume = false;
                    return;
                }
            }

            // Move to next partition
            partition_it->second->reset();
            ++partition_it;
            _chunk_it.reset();
        }
    }

    // Fetch chunks from HashMapWithKey.null_key_value, return true if reaches eos
    template <typename HashMapWithKey, typename Consumer>
    void _fetch_chunks_from_null_key_value(HashMapWithKey& hash_map_with_key, Consumer&& consumer) {
        if (_null_key_eos) {
            return;
        }

        std::vector<ChunkPtr>& chunks = hash_map_with_key.null_key_value.chunks;

        if (!_chunk_it.has_value()) {
            _chunk_it = chunks.begin();
        }

        using ChunkIterator = typename std::vector<ChunkPtr>::iterator;
        ChunkIterator chunk_it = std::any_cast<ChunkIterator>(_chunk_it);
        const ChunkIterator chunk_end = chunks.end();

        DeferOp defer([&]() {
            if (chunk_it == chunk_end) {
                hash_map_with_key.null_key_value.reset();
                _null_key_eos = true;
                _chunk_it.reset();
            } else {
                _chunk_it = chunk_it;
            }
        });

        while (chunk_it != chunk_end) {
            if (!consumer(hash_map_with_key.kNullKeyPartitionIdx, *chunk_it++)) {
                // Fetch suspend, and it may proceed the next call.
                return;
            }
        }
    }

    const bool _has_nullable_partition_column;
    const std::vector<ExprContext*> _partition_exprs;
    const std::vector<PartitionColumnType> _partition_types;

    RuntimeState* _state = nullptr;
    std::unique_ptr<MemPool> _mem_pool = nullptr;
    std::unique_ptr<ObjectPool> _obj_pool = nullptr;

    Columns _partition_columns;
    // Hash map which holds chunks of different partitions
    PartitionHashMapVariant _hash_map_variant;

    bool _is_passthrough = false;
    // We simply buffer chunks when partition cardinality is high
    std::queue<ChunkPtr> _passthrough_buffer;
    std::mutex _buffer_lock;

    // Iterator of partitions
    std::any _partition_it;
    // Iterator of chunks of current partition
    std::any _chunk_it;

    bool _hash_map_eos = false;
    bool _null_key_eos = false;
}; // namespace starrocks::vectorized
} // namespace starrocks::vectorized
