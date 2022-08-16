// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#pragma once
#include <map>

#include "column/column_hash.h"
#include "column/vectorized_fwd.h"
#include "gen_cpp/Types_types.h" // for TUniqueId
#include "runtime/descriptors.h" // for PlanNodeId

namespace starrocks {

// To manage pass through chunks between sink/sources in the same process.
using ChunkUniquePtrVector = std::vector<std::pair<vectorized::ChunkUniquePtr, int32_t>>;
class PassThroughChannel;

class PassThroughChunkBuffer {
public:
    using Key = std::tuple<TUniqueId, PlanNodeId>;

    struct KeyHash {
        size_t operator()(const Key& key) const {
            uint64_t hash = vectorized::CRC_HASH_SEED1;
            hash = vectorized::crc_hash_uint64(std::get<0>(key).hi, hash);
            hash = vectorized::crc_hash_uint64(std::get<0>(key).lo, hash);
            hash = vectorized::crc_hash_uint64(std::get<1>(key), hash);
            return hash;
        }
    };
    PassThroughChunkBuffer(const TUniqueId& query_id);
    ~PassThroughChunkBuffer();
    PassThroughChannel* get_or_create_channel(const Key& key);
    int ref() { return ++_ref_count; }
    int unref() {
        _ref_count -= 1;
        return _ref_count;
    }

private:
    std::mutex _mutex;
    const TUniqueId _query_id;
    std::unordered_map<Key, PassThroughChannel*, KeyHash> _key_to_channel;
    int _ref_count;
};

class PassThroughContext {
public:
    PassThroughContext(PassThroughChunkBuffer* chunk_buffer, TUniqueId fragment_instance_id, PlanNodeId node_id)
            : _chunk_buffer(chunk_buffer), _fragment_instance_id(fragment_instance_id), _node_id(node_id) {}
    void init();
    void append_chunk(int sender_id, const vectorized::Chunk* chunk, size_t chunk_size, int32_t driver_sequence);
    void pull_chunks(int sender_id, ChunkUniquePtrVector* chunks, std::vector<size_t>* bytes);

private:
    // hold this chunk buffer to avoid early deallocation.
    PassThroughChunkBuffer* _chunk_buffer;
    TUniqueId _fragment_instance_id;
    PlanNodeId _node_id;
    PassThroughChannel* _channel = nullptr;
};

class PassThroughChunkBufferManager {
public:
    // Called when fragment instance is about to open/close
    // We don't care open/close by which fragment instance,
    // just to want to make sure that fragment instances in a query can
    // share the same `PassThroughChunkBuffer*` struct
    void open_fragment_instance(const TUniqueId& query_id);
    void close_fragment_instance(const TUniqueId& query_id);
    PassThroughChunkBuffer* get(const TUniqueId& query_id);

private:
    std::mutex _mutex;
    std::unordered_map<TUniqueId, PassThroughChunkBuffer*> _query_id_to_buffer;
};

} // namespace starrocks
