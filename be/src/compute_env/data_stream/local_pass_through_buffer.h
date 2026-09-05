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

#pragma once
#include <map>
#include <memory>

#include "base/hash/hash.h"
#include "base/hash/hash_std.hpp"
#include "base/uid_util.h"
#include "column/column_hash.h"
#include "column/vectorized_fwd.h"
#include "gen_cpp/Types_types.h" // for TUniqueId
#include "runtime/descriptors.h" // for PlanNodeId

namespace starrocks {

// To manage pass through chunks between sink/sources in the same process.
using ChunkUniquePtrVector = std::vector<std::pair<ChunkUniquePtr, int32_t>>;
class PassThroughChannel;

// A channel is shared by the local sink and the local receiver of one
// [fragment_instance_id, dest_node_id] pair, and both of them keep using it while they are being
// torn down. Ownership is therefore shared: the PassThroughChunkBuffer publishes the channel, but
// it does not outlive every user of it. See PassThroughContext below.
using PassThroughChannelPtr = std::shared_ptr<PassThroughChannel>;

class PassThroughChunkBuffer {
public:
    using Key = std::tuple<TUniqueId, PlanNodeId>;

    struct KeyHash {
        size_t operator()(const Key& key) const {
            uint64_t hash = CRC_HASH_SEEDS::CRC_HASH_SEED1;
            hash = crc_hash_uint64(std::get<0>(key).hi, hash);
            hash = crc_hash_uint64(std::get<0>(key).lo, hash);
            hash = crc_hash_uint64(std::get<1>(key), hash);
            return hash;
        }
    };
    PassThroughChunkBuffer(const TUniqueId& query_id);
    ~PassThroughChunkBuffer();
    PassThroughChannelPtr get_or_create_channel(const Key& key);
    int ref() { return ++_ref_count; }
    int unref() {
        _ref_count -= 1;
        return _ref_count;
    }

private:
    std::mutex _mutex;
    const TUniqueId _query_id;
    std::unordered_map<Key, PassThroughChannelPtr, KeyHash> _key_to_channel;
    int _ref_count{1};
};

class PassThroughContext {
public:
    PassThroughContext(PassThroughChunkBuffer* chunk_buffer, const TUniqueId& fragment_instance_id, PlanNodeId node_id)
            : _chunk_buffer(chunk_buffer), _fragment_instance_id(fragment_instance_id), _node_id(node_id) {}
    void init();
    void append_chunk(int sender_id, const Chunk* chunk, size_t chunk_size, int32_t driver_sequence);
    void pull_chunks(int sender_id, ChunkUniquePtrVector* chunks, std::vector<size_t>* bytes);
    int64_t total_bytes() const;
    // Mark the shared channel cancelled so the sink stops appending; called by the receiver.
    void set_cancelled();
    bool is_cancelled() const;

private:
    // Only used by init(), which runs while the owning fragment still holds its
    // PassThroughChunkBufferGuard. Never dereferenced afterwards.
    PassThroughChunkBuffer* _chunk_buffer;
    TUniqueId _fragment_instance_id;
    PlanNodeId _node_id;
    // Owning reference. The PassThroughChunkBuffer is released per fragment
    // (FragmentContext::destroy_pass_through_chunk_buffer), which can happen before the pipelines
    // that hold this context are torn down; keeping a strong reference here is what makes the
    // channel outlive both the sink and the receiver instead of only the buffer.
    PassThroughChannelPtr _channel;
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
    void close();

private:
    std::mutex _mutex;
    std::unordered_map<TUniqueId, PassThroughChunkBuffer*> _query_id_to_buffer;
};

} // namespace starrocks
