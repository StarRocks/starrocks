// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/be/src/runtime/tablets_channel.h

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include <cstdint>
#include <unordered_map>
#include <utility>
#include <vector>

#include "column/chunk.h"
#include "gen_cpp/InternalService_types.h"
#include "gen_cpp/Types_types.h"
#include "gen_cpp/internal_service.pb.h"
#include "runtime/descriptors.h"
#include "runtime/global_dicts.h"
#include "runtime/mem_tracker.h"
#include "util/bitmap.h"
#include "util/priority_thread_pool.hpp"
#include "util/uid_util.h"

namespace starrocks {

namespace vectorized {
class DeltaWriter;
}

struct TabletsChannelKey {
    UniqueId id;
    int64_t index_id;

    TabletsChannelKey(const PUniqueId& pid, int64_t index_id_) : id(pid), index_id(index_id_) {}

    ~TabletsChannelKey() noexcept = default;

    bool operator==(const TabletsChannelKey& rhs) const noexcept { return index_id == rhs.index_id && id == rhs.id; }

    std::string to_string() const;
};

std::ostream& operator<<(std::ostream& os, const TabletsChannelKey& key);

class DeltaWriter;
class OlapTableSchemaParam;

// Write channel for a particular (load, index).
class TabletsChannel {
public:
    TabletsChannel(const TabletsChannelKey& key, MemTracker* mem_tracker);

    ~TabletsChannel();

    Status open(const PTabletWriterOpenRequest& params);

    // no-op when this channel has been closed or cancelled
    Status add_batch(const PTabletWriterAddBatchRequest& batch);

    // no-op when this channel has been closed or cancelled
    Status add_chunk(const PTabletWriterAddChunkRequest& batch);

    // Mark sender with 'sender_id' as closed.
    // If all senders are closed, close this channel, set '*finished' to true, update 'tablet_vec'
    // to include all tablets written in this channel.
    // no-op when this channel has been closed or cancelled
    Status close(int sender_id, bool* finished, const google::protobuf::RepeatedField<int64_t>& partition_ids,
                 google::protobuf::RepeatedPtrField<PTabletInfo>* tablet_vec);

    // no-op when this channel has been closed or cancelled
    Status cancel();

    // upper application may call this to try to reduce the mem usage of this channel.
    // eg. flush the largest memtable async.
    // no-op when this channel has been closed or cancelled.
    // return Status::OK if flush async success or no-op.
    Status reduce_mem_usage_async(const std::set<int64_t>& flush_tablet_ids, int64_t* tablet_id,
                                  int64_t* tablet_mem_consumption);
    // wait tablet memtables in flush queue to be flushed.
    Status wait_mem_usage_reduced(int64_t tablet_id);

    int64_t mem_consumption() const { return _mem_tracker->consumption(); }

private:
    // open all writer
    Status _open_all_writers(const PTabletWriterOpenRequest& params);

    Status _build_chunk_meta(const ChunkPB& pb_chunk);

    // id of this load channel
    TabletsChannelKey _key;

    // make execute sequece
    std::mutex _global_lock;
    static constexpr int k_shard_size = 127;
    std::array<std::mutex, k_shard_size + 1> _tablet_locks;

    enum State {
        kInitialized,
        kOpened,
        kFinished // closed or cancelled
    };
    State _state;

    // initialized in open function
    int64_t _txn_id = -1;
    int64_t _index_id = -1;
    OlapTableSchemaParam* _schema = nullptr;
    TupleDescriptor* _tuple_desc = nullptr;
    // row_desc used to construct
    RowDescriptor* _row_desc = nullptr;

    // next sequence we expect
    int _num_remaining_senders = 0;
    std::vector<int64_t> _next_seqs;
    Bitmap _closed_senders;
    // status to return when operate on an already closed/cancelled channel
    // currently it's OK.
    Status _close_status;

    // tablet_id -> TabletChannel
    std::unordered_map<int64_t, std::shared_ptr<DeltaWriter>> _tablet_writers;

    std::unordered_set<int64_t> _partition_ids;

    std::unique_ptr<MemTracker> _mem_tracker;

    static std::atomic<uint64_t> _s_tablet_writer_count;

    bool _is_vectorized = true;
    vectorized::RuntimeChunkMeta _chunk_meta;
    std::unordered_map<int64_t, uint32_t> _tablet_id_to_sorted_indexes;
    // tablet_id -> TabletChannel
    std::unordered_map<int64_t, std::shared_ptr<vectorized::DeltaWriter>> _vectorized_tablet_writers;

    vectorized::GlobalDictByNameMaps _global_dicts;
    std::unique_ptr<MemPool> _mem_pool;
};

} // namespace starrocks
