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

#include "exec/tablet_sink_index_channel.h"

namespace starrocks::stream_load {
// TabletSinkSender will control one index/table's send chunks.
class TabletSinkSender {
public:
    TabletSinkSender(PUniqueId load_id, int64_t txn_id, IndexIdToTabletBEMap index_id_to_tablet_be_map,
                     OlapTablePartitionParam* vectorized_partition, std::vector<IndexChannel*> channels,
                     std::unordered_map<int64_t, NodeChannel*> node_channels,
                     std::vector<ExprContext*> output_expr_ctxs, bool enable_replicated_storage,
                     TWriteQuorumType::type write_quorum_type, int num_repicas);

    virtual ~TabletSinkSender() = default;

public:
    virtual Status send_chunk(const OlapTableSchemaParam* schema, const std::vector<OlapTablePartition*>& partitions,
                              const std::vector<uint32_t>& tablet_indexes,
                              const std::vector<uint16_t>& validate_select_idx,
                              std::unordered_map<int64_t, std::set<int64_t>>& index_id_partition_id, Chunk* chunk);

    virtual Status try_open(RuntimeState* state);
    virtual Status open_wait();
    // async close interface: try_close() -> [is_close_done()] -> close_wait()
    // if is_close_done() return true, close_wait() will not block
    // otherwise close_wait() will block
    virtual Status try_close(RuntimeState* state);
    virtual Status close_wait(RuntimeState* state, Status close_status, TabletSinkProfile* ts_profile);

    virtual bool is_open_done();
    virtual bool is_full();
    virtual bool is_close_done();

    virtual bool get_immutable_partition_ids(std::set<int64_t>* partition_ids);

    // mutable
    IndexIdToTabletBEMap* index_id_to_tablet_be_map() { return &_index_id_to_tablet_be_map; }

    void for_each_node_channel(const std::function<void(NodeChannel*)>& func) {
        for (auto& it : _node_channels) {
            func(it.second);
        }
    }

    void for_each_index_channel(const std::function<void(NodeChannel*)>& func) {
        for (auto& index_channel : _channels) {
            index_channel->for_each_node_channel(func);
        }
    }

protected:
    Status _send_chunk_by_node(Chunk* chunk, IndexChannel* channel, const std::vector<uint16_t>& selection_idx);

    void _mark_as_failed(const NodeChannel* ch) { _failed_channels.insert(ch->node_id()); }
    bool _is_failed_channel(const NodeChannel* ch) { return _failed_channels.count(ch->node_id()) != 0; }
    bool _has_intolerable_failure() {
        if (_write_quorum_type == TWriteQuorumType::ALL) {
            return _failed_channels.size() > 0;
        } else if (_write_quorum_type == TWriteQuorumType::ONE) {
            return _failed_channels.size() >= _num_repicas;
        } else {
            return _failed_channels.size() >= ((_num_repicas + 1) / 2);
        }
    }

protected:
    // unique load id
    PUniqueId _load_id;
    int64_t _txn_id = -1;
    // index_id -> (tablet_id -> bes) map
    IndexIdToTabletBEMap _index_id_to_tablet_be_map;
    // partition schema
    OlapTablePartitionParam* _vectorized_partition = nullptr;
    // index_channel
    std::vector<IndexChannel*> _channels;
    std::unordered_map<int64_t, NodeChannel*> _node_channels;
    std::vector<ExprContext*> _output_expr_ctxs;
    bool _enable_replicated_storage{false};
    TWriteQuorumType::type _write_quorum_type = TWriteQuorumType::MAJORITY;
    int _num_repicas = -1;

    bool _open_done = false;
    bool _close_done = false;
    // one chunk selection for BE node
    std::vector<uint32_t> _node_select_idx;
    std::vector<int64_t> _tablet_ids;
    std::set<int64_t> _failed_channels;
};

} // namespace starrocks::stream_load
