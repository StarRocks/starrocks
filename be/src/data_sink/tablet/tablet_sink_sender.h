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

#include "common/global_types.h"
#include "data_sink/tablet/range_router.h"
#include "data_sink/tablet/tablet_sink_index_channel.h"
#include "storage/lake/combined_txn_log_writer.h"

namespace starrocks {

class CombinedTxnLogPB;

// Pick which of a tablet's writer nodes a key belongs to, given the key's crc32 hash.
//
// The modulo cannot be taken on the raw hash. The key hash and the hash that chose the TABLET are
// computed identically -- crc32, seed 0, folded per column (_compute_key_hashes here against
// OlapTablePartitionParam::_compute_hashes) -- so for a table whose distribution columns are its key
// columns, e.g. `PRIMARY KEY(k1,k2) DISTRIBUTED BY HASH(k1,k2)`, they are the same number h. The
// tablet is h % T and the node would be h % N, which ties them: every row of tablet t has
// h = t (mod T), so only N / gcd(T, N) of the N nodes are reachable -- 3 of 6 on a 2-bucket
// partition, 2 of 6 on a 3-bucket one, and a single node when T = N. The nodes that can never be
// chosen still cost an open/close round trip, a delta writer and an empty partial txn log through
// the fold, and MultiNodeWriteNodes still reports N, so the profile shows nothing.
//
// Avalanching first breaks the tie. Note a non-zero crc SEED does not: crc is linear, so a constant
// seed only contributes a constant XOR for a fixed-width key, and XOR preserves the low bits -- with
// T a power of two the parity of h is still fixed within a tablet, and h % N keeps that parity, so
// 2 and 4 buckets stay at 3 of 6. fmix32 is a bijection, so the same key still reaches the same node
// from every sink instance -- the property this routing exists for -- while the node now depends on
// all 32 bits rather than agreeing with the tablet on the low ones.
inline uint32_t multi_node_write_node_slot(uint32_t key_hash, size_t num_nodes) {
    uint32_t h = key_hash;
    h ^= h >> 16;
    h *= 0x85ebca6b;
    h ^= h >> 13;
    h *= 0xc2b2ae35;
    h ^= h >> 16;
    return h % num_nodes;
}

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
                              const std::vector<uint32_t>& record_hashes,
                              const std::vector<uint16_t>& validate_select_idx,
                              std::unordered_map<int64_t, std::set<int64_t>>& index_id_partition_id, Chunk* chunk);

    virtual Status try_open(RuntimeState* state);
    virtual Status open_wait();
    // async close interface: try_close() -> [is_close_done()] -> close_wait()
    // if is_close_done() return true, close_wait() will not block
    // otherwise close_wait() will block
    virtual Status try_close(RuntimeState* state);
    virtual Status close_wait(RuntimeState* state, Status close_status, TabletSinkProfile* ts_profile,
                              bool write_txn_log);

    virtual bool is_open_done();
    virtual bool is_full();
    virtual bool is_close_done();

    virtual bool get_immutable_partition_ids(std::set<int64_t>* partition_ids);

    // mutable
    IndexIdToTabletBEMap* index_id_to_tablet_be_map() { return &_index_id_to_tablet_be_map; }

    // See TOlapTableSink.enable_multi_node_write: a tablet's node list is a SHARD set (one node per row)
    // instead of a replica set (same rows to every node).
    //
    // Which of the list a row goes to depends on whether the table has a key whose repeats must stay
    // ordered. |key_slots_by_index| is empty for DUPLICATE KEY, where a rowset is simply the union of
    // its segments: no two rows are in an order relation, so each row stays on the node its sink
    // instance already runs on and never crosses the network. It is non-empty for PRIMARY / AGGREGATE
    // / UNIQUE, where rows sharing a key DO resolve against each other; hashing the key sends all of
    // a key's rows to one node, so the order they are folded in stops being observable.
    void set_enable_multi_node_write(bool enable, std::unordered_map<int64_t, std::vector<SlotId>> key_slots_by_index);

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
    // Virtual to allow tests or derived senders (e.g. colocate sender) to intercept
    // how chunks are dispatched to BE nodes.
    virtual Status _send_chunk_by_node(Chunk* chunk, IndexChannel* channel, const std::vector<uint16_t>& selection_idx);
    // Multi-node write only. Fill _row_target_node with the single node each selected row goes to.
    // |channel|'s node-channel mutex must already be held in shared mode: local-first probes the
    // local node channel's backpressure through that map.
    Status _assign_multi_node_write_targets(Chunk* chunk, IndexChannel* channel,
                                            const std::unordered_map<int64_t, std::vector<int64_t>>& tablet_to_be,
                                            const std::vector<uint16_t>& selection_idx);
    // Key-hash routing only. Fill _key_hashes with one CRC32 per row over |channel|'s key columns.
    // The table's own distribution hash cannot be reused here: a range-distributed table has no
    // distribution columns at all, so OlapTablePartitionParam leaves those hashes at zero -- and a
    // range table is precisely the shape this feature exists for.
    void _compute_key_hashes(Chunk* chunk, const std::vector<SlotId>& key_slots);
    // Whether this node is one of |be_ids| and its channel is usable.
    bool _can_keep_rows_local(IndexChannel* channel, const std::vector<int64_t>& be_ids) const;
    // Move every node channel's txn logs into _txn_log_map, folding the several partial logs a
    // multi-node write tablet produces into one. See merge_multi_node_write_txn_log.
    Status _collect_txn_logs();
    Status _write_combined_txn_log();

    // For every partition this sink is about to write a combined txn log for, the set of tablets
    // that log must cover, taken from the partition metadata the FE dispatched -- a source
    // independent of the collected logs themselves. Partitions with no dispatched metadata here
    // (e.g. dropped from _partition_params by remove_partitions() on the immutable-partition
    // path) are left out, so they stay unchecked rather than being judged against a guess.
    // Partitions brought in by an incremental open do have metadata here and are checked.
    ExpectedTabletsByPartition _expected_tablets_by_partition() const;
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
    OlapTablePartitionParam* _partition_params = nullptr;
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
    bool _enable_multi_node_write = false;
    // This instance's own backend id, resolved once at prepare time (-1 when unknown, e.g. a CN that
    // has not completed its first FE heartbeat, which just disables multi-node write for the load).
    int64_t _local_node_id = -1;
    // Multi-node write only. Indexed like _tablet_ids: the single node each row was assigned to, decided
    // once per chunk before the per-node dispatch loop.
    std::vector<int64_t> _row_target_node;
    // Multi-node write only. Per-tablet round-robin cursor; lives across chunks so the spread stays even
    // when a chunk carries only a few rows of a tablet. Unused under key-hash routing, which is
    // stateless.
    std::unordered_map<int64_t, uint64_t> _multi_node_write_counters;
    // Multi-node write only. Per index, the slots of the columns whose repeats must stay ordered. Empty
    // (for every index) means local-first routing; non-empty selects key-hash routing.
    std::unordered_map<int64_t, std::vector<SlotId>> _multi_node_write_key_slots;
    // Scratch for _compute_key_hashes, reused across chunks.
    std::vector<uint32_t> _key_hashes;
    // Multi-node write only, for the profile: how the rows of this instance were split between its own
    // node and the rest. Read it against the routing in use: under local-first a remote count means
    // this node is not in the tablet's list (expected once the list is bounded by
    // lake_multi_node_write_max_nodes), while key-hash routing reports roughly (N-1)/N remote by
    // construction -- the key decides the node, not where the row was produced.
    int64_t _multi_node_write_local_rows = 0;
    int64_t _multi_node_write_remote_rows = 0;
    std::set<int64_t> _failed_channels;
    // mapping from partition id to CombinedTxnLogPB
    std::map<int64_t, CombinedTxnLogPB> _txn_log_map;
};

} // namespace starrocks
