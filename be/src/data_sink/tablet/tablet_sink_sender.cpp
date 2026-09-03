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

#include "data_sink/tablet/tablet_sink_sender.h"

#include <algorithm>
#include <utility>

#include "base/testutil/sync_point.h"
#include "column/chunk.h"
#include "common/config_ingest_fwd.h"
#include "common/runtime_profile.h"
#include "common/statusor.h"
#include "common/system/master_info.h"
#include "exprs/expr.h"
#include "exprs/expr_executor.h"
#include "fmt/format.h"
#include "runtime/runtime_state.h"
#include "storage/lake/combined_txn_log_writer.h"
#include "storage/lake/lake_proto_normalizer.h"
#include "storage/lake/shard_write_txn_log.h"

namespace starrocks {

TabletSinkSender::TabletSinkSender(PUniqueId load_id, int64_t txn_id, IndexIdToTabletBEMap index_id_to_tablet_be_map,
                                   OlapTablePartitionParam* partition_params, std::vector<IndexChannel*> channels,
                                   std::unordered_map<int64_t, NodeChannel*> node_channels,
                                   std::vector<ExprContext*> output_expr_ctxs, bool enable_replicated_storage,
                                   TWriteQuorumType::type write_quorum_type, int num_repicas)
        : _load_id(std::move(load_id)),
          _txn_id(txn_id),
          _index_id_to_tablet_be_map(std::move(index_id_to_tablet_be_map)),
          _partition_params(partition_params),
          _channels(std::move(channels)),
          _node_channels(std::move(node_channels)),
          _output_expr_ctxs(std::move(output_expr_ctxs)),
          _enable_replicated_storage(enable_replicated_storage),
          _write_quorum_type(write_quorum_type),
          _num_repicas(num_repicas) {}

Status TabletSinkSender::send_chunk(const OlapTableSchemaParam* schema,
                                    const std::vector<OlapTablePartition*>& partitions,
                                    const std::vector<uint32_t>& record_hashes,
                                    const std::vector<uint16_t>& validate_select_idx,
                                    std::unordered_map<int64_t, std::set<int64_t>>& index_id_partition_id,
                                    Chunk* chunk) {
    // Range distribution is handled by the base class implementation for RangeTabletSinkSender
    // Normal hash distribution continues here

    size_t num_rows = chunk->num_rows();
    size_t selection_size = validate_select_idx.size();
    if (selection_size == 0) {
        return Status::OK();
    }
    _tablet_ids.resize(num_rows);
    if (num_rows > selection_size) {
        size_t index_size = partitions[validate_select_idx[0]]->indexes.size();
        for (size_t i = 0; i < index_size; ++i) {
            auto* index = schema->indexes()[i];
            for (size_t j = 0; j < selection_size; ++j) {
                uint16_t selection = validate_select_idx[j];
                const auto* partition = partitions[selection];
                index_id_partition_id[index->index_id].emplace(partition->id);
                const auto& tablet_ids = partition->indexes[i].tablet_ids;
                _tablet_ids[selection] = tablet_ids[record_hashes[selection] % tablet_ids.size()];
            }
            RETURN_IF_ERROR(_send_chunk_by_node(chunk, _channels[i], validate_select_idx));
        }
    } else { // Improve for all rows are selected
        size_t index_size = partitions[0]->indexes.size();
        for (size_t i = 0; i < index_size; ++i) {
            auto* index = schema->indexes()[i];
            for (size_t j = 0; j < num_rows; ++j) {
                const auto* partition = partitions[j];
                index_id_partition_id[index->index_id].emplace(partition->id);
                const auto& tablet_ids = partition->indexes[i].tablet_ids;
                _tablet_ids[j] = tablet_ids[record_hashes[j] % tablet_ids.size()];
            }
            RETURN_IF_ERROR(_send_chunk_by_node(chunk, _channels[i], validate_select_idx));
        }
    }
    return Status::OK();
}

void TabletSinkSender::set_enable_shard_write(bool enable, bool local_first) {
    _enable_shard_write = enable;
    _shard_write_local_first = enable && local_first;
    if (_shard_write_local_first) {
        // Resolved once: the backend id comes from the FE heartbeat and does not change while the
        // process runs. Absent (a CN that has not been assigned one yet) simply leaves local-first
        // off for this load -- the round-robin spread below is always a correct fallback.
        _local_node_id = get_backend_id().value_or(-1);
    }
}

// Decide the ONE node each selected row goes to. This cannot be folded into the per-node dispatch
// loop in _send_chunk_by_node: that loop visits the same row once per node, so a cursor advanced
// there would step a different number of times on each pass and a row could end up claimed by
// several nodes (duplication) or by none (loss).
Status TabletSinkSender::_assign_shard_write_targets(
        IndexChannel* channel, const std::unordered_map<int64_t, std::vector<int64_t>>& tablet_to_be,
        const std::vector<uint16_t>& selection_idx) {
    if (_row_target_node.size() < _tablet_ids.size()) {
        _row_target_node.resize(_tablet_ids.size());
    }
    // Rows are handed out in runs of `shard_write_rows_per_node`. A run of 1 spreads every row and
    // balances perfectly, but leaves each node a strided 1/N slice of the chunk, so the sender does
    // N small per-column appends where it used to do one large one. A run at or above the chunk size
    // routes a whole chunk's rows for a tablet to one node instead.
    const uint64_t stride = std::max(1, config::shard_write_rows_per_node);
    int64_t last_tablet_id = -1;
    const std::vector<int64_t>* last_be_ids = nullptr;
    uint64_t* last_counter = nullptr;
    // Whether this tablet's rows in THIS chunk stay on the local node, and the spread to use when
    // they do not. Both are decided once per (chunk, tablet): is_full() is a coarse backpressure
    // signal and re-probing it per row would only add noise to the split.
    bool keep_local = false;
    const std::vector<int64_t>* spread = nullptr;
    for (unsigned short selection : selection_idx) {
        const int64_t tablet_id = _tablet_ids[selection];
        if (tablet_id != last_tablet_id) {
            auto iter = tablet_to_be.find(tablet_id);
            DCHECK(iter != tablet_to_be.end());
            if (iter == tablet_to_be.end()) {
                return Status::InternalError(fmt::format("Unknown tablet_id {} in tablet be map", tablet_id));
            }
            last_tablet_id = tablet_id;
            last_be_ids = &iter->second;
            last_counter = &_shard_write_counters[tablet_id];
            keep_local = _shard_write_local_first && _can_keep_rows_local(channel, *last_be_ids);
            // Spilling: spread over the OTHER nodes. Leaving the full local node in the rotation
            // would send 1/N of the spilled rows straight back into the channel that is already
            // backpressured, which is the thing spilling exists to avoid.
            if (keep_local) {
                spread = nullptr;
            } else if (_shard_write_local_first && _local_node_id >= 0 && last_be_ids->size() > 1) {
                _shard_write_spill_targets.clear();
                for (int64_t node_id : *last_be_ids) {
                    if (node_id != _local_node_id) {
                        _shard_write_spill_targets.emplace_back(node_id);
                    }
                }
                spread = _shard_write_spill_targets.empty() ? last_be_ids : &_shard_write_spill_targets;
            } else {
                spread = last_be_ids;
            }
        }
        DCHECK(!last_be_ids->empty());
        const int64_t target = keep_local ? _local_node_id : (*spread)[((*last_counter)++ / stride) % spread->size()];
        _row_target_node[selection] = target;
        ++(target == _local_node_id ? _shard_write_local_rows : _shard_write_remote_rows);
    }
    return Status::OK();
}

// Local-first is on, this instance knows its own node, that node is one of the tablet's writers, and
// its channel is neither failed nor backpressured. is_full() is what keeps a load whose sink runs on
// a SINGLE instance (a stream load) from collapsing onto one machine: once the local channel fills,
// the rows spill to the other nodes and the fan-out is recovered.
bool TabletSinkSender::_can_keep_rows_local(IndexChannel* channel, const std::vector<int64_t>& be_ids) const {
    if (_local_node_id < 0) {
        return false;
    }
    if (std::find(be_ids.begin(), be_ids.end(), _local_node_id) == be_ids.end()) {
        return false;
    }
    auto iter = channel->_node_channels.find(_local_node_id);
    if (iter == channel->_node_channels.end() || iter->second == nullptr) {
        return false;
    }
    NodeChannel* local = iter->second.get();
    return !channel->is_failed_channel(local) && !local->is_full();
}

Status TabletSinkSender::_send_chunk_by_node(Chunk* chunk, IndexChannel* channel,
                                             const std::vector<uint16_t>& selection_idx) {
    Status err_st = Status::OK();

    DCHECK(_index_id_to_tablet_be_map.find(channel->index_id()) != _index_id_to_tablet_be_map.end());
    auto& tablet_to_be = _index_id_to_tablet_be_map.find(channel->index_id())->second;
    // Acquire shared lock to protect against concurrent modification of _node_channels
    // during incremental partition opens (see IndexChannel::init with is_incremental=true).
    // Held across the shard-write routing decision too: local-first probes the local node channel
    // through this same map.
    std::shared_lock<std::shared_mutex> lock(channel->_node_channels_mutex);
    TEST_SYNC_POINT("TabletSinkSender::_send_chunk_by_node::after_lock");
    if (_enable_shard_write) {
        RETURN_IF_ERROR(_assign_shard_write_targets(channel, tablet_to_be, selection_idx));
    }
    for (auto& it : channel->_node_channels) {
        NodeChannel* node = it.second.get();
        if (channel->is_failed_channel(node)) {
            // skip open fail channel
            continue;
        }
        int64_t be_id = it.first;
        _node_select_idx.clear();
        _node_select_idx.reserve(selection_idx.size());

        if (_enable_shard_write) {
            // The node list of a tablet is a SHARD set, not a replica set: every row goes to exactly
            // one of them, so each node receives a disjoint part of the tablet.
            for (unsigned short selection : selection_idx) {
                if (_row_target_node[selection] == be_id) {
                    _node_select_idx.emplace_back(selection);
                }
            }
        } else if (_enable_replicated_storage) {
            for (unsigned short selection : selection_idx) {
                DCHECK(tablet_to_be.find(_tablet_ids[selection]) != tablet_to_be.end());
                std::vector<int64_t>& be_ids = tablet_to_be.find(_tablet_ids[selection])->second;
                DCHECK_LT(0, be_ids.size());
                // TODO(meegoo): add backlist policy
                // first replica is primary replica, which determined by FE now
                // only send to primary replica when enable replicated storage engine
                if (be_ids[0] == be_id) {
                    _node_select_idx.emplace_back(selection);
                }
            }
        } else {
            for (unsigned short selection : selection_idx) {
                DCHECK(tablet_to_be.find(_tablet_ids[selection]) != tablet_to_be.end());
                std::vector<int64_t>& be_ids = tablet_to_be.find(_tablet_ids[selection])->second;
                DCHECK_LT(0, be_ids.size());
                if (std::find(be_ids.begin(), be_ids.end(), be_id) != be_ids.end()) {
                    _node_select_idx.emplace_back(selection);
                }
            }
        }

        auto st = node->add_chunk(chunk, _tablet_ids, _node_select_idx, 0, _node_select_idx.size());

        if (!st.ok()) {
            LOG(WARNING) << node->name() << ", tablet add chunk failed, " << node->print_load_info()
                         << ", node=" << node->node_info()->host << ":" << node->node_info()->brpc_port
                         << ", errmsg=" << st.message();
            channel->mark_as_failed(node);
            err_st = st;
            // we only send to primary replica, if it fail whole load fail
            if (_enable_replicated_storage) {
                return err_st;
            }
        }
        if (channel->has_intolerable_failure()) {
            return err_st;
        }
    }
    return Status::OK();
}

Status TabletSinkSender::try_open(RuntimeState* state) {
    // Prepare the exprs to run.
    RETURN_IF_ERROR(ExprExecutor::open(_output_expr_ctxs, state));
    RETURN_IF_ERROR(_partition_params->open(state));
    for_each_index_channel([](NodeChannel* ch) { ch->try_open(); });
    return Status::OK();
}

bool TabletSinkSender::is_open_done() {
    if (!_open_done) {
        bool open_done = true;
        for_each_index_channel([&open_done](NodeChannel* ch) { open_done &= ch->is_open_done(); });
        _open_done = open_done;
    }

    return _open_done;
}

bool TabletSinkSender::is_full() {
    bool full = false;
    for_each_index_channel([&full](NodeChannel* ch) { full |= ch->is_full(); });
    return full;
}

Status TabletSinkSender::open_wait() {
    Status err_st = Status::OK();

    for (auto& index_channel : _channels) {
        index_channel->for_each_node_channel([&index_channel, &err_st](NodeChannel* ch) {
            auto st = ch->open_wait();
            if (!st.ok()) {
                LOG(WARNING) << ch->name() << ", tablet open failed, " << ch->print_load_info()
                             << ", node=" << ch->node_info()->host << ":" << ch->node_info()->brpc_port
                             << ", errmsg=" << st.message();
                err_st = st.clone_and_append(std::string(" be:") + ch->node_info()->host);
                index_channel->mark_as_failed(ch);
            }
        });

        if (index_channel->has_intolerable_failure()) {
            LOG(WARNING) << "Open channel failed. load_id: " << print_id(_load_id) << ", error: " << err_st.to_string();
            return err_st;
        }
    }

    return Status::OK();
}

Status TabletSinkSender::try_close(RuntimeState* state) {
    Status err_st = Status::OK();
    bool intolerable_failure = false;
    for (auto& index_channel : _channels) {
        if (index_channel->has_incremental_node_channel()) {
            // try to finish initial node channel and wait it done
            // This is added for automatic partition. We need to ensure that
            // all data has been sent before the incremental channel is closed.
            index_channel->for_each_initial_node_channel([&index_channel, &err_st,
                                                          &intolerable_failure](NodeChannel* ch) {
                if (!index_channel->is_failed_channel(ch)) {
                    auto st = ch->try_finish();
                    if (!st.ok()) {
                        LOG(WARNING) << "close initial channel failed. channel_name=" << ch->name()
                                     << ", load_info=" << ch->print_load_info() << ", error_msg=" << st.message();
                        err_st = st;
                        index_channel->mark_as_failed(ch);
                    }
                } else {
                    ch->cancel();
                }
                if (index_channel->has_intolerable_failure()) {
                    intolerable_failure = true;
                }
            });

            if (intolerable_failure) {
                break;
            }

            bool is_initial_node_channel_finished = true;
            index_channel->for_each_initial_node_channel([&is_initial_node_channel_finished](NodeChannel* ch) {
                is_initial_node_channel_finished &= ch->is_finished();
            });

            // initial node channel not finish, can not close incremental node channel
            if (!is_initial_node_channel_finished) {
                break;
            }

            // close both initial & incremental node channel
            index_channel->for_each_node_channel([&index_channel, &err_st, &intolerable_failure](NodeChannel* ch) {
                if (!index_channel->is_failed_channel(ch)) {
                    auto st = ch->try_close();
                    if (!st.ok()) {
                        LOG(WARNING) << "close incremental channel failed. channel_name=" << ch->name()
                                     << ", load_info=" << ch->print_load_info() << ", error_msg=" << st.message();
                        err_st = st;
                        index_channel->mark_as_failed(ch);
                    }
                } else {
                    ch->cancel();
                }
                if (index_channel->has_intolerable_failure()) {
                    intolerable_failure = true;
                }
            });

        } else {
            index_channel->for_each_node_channel([&index_channel, &err_st, &intolerable_failure](NodeChannel* ch) {
                if (!index_channel->is_failed_channel(ch)) {
                    auto st = ch->try_close();
                    if (!st.ok()) {
                        LOG(WARNING) << "close channel failed. channel_name=" << ch->name()
                                     << ", load_info=" << ch->print_load_info() << ", error_msg=" << st.message();
                        err_st = st;
                        index_channel->mark_as_failed(ch);
                    }
                } else {
                    ch->cancel();
                }
                if (index_channel->has_intolerable_failure()) {
                    intolerable_failure = true;
                }
            });
        }
    }

    // when enable replicated storage, we only send to primary replica, one node channel lead to indicate whole load fail
    if (intolerable_failure) {
        return err_st;
    } else {
        return Status::OK();
    }
}

bool TabletSinkSender::is_close_done() {
    if (!_close_done) {
        bool close_done = true;
        for_each_index_channel([&close_done](NodeChannel* ch) { close_done &= ch->is_close_done(); });
        _close_done = close_done;
    }

    return _close_done;
}

Status TabletSinkSender::close_wait(RuntimeState* state, Status close_status, TabletSinkProfile* ts_profile,
                                    bool write_txn_log) {
    Status status = std::move(close_status);
    if (_enable_shard_write && ts_profile != nullptr && ts_profile->runtime_profile != nullptr) {
        // How this instance's rows were split. Under local-first a non-zero remote count means the
        // local channel was backpressured (or this node is not one of the tablet's writers); under
        // round-robin the split is the expected ~(N-1)/N.
        COUNTER_UPDATE(ADD_COUNTER(ts_profile->runtime_profile, "ShardWriteLocalRows", TUnit::UNIT),
                       _shard_write_local_rows);
        COUNTER_UPDATE(ADD_COUNTER(ts_profile->runtime_profile, "ShardWriteRemoteRows", TUnit::UNIT),
                       _shard_write_remote_rows);
    }
    // BE id -> add_batch method counter
    std::unordered_map<int64_t, AddBatchCounter> node_add_batch_counter_map;
    int64_t serialize_batch_ns = 0, actual_consume_ns = 0;
    if (status.ok()) {
        {
            SCOPED_TIMER(ts_profile->close_timer);
            Status err_st = Status::OK();
            for (auto& index_channel : _channels) {
                index_channel->for_each_node_channel([&index_channel, &state, &node_add_batch_counter_map,
                                                      &serialize_batch_ns, &actual_consume_ns,
                                                      &err_st](NodeChannel* ch) {
                    auto channel_status = ch->close_wait(state);
                    if (!channel_status.ok()) {
                        LOG(WARNING) << "close channel failed. channel_name=" << ch->name()
                                     << ", load_info=" << ch->print_load_info()
                                     << ", error_msg=" << channel_status.message();
                        err_st = channel_status;
                        index_channel->mark_as_failed(ch);
                    }
                    ch->time_report(&node_add_batch_counter_map, &serialize_batch_ns, &actual_consume_ns);
                });
                // when enable replicated storage, we only send to primary replica, one node channel lead to indicate whole load fail
                if (index_channel->has_intolerable_failure()) {
                    status = err_st;
                    index_channel->for_each_node_channel([&status](NodeChannel* ch) { ch->cancel(status); });
                }
            }
        }
        if (status.ok() && write_txn_log) {
            status.update(_collect_txn_logs());
            if (status.ok()) {
                status.update(_write_combined_txn_log());
            }
        }
    } else {
        for_each_index_channel(
                [&status, &node_add_batch_counter_map, &serialize_batch_ns, &actual_consume_ns](NodeChannel* ch) {
                    ch->cancel(status);
                    ch->time_report(&node_add_batch_counter_map, &serialize_batch_ns, &actual_consume_ns);
                });
    }

    SCOPED_TIMER(ts_profile->runtime_profile->total_time_counter());
    COUNTER_SET(ts_profile->serialize_chunk_timer, serialize_batch_ns);
    COUNTER_SET(ts_profile->send_rpc_timer, actual_consume_ns);

    int64_t total_server_rpc_time_us = 0;
    int64_t total_server_wait_memtable_flush_time_us = 0;
    // print log of add batch time of all node, for tracing load performance easily
    std::stringstream ss;
    ss << "Olap table sink statistics. load_id: " << print_id(_load_id) << ", txn_id: " << _txn_id
       << ", add chunk time(ms)/wait lock time(ms)/wait memtable flush time(ms)/wait writer time(ms)/other "
          "time(ms)/num: ";
    for (auto const& pair : node_add_batch_counter_map) {
        total_server_rpc_time_us += pair.second.add_batch_execution_time_us;
        total_server_wait_memtable_flush_time_us += pair.second.add_batch_wait_memtable_flush_time_us;
        // Residual of execution_time not covered by the lock/flush/writer buckets (e.g. write-context
        // setup, delta-writer open, the immutable-partition scan and stale-memtable flush). Clamp to 0
        // since the buckets are sampled independently.
        int64_t other_time_us = pair.second.add_batch_execution_time_us - pair.second.add_batch_wait_lock_time_us -
                                pair.second.add_batch_wait_memtable_flush_time_us -
                                pair.second.add_batch_wait_writer_time_us;
        other_time_us = other_time_us > 0 ? other_time_us : 0;
        ss << "{" << pair.first << ":(" << (pair.second.add_batch_execution_time_us / 1000) << ")("
           << (pair.second.add_batch_wait_lock_time_us / 1000) << ")("
           << (pair.second.add_batch_wait_memtable_flush_time_us / 1000) << ")("
           << (pair.second.add_batch_wait_writer_time_us / 1000) << ")(" << (other_time_us / 1000) << ")("
           << pair.second.add_batch_num << ")} ";
    }
    COUNTER_UPDATE(ts_profile->server_rpc_timer, total_server_rpc_time_us * 1000);
    COUNTER_UPDATE(ts_profile->server_wait_flush_timer, total_server_wait_memtable_flush_time_us * 1000);
    LOG(INFO) << ss.str();

    ExprExecutor::close(_output_expr_ctxs, state);
    if (_partition_params) {
        _partition_params->close(state);
    }
    return status;
}

bool TabletSinkSender::get_immutable_partition_ids(std::set<int64_t>* partition_ids) {
    bool has_immutable_partition = false;
    for_each_index_channel([&has_immutable_partition, partition_ids](NodeChannel* ch) {
        if (ch->has_immutable_partition()) {
            has_immutable_partition = true;
            partition_ids->merge(ch->immutable_partition_ids());
            ch->reset_immutable_partition_ids();
        }
    });
    return has_immutable_partition;
}

ExpectedTabletsByPartition TabletSinkSender::_expected_tablets_by_partition() const {
    ExpectedTabletsByPartition expected;
    if (_partition_params == nullptr) {
        return expected;
    }
    std::unordered_set<int64_t> participating_indexes;
    participating_indexes.reserve(_channels.size());
    for (const auto* index_channel : _channels) {
        participating_indexes.insert(index_channel->index_id());
    }
    const auto& partitions = _partition_params->get_partitions();
    for (const auto& [partition_id, logs] : _txn_log_map) {
        (void)logs;
        auto it = partitions.find(partition_id);
        if (it == partitions.end() || it->second == nullptr) {
            continue;
        }
        std::set<int64_t> tablets;
        for (const auto& index : it->second->indexes) {
            if (participating_indexes.count(index.index_id) == 0) {
                continue;
            }
            tablets.insert(index.tablet_ids.begin(), index.tablet_ids.end());
        }
        if (!tablets.empty()) {
            expected.emplace(partition_id, std::move(tablets));
        }
    }
    return expected;
}

Status TabletSinkSender::_collect_txn_logs() {
    if (!_enable_shard_write) {
        for (auto& index_channel : _channels) {
            index_channel->for_each_node_channel([this](NodeChannel* channel) {
                for (auto& log : channel->txn_logs()) {
                    _txn_log_map[log.partition_id()].add_txn_logs()->Swap(&log);
                }
            });
        }
        return Status::OK();
    }

    // Shard write: several nodes wrote the same tablet, so each of them handed back a PARTIAL txn log
    // for it. Fold them into a single log per tablet here, before the combined log is written out, so
    // the {txn_id}.logs file publish reads still holds exactly one log per tablet.
    //
    // Walk the node channels in node-id order: the fold is a concatenation, and a stable input order
    // keeps the resulting segment layout reproducible across retries of the same load.
    std::map<int64_t, std::map<int64_t, TxnLogPB*>> tablet_log_by_partition;
    size_t merged_logs = 0;
    for (auto& index_channel : _channels) {
        std::map<int64_t, NodeChannel*> ordered_nodes;
        index_channel->for_each_node_channel(
                [&ordered_nodes](NodeChannel* channel) { ordered_nodes.emplace(channel->node_id(), channel); });
        for (auto& [node_id, channel] : ordered_nodes) {
            for (auto& log : channel->txn_logs()) {
                auto& tablet_logs = tablet_log_by_partition[log.partition_id()];
                auto it = tablet_logs.find(log.tablet_id());
                if (it == tablet_logs.end()) {
                    auto* slot = _txn_log_map[log.partition_id()].add_txn_logs();
                    slot->Swap(&log);
                    // The structured arrays are the only ones merge_shard_write_txn_log maintains;
                    // after-load makes them canonical and drops the deprecated parallel arrays that
                    // the producing CN dual-wrote. put_combined_txn_log rebuilds those on save.
                    lake::normalize_txn_log_after_load(slot);
                    tablet_logs.emplace(slot->tablet_id(), slot);
                    continue;
                }
                lake::normalize_txn_log_after_load(&log);
                RETURN_IF_ERROR(lake::merge_shard_write_txn_log(it->second, &log));
                ++merged_logs;
            }
        }
    }
    if (merged_logs > 0) {
        VLOG(2) << "shard write: folded " << merged_logs << " extra txn logs, txn_id=" << _txn_id;
    }
    return Status::OK();
}

Status TabletSinkSender::_write_combined_txn_log() {
    // A combined txn log is the only record of a partition's rowset metadata -- publish resolves
    // each tablet inside it and has no per-tablet fallback -- so an object written short of an
    // entry leaves the transaction permanently unpublishable once it commits. Check coverage
    // against what the FE dispatched before anything reaches object storage.
    const ExpectedTabletsByPartition expected = _expected_tablets_by_partition();
    if (config::enable_put_combinded_txn_log_parallel) {
        return write_combined_txn_log_parallel(_txn_log_map, expected);
    }

    static const std::set<int64_t> kNoExpectation;
    for (const auto& [partition_id, logs] : _txn_log_map) {
        auto it = expected.find(partition_id);
        RETURN_IF_ERROR(write_combined_txn_log(logs, it != expected.end() ? it->second : kNoExpectation));
    }
    return Status::OK();
}

} // namespace starrocks
