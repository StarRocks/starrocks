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

#include "exec/tablet_sink_sender.h"

#include "column/chunk.h"
#include "column/column_helper.h"
#include "common/statusor.h"
#include "exprs/expr.h"
#include "runtime/runtime_state.h"

namespace starrocks::stream_load {

TabletSinkSender::TabletSinkSender(PUniqueId load_id, int64_t txn_id, IndexIdToTabletBEMap index_id_to_tablet_be_map,
                                   OlapTablePartitionParam* vectorized_partition, std::vector<IndexChannel*> channels,
                                   std::unordered_map<int64_t, NodeChannel*> node_channels,
                                   std::vector<ExprContext*> output_expr_ctxs, bool enable_replicated_storage,
                                   TWriteQuorumType::type write_quorum_type, int num_repicas)
        : _load_id(load_id),
          _txn_id(txn_id),
          _index_id_to_tablet_be_map(std::move(index_id_to_tablet_be_map)),
          _vectorized_partition(vectorized_partition),
          _channels(std::move(channels)),
          _node_channels(std::move(node_channels)),
          _output_expr_ctxs(std::move(output_expr_ctxs)),
          _enable_replicated_storage(enable_replicated_storage),
          _write_quorum_type(write_quorum_type),
          _num_repicas(num_repicas) {}

Status TabletSinkSender::send_chunk(const OlapTableSchemaParam* schema,
                                    const std::vector<OlapTablePartition*>& partitions,
                                    const std::vector<uint32_t>& tablet_indexes,
                                    const std::vector<uint16_t>& validate_select_idx,
                                    std::unordered_map<int64_t, std::set<int64_t>>& index_id_partition_id,
                                    Chunk* chunk) {
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
                index_id_partition_id[index->index_id].emplace(partitions[selection]->id);
                _tablet_ids[selection] = partitions[selection]->indexes[i].tablets[tablet_indexes[selection]];
            }
            RETURN_IF_ERROR(_send_chunk_by_node(chunk, _channels[i], validate_select_idx));
        }
    } else { // Improve for all rows are selected
        size_t index_size = partitions[0]->indexes.size();
        for (size_t i = 0; i < index_size; ++i) {
            auto* index = schema->indexes()[i];
            for (size_t j = 0; j < num_rows; ++j) {
                index_id_partition_id[index->index_id].emplace(partitions[j]->id);
                _tablet_ids[j] = partitions[j]->indexes[i].tablets[tablet_indexes[j]];
            }
            RETURN_IF_ERROR(_send_chunk_by_node(chunk, _channels[i], validate_select_idx));
        }
    }
    return Status::OK();
}

Status TabletSinkSender::_send_chunk_by_node(Chunk* chunk, IndexChannel* channel,
                                             const std::vector<uint16_t>& selection_idx) {
    Status err_st = Status::OK();

    DCHECK(_index_id_to_tablet_be_map.find(channel->index_id()) != _index_id_to_tablet_be_map.end());
    auto& tablet_to_be = _index_id_to_tablet_be_map.find(channel->index_id())->second;
    for (auto& it : channel->_node_channels) {
        NodeChannel* node = it.second.get();
        if (channel->is_failed_channel(node)) {
            // skip open fail channel
            continue;
        }
        int64_t be_id = it.first;
        _node_select_idx.clear();
        _node_select_idx.reserve(selection_idx.size());

        if (_enable_replicated_storage) {
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
    RETURN_IF_ERROR(Expr::open(_output_expr_ctxs, state));
    RETURN_IF_ERROR(_vectorized_partition->open(state));
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
            LOG(WARNING) << "Open channel failed. load_id: " << _load_id << ", error: " << err_st.to_string();
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
            // close initial node channel and wait it done
            index_channel->for_each_initial_node_channel([&index_channel, &err_st,
                                                          &intolerable_failure](NodeChannel* ch) {
                if (!index_channel->is_failed_channel(ch)) {
                    auto st = ch->try_close(true);
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

            bool is_initial_node_channel_close_done = true;
            index_channel->for_each_initial_node_channel([&is_initial_node_channel_close_done](NodeChannel* ch) {
                is_initial_node_channel_close_done &= ch->is_close_done();
            });

            // close initial node channel not finish, can not close incremental node channel
            if (!is_initial_node_channel_close_done) {
                break;
            }

            // close incremental node channel
            index_channel->for_each_incremental_node_channel([&index_channel, &err_st,
                                                              &intolerable_failure](NodeChannel* ch) {
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

Status TabletSinkSender::close_wait(RuntimeState* state, Status close_status, TabletSinkProfile* ts_profile) {
    Status status = std::move(close_status);
    if (status.ok()) {
        // BE id -> add_batch method counter
        std::unordered_map<int64_t, AddBatchCounter> node_add_batch_counter_map;
        int64_t serialize_batch_ns = 0, actual_consume_ns = 0;
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

        // only if status is ok can we call this _profile->total_time_counter().
        // if status is not ok, this sink may not be prepared, so that _profile is null
        SCOPED_TIMER(ts_profile->runtime_profile->total_time_counter());
        COUNTER_SET(ts_profile->serialize_chunk_timer, serialize_batch_ns);
        COUNTER_SET(ts_profile->send_rpc_timer, actual_consume_ns);

        int64_t total_server_rpc_time_us = 0;
        int64_t total_server_wait_memtable_flush_time_us = 0;
        // print log of add batch time of all node, for tracing load performance easily
        std::stringstream ss;
        ss << "Olap table sink statistics. load_id: " << print_id(_load_id) << ", txn_id: " << _txn_id
           << ", add chunk time(ms)/wait lock time(ms)/num: ";
        for (auto const& pair : node_add_batch_counter_map) {
            total_server_rpc_time_us += pair.second.add_batch_execution_time_us;
            total_server_wait_memtable_flush_time_us += pair.second.add_batch_wait_memtable_flush_time_us;
            ss << "{" << pair.first << ":(" << (pair.second.add_batch_execution_time_us / 1000) << ")("
               << (pair.second.add_batch_wait_lock_time_us / 1000) << ")(" << pair.second.add_batch_num << ")} ";
        }
        ts_profile->server_rpc_timer->update(total_server_rpc_time_us * 1000);
        ts_profile->server_wait_flush_timer->update(total_server_wait_memtable_flush_time_us * 1000);
        LOG(INFO) << ss.str();
    } else {
        for_each_index_channel([&status](NodeChannel* ch) { ch->cancel(status); });
    }

    Expr::close(_output_expr_ctxs, state);
    if (_vectorized_partition) {
        _vectorized_partition->close(state);
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

} // namespace starrocks::stream_load
