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

#include "exec/tablet_sink_colocate_sender.h"

#include "column/chunk.h"
#include "column/column_helper.h"
#include "common/statusor.h"
#include "exprs/expr.h"
#include "runtime/runtime_state.h"

namespace starrocks::stream_load {

TabletSinkColocateSender::TabletSinkColocateSender(
        PUniqueId load_id, int64_t txn_id, IndexIdToTabletBEMap index_id_to_tablet_be_map,
        OlapTablePartitionParam* vectorized_partition, std::vector<IndexChannel*> channels,
        std::unordered_map<int64_t, NodeChannel*> node_channels, std::vector<ExprContext*> output_expr_ctxs,
        bool enable_replicated_storage, TWriteQuorumType::type write_quorum_type, int num_repicas)
        : TabletSinkSender(load_id, txn_id, std::move(index_id_to_tablet_be_map), vectorized_partition,
                           std::move(channels), std::move(node_channels), std::move(output_expr_ctxs),
                           enable_replicated_storage, write_quorum_type, num_repicas) {}

Status TabletSinkColocateSender::send_chunk(const OlapTableSchemaParam* schema,
                                            const std::vector<OlapTablePartition*>& partitions,
                                            const std::vector<uint32_t>& tablet_indexes,
                                            const std::vector<uint16_t>& validate_select_idx,
                                            std::unordered_map<int64_t, std::set<int64_t>>& index_id_partition_id,
                                            Chunk* chunk) {
    if (UNLIKELY(!_colocate_mv_index)) {
        return TabletSinkSender::send_chunk(schema, partitions, tablet_indexes, validate_select_idx,
                                            index_id_partition_id, chunk);
    }

    Status err_st = Status::OK();
    size_t num_rows = chunk->num_rows();
    size_t selection_size = validate_select_idx.size();
    if (selection_size == 0) {
        return Status::OK();
    }

    if (num_rows > selection_size) {
        size_t index_size = partitions[validate_select_idx[0]]->indexes.size();
        _index_tablet_ids.resize(index_size);
        for (size_t i = 0; i < index_size; ++i) {
            _index_tablet_ids[i].resize(num_rows);
            auto* index = schema->indexes()[i];
            for (size_t j = 0; j < selection_size; ++j) {
                uint16_t selection = validate_select_idx[j];
                index_id_partition_id[index->index_id].emplace(partitions[selection]->id);
                _index_tablet_ids[i][selection] = partitions[selection]->indexes[i].tablets[tablet_indexes[selection]];
                _update_partition_rows(partitions[selection]->id);
            }
        }
        return _send_chunks(schema, chunk, _index_tablet_ids, validate_select_idx);
    } else { // Improve for all rows are selected
        size_t index_size = partitions[0]->indexes.size();
        _index_tablet_ids.resize(index_size);
        for (size_t i = 0; i < index_size; ++i) {
            auto* index = schema->indexes()[i];
            _index_tablet_ids[i].resize(num_rows);
            for (size_t j = 0; j < num_rows; ++j) {
                index_id_partition_id[index->index_id].emplace(partitions[j]->id);
                _index_tablet_ids[i][j] = partitions[j]->indexes[i].tablets[tablet_indexes[j]];
                _update_partition_rows(partitions[j]->id);
            }
        }
        return _send_chunks(schema, chunk, _index_tablet_ids, validate_select_idx);
    }
}

Status TabletSinkColocateSender::_send_chunks(const OlapTableSchemaParam* schema, Chunk* chunk,
                                              const std::vector<std::vector<int64_t>>& index_tablet_ids,
                                              const std::vector<uint16_t>& selection_idx) {
    Status err_st = Status::OK();
    // use 1th index to generate selective vel.
    auto* index = schema->indexes()[0];
    DCHECK_LT(0, index_tablet_ids.size());
    auto& tablet_id_selections = index_tablet_ids[0];
    DCHECK(_index_id_to_tablet_be_map.find(index->index_id) != _index_id_to_tablet_be_map.end());
    auto& tablet_to_bes = _index_id_to_tablet_be_map.find(index->index_id)->second;

    for (auto& it : _node_channels) {
        _node_select_idx.clear();
        _node_select_idx.reserve(selection_idx.size());

        auto* node = it.second;
        if (_is_failed_channel(node)) {
            // skip open fail channel
            continue;
        }
        bool has_send_data = false;
        if (_enable_replicated_storage) {
            for (unsigned short selection : selection_idx) {
                auto choose_tablet_id = tablet_id_selections[selection];
                DCHECK(tablet_to_bes.find(choose_tablet_id) != tablet_to_bes.end());
                auto& be_ids = tablet_to_bes.find(choose_tablet_id)->second;
                DCHECK_LT(0, be_ids.size());
                // TODO(meegoo): add backlist policy
                // first replica is primary replica, which determined by FE now
                // only send to primary replica when enable replicated storage engine
                // NOTE: FE will keep all indexes' primary tablet is in the same node.
                if (be_ids[0] == node->node_id()) {
                    _node_select_idx.emplace_back(selection);
                    has_send_data = true;
                }
            }
        } else {
            auto& node_tablet_ids = node->tablet_ids_of_index(index->index_id);
            for (unsigned short selection : selection_idx) {
                auto choose_tablet_id = tablet_id_selections[selection];
                if (node_tablet_ids.find(choose_tablet_id) != node_tablet_ids.end()) {
                    _node_select_idx.emplace_back(selection);
                    has_send_data = true;
                }
            }
        }
        if (!has_send_data) {
            continue;
        }
        auto st = node->add_chunks(chunk, index_tablet_ids, _node_select_idx, 0, _node_select_idx.size());

        if (!st.ok()) {
            LOG(WARNING) << node->name() << ", tablet add chunk failed, " << node->print_load_info()
                         << ", node=" << node->node_info()->host << ":" << node->node_info()->brpc_port
                         << ", errmsg=" << st.message();
            err_st = st;
            // we only send to primary replica, if it fail whole load fail
            if (_enable_replicated_storage) {
                return err_st;
            }
        }
    }
    return Status::OK();
}

Status TabletSinkColocateSender::try_open(RuntimeState* state) {
    if (UNLIKELY(!_colocate_mv_index)) {
        return TabletSinkSender::try_open(state);
    }
    // Prepare the exprs to run.
    RETURN_IF_ERROR(Expr::open(_output_expr_ctxs, state));
    RETURN_IF_ERROR(_vectorized_partition->open(state));
    for_each_node_channel([](NodeChannel* ch) { ch->try_open(); });
    return Status::OK();
}

bool TabletSinkColocateSender::is_open_done() {
    if (UNLIKELY(!_colocate_mv_index)) {
        return TabletSinkSender::is_open_done();
    }
    if (!_open_done) {
        bool open_done = true;
        for_each_node_channel([&open_done](NodeChannel* ch) { open_done &= ch->is_open_done(); });
        _open_done = open_done;
    }

    return _open_done;
}

bool TabletSinkColocateSender::is_full() {
    if (UNLIKELY(!_colocate_mv_index)) {
        return TabletSinkSender::is_full();
    }
    bool full = false;
    for_each_node_channel([&full](NodeChannel* ch) { full |= ch->is_full(); });
    return full;
}

Status TabletSinkColocateSender::open_wait() {
    Status err_st = Status::OK();
    VLOG(2) << "start open_wait, colocate_mv_index:" << _colocate_mv_index;
    for_each_node_channel([this, &err_st](NodeChannel* ch) {
        auto st = ch->open_wait();
        if (!st.ok()) {
            LOG(WARNING) << ch->name() << ", tablet open failed, " << ch->print_load_info()
                         << ", node=" << ch->node_info()->host << ":" << ch->node_info()->brpc_port
                         << ", errmsg=" << st.message();
            err_st = st.clone_and_append(std::string(" be:") + ch->node_info()->host);
            this->_mark_as_failed(ch);
        }
        // disable colocate mv index load if other BE not supported
        if (!ch->enable_colocate_mv_index()) {
            LOG(WARNING) << "node channel cannot support colocate, set it to false";
            _colocate_mv_index = false;
        }
    });

    if (_has_intolerable_failure()) {
        LOG(WARNING) << "Open channel failed. load_id: " << _load_id << ", error: " << err_st.to_string();
        return err_st;
    }

    VLOG(2) << "end open_wait, colocate_mv_index:" << _colocate_mv_index;
    return Status::OK();
}

Status TabletSinkColocateSender::try_close(RuntimeState* state) {
    if (UNLIKELY(!_colocate_mv_index)) {
        return TabletSinkSender::try_close(state);
    }
    Status err_st = Status::OK();
    bool intolerable_failure = false;
    for_each_node_channel([this, &err_st, &intolerable_failure](NodeChannel* ch) {
        if (!this->_is_failed_channel(ch)) {
            auto st = ch->try_close();
            if (!st.ok()) {
                LOG(WARNING) << "close channel failed. channel_name=" << ch->name()
                             << ", load_info=" << ch->print_load_info() << ", error_msg=" << st.message();
                err_st = st;
                this->_mark_as_failed(ch);
            }
        }
        if (this->_has_intolerable_failure()) {
            intolerable_failure = true;
        }
    });

    // update the info of partition num rows
    state->update_partition_num_rows(_partition_to_num_rows);

    if (intolerable_failure) {
        return err_st;
    } else {
        return Status::OK();
    }
}

bool TabletSinkColocateSender::is_close_done() {
    if (UNLIKELY(!_colocate_mv_index)) {
        return TabletSinkSender::is_close_done();
    }
    if (!_close_done) {
        bool close_done = true;
        for_each_node_channel([&close_done](NodeChannel* ch) { close_done &= ch->is_close_done(); });
        _close_done = close_done;
    }

    return _close_done;
}

Status TabletSinkColocateSender::close_wait(RuntimeState* state, Status close_status, TabletSinkProfile* ts_profile) {
    if (UNLIKELY(!_colocate_mv_index)) {
        return TabletSinkColocateSender::close_wait(state, close_status, ts_profile);
    }
    Status status = std::move(close_status);
    if (status.ok()) {
        // BE id -> add_batch method counter
        std::unordered_map<int64_t, AddBatchCounter> node_add_batch_counter_map;
        int64_t serialize_batch_ns = 0, actual_consume_ns = 0;
        {
            SCOPED_TIMER(ts_profile->close_timer);
            Status err_st = Status::OK();

            for_each_node_channel([this, &state, &node_add_batch_counter_map, &serialize_batch_ns, &actual_consume_ns,
                                   &err_st](NodeChannel* ch) {
                auto channel_status = ch->close_wait(state);
                if (!channel_status.ok()) {
                    LOG(WARNING) << "close channel failed. channel_name=" << ch->name()
                                 << ", load_info=" << ch->print_load_info()
                                 << ", error_msg=" << channel_status.message();
                    err_st = channel_status;
                    this->_mark_as_failed(ch);
                }
                ch->time_report(&node_add_batch_counter_map, &serialize_batch_ns, &actual_consume_ns);
            });
            if (_has_intolerable_failure()) {
                status = err_st;
                for_each_node_channel([&status](NodeChannel* ch) { ch->cancel(status); });
            }

            // only if status is ok can we call this _profile->total_time_counter().
            // if status is not ok, this sink may not be prepared, so that _profile is null
            SCOPED_TIMER(ts_profile->runtime_profile->total_time_counter());
            COUNTER_SET(ts_profile->serialize_chunk_timer, serialize_batch_ns);
            COUNTER_SET(ts_profile->send_rpc_timer, actual_consume_ns);
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
        }
    } else {
        for_each_node_channel([&status](NodeChannel* ch) { ch->cancel(status); });
    }

    Expr::close(_output_expr_ctxs, state);
    if (_vectorized_partition) {
        _vectorized_partition->close(state);
    }
    return status;
}

bool TabletSinkColocateSender::get_immutable_partition_ids(std::set<int64_t>* partition_ids) {
    bool has_immutable_partition = false;
    for_each_node_channel([&has_immutable_partition, partition_ids](NodeChannel* ch) {
        if (ch->has_immutable_partition()) {
            has_immutable_partition = true;
            partition_ids->merge(ch->immutable_partition_ids());
            ch->reset_immutable_partition_ids();
        }
    });
    return has_immutable_partition;
}

} // namespace starrocks::stream_load
