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

// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/be/src/exec/tablet_sink.h

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

#include "exec/tablet_sink_colocate_sender.h"

#include "agent/master_info.h"
#include "agent/utils.h"
#include "column/binary_column.h"
#include "column/chunk.h"
#include "column/column_helper.h"
#include "column/nullable_column.h"
#include "common/statusor.h"
#include "config.h"
#include "exec/pipeline/query_context.h"
#include "exec/pipeline/stream_epoch_manager.h"
#include "exec/tablet_sink.h"
#include "exprs/expr.h"
#include "gutil/strings/fastmem.h"
#include "gutil/strings/join.h"
#include "gutil/strings/substitute.h"
#include "runtime/current_thread.h"
#include "runtime/exec_env.h"
#include "runtime/runtime_state.h"
#include "serde/protobuf_serde.h"
#include "simd/simd.h"
#include "storage/storage_engine.h"
#include "storage/tablet_manager.h"
#include "types/hll.h"
#include "util/brpc_stub_cache.h"
#include "util/compression/compression_utils.h"
#include "util/defer_op.h"
#include "util/thread.h"
#include "util/thrift_rpc_helper.h"
#include "util/uid_util.h"

namespace starrocks::stream_load {

Status TabletSinkColocateSender::send_chunk(std::shared_ptr<OlapTableSchemaParam> schema,
                                            const std::vector<OlapTablePartition*>& partitions,
                                            const std::vector<uint32_t>& tablet_indexes,
                                            const std::vector<uint16_t>& validate_select_idx,
                                            std::unordered_map<int64_t, std::set<int64_t>>& index_id_partition_id,
                                            Chunk* chunk) {
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
            for (size_t j = 0; j < selection_size; ++j) {
                uint16_t selection = validate_select_idx[j];
                _index_tablet_ids[i][selection] = partitions[selection]->indexes[i].tablets[tablet_indexes[selection]];
            }
        }
    } else { // Improve for all rows are selected
        size_t index_size = partitions[0]->indexes.size();
        _index_tablet_ids.resize(index_size);
        for (size_t i = 0; i < index_size; ++i) {
            _index_tablet_ids[i].resize(num_rows);
            for (size_t j = 0; j < num_rows; ++j) {
                _index_tablet_ids[i][j] = partitions[j]->indexes[i].tablets[tablet_indexes[j]];
            }
        }
    }
    return Status::OK();
}

Status TabletSinkColocateSender::try_open(RuntimeState* state) {
    // Prepare the exprs to run.
    RETURN_IF_ERROR(Expr::open(_output_expr_ctxs, state));
    RETURN_IF_ERROR(_vectorized_partition->open(state));
    for_each_node_channel([](NodeChannel* ch) { ch->try_open(); });
    return Status::OK();
}

bool TabletSinkColocateSender::is_open_done() {
    if (!_colocate_mv_index) {
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
    if (!_colocate_mv_index) {
        return TabletSinkSender::is_full();
    }
    bool full = false;
    for_each_node_channel([&full](NodeChannel* ch) { full |= ch->is_full(); });
    return full;
}

Status TabletSinkColocateSender::open_wait() {
    Status err_st = Status::OK();
    for_each_node_channel([this, &err_st](NodeChannel* ch) {
        auto st = ch->open_wait();
        if (!st.ok()) {
            LOG(WARNING) << ch->name() << ", tablet open failed, " << ch->print_load_info()
                         << ", node=" << ch->node_info()->host << ":" << ch->node_info()->brpc_port
                         << ", errmsg=" << st.get_error_msg();
            err_st = st.clone_and_append(string(" be:") + ch->node_info()->host);
            this->_mark_as_failed(ch);
        }
        // disable colocate mv index load if other BE not supported
        if (!ch->enable_colocate_mv_index()) {
            _colocate_mv_index = false;
        }
    });

    if (_has_intolerable_failure()) {
        LOG(WARNING) << "Open channel failed. load_id: " << _load_id << ", error: " << err_st.to_string();
        return err_st;
    }

    return Status::OK();
}

Status TabletSinkColocateSender::try_close(RuntimeState* state) {
    if (!_colocate_mv_index) {
        return TabletSinkSender::try_close(state);
    }
    Status err_st = Status::OK();
    bool intolerable_failure = false;
    for_each_node_channel([this, &err_st, &intolerable_failure](NodeChannel* ch) {
        if (!this->_is_failed_channel(ch)) {
            auto st = ch->try_close();
            if (!st.ok()) {
                LOG(WARNING) << "close channel failed. channel_name=" << ch->name()
                             << ", load_info=" << ch->print_load_info() << ", error_msg=" << st.get_error_msg();
                err_st = st;
                this->_mark_as_failed(ch);
            }
        }
        if (this->_has_intolerable_failure()) {
            intolerable_failure = true;
        }
    });

    if (intolerable_failure) {
        return err_st;
    } else {
        return Status::OK();
    }
}

bool TabletSinkColocateSender::is_close_done() {
    if (!_colocate_mv_index) {
        return TabletSinkSender::is_close_done();
    }
    if (!_close_done) {
        bool close_done = true;
        for_each_node_channel([&close_done](NodeChannel* ch) { close_done &= ch->is_close_done(); });
        _close_done = close_done;
    }

    return _close_done;
}

Status TabletSinkColocateSender::close_wait(RuntimeState* state, Status close_status,
                                            RuntimeProfile::Counter* close_timer,
                                            RuntimeProfile::Counter* serialize_chunk_timer,
                                            RuntimeProfile::Counter* send_rpc_timer,
                                            RuntimeProfile::Counter* server_rpc_timer,
                                            RuntimeProfile::Counter* server_wait_flush_timer) {
    if (!_colocate_mv_index) {
        return TabletSinkColocateSender::close_wait(state, close_status, close_timer, serialize_chunk_timer,
                                                    send_rpc_timer, server_rpc_timer, server_wait_flush_timer);
    }
    Status status = std::move(close_status);
    if (status.ok()) {
        // BE id -> add_batch method counter
        std::unordered_map<int64_t, AddBatchCounter> node_add_batch_counter_map;
        int64_t serialize_batch_ns = 0, actual_consume_ns = 0;
        {
            SCOPED_TIMER(close_timer);
            Status err_st = Status::OK();

            for_each_node_channel([this, &state, &node_add_batch_counter_map, &serialize_batch_ns, &actual_consume_ns,
                                   &err_st](NodeChannel* ch) {
                auto channel_status = ch->close_wait(state);
                if (!channel_status.ok()) {
                    LOG(WARNING) << "close channel failed. channel_name=" << ch->name()
                                 << ", load_info=" << ch->print_load_info()
                                 << ", error_msg=" << channel_status.get_error_msg();
                    err_st = channel_status;
                    this->_mark_as_failed(ch);
                }
                ch->time_report(&node_add_batch_counter_map, &serialize_batch_ns, &actual_consume_ns);
            });
            if (_has_intolerable_failure()) {
                status = err_st;
                for_each_node_channel([&status](NodeChannel* ch) { ch->cancel(status); });
            }

            COUNTER_SET(serialize_chunk_timer, serialize_batch_ns);
            COUNTER_SET(send_rpc_timer, actual_consume_ns);

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
            server_rpc_timer->update(total_server_rpc_time_us * 1000);
            server_wait_flush_timer->update(total_server_wait_memtable_flush_time_us * 1000);
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

} // namespace starrocks::stream_load
