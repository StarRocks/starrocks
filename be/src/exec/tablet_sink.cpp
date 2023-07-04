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
//   https://github.com/apache/incubator-doris/blob/master/be/src/exec/tablet_sink.cpp

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

#include "exec/tablet_sink.h"

#include <memory>
#include <numeric>
#include <sstream>
#include <utility>

#include "agent/master_info.h"
#include "agent/utils.h"
#include "column/binary_column.h"
#include "column/chunk.h"
#include "column/column_helper.h"
#include "column/map_column.h"
#include "column/nullable_column.h"
#include "common/statusor.h"
#include "config.h"
#include "exec/pipeline/query_context.h"
#include "exec/tablet_sink_colocate_sender.h"
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
#include "util/brpc_stub_cache.h"
#include "util/compression/compression_utils.h"
#include "util/defer_op.h"
#include "util/thread.h"
#include "util/thrift_rpc_helper.h"
#include "util/uid_util.h"

static const uint8_t VALID_SEL_FAILED = 0x0;
static const uint8_t VALID_SEL_OK = 0x1;
// it's a valid value and selected, but it's null
// and we don't need following extra check
// make sure the least bit is 1.
static const uint8_t VALID_SEL_OK_AND_NULL = 0x3;

namespace starrocks::stream_load {

OlapTableSink::OlapTableSink(ObjectPool* pool, const std::vector<TExpr>& texprs, Status* status, RuntimeState* state)
        : _pool(pool), _rpc_http_min_size(state->get_rpc_http_min_size()) {
    if (!texprs.empty()) {
        *status = Expr::create_expr_trees(_pool, texprs, &_output_expr_ctxs, state);
    }
}

Status OlapTableSink::init(const TDataSink& t_sink, RuntimeState* state) {
    DCHECK(t_sink.__isset.olap_table_sink);
    const auto& table_sink = t_sink.olap_table_sink;
    _merge_condition = table_sink.merge_condition;
    _partial_update_mode = table_sink.partial_update_mode;
    _load_id.set_hi(table_sink.load_id.hi);
    _load_id.set_lo(table_sink.load_id.lo);
    _txn_id = table_sink.txn_id;
    _txn_trace_parent = table_sink.txn_trace_parent;
    _span = Tracer::Instance().start_trace_or_add_span("olap_table_sink", _txn_trace_parent);
    _num_repicas = table_sink.num_replicas;
    _need_gen_rollup = table_sink.need_gen_rollup;
    _tuple_desc_id = table_sink.tuple_id;
    _is_lake_table = table_sink.is_lake_table;
    _keys_type = table_sink.keys_type;
    if (table_sink.__isset.null_expr_in_auto_increment) {
        _null_expr_in_auto_increment = table_sink.null_expr_in_auto_increment;
        _miss_auto_increment_column = table_sink.miss_auto_increment_column;
        _auto_increment_slot_id = table_sink.auto_increment_slot_id;
    }
    if (table_sink.__isset.write_quorum_type) {
        _write_quorum_type = table_sink.write_quorum_type;
    }
    if (table_sink.__isset.enable_replicated_storage) {
        _enable_replicated_storage = table_sink.enable_replicated_storage;
    }
    if (table_sink.__isset.db_name) {
        state->set_db(table_sink.db_name);
    }
    state->set_txn_id(table_sink.txn_id);
    if (table_sink.__isset.label) {
        state->set_load_label(table_sink.label);
    }

    // profile must add to state's object pool
    _profile = state->obj_pool()->add(new RuntimeProfile("OlapTableSink"));
    _ts_profile = state->obj_pool()->add(new TabletSinkProfile());

    // add all counter
    _ts_profile->runtime_profile = _profile;
    _ts_profile->input_rows_counter = ADD_COUNTER(_profile, "RowsRead", TUnit::UNIT);
    _ts_profile->output_rows_counter = ADD_COUNTER(_profile, "RowsReturned", TUnit::UNIT);
    _ts_profile->filtered_rows_counter = ADD_COUNTER(_profile, "RowsFiltered", TUnit::UNIT);
    _ts_profile->open_timer = ADD_TIMER(_profile, "OpenTime");
    _ts_profile->close_timer = ADD_TIMER(_profile, "CloseWaitTime");
    _ts_profile->prepare_data_timer = ADD_TIMER(_profile, "PrepareDataTime");
    _ts_profile->convert_chunk_timer = ADD_CHILD_TIMER(_profile, "ConvertChunkTime", "PrepareDataTime");
    _ts_profile->validate_data_timer = ADD_CHILD_TIMER(_profile, "ValidateDataTime", "PrepareDataTime");
    _ts_profile->send_data_timer = ADD_TIMER(_profile, "SendDataTime");
    _ts_profile->pack_chunk_timer = ADD_CHILD_TIMER(_profile, "PackChunkTime", "SendDataTime");
    _ts_profile->send_rpc_timer = ADD_CHILD_TIMER(_profile, "SendRpcTime", "SendDataTime");
    _ts_profile->wait_response_timer = ADD_CHILD_TIMER(_profile, "WaitResponseTime", "SendDataTime");
    _ts_profile->serialize_chunk_timer = ADD_CHILD_TIMER(_profile, "SerializeChunkTime", "SendRpcTime");
    _ts_profile->compress_timer = ADD_CHILD_TIMER(_profile, "CompressTime", "SendRpcTime");
    _ts_profile->client_rpc_timer = ADD_TIMER(_profile, "RpcClientSideTime");
    _ts_profile->server_rpc_timer = ADD_TIMER(_profile, "RpcServerSideTime");
    _ts_profile->server_wait_flush_timer = ADD_TIMER(_profile, "RpcServerWaitFlushTime");

    _schema = std::make_shared<OlapTableSchemaParam>();
    RETURN_IF_ERROR(_schema->init(table_sink.schema));
    _vectorized_partition = _pool->add(new OlapTablePartitionParam(_schema, table_sink.partition));
    RETURN_IF_ERROR(_vectorized_partition->init(state));
    _location = _pool->add(new OlapTableLocationParam(table_sink.location));
    _nodes_info = _pool->add(new StarRocksNodesInfo(table_sink.nodes_info));

    if (table_sink.__isset.load_channel_timeout_s) {
        _load_channel_timeout_s = table_sink.load_channel_timeout_s;
    } else {
        _load_channel_timeout_s = config::streaming_load_rpc_max_alive_time_sec;
    }

    _enable_automatic_partition = _vectorized_partition->enable_automatic_partition();
    if (_enable_automatic_partition) {
        _automatic_partition_token =
                state->exec_env()->automatic_partition_pool()->new_token(ThreadPool::ExecutionMode::CONCURRENT);
    }
    // init _colocate_mv_index: Only use colocate mv when both FE/BE's config are set true.
    if (table_sink.__isset.enable_colocate_mv_index) {
        _colocate_mv_index = table_sink.enable_colocate_mv_index && config::enable_load_colocate_mv;
    }

    return Status::OK();
}

Status OlapTableSink::prepare(RuntimeState* state) {
    _span->AddEvent("prepare");

    _profile->add_info_string("TxnID", fmt::format("{}", _txn_id));
    _profile->add_info_string("IndexNum", fmt::format("{}", _schema->indexes().size()));
    _profile->add_info_string("ReplicatedStorage", fmt::format("{}", _enable_replicated_storage));
    _profile->add_info_string("AutomaticPartition", fmt::format("{}", _enable_automatic_partition));
    _ts_profile->alloc_auto_increment_timer = ADD_TIMER(_profile, "AllocAutoIncrementTime");

    SCOPED_TIMER(_profile->total_time_counter());

    RETURN_IF_ERROR(DataSink::prepare(state));

    _sender_id = state->per_fragment_instance_idx();
    _num_senders = state->num_per_fragment_instances();

    // Prepare the exprs to run.
    RETURN_IF_ERROR(Expr::prepare(_output_expr_ctxs, state));
    RETURN_IF_ERROR(_vectorized_partition->prepare(state));

    // get table's tuple descriptor
    _output_tuple_desc = state->desc_tbl().get_tuple_descriptor(_tuple_desc_id);
    if (_output_tuple_desc == nullptr) {
        LOG(WARNING) << "unknown destination tuple descriptor, id=" << _tuple_desc_id;
        return Status::InternalError("unknown destination tuple descriptor");
    }
    if (!_output_expr_ctxs.empty()) {
        if (_output_expr_ctxs.size() != _output_tuple_desc->slots().size()) {
            LOG(WARNING) << "number of exprs is not same with slots, num_exprs=" << _output_expr_ctxs.size()
                         << ", num_slots=" << _output_tuple_desc->slots().size();
            return Status::InternalError("number of exprs is not same with slots");
        }
        for (int i = 0; i < _output_expr_ctxs.size(); ++i) {
            if (!is_type_compatible(_output_expr_ctxs[i]->root()->type().type,
                                    _output_tuple_desc->slots()[i]->type().type)) {
                LOG(WARNING) << "type of exprs is not match slot's, expr_type="
                             << _output_expr_ctxs[i]->root()->type().type
                             << ", slot_type=" << _output_tuple_desc->slots()[i]->type().type
                             << ", slot_name=" << _output_tuple_desc->slots()[i]->col_name();
                return Status::InternalError("expr's type is not same with slot's");
            }
        }
    }

    _max_decimalv2_val.resize(_output_tuple_desc->slots().size());
    _min_decimalv2_val.resize(_output_tuple_desc->slots().size());
    // check if need validate batch
    for (int i = 0; i < _output_tuple_desc->slots().size(); ++i) {
        auto* slot = _output_tuple_desc->slots()[i];
        switch (slot->type().type) {
        case TYPE_DECIMALV2:
            _max_decimalv2_val[i].to_max_decimal(slot->type().precision, slot->type().scale);
            _min_decimalv2_val[i].to_min_decimal(slot->type().precision, slot->type().scale);
            break;
        case TYPE_CHAR:
        case TYPE_VARCHAR:
        case TYPE_DATE:
        case TYPE_DATETIME:
        case TYPE_HLL:
        case TYPE_OBJECT:
            break;
        default:
            break;
        }
    }

    _load_mem_limit = state->get_load_mem_limit();

    // map index_id to TabletBEMap(map tablet_id to backend id)
    IndexIdToTabletBEMap index_id_to_tablet_be_map;
    // open all channels
    RETURN_IF_ERROR(_init_node_channels(state, index_id_to_tablet_be_map));

    std::vector<IndexChannel*> index_channels;
    for (const auto& channel : _channels) {
        index_channels.emplace_back(channel.get());
    }
    std::unordered_map<int64_t, NodeChannel*> node_channels;
    for (auto& it : _node_channels) {
        node_channels[it.first] = it.second.get();
    }

    if (_colocate_mv_index) {
        _tablet_sink_sender = std::make_unique<TabletSinkColocateSender>(
                _load_id, _txn_id, std::move(index_id_to_tablet_be_map), _vectorized_partition,
                std::move(index_channels), std::move(node_channels), _output_expr_ctxs, _enable_replicated_storage,
                _write_quorum_type, _num_repicas);

    } else {
        _tablet_sink_sender = std::make_unique<TabletSinkSender>(
                _load_id, _txn_id, std::move(index_id_to_tablet_be_map), _vectorized_partition,
                std::move(index_channels), std::move(node_channels), _output_expr_ctxs, _enable_replicated_storage,
                _write_quorum_type, _num_repicas);
    }
    return Status::OK();
}

Status OlapTableSink::_init_node_channels(RuntimeState* state, IndexIdToTabletBEMap& index_id_to_tablet_be_map) {
    const auto& partitions = _vectorized_partition->get_partitions();
    for (int i = 0; i < _schema->indexes().size(); ++i) {
        // collect all tablets belong to this rollup
        std::vector<PTabletWithPartition> tablets;
        auto* index = _schema->indexes()[i];
        std::unordered_map<int64_t, std::vector<int64_t>> tablet_to_be;
        for (auto& [id, part] : partitions) {
            for (auto tablet : part->indexes[i].tablets) {
                PTabletWithPartition tablet_info;
                tablet_info.set_tablet_id(tablet);
                tablet_info.set_partition_id(part->id);

                // setup replicas
                auto* location = _location->find_tablet(tablet);
                if (location == nullptr) {
                    auto msg = fmt::format("Failed to find tablet {} location info", tablet);
                    return Status::NotFound(msg);
                }
                tablet_to_be.emplace(tablet, location->node_ids);

                auto node_ids_size = location->node_ids.size();
                for (size_t i = 0; i < node_ids_size; ++i) {
                    auto& node_id = location->node_ids[i];
                    auto node_info = _nodes_info->find_node(node_id);
                    if (node_info == nullptr) {
                        return Status::InvalidArgument(fmt::format("Unknown node_id: {}", node_id));
                    }
                    auto* replica = tablet_info.add_replicas();
                    replica->set_host(node_info->host);
                    replica->set_port(node_info->brpc_port);
                    replica->set_node_id(node_id);
                }

                // colocate mv load doesn't has IndexChannel, initialize NodeChannel here
                if (_colocate_mv_index) {
                    for (size_t i = 0; i < node_ids_size; ++i) {
                        auto& node_id = location->node_ids[i];
                        NodeChannel* node_channel = nullptr;
                        auto it = _node_channels.find(node_id);
                        if (it == std::end(_node_channels)) {
                            auto channel_ptr = std::make_unique<NodeChannel>(this, node_id, false);
                            node_channel = channel_ptr.get();
                            _node_channels.emplace(node_id, std::move(channel_ptr));
                        } else {
                            node_channel = it->second.get();
                        }
                        node_channel->add_tablet(index->index_id, tablet_info);
                    }
                }

                tablets.emplace_back(std::move(tablet_info));
            }
        }
        index_id_to_tablet_be_map.emplace(index->index_id, std::move(tablet_to_be));

        auto channel = std::make_unique<IndexChannel>(this, index->index_id);
        RETURN_IF_ERROR(channel->init(state, tablets, false));
        _channels.emplace_back(std::move(channel));
    }
    if (_colocate_mv_index) {
        for (auto& it : _node_channels) {
            RETURN_IF_ERROR(it.second->init(state));
        }
    }
    return Status::OK();
}

Status OlapTableSink::open(RuntimeState* state) {
    auto open_span = Tracer::Instance().add_span("open", _span);
    SCOPED_TIMER(_profile->total_time_counter());
    SCOPED_TIMER(_ts_profile->open_timer);
    RETURN_IF_ERROR(try_open(state));
    RETURN_IF_ERROR(open_wait());

    return Status::OK();
}

Status OlapTableSink::try_open(RuntimeState* state) {
    return _tablet_sink_sender->try_open(state);
}

bool OlapTableSink::is_open_done() {
    return _tablet_sink_sender->is_open_done();
}

Status OlapTableSink::open_wait() {
    return _tablet_sink_sender->open_wait();
}

bool OlapTableSink::is_full() {
    return _tablet_sink_sender->is_full() || _is_automatic_partition_running.load(std::memory_order_acquire);
}

Status OlapTableSink::_automatic_create_partition() {
    TCreatePartitionRequest request;
    TCreatePartitionResult result;
    request.__set_txn_id(_txn_id);
    request.__set_db_id(_vectorized_partition->db_id());
    request.__set_table_id(_vectorized_partition->table_id());
    request.__set_partition_values(_partition_not_exist_row_values);

    VLOG(1) << "automatic partition rpc begin request " << request;
    TNetworkAddress master_addr = get_master_address();
    auto timeout_ms = _runtime_state->query_options().query_timeout * 1000 / 2;
    RETURN_IF_ERROR(ThriftRpcHelper::rpc<FrontendServiceClient>(
            master_addr.hostname, master_addr.port,
            [&request, &result](FrontendServiceConnection& client) { client->createPartition(result, request); },
            timeout_ms));
    VLOG(1) << "automatic partition rpc end response " << result;
    if (result.status.status_code == TStatusCode::OK) {
        // add new created partitions
        RETURN_IF_ERROR(_vectorized_partition->add_partitions(result.partitions));

        // add new tablet locations
        _location->add_locations(result.tablets);

        // update new node info
        _nodes_info->add_nodes(result.nodes);

        // incremental open node channel
        RETURN_IF_ERROR(_incremental_open_node_channel(result.partitions));
    }

    return Status(result.status);
}

Status OlapTableSink::_update_immutable_partition() {
    TImmutablePartitionRequest request;
    TImmutablePartitionResult result;
    request.__set_txn_id(_txn_id);
    request.__set_db_id(_vectorized_partition->db_id());
    request.__set_table_id(_vectorized_partition->table_id());

    VLOG(1) << "immutable partition rpc begin request " << request;
    TNetworkAddress master_addr = get_master_address();
    auto timeout_ms = _runtime_state->query_options().query_timeout * 1000 / 2;
    RETURN_IF_ERROR(ThriftRpcHelper::rpc<FrontendServiceClient>(
            master_addr.hostname, master_addr.port,
            [&request, &result](FrontendServiceConnection& client) { client->updateImmutablePartition(result, request); },
            timeout_ms));
    VLOG(1) << "immutable partition rpc end response " << result;
    if (result.status.status_code == TStatusCode::OK) {
        // add new created partitions
        RETURN_IF_ERROR(_vectorized_partition->add_partitions(result.partitions));

        // add new tablet locations
        _location->add_locations(result.tablets);

        // update new node info
        _nodes_info->add_nodes(result.nodes);

        // incremental open node channel
        RETURN_IF_ERROR(_incremental_open_node_channel(result.partitions));
    }

    return Status(result.status);
}

Status OlapTableSink::_incremental_open_node_channel(const std::vector<TOlapTablePartition>& partitions) {
    std::map<int64_t, std::vector<PTabletWithPartition>> index_tablets_map;
    IndexIdToTabletBEMap index_tablet_bes_map;
    for (auto& t_part : partitions) {
        for (auto& index : t_part.indexes) {
            std::vector<PTabletWithPartition> tablets;
            // setup new partitions's tablets
            for (auto tablet : index.tablets) {
                PTabletWithPartition tablet_info;
                tablet_info.set_tablet_id(tablet);
                // TODO: support logical materialized views;
                tablet_info.set_partition_id(t_part.id);

                auto* location = _location->find_tablet(tablet);
                if (location == nullptr) {
                    auto msg = fmt::format("Failed to find tablet {} location info", tablet);
                    return Status::NotFound(msg);
                }

                for (auto& node_id : location->node_ids) {
                    auto node_info = _nodes_info->find_node(node_id);
                    if (node_info == nullptr) {
                        return Status::InvalidArgument(fmt::format("Unknown node_id: {}", node_id));
                    }
                    auto* replica = tablet_info.add_replicas();
                    replica->set_host(node_info->host);
                    replica->set_port(node_info->brpc_port);
                    replica->set_node_id(node_id);

                    index_tablet_bes_map[index.index_id][tablet].emplace_back(node_id);
                }

                index_tablets_map[index.index_id].emplace_back(std::move(tablet_info));
            }
        }
    }

    for (auto& channel : _channels) {
        int64_t index_id = channel->index_id();
        // initialize index channel
        RETURN_IF_ERROR(channel->init(_runtime_state, index_tablets_map[index_id], true));

        // add into index_id_to_tablet_be_map
        auto* index_id_to_tablet_be_map = _tablet_sink_sender->index_id_to_tablet_be_map();
        if (index_id_to_tablet_be_map->find(index_id) == index_id_to_tablet_be_map->end()) {
            LOG(WARNING) << "Incremental tablet open failed, index_id=" << index_id
                         << " not found in index_id_to_tablet_be_map";
            return Status::InternalError(
                    "Incremental tablet open failed, index_id not found in index_id_to_tablet_be_map");
        }
        for (auto& [tablet_id, bes] : index_tablet_bes_map[index_id]) {
            (*index_id_to_tablet_be_map)[index_id].emplace(tablet_id, std::move(bes));
        }

        // incremental open new partition's tablet on storage side
        channel->for_each_node_channel([](NodeChannel* ch) { ch->try_incremental_open(); });

        Status err_st = Status::OK();
        channel->for_each_node_channel([&channel, &err_st](NodeChannel* ch) {
            auto st = ch->open_wait();
            if (!st.ok()) {
                LOG(WARNING) << ch->name() << ", tablet open failed, " << ch->print_load_info()
                             << ", node=" << ch->node_info()->host << ":" << ch->node_info()->brpc_port
                             << ", errmsg=" << st.get_error_msg();
                err_st = st.clone_and_append(string(" be:") + ch->node_info()->host);
                channel->mark_as_failed(ch);
            }
        });

        if (channel->has_intolerable_failure()) {
            LOG(WARNING) << "Open channel failed. load_id: " << _load_id << ", error: " << err_st.to_string();
            return err_st;
        }
    }

    return Status::OK();
}

Status OlapTableSink::send_chunk(RuntimeState* state, Chunk* chunk) {
    SCOPED_TIMER(_profile->total_time_counter());
    DCHECK(chunk->num_rows() > 0);
    size_t num_rows = chunk->num_rows();
    size_t serialize_size = serde::ProtobufChunkSerde::max_serialized_size(*chunk);

    {
        SCOPED_TIMER(_ts_profile->prepare_data_timer);
        {
            SCOPED_RAW_TIMER(&_convert_batch_ns);
            if (!_output_expr_ctxs.empty()) {
                _output_chunk = std::make_unique<Chunk>();
                for (size_t i = 0; i < _output_expr_ctxs.size(); ++i) {
                    ASSIGN_OR_RETURN(ColumnPtr tmp, _output_expr_ctxs[i]->evaluate(chunk));
                    ColumnPtr output_column = nullptr;
                    if (tmp->only_null()) {
                        // Only null column maybe lost type info
                        output_column = ColumnHelper::create_column(_output_tuple_desc->slots()[i]->type(), true);
                        output_column->append_nulls(num_rows);
                    } else {
                        // Unpack normal const column
                        output_column = ColumnHelper::unpack_and_duplicate_const_column(num_rows, tmp);
                    }
                    DCHECK(output_column != nullptr);
                    _output_chunk->append_column(std::move(output_column), _output_tuple_desc->slots()[i]->id());
                }
                chunk = _output_chunk.get();
            } else {
                chunk->reset_slot_id_to_index();
                for (size_t i = 0; i < _output_tuple_desc->slots().size(); ++i) {
                    chunk->set_slot_id_to_index(_output_tuple_desc->slots()[i]->id(), i);
                }
            }
            DCHECK_EQ(chunk->get_slot_id_to_index_map().size(), _output_tuple_desc->slots().size());
        }

        {
            SCOPED_TIMER(_ts_profile->alloc_auto_increment_timer);
            RETURN_IF_ERROR(_fill_auto_increment_id(chunk));
        }

        {
            SCOPED_RAW_TIMER(&_validate_data_ns);
            _validate_selection.assign(num_rows, VALID_SEL_OK);
            _validate_data(state, chunk);
        }
        {
            uint32_t num_rows_after_validate = SIMD::count_nonzero(_validate_selection);
            std::vector<int> invalid_row_indexs;

            // _enable_automatic_partition is true means destination table using automatic partition
            // _has_automatic_partition is true means last send_chunk already create partition in nonblocking mode
            // we don't need to create again since it will resend last chunk
            if (_enable_automatic_partition && !_has_automatic_partition) {
                _partition_not_exist_row_values.clear();

                RETURN_IF_ERROR(_vectorized_partition->find_tablets(chunk, &_partitions, &_tablet_indexes,
                                                                    &_validate_selection, &invalid_row_indexs, _txn_id,
                                                                    &_partition_not_exist_row_values));

                if (_partition_not_exist_row_values.size() > 0 && !_partition_not_exist_row_values[0].empty()) {
                    _is_automatic_partition_running.store(true, std::memory_order_release);
                    RETURN_IF_ERROR(_automatic_partition_token->submit_func([this] {
                        this->_automatic_partition_status = this->_automatic_create_partition();
                        if (!this->_automatic_partition_status.ok()) {
                            LOG(WARNING) << "Failed to automatic create partition, err="
                                         << this->_automatic_partition_status;
                        }
                        _is_automatic_partition_running.store(false, std::memory_order_release);
                    }));

                    if (_nonblocking_send_chunk) {
                        _has_automatic_partition = true;
                        return Status::EAgain("");
                    } else {
                        _automatic_partition_token->wait();
                        // after the partition is created, go through the data again
                        RETURN_IF_ERROR(_vectorized_partition->find_tablets(chunk, &_partitions, &_tablet_indexes,
                                                                            &_validate_selection, &invalid_row_indexs,
                                                                            _txn_id, nullptr));
                    }
                }
            } else {
                RETURN_IF_ERROR(_vectorized_partition->find_tablets(chunk, &_partitions, &_tablet_indexes,
                                                                    &_validate_selection, &invalid_row_indexs, _txn_id,
                                                                    nullptr));
                _has_automatic_partition = false;
            }
            // Note: must padding char column after find_tablets.
            _padding_char_column(chunk);

            // Arrange selection_idx by merging _validate_selection
            // If chunk num_rows is 6
            // _validate_selection is [1, 0, 0, 0, 1, 1]
            // selection_idx after arrange will be : [0, 4, 5]
            _validate_select_idx.resize(num_rows);
            size_t selected_size = 0;
            for (uint16_t i = 0; i < num_rows; ++i) {
                _validate_select_idx[selected_size] = i;
                selected_size += (_validate_selection[i] & 0x1);
            }
            _validate_select_idx.resize(selected_size);

            if (num_rows_after_validate - _validate_select_idx.size() > 0) {
                std::stringstream ss;
                if (_enable_automatic_partition) {
                    ss << "The row create partition failed since " << _automatic_partition_status.to_string();
                } else {
                    ss << "The row is out of partition ranges. Please add a new partition.";
                }
                if (!state->has_reached_max_error_msg_num() && invalid_row_indexs.size() > 0) {
                    std::string debug_row = chunk->debug_row(invalid_row_indexs.back());
                    state->append_error_msg_to_file(debug_row, ss.str());
                }
                for (auto i : invalid_row_indexs) {
                    if (state->enable_log_rejected_record()) {
                        state->append_rejected_record_to_file(chunk->rebuild_csv_row(i, ","), ss.str(),
                                                              chunk->source_filename());
                    } else {
                        break;
                    }
                }
            }

            int64_t num_rows_load_filtered = num_rows - _validate_select_idx.size();
            if (num_rows_load_filtered > 0) {
                _number_filtered_rows += num_rows_load_filtered;
                state->update_num_rows_load_filtered(num_rows_load_filtered);
            }
            _number_output_rows += _validate_select_idx.size();
            state->update_num_rows_load_sink(_validate_select_idx.size());
        }
    }
    // update incrementally so that FE can get the progress.
    // the real 'num_rows_load_total' will be set when sink being closed.
    _number_input_rows += num_rows;
    state->update_num_bytes_load_sink(serialize_size);
    StarRocksMetrics::instance()->load_rows_total.increment(num_rows);
    StarRocksMetrics::instance()->load_bytes_total.increment(serialize_size);

    SCOPED_TIMER(_ts_profile->send_data_timer);
    return _tablet_sink_sender->send_chunk(_schema.get(), _partitions, _tablet_indexes, _validate_select_idx,
                                           _index_id_partition_ids, chunk);
}

Status OlapTableSink::_fill_auto_increment_id(Chunk* chunk) {
    if (_auto_increment_slot_id == -1) {
        return Status::OK();
    }
    _has_auto_increment = true;

    auto& slot = _output_tuple_desc->slots()[_auto_increment_slot_id];
    RETURN_IF_ERROR(_fill_auto_increment_id_internal(chunk, slot, _schema->table_id()));

    return Status::OK();
}

Status OlapTableSink::_fill_auto_increment_id_internal(Chunk* chunk, SlotDescriptor* slot, int64_t table_id) {
    ColumnPtr& col = chunk->get_column_by_slot_id(slot->id());
    Status st;

    // For simplicity, we set NULL Literal in auto increment column when planning if the user does not specify a value.
    // We reuse NullableColumn to represent the auto-increment column because NullableColumn
    // has null_column_data to indicate the which row has the null value. It means that we should replace the auto increment
    // column value of this row with a system-generated value.
    if (!col->is_nullable()) {
        return Status::OK();
    }

    ColumnPtr& data_col = std::dynamic_pointer_cast<NullableColumn>(col)->data_column();
    std::vector<uint8_t> filter(std::dynamic_pointer_cast<NullableColumn>(col)->immutable_null_column_data());

    if (_keys_type == TKeysType::PRIMARY_KEYS && _output_tuple_desc->slots().back()->col_name() == "__op") {
        size_t op_column_id = chunk->num_columns() - 1;
        ColumnPtr& op_col = chunk->get_column_by_index(op_column_id);
        auto* ops = reinterpret_cast<const uint8_t*>(op_col->raw_data());
        size_t row = chunk->num_rows();

        for (size_t i = 0; i < row; ++i) {
            if (ops[i] == TOpType::DELETE) {
                filter[i] = 0;
            }
        }
    }

    uint32_t null_rows = SIMD::count_nonzero(filter);

    if (null_rows == 0) {
        return Status::OK();
    }

    switch (slot->type().type) {
    case TYPE_BIGINT: {
        std::vector<int64_t> ids(null_rows);
        if (!_miss_auto_increment_column) {
            RETURN_IF_ERROR(StorageEngine::instance()->get_next_increment_id_interval(table_id, null_rows, ids));
        } else {
            // partial update does not specify an auto-increment column,
            // it will be allocate in DeltaWriter.
            ids.assign(null_rows, 0);
        }
        RETURN_IF_ERROR((std::dynamic_pointer_cast<Int64Column>(data_col))->fill_range(ids, filter));
        break;
    }
    default:
        auto msg = fmt::format("illegal type size for auto-increment column");
        LOG(ERROR) << msg;
        return Status::InternalError(msg);
    }

    return Status::OK();
}

bool OlapTableSink::is_close_done() {
    return _tablet_sink_sender->is_close_done();
}

Status OlapTableSink::close(RuntimeState* state, Status close_status) {
    if (close_status.ok()) {
        SCOPED_TIMER(_profile->total_time_counter());
        SCOPED_TIMER(_ts_profile->close_timer);
        do {
            close_status = try_close(state);
            if (!close_status.ok()) break;
            SleepFor(MonoDelta::FromMilliseconds(5));
        } while (!is_close_done());
    }
    return close_wait(state, close_status);
}

Status OlapTableSink::close_wait(RuntimeState* state, Status close_status) {
    DeferOp end_span([&] { _span->End(); });
    _span->AddEvent("close");
    _span->SetAttribute("input_rows", _number_input_rows);
    _span->SetAttribute("output_rows", _number_output_rows);

    COUNTER_SET(_ts_profile->input_rows_counter, _number_input_rows);
    COUNTER_SET(_ts_profile->output_rows_counter, _number_output_rows);
    COUNTER_SET(_ts_profile->filtered_rows_counter, _number_filtered_rows);
    COUNTER_SET(_ts_profile->convert_chunk_timer, _convert_batch_ns);
    COUNTER_SET(_ts_profile->validate_data_timer, _validate_data_ns);

    Status status = _tablet_sink_sender->close_wait(state, close_status, _ts_profile);
    if (!status.ok()) {
        _span->SetStatus(trace::StatusCode::kError, status.get_error_msg());
    }
    return status;
}

void OlapTableSink::_print_varchar_error_msg(RuntimeState* state, const Slice& str, SlotDescriptor* desc) {
    if (state->has_reached_max_error_msg_num()) {
        return;
    }
    std::string error_str = str.to_string();
    if (error_str.length() > 100) {
        error_str = error_str.substr(0, 100);
        error_str.append("...");
    }
    std::string error_msg = strings::Substitute("String '$0'(length=$1) is too long. The max length of '$2' is $3",
                                                error_str, str.size, desc->col_name(), desc->type().len);
#if BE_TEST
    LOG(INFO) << error_msg;
#else
    state->append_error_msg_to_file("", error_msg);
#endif
}

void OlapTableSink::_print_decimal_error_msg(RuntimeState* state, const DecimalV2Value& decimal, SlotDescriptor* desc) {
    if (state->has_reached_max_error_msg_num()) {
        return;
    }
    std::string error_msg = strings::Substitute("Decimal '$0' is out of range. The type of '$1' is $2'",
                                                decimal.to_string(), desc->col_name(), desc->type().debug_string());
#if BE_TEST
    LOG(INFO) << error_msg;
#else
    state->append_error_msg_to_file("", error_msg);
#endif
}

template <LogicalType LT, typename CppType = RunTimeCppType<LT>>
void _print_decimalv3_error_msg(RuntimeState* state, const CppType& decimal, const SlotDescriptor* desc) {
    if (state->has_reached_max_error_msg_num()) {
        return;
    }
    auto decimal_str = DecimalV3Cast::to_string<CppType>(decimal, desc->type().precision, desc->type().scale);
    std::string error_msg = strings::Substitute("Decimal '$0' is out of range. The type of '$1' is $2'", decimal_str,
                                                desc->col_name(), desc->type().debug_string());
#if BE_TEST
    LOG(INFO) << error_msg;
#else
    state->append_error_msg_to_file("", error_msg);
#endif
}

template <LogicalType LT>
void OlapTableSink::_validate_decimal(RuntimeState* state, Chunk* chunk, Column* column, const SlotDescriptor* desc,
                                      std::vector<uint8_t>* validate_selection) {
    using CppType = RunTimeCppType<LT>;
    using ColumnType = RunTimeColumnType<LT>;
    auto* data_column = down_cast<ColumnType*>(ColumnHelper::get_data_column(column));
    const auto num_rows = data_column->get_data().size();
    auto* data = &data_column->get_data().front();

    int precision = desc->type().precision;
    const auto max_decimal = get_max_decimal<CppType>(precision);
    const auto min_decimal = get_min_decimal<CppType>(precision);

    for (auto i = 0; i < num_rows; ++i) {
        if ((*validate_selection)[i] == VALID_SEL_OK) {
            const auto& datum = data[i];
            if (datum > max_decimal || datum < min_decimal) {
                (*validate_selection)[i] = VALID_SEL_FAILED;
                _print_decimalv3_error_msg<LT>(state, datum, desc);
                if (state->enable_log_rejected_record()) {
                    auto decimal_str =
                            DecimalV3Cast::to_string<CppType>(datum, desc->type().precision, desc->type().scale);
                    std::string error_msg =
                            strings::Substitute("Decimal '$0' is out of range. The type of '$1' is $2'", decimal_str,
                                                desc->col_name(), desc->type().debug_string());
                    state->append_rejected_record_to_file(chunk->rebuild_csv_row(i, ","), error_msg,
                                                          chunk->source_filename());
                }
            }
        }
    }
}

/// TODO: recursively validate columns for nested columns, including array, map, struct
void OlapTableSink::_validate_data(RuntimeState* state, Chunk* chunk) {
    size_t num_rows = chunk->num_rows();
    for (int i = 0; i < _output_tuple_desc->slots().size(); ++i) {
        SlotDescriptor* desc = _output_tuple_desc->slots()[i];
        const ColumnPtr& column_ptr = chunk->get_column_by_slot_id(desc->id());

        // change validation selection value back to OK/FAILED
        // because in previous run, some validation selection value could
        // already be changed to VALID_SEL_OK_AND_NULL, and if we don't change back
        // to OK/FAILED, some rows can not be discarded any more.
        for (size_t j = 0; j < num_rows; j++) {
            _validate_selection[j] &= 0x1;
        }

        // update_column for auto increment column.
        if (_has_auto_increment && _auto_increment_slot_id == desc->id() && column_ptr->is_nullable()) {
            auto* nullable = down_cast<NullableColumn*>(column_ptr.get());
            // If nullable->has_null() && _null_expr_in_auto_increment == true, it means that user specify a
            // null value in auto increment column, we abort the entire chunk and append a single error msg.
            // Because be know nothing about whether this row is specified by the user as null or setted during planning.
            if (nullable->has_null() && _null_expr_in_auto_increment) {
                std::stringstream ss;
                ss << "NULL value in auto increment column '" << desc->col_name() << "'";

                for (size_t j = 0; j < num_rows; ++j) {
                    _validate_selection[j] = VALID_SEL_FAILED;
                    // If enable_log_rejected_record is true, we need to log the rejected record.
                    if (nullable->is_null(j) && state->enable_log_rejected_record()) {
                        state->append_rejected_record_to_file(chunk->rebuild_csv_row(j, ","), ss.str(),
                                                              chunk->source_filename());
                    }
                }
#if BE_TEST
                LOG(INFO) << ss.str();
#else
                if (!state->has_reached_max_error_msg_num()) {
                    state->append_error_msg_to_file("", ss.str());
                }
#endif
            }
            chunk->update_column(nullable->data_column(), desc->id());
        }

        // Validate column nullable info
        // Column nullable info need to respect slot nullable info
        if (desc->is_nullable() && !column_ptr->is_nullable()) {
            ColumnPtr new_column = NullableColumn::create(column_ptr, NullColumn::create(num_rows, 0));
            chunk->update_column(std::move(new_column), desc->id());
            // Auto increment column is not nullable but use NullableColumn to implement. We should skip the check for it.
        } else if (!desc->is_nullable() && column_ptr->is_nullable() &&
                   (!_has_auto_increment || _auto_increment_slot_id != desc->id())) {
            auto* nullable = down_cast<NullableColumn*>(column_ptr.get());
            // Non-nullable column shouldn't have null value,
            // If there is null value, which means expr compute has a error.
            if (nullable->has_null()) {
                NullData& nulls = nullable->null_column_data();
                for (size_t j = 0; j < num_rows; ++j) {
                    if (nulls[j]) {
                        _validate_selection[j] = VALID_SEL_FAILED;
                        std::stringstream ss;
                        ss << "NULL value in non-nullable column '" << desc->col_name() << "'";
#if BE_TEST
                        LOG(INFO) << ss.str();
#else
                        if (!state->has_reached_max_error_msg_num()) {
                            state->append_error_msg_to_file(chunk->debug_row(j), ss.str());
                        }
#endif
                        if (state->enable_log_rejected_record()) {
                            state->append_rejected_record_to_file(chunk->rebuild_csv_row(j, ","), ss.str(),
                                                                  chunk->source_filename());
                        }
                    }
                }
            }
            chunk->update_column(nullable->data_column(), desc->id());
        } else if (column_ptr->has_null()) {
            auto* nullable = down_cast<NullableColumn*>(column_ptr.get());
            NullData& nulls = nullable->null_column_data();
            for (size_t j = 0; j < num_rows; ++j) {
                if (nulls[j] && _validate_selection[j] != VALID_SEL_FAILED) {
                    // for this column, there are some null values in the row
                    // and we should skip checking of those null values.
                    _validate_selection[j] = VALID_SEL_OK_AND_NULL;
                }
            }
        }

        Column* column = chunk->get_column_by_slot_id(desc->id()).get();
        switch (desc->type().type) {
        case TYPE_CHAR:
        case TYPE_VARCHAR:
        case TYPE_VARBINARY: {
            uint32_t len = desc->type().len;
            Column* data_column = ColumnHelper::get_data_column(column);
            auto* binary = down_cast<BinaryColumn*>(data_column);
            Offsets& offset = binary->get_offset();
            for (size_t j = 0; j < num_rows; ++j) {
                if (_validate_selection[j] == VALID_SEL_OK) {
                    if (offset[j + 1] - offset[j] > len) {
                        _validate_selection[j] = VALID_SEL_FAILED;
                        _print_varchar_error_msg(state, binary->get_slice(j), desc);
                        if (state->enable_log_rejected_record()) {
                            std::string error_msg =
                                    strings::Substitute("String (length=$0) is too long. The max length of '$1' is $2",
                                                        binary->get_slice(j).size, desc->col_name(), desc->type().len);
                            state->append_rejected_record_to_file(chunk->rebuild_csv_row(j, ","), error_msg,
                                                                  chunk->source_filename());
                        }
                    }
                }
            }
            break;
        }
        case TYPE_DECIMALV2: {
            column = ColumnHelper::get_data_column(column);
            auto* decimal = down_cast<DecimalColumn*>(column);
            std::vector<DecimalV2Value>& datas = decimal->get_data();
            int scale = desc->type().scale;
            for (size_t j = 0; j < num_rows; ++j) {
                if (_validate_selection[j] == VALID_SEL_OK) {
                    if (datas[j].greater_than_scale(scale)) {
                        datas[j].round(&datas[j], scale, HALF_UP);
                    }

                    if (datas[j] > _max_decimalv2_val[i] || datas[j] < _min_decimalv2_val[i]) {
                        _validate_selection[j] = VALID_SEL_FAILED;
                        _print_decimal_error_msg(state, datas[j], desc);
                        if (state->enable_log_rejected_record()) {
                            std::string error_msg = strings::Substitute(
                                    "Decimal '$0' is out of range. The type of '$1' is $2'", datas[j].to_string(),
                                    desc->col_name(), desc->type().debug_string());
                            state->append_rejected_record_to_file(chunk->rebuild_csv_row(j, ","), error_msg,
                                                                  chunk->source_filename());
                        }
                    }
                }
            }
            break;
        }
        case TYPE_DECIMAL32:
            _validate_decimal<TYPE_DECIMAL32>(state, chunk, column, desc, &_validate_selection);
            break;
        case TYPE_DECIMAL64:
            _validate_decimal<TYPE_DECIMAL64>(state, chunk, column, desc, &_validate_selection);
            break;
        case TYPE_DECIMAL128:
            _validate_decimal<TYPE_DECIMAL128>(state, chunk, column, desc, &_validate_selection);
            break;
        case TYPE_MAP: {
            column = ColumnHelper::get_data_column(column);
            auto* map = down_cast<MapColumn*>(column);
            map->remove_duplicated_keys(true);
            break;
        }
        default:
            break;
        }
    }
}

void OlapTableSink::_padding_char_column(Chunk* chunk) {
    size_t num_rows = chunk->num_rows();
    for (auto desc : _output_tuple_desc->slots()) {
        if (desc->type().type == TYPE_CHAR) {
            Column* column = chunk->get_column_by_slot_id(desc->id()).get();
            Column* data_column = ColumnHelper::get_data_column(column);
            auto* binary = down_cast<BinaryColumn*>(data_column);
            Offsets& offset = binary->get_offset();
            uint32_t len = desc->type().len;

            Bytes& bytes = binary->get_bytes();

            // Padding 0 to CHAR field, the storage bitmap index and zone map need it.
            // TODO(kks): we could improve this if there are many null values
            auto new_binary = BinaryColumn::create();
            Offsets& new_offset = new_binary->get_offset();
            Bytes& new_bytes = new_binary->get_bytes();
            new_offset.resize(num_rows + 1);
            new_bytes.assign(num_rows * len, 0); // padding 0

            uint32_t from = 0;
            for (size_t j = 0; j < num_rows; ++j) {
                uint32_t copy_data_len = std::min(len, offset[j + 1] - offset[j]);
                strings::memcpy_inlined(new_bytes.data() + from, bytes.data() + offset[j], copy_data_len);
                from += len; // no copy data will be 0
            }

            for (size_t j = 1; j <= num_rows; ++j) {
                new_offset[j] = len * j;
            }

            if (desc->is_nullable()) {
                auto* nullable_column = down_cast<NullableColumn*>(column);
                ColumnPtr new_column = NullableColumn::create(new_binary, nullable_column->null_column());
                chunk->update_column(new_column, desc->id());
            } else {
                chunk->update_column(new_binary, desc->id());
            }
        }
    }
}

Status OlapTableSink::reset_epoch(RuntimeState* state) {
    pipeline::StreamEpochManager* stream_epoch_manager = state->query_ctx()->stream_epoch_manager();
    DCHECK(stream_epoch_manager);
    _txn_id = stream_epoch_manager->epoch_info().txn_id;
    _channels.clear();
    _node_channels.clear();
    _failed_channels.clear();
    return Status::OK();
}

} // namespace starrocks::stream_load
