// This file is made available under Elastic License 2.0.
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
#include <sstream>

#include "column/binary_column.h"
#include "column/chunk.h"
#include "column/column_helper.h"
#include "column/nullable_column.h"
#include "exprs/expr.h"
#include "gutil/strings/fastmem.h"
#include "gutil/strings/substitute.h"
#include "runtime/current_thread.h"
#include "runtime/exec_env.h"
#include "runtime/runtime_state.h"
#include "serde/protobuf_serde.h"
#include "service/brpc.h"
#include "simd/simd.h"
#include "storage/hll.h"
#include "util/brpc_stub_cache.h"
#include "util/defer_op.h"
#include "util/monotime.h"
#include "util/thread.h"
#include "util/uid_util.h"

static const uint8_t VALID_SEL_FAILED = 0x0;
static const uint8_t VALID_SEL_OK = 0x1;
// it's a valid value and selected, but it's null
// and we don't need following extra check
// make sure the least bit is 1.
static const uint8_t VALID_SEL_OK_AND_NULL = 0x3;

namespace starrocks::stream_load {

NodeChannel::NodeChannel(OlapTableSink* parent, int64_t index_id, int64_t node_id, int32_t schema_hash)
        : _parent(parent), _index_id(index_id), _node_id(node_id), _schema_hash(schema_hash) {
    // restrict the chunk memory usage of send queue
    _mem_tracker = std::make_unique<MemTracker>(64 * 1024 * 1024, "", nullptr);
}

NodeChannel::~NodeChannel() {
    if (_open_closure != nullptr) {
        if (_open_closure->unref()) {
            delete _open_closure;
        }
        _open_closure = nullptr;
    }
    if (_add_batch_closure != nullptr) {
        // it's safe to delete, but may take some time to wait until brpc joined
        delete _add_batch_closure;
        _add_batch_closure = nullptr;
    }
    _cur_add_chunk_request.release_id();
}

Status NodeChannel::init(RuntimeState* state) {
    _tuple_desc = _parent->_output_tuple_desc;
    _node_info = _parent->_nodes_info->find_node(_node_id);
    if (_node_info == nullptr) {
        std::stringstream ss;
        ss << "unknown node id, id=" << _node_id;
        _cancelled = true;
        return Status::InternalError(ss.str());
    }

    _stub = state->exec_env()->brpc_stub_cache()->get_stub(_node_info->host, _node_info->brpc_port);
    if (_stub == nullptr) {
        LOG(WARNING) << "Get rpc stub failed, host=" << _node_info->host << ", port=" << _node_info->brpc_port;
        _cancelled = true;
        return Status::InternalError("get rpc stub failed");
    }

    // Initialize _cur_add_chunk_request
    _cur_add_chunk_request.set_allocated_id(&_parent->_load_id);
    _cur_add_chunk_request.set_index_id(_index_id);
    _cur_add_chunk_request.set_sender_id(_parent->_sender_id);
    _cur_add_chunk_request.set_eos(false);
    _cur_chunk = std::make_unique<vectorized::Chunk>();

    _rpc_timeout_ms = state->query_options().query_timeout * 1000;

    // for get global_dict
    _runtime_state = state;

    _load_info = "load_id=" + print_id(_parent->_load_id) + ", txn_id=" + std::to_string(_parent->_txn_id);
    _name = "NodeChannel[" + std::to_string(_index_id) + "-" + std::to_string(_node_id) + "]";
    return Status::OK();
}

void NodeChannel::open() {
    PTabletWriterOpenRequest request;
    request.set_allocated_id(&_parent->_load_id);
    request.set_index_id(_index_id);
    request.set_txn_id(_parent->_txn_id);
    request.set_allocated_schema(_parent->_schema->to_protobuf());
    for (auto& tablet : _all_tablets) {
        auto ptablet = request.add_tablets();
        ptablet->set_partition_id(tablet.partition_id);
        ptablet->set_tablet_id(tablet.tablet_id);
    }
    request.set_num_senders(_parent->_num_senders);
    request.set_need_gen_rollup(_parent->_need_gen_rollup);
    // load_mem_limit equal 0 means no limit
    if (_parent->_load_mem_limit != 0) {
        request.set_load_mem_limit(_parent->_load_mem_limit);
    }
    request.set_load_channel_timeout_s(_parent->_load_channel_timeout_s);
    // when load coordinator BE have upgrade to 2.1 but other BE still in 2.0 or previous
    // we need use is_vectorized to make other BE open vectorized delta writer
    request.set_is_vectorized(true);

    // set global dict
    const auto& global_dict = _runtime_state->get_load_global_dict_map();
    for (size_t i = 0; i < request.schema().slot_descs_size(); i++) {
        auto slot = request.mutable_schema()->mutable_slot_descs(i);
        auto it = global_dict.find(slot->id());
        if (it != global_dict.end()) {
            auto dict = it->second.first;
            for (auto& item : dict) {
                slot->add_global_dict_words(item.first.to_string());
            }
        }
    }

    _open_closure = new RefCountClosure<PTabletWriterOpenResult>();
    _open_closure->ref();

    // This ref is for RPC's reference
    _open_closure->ref();
    _open_closure->cntl.set_timeout_ms(config::tablet_writer_open_rpc_timeout_sec * 1000);
    _stub->tablet_writer_open(&_open_closure->cntl, &request, &_open_closure->result, _open_closure);
    request.release_id();
    request.release_schema();
}

Status NodeChannel::open_wait() {
    _open_closure->join();
    if (_open_closure->cntl.Failed()) {
        LOG(WARNING) << "failed to open tablet writer, error=" << berror(_open_closure->cntl.ErrorCode())
                     << ", error_text=" << _open_closure->cntl.ErrorText();
        _cancelled = true;
        return Status::InternalError("failed to open tablet writer");
    }
    Status status(_open_closure->result.status());
    if (_open_closure->unref()) {
        delete _open_closure;
    }
    _open_closure = nullptr;

    if (!status.ok()) {
        _cancelled = true;
        return status;
    }

    // add batch closure
    _add_batch_closure = ReusableClosure<PTabletWriterAddBatchResult>::create();
    _add_batch_closure->addFailedHandler([this]() {
        _cancelled = true;
        LOG(WARNING) << name() << " add batch req rpc failed, " << print_load_info() << ", node=" << node_info()->host
                     << ":" << node_info()->brpc_port;
    });

    _add_batch_closure->addSuccessHandler([this](const PTabletWriterAddBatchResult& result, bool is_last_rpc) {
        Status status(result.status());
        if (status.ok()) {
            if (is_last_rpc) {
                for (auto& tablet : result.tablet_vec()) {
                    TTabletCommitInfo commit_info;
                    commit_info.tabletId = tablet.tablet_id();
                    commit_info.backendId = _node_id;
                    std::vector<std::string> invalid_dict_cache_columns;
                    for (auto& col_name : tablet.invalid_dict_cache_columns()) {
                        invalid_dict_cache_columns.emplace_back(col_name);
                    }
                    commit_info.__set_invalid_dict_cache_columns(invalid_dict_cache_columns);

                    std::vector<std::string> valid_dict_cache_columns;
                    for (auto& col_name : tablet.valid_dict_cache_columns()) {
                        valid_dict_cache_columns.emplace_back(col_name);
                    }
                    commit_info.__set_valid_dict_cache_columns(valid_dict_cache_columns);

                    _tablet_commit_infos.emplace_back(std::move(commit_info));
                }
                _add_batches_finished = true;
            }
        } else {
            _cancelled = true;
            LOG(WARNING) << name() << " add batch req success but status isn't ok, " << print_load_info()
                         << ", node=" << node_info()->host << ":" << node_info()->brpc_port
                         << ", errmsg=" << status.get_error_msg();
        }

        if (result.has_execution_time_us()) {
            _add_batch_counter.add_batch_execution_time_us += result.execution_time_us();
            _add_batch_counter.add_batch_wait_lock_time_us += result.wait_lock_time_us();
            _add_batch_counter.add_batch_num++;
        }
    });

    return status;
}

Status NodeChannel::add_chunk(vectorized::Chunk* chunk, const int64_t* tablet_ids, const uint32_t* indexes,
                              uint32_t from, uint32_t size) {
    // If add_row() when _eos_is_produced==true, there must be sth wrong, we can only mark this channel as failed.
    if (_cancelled | _eos_is_produced) {
        return Status::InternalError("already stopped, can't add_row. cancelled/eos: ");
    }

    // We use OlapTableSink mem_tracker which has the same ancestor of _plan node,
    // so in the ideal case, mem limit is a matter for _plan node.
    // But there is still some unfinished things, we do mem limit here temporarily.
    // _cancelled may be set by rpc callback, and it's possible that _cancelled might be set in any of the steps below.
    // It's fine to do a fake add_row() and return OK, because we will check _cancelled in next add_row() or mark_close().
    while (!_cancelled && ((_mem_tracker->any_limit_exceeded() && _pending_batches_num > 0) ||
                           _pending_batches_num >= _max_pending_batches_num)) {
        SCOPED_RAW_TIMER(&_mem_exceeded_block_ns);
        SleepFor(MonoDelta::FromMilliseconds(10));
    }

    if (_cur_chunk->columns().empty()) {
        _cur_chunk = chunk->clone_empty_with_slot();
    }

    if (_cur_chunk->num_rows() >= _runtime_state->chunk_size()) {
        {
            SCOPED_RAW_TIMER(&_queue_push_lock_ns);
            std::lock_guard<std::mutex> l(_pending_batches_lock);
            _mem_tracker->consume(_cur_chunk->memory_usage());
            _pending_chunks.emplace(std::move(_cur_chunk), _cur_add_chunk_request);
            _pending_batches_num++;
        }
        _cur_chunk = chunk->clone_empty_with_slot();
        _cur_add_chunk_request.clear_tablet_ids();
    }

    _cur_chunk->append_selective(*chunk, indexes, from, size);
    for (size_t i = 0; i < size; ++i) {
        _cur_add_chunk_request.add_tablet_ids(tablet_ids[indexes[from + i]]);
    }
    return Status::OK();
}

Status NodeChannel::mark_close() {
    auto st = none_of({_cancelled, _eos_is_produced});
    if (!st.ok()) {
        return st.clone_and_prepend("already stopped, can't mark as closed. cancelled/eos: ");
    }

    _cur_add_chunk_request.set_eos(true);
    {
        std::lock_guard<std::mutex> l(_pending_batches_lock);
        DCHECK(_cur_chunk != nullptr);
        _mem_tracker->consume(_cur_chunk->memory_usage());
        _pending_chunks.emplace(std::move(_cur_chunk), _cur_add_chunk_request);
        _pending_batches_num++;
    }

    _eos_is_produced = true;
    return Status::OK();
}

Status NodeChannel::close_wait(RuntimeState* state) {
    auto st = none_of({_cancelled, !_eos_is_produced});
    if (!st.ok()) {
        return st.clone_and_prepend("already stopped, skip waiting for close. cancelled/!eos: ");
    }

    // waiting for finished, it may take a long time, so we could't set a timeout
    MonotonicStopWatch timer;
    timer.start();
    while (!_add_batches_finished && !_cancelled) {
        SleepFor(MonoDelta::FromMilliseconds(1));
    }
    timer.stop();
    VLOG(1) << name() << " close_wait cost: " << timer.elapsed_time() / 1000000 << " ms";

    if (_add_batches_finished) {
        {
            std::lock_guard<std::mutex> lg(_pending_batches_lock);
            CHECK(_pending_chunks.empty()) << name();
            CHECK(_cur_chunk == nullptr) << name();
        }
        state->tablet_commit_infos().insert(state->tablet_commit_infos().end(),
                                            std::make_move_iterator(_tablet_commit_infos.begin()),
                                            std::make_move_iterator(_tablet_commit_infos.end()));
        return Status::OK();
    }

    return Status::InternalError("close wait failed coz rpc error");
}

void NodeChannel::cancel() {
    // we don't need to wait last rpc finished, cause closure's release/reset will join.
    // But do we need brpc::StartCancel(call_id)?
    _cancelled = true;

    PTabletWriterCancelRequest request;
    request.set_allocated_id(&_parent->_load_id);
    request.set_index_id(_index_id);
    request.set_sender_id(_parent->_sender_id);

    auto closure = new RefCountClosure<PTabletWriterCancelResult>();

    closure->ref();
    closure->cntl.set_timeout_ms(_rpc_timeout_ms);
    _stub->tablet_writer_cancel(&closure->cntl, &request, &closure->result, closure);
    request.release_id();
}

int NodeChannel::try_send_chunk_and_fetch_status() {
    if (_cancelled | _send_finished) {
        return 0;
    }

    if (!_add_batch_closure->is_packet_in_flight() && _pending_batches_num > 0) {
        SCOPED_RAW_TIMER(&_actual_consume_ns);
        AddChunkReq send_chunk;
        {
            std::lock_guard<std::mutex> lg(_pending_batches_lock);
            DCHECK(!_pending_chunks.empty());
            send_chunk = std::move(_pending_chunks.front());
            _pending_chunks.pop();
            _mem_tracker->release(send_chunk.first->memory_usage());
            _pending_batches_num--;
        }

        auto chunk = std::move(send_chunk.first);
        DCHECK(chunk != nullptr);
        auto request = std::move(send_chunk.second); // doesn't need to be saved in heap

        // tablet_ids has already set when add row
        request.set_packet_seq(_next_packet_seq);
        if (chunk->num_rows() > 0) {
            SCOPED_RAW_TIMER(&_serialize_batch_ns);
            StatusOr<ChunkPB> chunk_pb = serde::ProtobufChunkSerde::serialize(*chunk);
            CHECK(chunk_pb.ok()) << chunk_pb.status(); // FIXME
            request.mutable_chunk()->Swap(&chunk_pb.value());
        }

        _add_batch_closure->reset();
        _add_batch_closure->cntl.set_timeout_ms(_rpc_timeout_ms);

        if (request.eos()) {
            for (auto pid : _parent->_partition_ids) {
                request.add_partition_ids(pid);
            }

            // eos request must be the last request
            _add_batch_closure->end_mark();
            _send_finished = true;
            DCHECK(_pending_batches_num == 0);
        }

        _add_batch_closure->set_in_flight();
        _stub->tablet_writer_add_chunk(&_add_batch_closure->cntl, &request, &_add_batch_closure->result,
                                       _add_batch_closure);
        _next_packet_seq++;
    }

    return _send_finished ? 0 : 1;
}

Status NodeChannel::none_of(std::initializer_list<bool> vars) {
    bool none = std::none_of(vars.begin(), vars.end(), [](bool var) { return var; });
    Status st = Status::OK();
    if (!none) {
        std::string vars_str;
        std::for_each(vars.begin(), vars.end(), [&vars_str](bool var) -> void { vars_str += (var ? "1/" : "0/"); });
        if (!vars_str.empty()) {
            vars_str.pop_back(); // 0/1/0/ -> 0/1/0
        }
        st = Status::InternalError(vars_str);
    }

    return st;
}

void NodeChannel::clear_all_batches() {
    std::lock_guard<std::mutex> lg(_pending_batches_lock);
    while (!_pending_chunks.empty()) {
        _pending_chunks.pop();
    }
    if (_cur_chunk != nullptr) {
        _cur_chunk.reset();
    }
}

IndexChannel::~IndexChannel() = default;

Status IndexChannel::init(RuntimeState* state, const std::vector<TTabletWithPartition>& tablets) {
    for (const auto& tablet : tablets) {
        auto* location = _parent->_location->find_tablet(tablet.tablet_id);
        if (location == nullptr) {
            LOG(WARNING) << "unknown tablet, tablet_id=" << tablet.tablet_id;
            return Status::InternalError("unknown tablet");
        }
        std::vector<NodeChannel*> channels;
        std::vector<int64_t> bes;
        for (auto& node_id : location->node_ids) {
            NodeChannel* channel = nullptr;
            auto it = _node_channels.find(node_id);
            if (it == std::end(_node_channels)) {
                auto channel_ptr = std::make_unique<NodeChannel>(_parent, _index_id, node_id, _schema_hash);
                channel = channel_ptr.get();
                _node_channels.emplace(node_id, std::move(channel_ptr));
            } else {
                channel = it->second.get();
            }
            channel->add_tablet(tablet);
            channels.push_back(channel);
            bes.emplace_back(node_id);
        }
        _channels_by_tablet.emplace(tablet.tablet_id, std::move(channels));
        _tablet_to_be.emplace(tablet.tablet_id, std::move(bes));
    }
    for (auto& it : _node_channels) {
        RETURN_IF_ERROR(it.second->init(state));
    }
    return Status::OK();
}

bool IndexChannel::has_intolerable_failure() {
    return _failed_channels.size() >= ((_parent->_num_repicas + 1) / 2);
}

OlapTableSink::OlapTableSink(ObjectPool* pool, const RowDescriptor& row_desc, const std::vector<TExpr>& texprs,
                             Status* status)
        : _pool(pool), _input_row_desc(row_desc), _filter_bitmap(1024) {
    if (!texprs.empty()) {
        *status = Expr::create_expr_trees(_pool, texprs, &_output_expr_ctxs);
    }
}

OlapTableSink::~OlapTableSink() {
    // We clear NodeChannels' batches here, cuz NodeChannels' batches destruction will use
    // OlapTableSink::_mem_tracker and its parents.
    // But their destructions are after OlapTableSink's.
    // TODO: can be remove after all MemTrackers become shared.
    for (auto& index_channel : _channels) {
        index_channel->for_each_node_channel([](NodeChannel* ch) { ch->clear_all_batches(); });
    }
}

Status OlapTableSink::init(const TDataSink& t_sink) {
    DCHECK(t_sink.__isset.olap_table_sink);
    const auto& table_sink = t_sink.olap_table_sink;
    _load_id.set_hi(table_sink.load_id.hi);
    _load_id.set_lo(table_sink.load_id.lo);
    _txn_id = table_sink.txn_id;
    _num_repicas = table_sink.num_replicas;
    _need_gen_rollup = table_sink.need_gen_rollup;
    _tuple_desc_id = table_sink.tuple_id;
    _schema = std::make_shared<OlapTableSchemaParam>();
    RETURN_IF_ERROR(_schema->init(table_sink.schema));
    _vectorized_partition = _pool->add(new vectorized::OlapTablePartitionParam(_schema, table_sink.partition));
    RETURN_IF_ERROR(_vectorized_partition->init());
    _location = _pool->add(new OlapTableLocationParam(table_sink.location));
    _nodes_info = _pool->add(new StarRocksNodesInfo(table_sink.nodes_info));

    if (table_sink.__isset.load_channel_timeout_s) {
        _load_channel_timeout_s = table_sink.load_channel_timeout_s;
    } else {
        _load_channel_timeout_s = config::streaming_load_rpc_max_alive_time_sec;
    }

    return Status::OK();
}

Status OlapTableSink::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(DataSink::prepare(state));

    _sender_id = state->per_fragment_instance_idx();
    _num_senders = state->num_per_fragment_instances();

    // profile must add to state's object pool
    _profile = state->obj_pool()->add(new RuntimeProfile("OlapTableSink"));

    SCOPED_TIMER(_profile->total_time_counter());

    // Prepare the exprs to run.
    RETURN_IF_ERROR(Expr::prepare(_output_expr_ctxs, state, _input_row_desc));

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

    _max_decimal_val.resize(_output_tuple_desc->slots().size());
    _min_decimal_val.resize(_output_tuple_desc->slots().size());

    _max_decimalv2_val.resize(_output_tuple_desc->slots().size());
    _min_decimalv2_val.resize(_output_tuple_desc->slots().size());
    // check if need validate batch
    for (int i = 0; i < _output_tuple_desc->slots().size(); ++i) {
        auto* slot = _output_tuple_desc->slots()[i];
        switch (slot->type().type) {
        case TYPE_DECIMAL:
            _max_decimal_val[i].to_max_decimal(slot->type().precision, slot->type().scale);
            _min_decimal_val[i].to_min_decimal(slot->type().precision, slot->type().scale);
            break;
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

    // add all counter
    _input_rows_counter = ADD_COUNTER(_profile, "RowsRead", TUnit::UNIT);
    _output_rows_counter = ADD_COUNTER(_profile, "RowsReturned", TUnit::UNIT);
    _filtered_rows_counter = ADD_COUNTER(_profile, "RowsFiltered", TUnit::UNIT);
    _send_data_timer = ADD_TIMER(_profile, "SendDataTime");
    _convert_batch_timer = ADD_TIMER(_profile, "ConvertBatchTime");
    _validate_data_timer = ADD_TIMER(_profile, "ValidateDataTime");
    _open_timer = ADD_TIMER(_profile, "OpenTime");
    _close_timer = ADD_TIMER(_profile, "CloseWaitTime");
    _non_blocking_send_timer = ADD_TIMER(_profile, "NonBlockingSendTime");
    _serialize_batch_timer = ADD_TIMER(_profile, "SerializeBatchTime");
    _load_mem_limit = state->get_load_mem_limit();

    // open all channels
    const auto& partitions = _vectorized_partition->get_partitions();
    for (int i = 0; i < _schema->indexes().size(); ++i) {
        // collect all tablets belong to this rollup
        std::vector<TTabletWithPartition> tablets;
        auto* index = _schema->indexes()[i];
        for (auto* part : partitions) {
            for (auto tablet : part->indexes[i].tablets) {
                TTabletWithPartition tablet_with_partition;
                tablet_with_partition.partition_id = part->id;
                tablet_with_partition.tablet_id = tablet;
                tablets.emplace_back(std::move(tablet_with_partition));
            }
        }
        auto channel = std::make_unique<IndexChannel>(this, index->index_id, index->schema_hash);
        RETURN_IF_ERROR(channel->init(state, tablets));
        _channels.emplace_back(std::move(channel));
    }

    return Status::OK();
}

Status OlapTableSink::open(RuntimeState* state) {
    SCOPED_TIMER(_profile->total_time_counter());
    SCOPED_TIMER(_open_timer);
    // Prepare the exprs to run.
    RETURN_IF_ERROR(Expr::open(_output_expr_ctxs, state));

    for (auto& index_channel : _channels) {
        index_channel->for_each_node_channel([](NodeChannel* ch) { ch->open(); });
    }

    for (auto& index_channel : _channels) {
        index_channel->for_each_node_channel([&index_channel](NodeChannel* ch) {
            auto st = ch->open_wait();
            if (!st.ok()) {
                LOG(WARNING) << ch->name() << ": tablet open failed, " << ch->print_load_info()
                             << ", node=" << ch->node_info()->host << ":" << ch->node_info()->brpc_port
                             << ", errmsg=" << st.get_error_msg();
                index_channel->mark_as_failed(ch);
            }
        });

        if (index_channel->has_intolerable_failure()) {
            LOG(WARNING) << "open failed, load_id=" << _load_id;
            return Status::InternalError("intolerable failure in opening node channels");
        }
    }

    _sender_thread = std::thread(&OlapTableSink::_send_chunk_process, this);
    Thread::set_thread_name(_sender_thread, "olap_table_send");
    return Status::OK();
}

Status OlapTableSink::send_chunk(RuntimeState* state, vectorized::Chunk* chunk) {
    SCOPED_TIMER(_profile->total_time_counter());
    DCHECK(chunk->num_rows() > 0);
    size_t num_rows = chunk->num_rows();
    _number_input_rows += num_rows;
    size_t serialize_size = serde::ProtobufChunkSerde::max_serialized_size(*chunk);
    // update incrementally so that FE can get the progress.
    // the real 'num_rows_load_total' will be set when sink being closed.
    state->update_num_rows_load_total(num_rows);
    state->update_num_bytes_load_total(serialize_size);
    StarRocksMetrics::instance()->load_rows_total.increment(num_rows);
    StarRocksMetrics::instance()->load_bytes_total.increment(serialize_size);

    {
        SCOPED_RAW_TIMER(&_convert_batch_ns);
        if (!_output_expr_ctxs.empty()) {
            _output_chunk = std::make_unique<vectorized::Chunk>();
            for (size_t i = 0; i < _output_expr_ctxs.size(); ++i) {
                ColumnPtr tmp = _output_expr_ctxs[i]->evaluate(chunk);
                ColumnPtr output_column = nullptr;
                if (tmp->only_null()) {
                    // Only null column maybe lost type info
                    output_column =
                            vectorized::ColumnHelper::create_column(_output_tuple_desc->slots()[i]->type(), true);
                    output_column->append_nulls(num_rows);
                } else {
                    // Unpack normal const column
                    output_column = vectorized::ColumnHelper::unpack_and_duplicate_const_column(num_rows, tmp);
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

    SCOPED_RAW_TIMER(&_send_data_ns);
    {
        _validate_selection.assign(num_rows, VALID_SEL_OK);
        SCOPED_RAW_TIMER(&_validate_data_ns);
        _validate_data(state, chunk);
    }
    {
        uint32_t num_rows_after_validate = SIMD::count_nonzero(_validate_selection);
        int invalid_row_index = 0;
        _vectorized_partition->find_tablets(chunk, &_partitions, &_tablet_indexes, &_validate_selection,
                                            &invalid_row_index);

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
            std::string debug_row = chunk->debug_row(invalid_row_index);
            state->append_error_msg_to_file(debug_row,
                                            "The row is out of partition ranges. Please add a new partition.");
        }

        _number_filtered_rows += (num_rows - _validate_select_idx.size());
        _number_output_rows += _validate_select_idx.size();
    }

    size_t selection_size = _validate_select_idx.size();
    if (selection_size == 0) {
        return Status::OK();
    }
    _tablet_ids.resize(num_rows);
    if (num_rows > selection_size) {
        for (size_t i = 0; i < selection_size; ++i) {
            _partition_ids.emplace(_partitions[_validate_select_idx[i]]->id);
        }

        size_t index_size = _partitions[_validate_select_idx[0]]->indexes.size();
        for (size_t i = 0; i < index_size; ++i) {
            for (size_t j = 0; j < selection_size; ++j) {
                uint16_t selection = _validate_select_idx[j];
                _tablet_ids[selection] = _partitions[selection]->indexes[i].tablets[_tablet_indexes[selection]];
            }
            RETURN_IF_ERROR(_send_chunk_by_node(chunk, _channels[i].get(), _validate_select_idx));
        }
    } else { // Improve for all rows are selected
        for (size_t i = 0; i < num_rows; ++i) {
            _partition_ids.emplace(_partitions[i]->id);
        }

        size_t index_size = _partitions[0]->indexes.size();
        for (size_t i = 0; i < index_size; ++i) {
            for (size_t j = 0; j < num_rows; ++j) {
                _tablet_ids[j] = _partitions[j]->indexes[i].tablets[_tablet_indexes[j]];
            }
            RETURN_IF_ERROR(_send_chunk_by_node(chunk, _channels[i].get(), _validate_select_idx));
        }
    }
    return Status::OK();
}

Status OlapTableSink::_send_chunk_by_node(vectorized::Chunk* chunk, IndexChannel* channel,
                                          std::vector<uint16_t>& selection_idx) {
    for (auto& it : channel->_node_channels) {
        int64_t be_id = it.first;
        _node_select_idx.clear();
        _node_select_idx.reserve(selection_idx.size());
        for (unsigned short selection : selection_idx) {
            std::vector<int64_t>& be_ids = channel->_tablet_to_be.find(_tablet_ids[selection])->second;
            if (std::find(be_ids.begin(), be_ids.end(), be_id) != be_ids.end()) {
                _node_select_idx.emplace_back(selection);
            }
        }
        NodeChannel* node = it.second.get();
        auto st = node->add_chunk(chunk, _tablet_ids.data(), _node_select_idx.data(), 0, _node_select_idx.size());

        if (!st.ok()) {
            channel->mark_as_failed(node);
        }
        if (channel->has_intolerable_failure()) {
            return Status::InternalError("index channel has intolerable failure");
        }
    }
    return Status::OK();
}

Status OlapTableSink::close(RuntimeState* state, Status close_status) {
    Status status = close_status;
    if (status.ok()) {
        // only if status is ok can we call this _profile->total_time_counter().
        // if status is not ok, this sink may not be prepared, so that _profile is null
        SCOPED_TIMER(_profile->total_time_counter());
        // BE id -> add_batch method counter
        std::unordered_map<int64_t, AddBatchCounter> node_add_batch_counter_map;
        int64_t serialize_batch_ns = 0, mem_exceeded_block_ns = 0, queue_push_lock_ns = 0, actual_consume_ns = 0;
        {
            SCOPED_TIMER(_close_timer);
            for (auto& index_channel : _channels) {
                index_channel->for_each_node_channel([](NodeChannel* ch) { ch->mark_close(); });
            }

            bool intolerable_failure = false;
            int ordinal = 0;
            while (ordinal < _channels.size() && !intolerable_failure) {
                auto& index_channel = _channels[ordinal];
                index_channel->for_each_node_channel([&index_channel, &state, &node_add_batch_counter_map,
                                                      &serialize_batch_ns, &mem_exceeded_block_ns, &queue_push_lock_ns,
                                                      &actual_consume_ns](NodeChannel* ch) {
                    auto channel_status = ch->close_wait(state);
                    if (!channel_status.ok()) {
                        LOG(WARNING) << "close channel failed. channel_name=" << ch->name()
                                     << ", load_info=" << ch->print_load_info()
                                     << ", error_msg=" << channel_status.get_error_msg();
                        index_channel->mark_as_failed(ch);
                    }
                    ch->time_report(&node_add_batch_counter_map, &serialize_batch_ns, &mem_exceeded_block_ns,
                                    &queue_push_lock_ns, &actual_consume_ns);
                });
                if (index_channel->has_intolerable_failure()) {
                    status = Status::InternalError(
                            strings::Substitute("close index channel failed, load_id=$0", print_id(_load_id)));
                    intolerable_failure = true;
                }
                ordinal++;
            }
            for (int i = ordinal; i < _channels.size(); ++i) {
                auto& index_channel = _channels[i];
                index_channel->for_each_node_channel([](NodeChannel* ch) { ch->cancel(); });
            }
        }
        // TODO need to be improved
        LOG(INFO) << "total mem_exceeded_block_ns=" << mem_exceeded_block_ns
                  << " total queue_push_lock_ns=" << queue_push_lock_ns
                  << " total actual_consume_ns=" << actual_consume_ns;

        COUNTER_SET(_input_rows_counter, _number_input_rows);
        COUNTER_SET(_output_rows_counter, _number_output_rows);
        COUNTER_SET(_filtered_rows_counter, _number_filtered_rows);
        COUNTER_SET(_send_data_timer, _send_data_ns);
        COUNTER_SET(_convert_batch_timer, _convert_batch_ns);
        COUNTER_SET(_validate_data_timer, _validate_data_ns);
        COUNTER_SET(_non_blocking_send_timer, _non_blocking_send_ns);
        COUNTER_SET(_serialize_batch_timer, serialize_batch_ns);
        // _number_input_rows don't contain num_rows_load_filtered and num_rows_load_unselected in scan node
        int64_t num_rows_load_total =
                _number_input_rows + state->num_rows_load_filtered() + state->num_rows_load_unselected();
        state->set_num_rows_load_total(num_rows_load_total);
        state->update_num_rows_load_filtered(_number_filtered_rows);

        // print log of add batch time of all node, for tracing load performance easily
        std::stringstream ss;
        ss << "Closed olap table sink load_id=" << print_id(_load_id) << " txn_id=" << _txn_id
           << ", node add batch time(ms)/wait lock time(ms)/num: ";
        for (auto const& pair : node_add_batch_counter_map) {
            ss << "{" << pair.first << ":(" << (pair.second.add_batch_execution_time_us / 1000) << ")("
               << (pair.second.add_batch_wait_lock_time_us / 1000) << ")(" << pair.second.add_batch_num << ")} ";
        }
        LOG(INFO) << ss.str();
    } else {
        for (auto& channel : _channels) {
            channel->for_each_node_channel([](NodeChannel* ch) { ch->cancel(); });
        }
    }

    // Sender join() must put after node channels mark_close/cancel.
    // But there is no specific sequence required between sender join() & close_wait().
    if (_sender_thread.joinable()) {
        _sender_thread.join();
    }

    Expr::close(_output_expr_ctxs, state);
    return status;
}

void OlapTableSink::_print_varchar_error_msg(RuntimeState* state, const Slice& str, SlotDescriptor* desc) {
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
    std::string error_msg = strings::Substitute("Decimal '$0' is out of range. The type of '$1' is $2'",
                                                decimal.to_string(), desc->col_name(), desc->type().debug_string());
#if BE_TEST
    LOG(INFO) << error_msg;
#else
    state->append_error_msg_to_file("", error_msg);
#endif
}

template <PrimitiveType PT, typename CppType = vectorized::RunTimeCppType<PT>>
void _print_decimalv3_error_msg(RuntimeState* state, const CppType& decimal, const SlotDescriptor* desc) {
    std::stringstream ss;
    auto decimal_str = DecimalV3Cast::to_string<CppType>(decimal, desc->type().precision, desc->type().scale);
    std::string error_msg = strings::Substitute("Decimal '$0' is out of range. The type of '$1' is $2'", decimal_str,
                                                desc->col_name(), desc->type().debug_string());
#if BE_TEST
    LOG(INFO) << error_msg;
#else
    state->append_error_msg_to_file("", error_msg);
#endif
}

template <PrimitiveType PT>
void OlapTableSink::_validate_decimal(RuntimeState* state, vectorized::Column* column, const SlotDescriptor* desc,
                                      std::vector<uint8_t>* validate_selection) {
    using CppType = vectorized::RunTimeCppType<PT>;
    using ColumnType = vectorized::RunTimeColumnType<PT>;
    auto* data_column = down_cast<ColumnType*>(vectorized::ColumnHelper::get_data_column(column));
    const auto num_rows = data_column->get_data().size();
    auto* data = &data_column->get_data().front();

    int precision = desc->type().precision;
    const auto max_decimal = get_scale_factor<CppType>(precision);
    const auto min_decimal = -max_decimal;

    for (auto i = 0; i < num_rows; ++i) {
        if ((*validate_selection)[i] == VALID_SEL_OK) {
            const auto& datum = data[i];
            if (datum > max_decimal || datum < min_decimal) {
                (*validate_selection)[i] = VALID_SEL_FAILED;
                _print_decimalv3_error_msg<PT>(state, datum, desc);
            }
        }
    }
}

void OlapTableSink::_validate_data(RuntimeState* state, vectorized::Chunk* chunk) {
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

        // Validate column nullable info
        // Column nullable info need to respect slot nullable info
        if (desc->is_nullable() && !column_ptr->is_nullable()) {
            ColumnPtr new_column =
                    vectorized::NullableColumn::create(column_ptr, vectorized::NullColumn::create(num_rows, 0));
            chunk->update_column(std::move(new_column), desc->id());
        } else if (!desc->is_nullable() && column_ptr->is_nullable()) {
            vectorized::NullableColumn* nullable = down_cast<vectorized::NullableColumn*>(column_ptr.get());
            // Non-nullable column shouldn't have null value,
            // If there is null value, which means expr compute has a error.
            if (nullable->has_null()) {
                vectorized::NullData& nulls = nullable->null_column_data();
                for (size_t j = 0; j < num_rows; ++j) {
                    if (nulls[j]) {
                        _validate_selection[j] = VALID_SEL_FAILED;
                        std::stringstream ss;
                        ss << "NULL value in non-nullable column '" << desc->col_name() << "'";
#if BE_TEST
                        LOG(INFO) << ss.str();
#else
                        state->append_error_msg_to_file(chunk->debug_row(j), ss.str());
#endif
                    }
                }
            }
            chunk->update_column(nullable->data_column(), desc->id());
        } else if (column_ptr->has_null()) {
            vectorized::NullableColumn* nullable = down_cast<vectorized::NullableColumn*>(column_ptr.get());
            vectorized::NullData& nulls = nullable->null_column_data();
            for (size_t j = 0; j < num_rows; ++j) {
                if (nulls[j] && _validate_selection[j] != VALID_SEL_FAILED) {
                    // for this column, there are some null values in the row
                    // and we should skip checking of those null values.
                    _validate_selection[j] = VALID_SEL_OK_AND_NULL;
                }
            }
        }

        vectorized::Column* column = chunk->get_column_by_slot_id(desc->id()).get();
        switch (desc->type().type) {
        case TYPE_CHAR:
        case TYPE_VARCHAR: {
            uint32_t len = desc->type().len;
            vectorized::Column* data_column = vectorized::ColumnHelper::get_data_column(column);
            vectorized::BinaryColumn* binary = down_cast<vectorized::BinaryColumn*>(data_column);
            vectorized::Offsets& offset = binary->get_offset();
            for (size_t j = 0; j < num_rows; ++j) {
                if (_validate_selection[j] == VALID_SEL_OK) {
                    if (offset[j + 1] - offset[j] > len) {
                        _validate_selection[j] = VALID_SEL_FAILED;
                        _print_varchar_error_msg(state, binary->get_slice(j), desc);
                    }
                }
            }
            break;
        }
        case TYPE_DECIMALV2: {
            column = vectorized::ColumnHelper::get_data_column(column);
            vectorized::DecimalColumn* decimal = down_cast<vectorized::DecimalColumn*>(column);
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
                    }
                }
            }
            break;
        }
        case TYPE_DECIMAL32:
            _validate_decimal<TYPE_DECIMAL32>(state, column, desc, &_validate_selection);
            break;
        case TYPE_DECIMAL64:
            _validate_decimal<TYPE_DECIMAL64>(state, column, desc, &_validate_selection);
            break;
        case TYPE_DECIMAL128:
            _validate_decimal<TYPE_DECIMAL128>(state, column, desc, &_validate_selection);
            break;
        default:
            break;
        }
    }
}

void OlapTableSink::_padding_char_column(vectorized::Chunk* chunk) {
    size_t num_rows = chunk->num_rows();
    for (auto desc : _output_tuple_desc->slots()) {
        if (desc->type().type == TYPE_CHAR) {
            vectorized::Column* column = chunk->get_column_by_slot_id(desc->id()).get();
            vectorized::Column* data_column = vectorized::ColumnHelper::get_data_column(column);
            vectorized::BinaryColumn* binary = down_cast<vectorized::BinaryColumn*>(data_column);
            vectorized::Offsets& offset = binary->get_offset();
            uint32_t len = desc->type().len;

            vectorized::Bytes& bytes = binary->get_bytes();

            // Padding 0 to CHAR field, the storage bitmap index and zone map need it.
            // TODO(kks): we could improve this if there are many null valus
            auto new_binary = vectorized::BinaryColumn::create();
            vectorized::Offsets& new_offset = new_binary->get_offset();
            vectorized::Bytes& new_bytes = new_binary->get_bytes();
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
                auto* nullable_column = down_cast<vectorized::NullableColumn*>(column);
                ColumnPtr new_column = vectorized::NullableColumn::create(new_binary, nullable_column->null_column());
                chunk->update_column(new_column, desc->id());
            } else {
                chunk->update_column(new_binary, desc->id());
            }
        }
    }
}

void OlapTableSink::_send_chunk_process() {
    SCOPED_THREAD_LOCAL_MEM_TRACKER_SETTER(_runtime_state->instance_mem_tracker());

    SCOPED_RAW_TIMER(&_non_blocking_send_ns);
    while (true) {
        int running_channels_num = 0;
        for (auto& index_channel : _channels) {
            index_channel->for_each_node_channel([&running_channels_num](NodeChannel* ch) {
                running_channels_num += ch->try_send_chunk_and_fetch_status();
            });
        }

        if (running_channels_num == 0) {
            LOG(INFO) << "Exiting consumer thread, no running channel";
            return;
        }
        // Don't sleep if only one channel
        if (running_channels_num > 1) {
            SleepFor(MonoDelta::FromMilliseconds(config::olap_table_sink_send_interval_ms));
        }
    }
}

} // namespace starrocks::stream_load
