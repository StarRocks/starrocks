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
#include "column/column_viewer.h"
#include "column/map_column.h"
#include "column/nullable_column.h"
#include "common/statusor.h"
#include "config.h"
#include "exec/pipeline/query_context.h"
#include "exec/pipeline/stream_epoch_manager.h"
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

static const uint8_t VALID_SEL_FAILED = 0x0;
static const uint8_t VALID_SEL_OK = 0x1;
// it's a valid value and selected, but it's null
// and we don't need following extra check
// make sure the least bit is 1.
static const uint8_t VALID_SEL_OK_AND_NULL = 0x3;

namespace starrocks {

namespace stream_load {

NodeChannel::NodeChannel(OlapTableSink* parent, int64_t node_id, bool is_incremental, ExprContext* where_clause)
        : _parent(parent), _node_id(node_id), _is_incremental(is_incremental), _where_clause(where_clause) {
    // restrict the chunk memory usage of send queue & brpc write buffer
    _mem_tracker = std::make_unique<MemTracker>(config::send_channel_buffer_limit, "", nullptr);
}

NodeChannel::~NodeChannel() noexcept {
    for (auto& _open_closure : _open_closures) {
        if (_open_closure != nullptr) {
            if (_open_closure->unref()) {
                delete _open_closure;
            }
            _open_closure = nullptr;
        }
    }

    for (auto& _add_batch_closure : _add_batch_closures) {
        if (_add_batch_closure != nullptr) {
            if (_add_batch_closure->unref()) {
                delete _add_batch_closure;
            }
            _add_batch_closure = nullptr;
        }
    }

    for (int i = 0; i < _rpc_request.requests_size(); i++) {
        _rpc_request.mutable_requests(i)->release_id();
    }
    _rpc_request.release_id();
}

Status NodeChannel::init(RuntimeState* state) {
    // already init success
    if (_runtime_state != nullptr) {
        return Status::OK();
    }

    _tuple_desc = _parent->_output_tuple_desc;
    _node_info = _parent->_nodes_info->find_node(_node_id);
    if (_node_info == nullptr) {
        _cancelled = true;
        _err_st = Status::InvalidArgument(fmt::format("Unknown node_id: {}", _node_id));
        return _err_st;
    }

    _stub = state->exec_env()->brpc_stub_cache()->get_stub(_node_info->host, _node_info->brpc_port);
    if (_stub == nullptr) {
        _cancelled = true;
        auto msg = fmt::format("Connect {}:{} failed.", _node_info->host, _node_info->brpc_port);
        LOG(WARNING) << msg;
        _err_st = Status::InternalError(msg);
        return _err_st;
    }

    _rpc_timeout_ms = state->query_options().query_timeout * 1000 / 2;

    // Initialize _rpc_request
    for (const auto& [index_id, tablets] : _index_tablets_map) {
        auto request = _rpc_request.add_requests();
        request->set_allocated_id(&_parent->_load_id);
        request->set_index_id(index_id);
        request->set_txn_id(_parent->_txn_id);
        request->set_sender_id(_parent->_sender_id);
        request->set_eos(false);
        request->set_timeout_ms(_rpc_timeout_ms);
    }
    _rpc_request.set_allocated_id(&_parent->_load_id);

    if (state->query_options().__isset.load_transmission_compression_type) {
        _compress_type = CompressionUtils::to_compression_pb(state->query_options().load_transmission_compression_type);
    }
    RETURN_IF_ERROR(get_block_compression_codec(_compress_type, &_compress_codec));

    if (state->query_options().__isset.load_dop) {
        _max_parallel_request_size = state->query_options().load_dop;
        if (_max_parallel_request_size > config::max_load_dop || _max_parallel_request_size < 1) {
            _err_st = Status::InternalError(fmt::format("load_dop should between [1-{}]", config::max_load_dop));
            return _err_st;
        }
    }

    // init add_chunk request closure
    for (size_t i = 0; i < _max_parallel_request_size; i++) {
        auto closure = new ReusableClosure<PTabletWriterAddBatchResult>();
        closure->ref();
        _add_batch_closures.emplace_back(closure);
    }

    if (_parent->_write_quorum_type == TWriteQuorumType::ONE) {
        _write_quorum_type = WriteQuorumTypePB::ONE;
    } else if (_parent->_write_quorum_type == TWriteQuorumType::ALL) {
        _write_quorum_type = WriteQuorumTypePB::ALL;
    }

    // for get global_dict
    _runtime_state = state;

    _load_info = "load_id=" + print_id(_parent->_load_id) + ", txn_id: " + std::to_string(_parent->_txn_id) +
                 ", parallel=" + std::to_string(_max_parallel_request_size) +
                 ", compress_type=" + std::to_string(_compress_type);
    _name = "NodeChannel[" + std::to_string(_node_id) + "]";
    return Status::OK();
}

void NodeChannel::try_open() {
    for (int i = 0; i < _rpc_request.requests_size(); i++) {
        _open_closures.emplace_back(new RefCountClosure<PTabletWriterOpenResult>());
        _open_closures.back()->ref();
        _open(_rpc_request.requests(i).index_id(), _open_closures[i],
              _index_tablets_map[_rpc_request.requests(i).index_id()], false);
    }
}

void NodeChannel::_open(int64_t index_id, RefCountClosure<PTabletWriterOpenResult>* open_closure,
                        std::vector<PTabletWithPartition>& tablets, bool incremental_open) {
    PTabletWriterOpenRequest request;
    request.set_merge_condition(_parent->_merge_condition);
    if (_parent->_partial_update_mode == TPartialUpdateMode::type::ROW_MODE) {
        request.set_partial_update_mode(PartialUpdateMode::ROW_MODE);
    } else if (_parent->_partial_update_mode == TPartialUpdateMode::type::AUTO_MODE) {
        request.set_partial_update_mode(PartialUpdateMode::AUTO_MODE);
    } else if (_parent->_partial_update_mode == TPartialUpdateMode::type::COLUMN_UPSERT_MODE) {
        request.set_partial_update_mode(PartialUpdateMode::COLUMN_UPSERT_MODE);
    } else if (_parent->_partial_update_mode == TPartialUpdateMode::type::COLUMN_UPDATE_MODE) {
        request.set_partial_update_mode(PartialUpdateMode::COLUMN_UPDATE_MODE);
    }
    request.set_allocated_id(&_parent->_load_id);
    request.set_index_id(index_id);
    request.set_txn_id(_parent->_txn_id);
    request.set_txn_trace_parent(_parent->_txn_trace_parent);
    request.set_allocated_schema(_parent->_schema->to_protobuf());
    request.set_is_lake_tablet(_parent->_is_lake_table);
    request.set_is_replicated_storage(_parent->_enable_replicated_storage);
    request.set_node_id(_node_id);
    request.set_write_quorum(_write_quorum_type);
    request.set_miss_auto_increment_column(_parent->_miss_auto_increment_column);
    request.set_table_id(_parent->_schema->table_id());
    request.set_is_incremental(incremental_open);
    request.set_sender_id(_parent->_sender_id);
    for (auto& tablet : tablets) {
        auto ptablet = request.add_tablets();
        ptablet->CopyFrom(tablet);
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
    request.set_timeout_ms(_rpc_timeout_ms);

    // set global dict
    const auto& global_dict = _runtime_state->get_load_global_dict_map();
    const auto& dict_version = _runtime_state->load_dict_versions();
    for (size_t i = 0; i < request.schema().slot_descs_size(); i++) {
        auto slot = request.mutable_schema()->mutable_slot_descs(i);
        auto it = global_dict.find(slot->id());
        if (it != global_dict.end()) {
            auto dict = it->second.first;
            for (auto& item : dict) {
                slot->add_global_dict_words(item.first.to_string());
            }
            auto it_version = dict_version.find(slot->id());
            if (it_version != dict_version.end()) {
                slot->set_global_dict_version(it_version->second);
            }
        }
    }

    // This ref is for RPC's reference
    open_closure->ref();
    open_closure->cntl.set_timeout_ms(_rpc_timeout_ms);
    open_closure->cntl.ignore_eovercrowded();

    if (request.ByteSizeLong() > _parent->_rpc_http_min_size) {
        TNetworkAddress brpc_addr;
        brpc_addr.hostname = _node_info->host;
        brpc_addr.port = _node_info->brpc_port;
        open_closure->cntl.http_request().set_content_type("application/proto");
        auto res = HttpBrpcStubCache::getInstance()->get_http_stub(brpc_addr);
        if (!res.ok()) {
            LOG(ERROR) << res.status().get_error_msg();
            return;
        }
        res.value()->tablet_writer_open(&open_closure->cntl, &request, &open_closure->result, open_closure);
        VLOG(2) << "NodeChannel::_open() issue a http rpc, request size = " << request.ByteSizeLong();
    } else {
        _stub->tablet_writer_open(&open_closure->cntl, &request, &open_closure->result, open_closure);
    }
    request.release_id();
    request.release_schema();

    VLOG(2) << "NodeChannel[" << _load_info << "] send open request [incremental: " << incremental_open << "] to ["
            << _node_info->host << ":" << _node_info->brpc_port << "]";
}

void NodeChannel::try_incremental_open() {
    for (int i = 0; i < _rpc_request.requests_size(); i++) {
        _open_closures.emplace_back(new RefCountClosure<PTabletWriterOpenResult>());
        _open_closures.back()->ref();

        _open(_rpc_request.requests(i).index_id(), _open_closures[i],
              _index_tablets_map[_rpc_request.requests(i).index_id()], true);
    }
}

bool NodeChannel::is_open_done() {
    bool open_done = true;
    for (int i = 0; i < _rpc_request.requests_size(); i++) {
        if (_open_closures[i] != nullptr) {
            // open request already finished
            open_done &= (_open_closures[i]->count() != 2);
        }
    }

    return open_done;
}

Status NodeChannel::open_wait() {
    Status res = Status::OK();
    for (int i = 0; i < _rpc_request.requests_size(); i++) {
        auto st = _open_wait(_open_closures[i]);
        if (!st.ok()) {
            res = st;
        }
        if (_open_closures[i]->unref()) {
            delete _open_closures[i];
        }
        _open_closures[i] = nullptr;
    }
    _open_closures.clear();

    return res;
}

Status NodeChannel::_open_wait(RefCountClosure<PTabletWriterOpenResult>* open_closure) {
    if (open_closure == nullptr) {
        return _err_st;
    }
    open_closure->join();
    if (open_closure->cntl.Failed()) {
        _cancelled = true;
        _err_st = Status::InternalError(open_closure->cntl.ErrorText());

        // tablet_id == -1 means add backend to blacklist
        TTabletFailInfo fail_info;
        fail_info.__set_tabletId(-1);
        fail_info.__set_backendId(_node_id);
        _runtime_state->append_tablet_fail_infos(std::move(fail_info));

        return _err_st;
    }
    Status status(open_closure->result.status());

    if (!status.ok()) {
        _cancelled = true;
        _err_st = status;

        TTabletFailInfo fail_info;
        fail_info.__set_tabletId(-1);
        fail_info.__set_backendId(_node_id);
        _runtime_state->append_tablet_fail_infos(std::move(fail_info));

        return _err_st;
    }

    if (open_closure->result.has_is_repeated_chunk()) {
        _enable_colocate_mv_index &= open_closure->result.is_repeated_chunk();
    } else {
        _enable_colocate_mv_index = false;
    }

    return status;
}

Status NodeChannel::_serialize_chunk(const Chunk* src, ChunkPB* dst) {
    VLOG_ROW << "serializing " << src->num_rows() << " rows";

    {
        SCOPED_RAW_TIMER(&_serialize_batch_ns);
        StatusOr<ChunkPB> res = Status::OK();
        TRY_CATCH_BAD_ALLOC(res = serde::ProtobufChunkSerde::serialize(*src));
        if (!res.ok()) {
            _cancelled = true;
            _err_st = res.status();
            return _err_st;
        }
        res->Swap(dst);
    }
    DCHECK(dst->has_uncompressed_size());
    DCHECK_EQ(dst->uncompressed_size(), dst->data().size());

    size_t uncompressed_size = dst->uncompressed_size();

    if (_compress_codec != nullptr && _compress_codec->exceed_max_input_size(uncompressed_size)) {
        _cancelled = true;
        _err_st = Status::InternalError(fmt::format("The input size for compression should be less than {}",
                                                    _compress_codec->max_input_size()));
        return _err_st;
    }

    // try compress the ChunkPB data
    if (_compress_codec != nullptr && uncompressed_size > 0) {
        SCOPED_TIMER(_parent->_compress_timer);

        if (use_compression_pool(_compress_codec->type())) {
            Slice compressed_slice;
            Slice input(dst->data());
            RETURN_IF_ERROR(_compress_codec->compress(input, &compressed_slice, true, uncompressed_size, nullptr,
                                                      &_compression_scratch));
        } else {
            int max_compressed_size = _compress_codec->max_compressed_len(uncompressed_size);

            if (_compression_scratch.size() < max_compressed_size) {
                _compression_scratch.resize(max_compressed_size);
            }

            Slice compressed_slice{_compression_scratch.data(), _compression_scratch.size()};

            Slice input(dst->data());
            RETURN_IF_ERROR(_compress_codec->compress(input, &compressed_slice));
            _compression_scratch.resize(compressed_slice.size);
        }

        double compress_ratio = (static_cast<double>(uncompressed_size)) / _compression_scratch.size();
        if (LIKELY(compress_ratio > config::rpc_compress_ratio_threshold)) {
            dst->mutable_data()->swap(reinterpret_cast<std::string&>(_compression_scratch));
            dst->set_compress_type(_compress_type);
        }

        VLOG_ROW << "uncompressed size: " << uncompressed_size << ", compressed size: " << _compression_scratch.size();
    }

    return Status::OK();
}

bool NodeChannel::is_full() {
    if (_request_queue.size() >= _max_request_queue_size || _mem_tracker->limit()) {
        if (!_check_prev_request_done()) {
            return true;
        }
    }
    return false;
}

Status NodeChannel::add_chunk(Chunk* input, const std::vector<int64_t>& tablet_ids,
                              const std::vector<uint32_t>& indexes, uint32_t from, uint32_t size) {
    if (_cancelled || _send_finished) {
        return _err_st;
    }

    DCHECK(_rpc_request.requests_size() == 1);
    if (UNLIKELY(_cur_chunk == nullptr)) {
        _cur_chunk = input->clone_empty_with_slot();
    }

    if (is_full()) {
        SCOPED_TIMER(_parent->_wait_response_timer);
        // wait previous request done then we can pop data from queue to send request
        // and make new space to push data.
        RETURN_IF_ERROR(_wait_one_prev_request());
    }

    SCOPED_TIMER(_parent->_pack_chunk_timer);
    // 1. append data
    if (_where_clause == nullptr) {
        _cur_chunk->append_selective(*input, indexes.data(), from, size);
        auto req = _rpc_request.mutable_requests(0);
        for (size_t i = 0; i < size; ++i) {
            req->add_tablet_ids(tablet_ids[indexes[from + i]]);
        }
    } else {
        std::vector<uint32_t> filtered_indexes;
        RETURN_IF_ERROR(_filter_indexes_with_where_expr(input, indexes, filtered_indexes));
        size_t filter_size = filtered_indexes.size();
        _cur_chunk->append_selective(*input, filtered_indexes.data(), from, filter_size);
        auto req = _rpc_request.mutable_requests(0);
        for (size_t i = 0; i < filter_size; ++i) {
            req->add_tablet_ids(tablet_ids[filtered_indexes[from + i]]);
        }
    }

    if (_cur_chunk->num_rows() < _runtime_state->chunk_size()) {
        // 2. chunk not full
        if (_request_queue.empty()) {
            return Status::OK();
        }
        // passthrough: try to send data if queue not empty
    } else {
        // 3. chunk full push back to queue
        _mem_tracker->consume(_cur_chunk->memory_usage());
        _request_queue.emplace_back(std::move(_cur_chunk), _rpc_request);
        _cur_chunk = input->clone_empty_with_slot();
        _rpc_request.mutable_requests(0)->clear_tablet_ids();
    }

    // 4. check last request
    if (!_check_prev_request_done()) {
        // 4.1 noblock here so that other node channel can send data
        return Status::OK();
    }

    return _send_request(false);
}

Status NodeChannel::_filter_indexes_with_where_expr(Chunk* input, const std::vector<uint32_t>& indexes,
                                                    std::vector<uint32_t>& filtered_indexes) {
    DCHECK(_where_clause != nullptr);
    // Filter data
    ASSIGN_OR_RETURN(ColumnPtr filter_col, _where_clause->evaluate(input))

    size_t size = filter_col->size();
    Buffer<uint8_t> filter(size, 0);
    ColumnViewer<TYPE_BOOLEAN> col(filter_col);
    for (size_t i = 0; i < size; ++i) {
        filter[i] = !col.is_null(i) && col.value(i);
    }

    for (auto index : indexes) {
        if (filter[index]) {
            filtered_indexes.emplace_back(index);
        }
    }
    return Status::OK();
}

Status NodeChannel::add_chunks(Chunk* input, const std::vector<std::vector<int64_t>>& tablet_ids,
                               const std::vector<uint32_t>& indexes, uint32_t from, uint32_t size) {
    if (_cancelled || _send_finished) {
        return _err_st;
    }

    DCHECK(tablet_ids.size() == _rpc_request.requests_size());
    if (UNLIKELY(_cur_chunk == nullptr)) {
        _cur_chunk = input->clone_empty_with_slot();
    }

    if (is_full()) {
        // wait previous request done then we can pop data from queue to send request
        // and make new space to push data.
        RETURN_IF_ERROR(_wait_one_prev_request());
    }

    SCOPED_TIMER(_parent->_pack_chunk_timer);
    // 1. append data
    _cur_chunk->append_selective(*input, indexes.data(), from, size);
    for (size_t index_i = 0; index_i < tablet_ids.size(); ++index_i) {
        auto req = _rpc_request.mutable_requests(index_i);
        for (size_t i = 0; i < size; ++i) {
            req->add_tablet_ids(tablet_ids[index_i][indexes[from + i]]);
        }
    }

    if (_cur_chunk->num_rows() < _runtime_state->chunk_size()) {
        // 2. chunk not full
        if (_request_queue.empty()) {
            return Status::OK();
        }
        // passthrough: try to send data if queue not empty
    } else {
        // 3. chunk full push back to queue
        _mem_tracker->consume(_cur_chunk->memory_usage());
        _request_queue.emplace_back(std::move(_cur_chunk), _rpc_request);
        _cur_chunk = input->clone_empty_with_slot();
        for (size_t index_i = 0; index_i < tablet_ids.size(); ++index_i) {
            _rpc_request.mutable_requests(index_i)->clear_tablet_ids();
        }
    }

    // 4. check last request
    if (!_check_prev_request_done()) {
        // 4.1 noblock here so that other node channel can send data
        return Status::OK();
    }

    return _send_request(false);
}

Status NodeChannel::_send_request(bool eos, bool wait_all_sender_close) {
    if (eos) {
        if (_request_queue.empty()) {
            if (_cur_chunk.get() == nullptr) {
                _cur_chunk = std::make_unique<Chunk>();
            }
            _mem_tracker->consume(_cur_chunk->memory_usage());
            _request_queue.emplace_back(std::move(_cur_chunk), _rpc_request);
            _cur_chunk = nullptr;
        }

        // try to send chunk in queue first
        if (_request_queue.size() > 1) {
            eos = false;
        }
    }

    AddMultiChunkReq add_chunk = std::move(_request_queue.front());
    _request_queue.pop_front();

    auto request = add_chunk.second;
    auto chunk = std::move(add_chunk.first);

    _mem_tracker->release(chunk->memory_usage());

    RETURN_IF_ERROR(_wait_one_prev_request());

    SCOPED_RAW_TIMER(&_actual_consume_ns);

    for (int i = 0; i < request.requests_size(); i++) {
        auto req = request.mutable_requests(i);
        if (UNLIKELY(eos)) {
            req->set_eos(true);
            if (wait_all_sender_close) {
                req->set_wait_all_sender_close(true);
            }
            for (auto pid : _parent->_partition_ids) {
                req->add_partition_ids(pid);
            }
            // eos request must be the last request
            _send_finished = true;
        }

        req->set_packet_seq(_next_packet_seq);

        // only serialize one chunk if is_repeated_request is true
        if ((!_enable_colocate_mv_index || i == 0) && chunk->num_rows() > 0) {
            auto pchunk = req->mutable_chunk();
            RETURN_IF_ERROR(_serialize_chunk(chunk.get(), pchunk));
        }
    }

    _add_batch_closures[_current_request_index]->ref();
    _add_batch_closures[_current_request_index]->reset();
    _add_batch_closures[_current_request_index]->cntl.set_timeout_ms(_rpc_timeout_ms);
    _add_batch_closures[_current_request_index]->cntl.ignore_eovercrowded();
    _add_batch_closures[_current_request_index]->request_size = request.ByteSizeLong();

    _mem_tracker->consume(_add_batch_closures[_current_request_index]->request_size);

    if (_enable_colocate_mv_index) {
        request.set_is_repeated_chunk(true);
        if (UNLIKELY(request.ByteSizeLong() > _parent->_rpc_http_min_size)) {
            TNetworkAddress brpc_addr;
            brpc_addr.hostname = _node_info->host;
            brpc_addr.port = _node_info->brpc_port;
            _add_batch_closures[_current_request_index]->cntl.http_request().set_content_type("application/proto");
            auto res = HttpBrpcStubCache::getInstance()->get_http_stub(brpc_addr);
            if (!res.ok()) {
                return res.status();
            }
            res.value()->tablet_writer_add_chunks(&_add_batch_closures[_current_request_index]->cntl, &request,
                                                  &_add_batch_closures[_current_request_index]->result,
                                                  _add_batch_closures[_current_request_index]);
            VLOG(2) << "NodeChannel::_send_request() issue a http rpc, request size = " << request.ByteSizeLong();
        } else {
            _stub->tablet_writer_add_chunks(&_add_batch_closures[_current_request_index]->cntl, &request,
                                            &_add_batch_closures[_current_request_index]->result,
                                            _add_batch_closures[_current_request_index]);
        }
    } else {
        DCHECK(request.requests_size() == 1);
        if (UNLIKELY(request.ByteSizeLong() > _parent->_rpc_http_min_size)) {
            TNetworkAddress brpc_addr;
            brpc_addr.hostname = _node_info->host;
            brpc_addr.port = _node_info->brpc_port;
            _add_batch_closures[_current_request_index]->cntl.http_request().set_content_type("application/proto");
            auto res = HttpBrpcStubCache::getInstance()->get_http_stub(brpc_addr);
            if (!res.ok()) {
                return res.status();
            }
            res.value()->tablet_writer_add_chunk(
                    &_add_batch_closures[_current_request_index]->cntl, request.mutable_requests(0),
                    &_add_batch_closures[_current_request_index]->result, _add_batch_closures[_current_request_index]);
            VLOG(2) << "NodeChannel::_send_request() issue a http rpc, request size = " << request.ByteSizeLong();
        } else {
            _stub->tablet_writer_add_chunk(
                    &_add_batch_closures[_current_request_index]->cntl, request.mutable_requests(0),
                    &_add_batch_closures[_current_request_index]->result, _add_batch_closures[_current_request_index]);
        }
    }
    _next_packet_seq++;

    VLOG(2) << "NodeChannel[" << _load_info << "] send chunk request [rows: " << chunk->num_rows() << " eos: " << eos
            << "] to [" << _node_info->host << ":" << _node_info->brpc_port << "]";

    return Status::OK();
}

Status NodeChannel::_wait_request(ReusableClosure<PTabletWriterAddBatchResult>* closure) {
    if (!closure->join()) {
        return Status::OK();
    }
    _mem_tracker->release(closure->request_size);

    _parent->_client_rpc_timer->update(closure->latency());

    if (closure->cntl.Failed()) {
        _cancelled = true;
        _err_st = Status::InternalError(closure->cntl.ErrorText());

        TTabletFailInfo fail_info;
        fail_info.__set_tabletId(-1);
        fail_info.__set_backendId(_node_id);
        _runtime_state->append_tablet_fail_infos(std::move(fail_info));
        return _err_st;
    }

    VLOG(2) << "NodeChannel[" << _load_info << "] recevied response : " << closure->result.DebugString() << "] from ["
            << _node_info->host << ":" << _node_info->brpc_port << "]";

    Status st(closure->result.status());
    if (!st.ok()) {
        _cancelled = true;
        _err_st = st;

        for (auto& tablet : closure->result.failed_tablet_vec()) {
            TTabletFailInfo fail_info;
            fail_info.__set_tabletId(tablet.tablet_id());
            if (tablet.has_node_id()) {
                fail_info.__set_backendId(tablet.node_id());
            } else {
                fail_info.__set_backendId(_node_id);
            }
            _runtime_state->append_tablet_fail_infos(std::move(fail_info));
        }

        return _err_st;
    }

    if (closure->result.has_execution_time_us()) {
        _add_batch_counter.add_batch_execution_time_us += closure->result.execution_time_us();
        _add_batch_counter.add_batch_wait_lock_time_us += closure->result.wait_lock_time_us();
        _add_batch_counter.add_batch_wait_memtable_flush_time_us += closure->result.wait_memtable_flush_time_us();
        _add_batch_counter.add_batch_num++;
    }

    std::vector<int64_t> tablet_ids;
    for (auto& tablet : closure->result.tablet_vec()) {
        TTabletCommitInfo commit_info;
        commit_info.tabletId = tablet.tablet_id();
        if (tablet.has_node_id()) {
            commit_info.backendId = tablet.node_id();
        } else {
            commit_info.backendId = _node_id;
        }

        for (const auto& col_name : tablet.invalid_dict_cache_columns()) {
            _valid_dict_cache_info.invalid_dict_cache_column_set.insert(col_name);
        }

        for (size_t i = 0; i < tablet.valid_dict_cache_columns_size(); ++i) {
            int64_t version = 0;
            // Some BEs don't have this field during grayscale upgrades, and we need to detect this case
            if (tablet.valid_dict_collected_version_size() == tablet.valid_dict_cache_columns_size()) {
                version = tablet.valid_dict_collected_version(i);
            }
            const auto& col_name = tablet.valid_dict_cache_columns(i);
            _valid_dict_cache_info.valid_dict_cache_column_set.emplace(std::make_pair(col_name, version));
        }

        _tablet_commit_infos.emplace_back(std::move(commit_info));

        if (tablet_ids.size() < 128) {
            tablet_ids.emplace_back(commit_info.tabletId);
        }
    }

    if (!tablet_ids.empty()) {
        string commit_tablet_id_list_str;
        JoinInts(tablet_ids, ",", &commit_tablet_id_list_str);
        LOG(INFO) << "OlapTableSink txn_id: " << _parent->_txn_id << " load_id: " << print_id(_parent->_load_id)
                  << " commit " << _tablet_commit_infos.size() << " tablets: " << commit_tablet_id_list_str;
    }

    return Status::OK();
}

Status NodeChannel::_wait_all_prev_request() {
    if (_next_packet_seq == 0) {
        return Status::OK();
    }
    for (auto closure : _add_batch_closures) {
        RETURN_IF_ERROR(_wait_request(closure));
    }

    return Status::OK();
}

bool NodeChannel::_check_prev_request_done() {
    if (UNLIKELY(_next_packet_seq == 0)) {
        return true;
    }

    for (size_t i = 0; i < _max_parallel_request_size; i++) {
        if (_add_batch_closures[i]->count() == 1) {
            _current_request_index = i;
            return true;
        }
    }

    return false;
}

bool NodeChannel::_check_all_prev_request_done() {
    if (UNLIKELY(_next_packet_seq == 0)) {
        return true;
    }

    for (size_t i = 0; i < _max_parallel_request_size; i++) {
        if (_add_batch_closures[i]->count() != 1) {
            return false;
        }
    }

    return true;
}

Status NodeChannel::_wait_one_prev_request() {
    if (_next_packet_seq == 0) {
        return Status::OK();
    }

    // 1. unblocking check last request for short-circuit
    // count() == 1 means request already finish so it wouldn't block
    if (_add_batch_closures[_current_request_index]->count() == 1) {
        RETURN_IF_ERROR(_wait_request(_add_batch_closures[_current_request_index]));
        return Status::OK();
    }

    // 2. unblocking check all other requests
    for (size_t i = 0; i < _max_parallel_request_size; i++) {
        if (_add_batch_closures[i]->count() == 1) {
            _current_request_index = i;
            RETURN_IF_ERROR(_wait_request(_add_batch_closures[i]));
            return Status::OK();
        }
    }

    // 3. waiting one request
    // TODO(meegoo): optimize to wait first finish request
    _current_request_index = 0;
    RETURN_IF_ERROR(_wait_request(_add_batch_closures[_current_request_index]));

    return Status::OK();
}

Status NodeChannel::try_close(bool wait_all_sender_close) {
    if (_cancelled || _send_finished) {
        return _err_st;
    }

    if (_check_prev_request_done()) {
        auto st = _send_request(true /* eos */, wait_all_sender_close);
        if (!st.ok()) {
            _cancelled = true;
            _err_st = st;
            return _err_st;
        }
    }

    return Status::OK();
}

bool NodeChannel::is_close_done() {
    return (_send_finished && _check_all_prev_request_done()) || _cancelled;
}

Status NodeChannel::close_wait(RuntimeState* state) {
    if (_cancelled) {
        return _err_st;
    }

    // 1. send eos request to commit write util finish
    while (!_send_finished) {
        RETURN_IF_ERROR(_send_request(true /* eos */));
    }

    // 2. wait eos request finish
    RETURN_IF_ERROR(_wait_all_prev_request());

    // 3. commit tablet infos
    state->append_tablet_commit_infos(_tablet_commit_infos);

    return _err_st;
}

void NodeChannel::cancel(const Status& err_st) {
    // cancel rpc request, accelerate the release of related resources
    for (auto closure : _add_batch_closures) {
        closure->cancel();
    }

    for (int i = 0; i < _rpc_request.requests_size(); i++) {
        _cancel(_rpc_request.requests(i).index_id(), err_st);
    }
}

void NodeChannel::_cancel(int64_t index_id, const Status& err_st) {
    _cancelled = true;
    _err_st = err_st;

    PTabletWriterCancelRequest request;
    request.set_allocated_id(&_parent->_load_id);
    request.set_index_id(index_id);
    request.set_sender_id(_parent->_sender_id);
    request.set_txn_id(_parent->_txn_id);

    auto closure = new RefCountClosure<PTabletWriterCancelResult>();

    closure->ref();
    closure->cntl.set_timeout_ms(_rpc_timeout_ms);
    closure->cntl.ignore_eovercrowded();
    _stub->tablet_writer_cancel(&closure->cntl, &request, &closure->result, closure);
    request.release_id();
}

IndexChannel::~IndexChannel() {
    if (_where_clause != nullptr) {
        _where_clause->close(_parent->_state);
    }
}

Status IndexChannel::init(RuntimeState* state, const std::vector<PTabletWithPartition>& tablets, bool is_incremental) {
    for (const auto& tablet : tablets) {
        auto* location = _parent->_location->find_tablet(tablet.tablet_id());
        if (location == nullptr) {
            auto msg = fmt::format("Not found tablet: {}", tablet.tablet_id());
            return Status::NotFound(msg);
        }
        std::vector<int64_t> bes;
        for (auto& node_id : location->node_ids) {
            NodeChannel* channel = nullptr;
            auto it = _node_channels.find(node_id);
            if (it == std::end(_node_channels)) {
                auto channel_ptr = std::make_unique<NodeChannel>(_parent, node_id, is_incremental, _where_clause);
                channel = channel_ptr.get();
                _node_channels.emplace(node_id, std::move(channel_ptr));
                if (is_incremental) {
                    _has_incremental_node_channel = true;
                }
            } else {
                channel = it->second.get();
            }
            channel->add_tablet(_index_id, tablet);
            bes.emplace_back(node_id);
        }
        _tablet_to_be.emplace(tablet.tablet_id(), std::move(bes));
    }
    for (auto& it : _node_channels) {
        RETURN_IF_ERROR(it.second->init(state));
    }
    if (_where_clause != nullptr) {
        RETURN_IF_ERROR(_where_clause->prepare(_parent->_state));
        RETURN_IF_ERROR(_where_clause->open(_parent->_state));
    }
    _write_quorum_type = _parent->_write_quorum_type;
    return Status::OK();
}

bool IndexChannel::has_intolerable_failure() {
    if (_write_quorum_type == TWriteQuorumType::ALL) {
        return _failed_channels.size() > 0;
    } else if (_write_quorum_type == TWriteQuorumType::ONE) {
        return _failed_channels.size() >= _parent->_num_repicas;
    } else {
        return _failed_channels.size() >= ((_parent->_num_repicas + 1) / 2);
    }
}

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

    // add all counter
    _input_rows_counter = ADD_COUNTER(_profile, "RowsRead", TUnit::UNIT);
    _output_rows_counter = ADD_COUNTER(_profile, "RowsReturned", TUnit::UNIT);
    _filtered_rows_counter = ADD_COUNTER(_profile, "RowsFiltered", TUnit::UNIT);
    _open_timer = ADD_TIMER(_profile, "OpenTime");
    _close_timer = ADD_TIMER(_profile, "CloseWaitTime");
    _prepare_data_timer = ADD_TIMER(_profile, "PrepareDataTime");
    _convert_chunk_timer = ADD_CHILD_TIMER(_profile, "ConvertChunkTime", "PrepareDataTime");
    _validate_data_timer = ADD_CHILD_TIMER(_profile, "ValidateDataTime", "PrepareDataTime");
    _send_data_timer = ADD_TIMER(_profile, "SendDataTime");
    _pack_chunk_timer = ADD_CHILD_TIMER(_profile, "PackChunkTime", "SendDataTime");
    _send_rpc_timer = ADD_CHILD_TIMER(_profile, "SendRpcTime", "SendDataTime");
    _wait_response_timer = ADD_CHILD_TIMER(_profile, "WaitResponseTime", "SendDataTime");
    _serialize_chunk_timer = ADD_CHILD_TIMER(_profile, "SerializeChunkTime", "SendRpcTime");
    _compress_timer = ADD_CHILD_TIMER(_profile, "CompressTime", "SendRpcTime");
    _client_rpc_timer = ADD_TIMER(_profile, "RpcClientSideTime");
    _server_rpc_timer = ADD_TIMER(_profile, "RpcServerSideTime");
    _server_wait_flush_timer = ADD_TIMER(_profile, "RpcServerWaitFlushTime");

    _schema = std::make_shared<OlapTableSchemaParam>();
    RETURN_IF_ERROR(_schema->init(table_sink.schema, state));
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

    return Status::OK();
}

Status OlapTableSink::prepare(RuntimeState* state) {
    _span->AddEvent("prepare");

    _profile->add_info_string("TxnID", fmt::format("{}", _txn_id));
    _profile->add_info_string("IndexNum", fmt::format("{}", _schema->indexes().size()));
    _profile->add_info_string("ReplicatedStorage", fmt::format("{}", _enable_replicated_storage));
    _alloc_auto_increment_timer = ADD_TIMER(_profile, "AllocAutoIncrementTime");
    _profile->add_info_string("AutomaticPartition", fmt::format("{}", _enable_automatic_partition));

    SCOPED_TIMER(_profile->total_time_counter());

    RETURN_IF_ERROR(DataSink::prepare(state));

    _state = state;

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

    // open all channels
    return _init_node_channels(state);
}

Status OlapTableSink::_init_node_channels(RuntimeState* state) {
    const auto& partitions = _vectorized_partition->get_partitions();
    for (int i = 0; i < _schema->indexes().size(); ++i) {
        // collect all tablets belong to this rollup
        std::vector<PTabletWithPartition> tablets;
        auto* index = _schema->indexes()[i];
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
                        if (_enable_replicated_storage && i == 0) {
                            node_channel->set_has_primary_replica(true);
                        }
                    }
                }

                tablets.emplace_back(std::move(tablet_info));
            }
        }
        auto channel = std::make_unique<IndexChannel>(this, index->index_id, index->where_clause);
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
    SCOPED_TIMER(_open_timer);
    RETURN_IF_ERROR(try_open(state));
    RETURN_IF_ERROR(open_wait());

    return Status::OK();
}

Status OlapTableSink::try_open(RuntimeState* state) {
    // Prepare the exprs to run.
    RETURN_IF_ERROR(Expr::open(_output_expr_ctxs, state));
    RETURN_IF_ERROR(_vectorized_partition->open(state));

    if (_colocate_mv_index) {
        for_each_node_channel([](NodeChannel* ch) { ch->try_open(); });
    } else {
        for_each_index_channel([](NodeChannel* ch) { ch->try_open(); });
    }

    return Status::OK();
}

bool OlapTableSink::is_open_done() {
    if (!_open_done) {
        bool open_done = true;
        if (_colocate_mv_index) {
            for_each_node_channel([&open_done](NodeChannel* ch) { open_done &= ch->is_open_done(); });
        } else {
            for_each_index_channel([&open_done](NodeChannel* ch) { open_done &= ch->is_open_done(); });
        }
        _open_done = open_done;
    }

    return _open_done;
}

Status OlapTableSink::open_wait() {
    Status err_st = Status::OK();
    if (_colocate_mv_index) {
        for_each_node_channel([this, &err_st](NodeChannel* ch) {
            auto st = ch->open_wait();
            if (!st.ok()) {
                LOG(WARNING) << ch->name() << ", tablet open failed, " << ch->print_load_info()
                             << ", node=" << ch->node_info()->host << ":" << ch->node_info()->brpc_port
                             << ", errmsg=" << st.get_error_msg();
                err_st = st.clone_and_append(string(" be:") + ch->node_info()->host);
                this->mark_as_failed(ch);
            }
            // disable colocate mv index load if other BE not supported
            if (!ch->enable_colocate_mv_index()) {
                _colocate_mv_index = false;
            }
        });

        // when enable replicated storage, we only send to primary replica, one node channel fail lead to indicate whole load fail
        if (has_intolerable_failure() || (_enable_replicated_storage && !err_st.ok())) {
            LOG(WARNING) << "Open channel failed. load_id: " << _load_id << ", error: " << err_st.to_string();
            return err_st;
        }
    } else {
        for (auto& index_channel : _channels) {
            index_channel->for_each_node_channel([&index_channel, &err_st](NodeChannel* ch) {
                auto st = ch->open_wait();
                if (!st.ok()) {
                    LOG(WARNING) << ch->name() << ", tablet open failed, " << ch->print_load_info()
                                 << ", node=" << ch->node_info()->host << ":" << ch->node_info()->brpc_port
                                 << ", errmsg=" << st.get_error_msg();
                    err_st = st.clone_and_append(string(" be:") + ch->node_info()->host);
                    index_channel->mark_as_failed(ch);
                }
            });

            if (index_channel->has_intolerable_failure() || (_enable_replicated_storage && !err_st.ok())) {
                LOG(WARNING) << "Open channel failed. load_id: " << _load_id << ", error: " << err_st.to_string();
                return err_st;
            }
        }
    }

    return Status::OK();
}

bool OlapTableSink::is_full() {
    bool full = false;

    if (_colocate_mv_index) {
        for_each_node_channel([&full](NodeChannel* ch) { full |= ch->is_full(); });
    } else {
        for_each_index_channel([&full](NodeChannel* ch) { full |= ch->is_full(); });
    }

    return full || _is_automatic_partition_running.load(std::memory_order_acquire);
}

Status OlapTableSink::_automatic_create_partition() {
    TCreatePartitionRequest request;
    TCreatePartitionResult result;
    request.__set_txn_id(_txn_id);
    request.__set_db_id(_vectorized_partition->db_id());
    request.__set_table_id(_vectorized_partition->table_id());
    request.__set_partition_values(_partition_not_exist_row_values);

    LOG(INFO) << "load_id=" << print_id(_load_id) << ", txn_id: " << std::to_string(_txn_id)
              << "automatic partition rpc begin request " << request;
    TNetworkAddress master_addr = get_master_address();
    auto timeout_ms = _runtime_state->query_options().query_timeout * 1000 / 2;
    int retry_times = 0;
    int64_t start_ts = butil::gettimeofday_s();

    do {
        if (retry_times++ > 1) {
            SleepFor(MonoDelta::FromMilliseconds(std::min(5000, timeout_ms)));
            VLOG(1) << "load_id=" << print_id(_load_id) << ", txn_id: " << std::to_string(_txn_id)
                    << "automatic partition rpc retry " << retry_times;
        }
        RETURN_IF_ERROR(ThriftRpcHelper::rpc<FrontendServiceClient>(
                master_addr.hostname, master_addr.port,
                [&request, &result](FrontendServiceConnection& client) { client->createPartition(result, request); },
                timeout_ms));
    } while (result.status.status_code == TStatusCode::SERVICE_UNAVAILABLE &&
             butil::gettimeofday_s() - start_ts < timeout_ms / 1000);

    LOG(INFO) << "load_id=" << print_id(_load_id) << ", txn_id: " << std::to_string(_txn_id)
              << "automatic partition rpc end response " << result;
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
    for (auto& t_part : partitions) {
        for (auto& index : t_part.indexes) {
            std::vector<PTabletWithPartition> tablets;
            // setup new partitions's tablets
            for (auto tablet : index.tablets) {
                PTabletWithPartition tablet_info;
                tablet_info.set_tablet_id(tablet);
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
                }

                index_tablets_map[index.index_id].emplace_back(std::move(tablet_info));
            }
        }
    }

    for (auto& channel : _channels) {
        // initialize index channel
        RETURN_IF_ERROR(channel->init(_runtime_state, index_tablets_map[channel->_index_id], true));

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

        if (channel->has_intolerable_failure() || (_enable_replicated_storage && !err_st.ok())) {
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
        SCOPED_TIMER(_prepare_data_timer);
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
            SCOPED_TIMER(_alloc_auto_increment_timer);
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

    SCOPED_TIMER(_send_data_timer);

    if (_colocate_mv_index) {
        return _send_chunk_with_colocate_index(chunk);
    } else {
        return _send_chunk(chunk);
    }
}

Status OlapTableSink::_send_chunk(Chunk* chunk) {
    size_t num_rows = chunk->num_rows();
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

Status OlapTableSink::_send_chunk_with_colocate_index(Chunk* chunk) {
    Status err_st = Status::OK();
    size_t num_rows = chunk->num_rows();
    size_t selection_size = _validate_select_idx.size();
    if (selection_size == 0) {
        return Status::OK();
    }
    if (num_rows > selection_size) {
        for (size_t i = 0; i < selection_size; ++i) {
            _partition_ids.emplace(_partitions[_validate_select_idx[i]]->id);
        }

        size_t index_size = _partitions[_validate_select_idx[0]]->indexes.size();
        _index_tablet_ids.resize(index_size);
        for (size_t i = 0; i < index_size; ++i) {
            _index_tablet_ids[i].resize(num_rows);
            for (size_t j = 0; j < selection_size; ++j) {
                uint16_t selection = _validate_select_idx[j];
                _index_tablet_ids[i][selection] =
                        _partitions[selection]->indexes[i].tablets[_tablet_indexes[selection]];
            }
        }
    } else { // Improve for all rows are selected
        for (size_t i = 0; i < num_rows; ++i) {
            _partition_ids.emplace(_partitions[i]->id);
        }

        size_t index_size = _partitions[0]->indexes.size();
        _index_tablet_ids.resize(index_size);
        for (size_t i = 0; i < index_size; ++i) {
            _index_tablet_ids[i].resize(num_rows);
            for (size_t j = 0; j < num_rows; ++j) {
                _index_tablet_ids[i][j] = _partitions[j]->indexes[i].tablets[_tablet_indexes[j]];
            }
        }
    }
    return Status::OK();
}

Status OlapTableSink::_send_chunk_by_node(Chunk* chunk, IndexChannel* channel, std::vector<uint16_t>& selection_idx) {
    Status err_st = Status::OK();
    for (auto& it : channel->_node_channels) {
        NodeChannel* node = it.second.get();
        if (channel->is_failed_channel(node)) {
            // skip open fail channel
            continue;
        }
        int64_t be_id = it.first;
        _node_select_idx.clear();
        _node_select_idx.reserve(selection_idx.size());
        for (unsigned short selection : selection_idx) {
            std::vector<int64_t>& be_ids = channel->_tablet_to_be.find(_tablet_ids[selection])->second;
            if (_enable_replicated_storage) {
                // TODO(meegoo): add backlist policy
                // first replica is primary replica, which determined by FE now
                // only send to primary replica when enable replicated storage engine
                if (be_ids[0] == be_id) {
                    _node_select_idx.emplace_back(selection);
                }
            } else {
                if (std::find(be_ids.begin(), be_ids.end(), be_id) != be_ids.end()) {
                    _node_select_idx.emplace_back(selection);
                }
            }
        }

        auto st = node->add_chunk(chunk, _tablet_ids, _node_select_idx, 0, _node_select_idx.size());

        if (!st.ok()) {
            LOG(WARNING) << node->name() << ", tablet add chunk failed, " << node->print_load_info()
                         << ", node=" << node->node_info()->host << ":" << node->node_info()->brpc_port
                         << ", errmsg=" << st.get_error_msg();
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

Status OlapTableSink::try_close(RuntimeState* state) {
    Status err_st = Status::OK();
    bool intolerable_failure = false;
    if (_colocate_mv_index) {
        for_each_node_channel([this, &err_st, &intolerable_failure](NodeChannel* ch) {
            if (!this->is_failed_channel(ch)) {
                auto st = ch->try_close();
                if (!st.ok()) {
                    LOG(WARNING) << "close channel failed. channel_name=" << ch->name()
                                 << ", load_info=" << ch->print_load_info() << ", error_msg=" << st.get_error_msg();
                    err_st = st;
                    this->mark_as_failed(ch);
                }
            }
            if (this->has_intolerable_failure()) {
                intolerable_failure = true;
            }
        });
    } else {
        for (auto& index_channel : _channels) {
            if (index_channel->has_incremental_node_channel()) {
                // close initial node channel and wait it done
                index_channel->for_each_initial_node_channel(
                        [&index_channel, &err_st, &intolerable_failure](NodeChannel* ch) {
                            if (!index_channel->is_failed_channel(ch)) {
                                auto st = ch->try_close(true);
                                if (!st.ok()) {
                                    LOG(WARNING) << "close initial channel failed. channel_name=" << ch->name()
                                                 << ", load_info=" << ch->print_load_info()
                                                 << ", error_msg=" << st.get_error_msg();
                                    err_st = st;
                                    index_channel->mark_as_failed(ch);
                                }
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
                index_channel->for_each_incremental_node_channel(
                        [&index_channel, &err_st, &intolerable_failure](NodeChannel* ch) {
                            if (!index_channel->is_failed_channel(ch)) {
                                auto st = ch->try_close();
                                if (!st.ok()) {
                                    LOG(WARNING) << "close incremental channel failed. channel_name=" << ch->name()
                                                 << ", load_info=" << ch->print_load_info()
                                                 << ", error_msg=" << st.get_error_msg();
                                    err_st = st;
                                    index_channel->mark_as_failed(ch);
                                }
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
                                         << ", load_info=" << ch->print_load_info()
                                         << ", error_msg=" << st.get_error_msg();
                            err_st = st;
                            index_channel->mark_as_failed(ch);
                        }
                    }
                    if (index_channel->has_intolerable_failure()) {
                        intolerable_failure = true;
                    }
                });
            }
        }
    }

    if (intolerable_failure || (_enable_replicated_storage && !err_st.ok())) {
        return err_st;
    } else {
        return Status::OK();
    }
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

    std::vector<uint8_t> init_filter(chunk->num_rows(), 0);

    if (_keys_type == TKeysType::PRIMARY_KEYS && _output_tuple_desc->slots().back()->col_name() == "__op") {
        size_t op_column_id = chunk->num_columns() - 1;
        ColumnPtr& op_col = chunk->get_column_by_index(op_column_id);
        auto* ops = reinterpret_cast<const uint8_t*>(op_col->raw_data());
        size_t row = chunk->num_rows();

        for (size_t i = 0; i < row; ++i) {
            if (ops[i] == TOpType::DELETE) {
                // Just init when user do not specify the column value
                if (filter[i] != 0) {
                    init_filter[i] = 1;
                    filter[i] = 0;
                }
            }
        }
    }

    // In many cases, it is safe if the auto increment column value is un-inited for the deleted row
    // Because, this row will be deleteed any way. We don't care about the value any more.
    // But if auto increment column is the key, the value of increment column will decide which row
    // will be deleteed and it is matter in this case.
    // Here we just set 0 value in this case.
    uint32 del_rows = SIMD::count_nonzero(init_filter);
    if (del_rows != 0) {
        RETURN_IF_ERROR((std::dynamic_pointer_cast<Int64Column>(data_col))
                                ->fill_range(std::vector<int64_t>(del_rows, 0), init_filter));
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
    if (!_close_done) {
        bool close_done = true;
        if (_colocate_mv_index) {
            for_each_node_channel([&close_done](NodeChannel* ch) { close_done &= ch->is_close_done(); });
        } else {
            for_each_index_channel([&close_done](NodeChannel* ch) { close_done &= ch->is_close_done(); });
        }
        _close_done = close_done;
    }

    return _close_done;
}

Status OlapTableSink::close(RuntimeState* state, Status close_status) {
    if (close_status.ok()) {
        SCOPED_TIMER(_profile->total_time_counter());
        SCOPED_TIMER(_close_timer);
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
    Status status = std::move(close_status);
    if (status.ok()) {
        // only if status is ok can we call this _profile->total_time_counter().
        // if status is not ok, this sink may not be prepared, so that _profile is null
        SCOPED_TIMER(_profile->total_time_counter());
        // BE id -> add_batch method counter
        std::unordered_map<int64_t, AddBatchCounter> node_add_batch_counter_map;
        int64_t serialize_batch_ns = 0, actual_consume_ns = 0;
        {
            SCOPED_TIMER(_close_timer);
            Status err_st = Status::OK();

            if (_colocate_mv_index) {
                for_each_node_channel([this, &state, &node_add_batch_counter_map, &serialize_batch_ns,
                                       &actual_consume_ns, &err_st](NodeChannel* ch) {
                    auto channel_status = ch->close_wait(state);
                    if (!channel_status.ok()) {
                        LOG(WARNING) << "close channel failed. channel_name=" << ch->name()
                                     << ", load_info=" << ch->print_load_info()
                                     << ", error_msg=" << channel_status.get_error_msg();
                        err_st = channel_status;
                        this->mark_as_failed(ch);
                    }
                    ch->time_report(&node_add_batch_counter_map, &serialize_batch_ns, &actual_consume_ns);
                });
                if (has_intolerable_failure() || (_enable_replicated_storage && !err_st.ok())) {
                    status = err_st;
                    for_each_node_channel([&status](NodeChannel* ch) { ch->cancel(status); });
                }
            } else {
                for (auto& index_channel : _channels) {
                    index_channel->for_each_node_channel([&index_channel, &state, &node_add_batch_counter_map,
                                                          &serialize_batch_ns, &actual_consume_ns,
                                                          &err_st](NodeChannel* ch) {
                        auto channel_status = ch->close_wait(state);
                        if (!channel_status.ok()) {
                            LOG(WARNING) << "close channel failed. channel_name=" << ch->name()
                                         << ", load_info=" << ch->print_load_info()
                                         << ", error_msg=" << channel_status.get_error_msg();
                            err_st = channel_status;
                            index_channel->mark_as_failed(ch);
                        }
                        ch->time_report(&node_add_batch_counter_map, &serialize_batch_ns, &actual_consume_ns);
                    });
                    if (index_channel->has_intolerable_failure() || (_enable_replicated_storage && !err_st.ok())) {
                        status = err_st;
                        index_channel->for_each_node_channel([&status](NodeChannel* ch) { ch->cancel(status); });
                    }
                }
            }
        }
        COUNTER_SET(_input_rows_counter, _number_input_rows);
        COUNTER_SET(_output_rows_counter, _number_output_rows);
        COUNTER_SET(_filtered_rows_counter, _number_filtered_rows);
        COUNTER_SET(_convert_chunk_timer, _convert_batch_ns);
        COUNTER_SET(_validate_data_timer, _validate_data_ns);
        COUNTER_SET(_serialize_chunk_timer, serialize_batch_ns);
        COUNTER_SET(_send_rpc_timer, actual_consume_ns);

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
        _server_rpc_timer->update(total_server_rpc_time_us * 1000);
        _server_wait_flush_timer->update(total_server_wait_memtable_flush_time_us * 1000);
        LOG(INFO) << ss.str();
    } else {
        COUNTER_SET(_input_rows_counter, _number_input_rows);
        COUNTER_SET(_output_rows_counter, _number_output_rows);
        COUNTER_SET(_filtered_rows_counter, _number_filtered_rows);
        COUNTER_SET(_convert_chunk_timer, _convert_batch_ns);
        COUNTER_SET(_validate_data_timer, _validate_data_ns);

        if (_colocate_mv_index) {
            for_each_node_channel([&status](NodeChannel* ch) { ch->cancel(status); });
        } else {
            for_each_index_channel([&status](NodeChannel* ch) { ch->cancel(status); });
        }
    }

    Expr::close(_output_expr_ctxs, state);
    if (_vectorized_partition) {
        _vectorized_partition->close(state);
    }
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
            if (!config::enable_check_string_lengths) {
                continue;
            }
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
    _partition_ids.clear();
    return Status::OK();
}

} // namespace stream_load
} // namespace starrocks
