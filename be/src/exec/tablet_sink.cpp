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
#include <utility>

#include "column/binary_column.h"
#include "column/chunk.h"
#include "column/column_helper.h"
#include "column/nullable_column.h"
#include "common/statusor.h"
#include "config.h"
#include "exprs/expr.h"
#include "gutil/strings/fastmem.h"
#include "gutil/strings/substitute.h"
#include "runtime/current_thread.h"
#include "runtime/exec_env.h"
#include "runtime/runtime_state.h"
#include "serde/protobuf_serde.h"
#include "simd/simd.h"
#include "types/hll.h"
#include "util/brpc_stub_cache.h"
#include "util/compression/compression_utils.h"
#include "util/defer_op.h"
#include "util/thread.h"
#include "util/uid_util.h"

static const uint8_t VALID_SEL_FAILED = 0x0;
static const uint8_t VALID_SEL_OK = 0x1;
// it's a valid value and selected, but it's null
// and we don't need following extra check
// make sure the least bit is 1.
static const uint8_t VALID_SEL_OK_AND_NULL = 0x3;

namespace starrocks::stream_load {

NodeChannel::NodeChannel(OlapTableSink* parent, int64_t node_id) : _parent(parent), _node_id(node_id) {
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

        _open(_rpc_request.requests(i).index_id(), _open_closures[i]);
    }
}

void NodeChannel::_open(int64_t index_id, RefCountClosure<PTabletWriterOpenResult>* open_closure) {
    PTabletWriterOpenRequest request;
    request.set_merge_condition(_parent->_merge_condition);
    request.set_allocated_id(&_parent->_load_id);
    request.set_index_id(index_id);
    request.set_txn_id(_parent->_txn_id);
    request.set_txn_trace_parent(_parent->_txn_trace_parent);
    request.set_allocated_schema(_parent->_schema->to_protobuf());
    request.set_is_lake_tablet(_parent->_is_lake_table);
    request.set_is_replicated_storage(_parent->_enable_replicated_storage);
    request.set_node_id(_node_id);
    request.set_write_quorum(_write_quorum_type);
    for (auto& tablet : _index_tablets_map[index_id]) {
        auto ptablet = request.add_tablets();
        ptablet->Swap(&tablet);
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

    open_closure->cntl.set_timeout_ms(config::tablet_writer_open_rpc_timeout_sec * 1000);
    open_closure->cntl.ignore_eovercrowded();

    if (request.ByteSizeLong() > _parent->_rpc_http_min_size) {
        TNetworkAddress brpc_addr;
        brpc_addr.hostname = _node_info->host;
        brpc_addr.port = _node_info->brpc_port;
        open_closure->cntl.http_request().set_content_type("application/proto");
        auto res = BrpcStubCache::create_http_stub(brpc_addr);
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

    VLOG(2) << "NodeChannel[" << _load_info << "] send open request to [" << _node_info->host << ":"
            << _node_info->brpc_port << "]";
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

Status NodeChannel::_serialize_chunk(const vectorized::Chunk* src, ChunkPB* dst) {
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

Status NodeChannel::add_chunk(vectorized::Chunk* input, const std::vector<int64_t>& tablet_ids,
                              const std::vector<uint32_t>& indexes, uint32_t from, uint32_t size, bool eos) {
    if (_cancelled || _send_finished) {
        return _err_st;
    }

    if (LIKELY(!eos)) {
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
        _cur_chunk->append_selective(*input, indexes.data(), from, size);
        auto req = _rpc_request.mutable_requests(0);
        for (size_t i = 0; i < size; ++i) {
            req->add_tablet_ids(tablet_ids[indexes[from + i]]);
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
    }

    return _send_request(eos);
}

Status NodeChannel::add_chunks(vectorized::Chunk* input, const std::vector<std::vector<int64_t>>& tablet_ids,
                               const std::vector<uint32_t>& indexes, uint32_t from, uint32_t size, bool eos) {
    if (_cancelled || _send_finished) {
        return _err_st;
    }

    if (LIKELY(!eos)) {
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
    }

    return _send_request(eos);
}

Status NodeChannel::_send_request(bool eos) {
    if (eos) {
        if (_request_queue.empty()) {
            if (_cur_chunk.get() == nullptr) {
                _cur_chunk = std::make_unique<vectorized::Chunk>();
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
            auto res = BrpcStubCache::create_http_stub(brpc_addr);
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
            auto res = BrpcStubCache::create_http_stub(brpc_addr);
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

Status NodeChannel::try_close() {
    if (_cancelled || _send_finished) {
        return _err_st;
    }

    if (_check_prev_request_done()) {
        auto st = _send_request(true /* eos */);
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

IndexChannel::~IndexChannel() = default;

Status IndexChannel::init(RuntimeState* state, const std::vector<PTabletWithPartition>& tablets) {
    for (const auto& tablet : tablets) {
        auto* location = _parent->_location->find_tablet(tablet.tablet_id());
        if (location == nullptr) {
            auto msg = fmt::format("Not found tablet: {}", tablet.tablet_id());
            return Status::NotFound(msg);
        }
        std::vector<NodeChannel*> channels;
        std::vector<int64_t> bes;
        for (auto& node_id : location->node_ids) {
            NodeChannel* channel = nullptr;
            auto it = _node_channels.find(node_id);
            if (it == std::end(_node_channels)) {
                auto channel_ptr = std::make_unique<NodeChannel>(_parent, node_id);
                channel = channel_ptr.get();
                _node_channels.emplace(node_id, std::move(channel_ptr));
            } else {
                channel = it->second.get();
            }
            channel->add_tablet(_index_id, tablet);
            channels.push_back(channel);
            bes.emplace_back(node_id);
        }
        _tablet_to_be.emplace(tablet.tablet_id(), std::move(bes));
    }
    for (auto& it : _node_channels) {
        RETURN_IF_ERROR(it.second->init(state));
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

OlapTableSink::OlapTableSink(ObjectPool* pool, const std::vector<TExpr>& texprs, Status* status) : _pool(pool) {
    if (!texprs.empty()) {
        *status = Expr::create_expr_trees(_pool, texprs, &_output_expr_ctxs);
    }
}

Status OlapTableSink::init(const TDataSink& t_sink) {
    DCHECK(t_sink.__isset.olap_table_sink);
    const auto& table_sink = t_sink.olap_table_sink;
    _merge_condition = table_sink.merge_condition;
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
    if (table_sink.__isset.write_quorum_type) {
        _write_quorum_type = table_sink.write_quorum_type;
    }
    if (table_sink.__isset.enable_replicated_storage) {
        _enable_replicated_storage = table_sink.enable_replicated_storage;
    }
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
    _span->AddEvent("prepare");

    // profile must add to state's object pool
    _profile = state->obj_pool()->add(new RuntimeProfile("OlapTableSink"));

    _profile->add_info_string("TxnID", fmt::format("{}", _txn_id));
    _profile->add_info_string("IndexNum", fmt::format("{}", _schema->indexes().size()));
    _profile->add_info_string("ReplicatedStorage", fmt::format("{}", _enable_replicated_storage));
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

    _load_mem_limit = state->get_load_mem_limit();
    _rpc_http_min_size = state->get_rpc_http_min_size();
    // open all channels
    return _init_node_channels(state);
}

Status OlapTableSink::_init_node_channels(RuntimeState* state) {
    const auto& partitions = _vectorized_partition->get_partitions();
    for (int i = 0; i < _schema->indexes().size(); ++i) {
        // collect all tablets belong to this rollup
        std::vector<PTabletWithPartition> tablets;
        auto* index = _schema->indexes()[i];
        for (auto* part : partitions) {
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
                            auto channel_ptr = std::make_unique<NodeChannel>(this, node_id);
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
        auto channel = std::make_unique<IndexChannel>(this, index->index_id);
        RETURN_IF_ERROR(channel->init(state, tablets));
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

    return full;
}

Status OlapTableSink::send_chunk(RuntimeState* state, vectorized::Chunk* chunk) {
    SCOPED_TIMER(_profile->total_time_counter());
    DCHECK(chunk->num_rows() > 0);
    size_t num_rows = chunk->num_rows();
    _number_input_rows += num_rows;
    size_t serialize_size = serde::ProtobufChunkSerde::max_serialized_size(*chunk);
    // update incrementally so that FE can get the progress.
    // the real 'num_rows_load_total' will be set when sink being closed.
    state->update_num_rows_load_from_sink(num_rows);
    state->update_num_bytes_load_from_sink(serialize_size);
    StarRocksMetrics::instance()->load_rows_total.increment(num_rows);
    StarRocksMetrics::instance()->load_bytes_total.increment(serialize_size);

    {
        SCOPED_TIMER(_prepare_data_timer);
        {
            SCOPED_RAW_TIMER(&_convert_batch_ns);
            if (!_output_expr_ctxs.empty()) {
                _output_chunk = std::make_unique<vectorized::Chunk>();
                for (size_t i = 0; i < _output_expr_ctxs.size(); ++i) {
                    ASSIGN_OR_RETURN(ColumnPtr tmp, _output_expr_ctxs[i]->evaluate(chunk));
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

        {
            SCOPED_RAW_TIMER(&_validate_data_ns);
            _validate_selection.assign(num_rows, VALID_SEL_OK);
            _validate_data(state, chunk);
        }
        {
            uint32_t num_rows_after_validate = SIMD::count_nonzero(_validate_selection);
            int invalid_row_index = 0;
            RETURN_IF_ERROR(_vectorized_partition->find_tablets(chunk, &_partitions, &_tablet_indexes,
                                                                &_validate_selection, &invalid_row_index));

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
                if (!state->has_reached_max_error_msg_num()) {
                    std::string debug_row = chunk->debug_row(invalid_row_index);
                    state->append_error_msg_to_file(debug_row,
                                                    "The row is out of partition ranges. Please add a new partition.");
                }
            }

            _number_filtered_rows += (num_rows - _validate_select_idx.size());
            _number_output_rows += _validate_select_idx.size();
        }
    }

    SCOPED_TIMER(_send_data_timer);

    if (_colocate_mv_index) {
        return _send_chunk_with_colocate_index(chunk);
    } else {
        return _send_chunk(chunk);
    }
}

Status OlapTableSink::_send_chunk(vectorized::Chunk* chunk) {
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

Status OlapTableSink::_send_chunk_with_colocate_index(vectorized::Chunk* chunk) {
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

Status OlapTableSink::_send_chunk_by_node(vectorized::Chunk* chunk, IndexChannel* channel,
                                          std::vector<uint16_t>& selection_idx) {
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

        auto st = node->add_chunk(chunk, _tablet_ids, _node_select_idx, 0, _node_select_idx.size(), false /* eos */);

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
            } else {
                ch->cancel(Status::Cancelled("channel failed"));
            }
            if (this->has_intolerable_failure()) {
                intolerable_failure = true;
            }
        });
    } else {
        for (auto& index_channel : _channels) {
            index_channel->for_each_node_channel([&index_channel, &err_st, &intolerable_failure](NodeChannel* ch) {
                if (!index_channel->is_failed_channel(ch)) {
                    auto st = ch->try_close();
                    if (!st.ok()) {
                        LOG(WARNING) << "close channel failed. channel_name=" << ch->name()
                                     << ", load_info=" << ch->print_load_info() << ", error_msg=" << st.get_error_msg();
                        err_st = st;
                        index_channel->mark_as_failed(ch);
                    }
                } else {
                    ch->cancel(Status::Cancelled("channel failed"));
                }
                if (index_channel->has_intolerable_failure()) {
                    intolerable_failure = true;
                }
            });
        }
    }

    if (intolerable_failure || (_enable_replicated_storage && !err_st.ok())) {
        return err_st;
    } else {
        return Status::OK();
    }
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

        // _number_input_rows don't contain num_rows_load_filtered and num_rows_load_unselected in scan node
        state->update_num_rows_load_from_sink(state->num_rows_load_filtered() + state->num_rows_load_unselected());
        state->update_num_rows_load_filtered(_number_filtered_rows);

        int64_t total_server_rpc_time_us = 0;
        // print log of add batch time of all node, for tracing load performance easily
        std::stringstream ss;
        ss << "Olap table sink statistics. load_id: " << print_id(_load_id) << ", txn_id: " << _txn_id
           << ", add chunk time(ms)/wait lock time(ms)/num: ";
        for (auto const& pair : node_add_batch_counter_map) {
            total_server_rpc_time_us += pair.second.add_batch_execution_time_us;
            ss << "{" << pair.first << ":(" << (pair.second.add_batch_execution_time_us / 1000) << ")("
               << (pair.second.add_batch_wait_lock_time_us / 1000) << ")(" << pair.second.add_batch_num << ")} ";
        }
        _server_rpc_timer->update(total_server_rpc_time_us * 1000);
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
    _vectorized_partition->close(state);
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

template <PrimitiveType PT, typename CppType = vectorized::RunTimeCppType<PT>>
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

template <PrimitiveType PT>
void OlapTableSink::_validate_decimal(RuntimeState* state, vectorized::Column* column, const SlotDescriptor* desc,
                                      std::vector<uint8_t>* validate_selection) {
    using CppType = vectorized::RunTimeCppType<PT>;
    using ColumnType = vectorized::RunTimeColumnType<PT>;
    auto* data_column = down_cast<ColumnType*>(vectorized::ColumnHelper::get_data_column(column));
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
            auto* nullable = down_cast<vectorized::NullableColumn*>(column_ptr.get());
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
                        if (!state->has_reached_max_error_msg_num()) {
                            state->append_error_msg_to_file(chunk->debug_row(j), ss.str());
                        }
#endif
                    }
                }
            }
            chunk->update_column(nullable->data_column(), desc->id());
        } else if (column_ptr->has_null()) {
            auto* nullable = down_cast<vectorized::NullableColumn*>(column_ptr.get());
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
            auto* binary = down_cast<vectorized::BinaryColumn*>(data_column);
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
            auto* decimal = down_cast<vectorized::DecimalColumn*>(column);
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
            auto* binary = down_cast<vectorized::BinaryColumn*>(data_column);
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

} // namespace starrocks::stream_load
