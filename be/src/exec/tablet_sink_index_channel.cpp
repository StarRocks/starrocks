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

#include "exec/tablet_sink_index_channel.h"

#include "column/chunk.h"
#include "column/column_helper.h"
#include "column/column_viewer.h"
#include "column/nullable_column.h"
#include "common/statusor.h"
#include "config.h"
#include "exec/tablet_sink.h"
#include "exprs/expr_context.h"
#include "gutil/strings/fastmem.h"
#include "gutil/strings/join.h"
#include "gutil/strings/substitute.h"
#include "runtime/current_thread.h"
#include "runtime/exec_env.h"
#include "runtime/runtime_state.h"
#include "serde/protobuf_serde.h"
#include "util/brpc_stub_cache.h"
#include "util/compression/compression_utils.h"
#include "util/thrift_rpc_helper.h"

namespace starrocks::stream_load {

class OlapTableSink; // forward declaration
NodeChannel::NodeChannel(OlapTableSink* parent, int64_t node_id, bool is_incremental, ExprContext* where_clause)
        : _parent(parent), _node_id(node_id), _is_incremental(is_incremental), _where_clause(where_clause) {
    // restrict the chunk memory usage of send queue & brpc write buffer
    _mem_tracker = std::make_unique<MemTracker>(config::send_channel_buffer_limit, "", nullptr);
    _ts_profile = _parent->ts_profile();
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
    request.set_immutable_tablet_size(_parent->_automatic_bucket_size);
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
        }
        auto it_version = dict_version.find(slot->id());
        if (it_version != dict_version.end()) {
            slot->set_global_dict_version(it_version->second);
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
        VLOG(2) << "open colocate index failed";
        _enable_colocate_mv_index = false;
    }

    if (open_closure->result.immutable_partition_ids_size() > 0) {
        auto immutable_partition_ids_size = _immutable_partition_ids.size();
        _immutable_partition_ids.insert(open_closure->result.immutable_partition_ids().begin(),
                                        open_closure->result.immutable_partition_ids().end());
        if (_immutable_partition_ids.size() != immutable_partition_ids_size) {
            string partition_ids_str;
            JoinInts(_immutable_partition_ids, ",", &partition_ids_str);
            LOG(INFO) << "NodeChannel[" << _load_info << "] immutable partition ids : " << partition_ids_str;
        }
    }

    VLOG(2) << "open colocate index, enable_colocate_mv_index=" << _enable_colocate_mv_index;

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
        SCOPED_TIMER(_ts_profile->compress_timer);

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
        SCOPED_TIMER(_ts_profile->wait_response_timer);
        // wait previous request done then we can pop data from queue to send request
        // and make new space to push data.
        RETURN_IF_ERROR(_wait_one_prev_request());
    }

    SCOPED_TIMER(_ts_profile->pack_chunk_timer);
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

Status NodeChannel::add_chunks(Chunk* input, const std::vector<std::vector<int64_t>>& index_tablet_ids,
                               const std::vector<uint32_t>& indexes, uint32_t from, uint32_t size) {
    if (_cancelled || _send_finished) {
        return _err_st;
    }

    DCHECK(index_tablet_ids.size() == _rpc_request.requests_size());
    if (UNLIKELY(_cur_chunk == nullptr)) {
        _cur_chunk = input->clone_empty_with_slot();
    }

    if (is_full()) {
        // wait previous request done then we can pop data from queue to send request
        // and make new space to push data.
        RETURN_IF_ERROR(_wait_one_prev_request());
    }

    SCOPED_TIMER(_ts_profile->pack_chunk_timer);
    // 1. append data
    _cur_chunk->append_selective(*input, indexes.data(), from, size);
    for (size_t index_i = 0; index_i < index_tablet_ids.size(); ++index_i) {
        auto req = _rpc_request.mutable_requests(index_i);
        for (size_t i = from; i < size; ++i) {
            req->add_tablet_ids(index_tablet_ids[index_i][indexes[from + i]]);
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
        for (size_t index_i = 0; index_i < index_tablet_ids.size(); ++index_i) {
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
            auto& partition_ids = _parent->_index_id_partition_ids[req->index_id()];
            if (!partition_ids.empty()) {
                VLOG(2) << "partition_ids:" << std::string(partition_ids.begin(), partition_ids.end());
            }
            for (auto pid : partition_ids) {
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

    VLOG(2) << "NodeChannel[" << _load_info << "] send chunk request [rows: " << chunk->num_rows() << " eos: " << eos
            << "] to [" << _node_info->host << ":" << _node_info->brpc_port << "]";

    return Status::OK();
}

Status NodeChannel::_wait_request(ReusableClosure<PTabletWriterAddBatchResult>* closure) {
    if (!closure->join()) {
        return Status::OK();
    }
    _mem_tracker->release(closure->request_size);

    _ts_profile->client_rpc_timer->update(closure->latency());

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

    if (closure->result.immutable_partition_ids_size() > 0) {
        auto immutable_partition_ids_size = _immutable_partition_ids.size();
        _immutable_partition_ids.insert(closure->result.immutable_partition_ids().begin(),
                                        closure->result.immutable_partition_ids().end());
        if (_immutable_partition_ids.size() != immutable_partition_ids_size) {
            string partition_ids_str;
            JoinInts(_immutable_partition_ids, ",", &partition_ids_str);
            LOG(INFO) << "NodeChannel[" << _load_info << "] immutable partition ids : " << partition_ids_str;
        }
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

    // assign tablet dict infos
    if (!_tablet_commit_infos.empty()) {
        std::vector<std::string> invalid_dict_cache_columns;
        invalid_dict_cache_columns.assign(_valid_dict_cache_info.invalid_dict_cache_column_set.begin(),
                                          _valid_dict_cache_info.invalid_dict_cache_column_set.end());
        _tablet_commit_infos[0].__set_invalid_dict_cache_columns(invalid_dict_cache_columns);

        std::vector<std::string> valid_dict_cache_columns;
        std::vector<int64_t> valid_dict_collected_versions;
        for (const auto& [name, version] : _valid_dict_cache_info.valid_dict_cache_column_set) {
            if (_valid_dict_cache_info.invalid_dict_cache_column_set.count(name) == 0) {
                valid_dict_cache_columns.emplace_back(name);
                valid_dict_collected_versions.emplace_back(version);
            }
        }
        _tablet_commit_infos[0].__set_valid_dict_cache_columns(valid_dict_cache_columns);
        _tablet_commit_infos[0].__set_valid_dict_collected_versions(valid_dict_collected_versions);
    }

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
        }
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

} // namespace starrocks::stream_load
