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
//   https://github.com/apache/incubator-doris/blob/master/be/src/runtime/buffer_control_block.cpp

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

#include "runtime/buffer_control_block.h"

#include <utility>

#include "gen_cpp/InternalService_types.h"
#include "gen_cpp/internal_service.pb.h"
#include "service/brpc.h"
#include "util/defer_op.h"
#include "util/thrift_util.h"

namespace starrocks {

void GetResultBatchCtx::on_failure(const Status& status) {
    DCHECK(!status.ok()) << "status is ok, errmsg=" << status.get_error_msg();
    status.to_protobuf(result->mutable_status());
    done->Run();
    delete this;
}

void GetResultBatchCtx::on_close(int64_t packet_seq, QueryStatistics* statistics) {
    Status status;
    status.to_protobuf(result->mutable_status());
    if (statistics != nullptr) {
        statistics->to_pb(result->mutable_query_statistics());
    }
    result->set_packet_seq(packet_seq);
    result->set_eos(true);
    done->Run();
    delete this;
}

void GetResultBatchCtx::on_data(TFetchDataResult* t_result, int64_t packet_seq, bool eos) {
    uint8_t* buf = nullptr;
    uint32_t len = 0;
    ThriftSerializer ser(false, 4096);
    auto st = ser.serialize(&t_result->result_batch, &len, &buf);
    if (st.ok()) {
        cntl->response_attachment().append(buf, len);
        result->set_packet_seq(packet_seq);
        result->set_eos(eos);
    } else {
        LOG(WARNING) << "TFetchDataResult serialize failed, errmsg=" << st.get_error_msg();
    }
    st.to_protobuf(result->mutable_status());
    done->Run();
    delete this;
}

void GetResultBatchCtx::on_data(SerializeRes* res, int64_t packet_seq, bool eos) {
    auto st = Status::OK();
    cntl->response_attachment().swap(res->attachment);
    result->set_packet_seq(packet_seq);
    result->set_eos(eos);
    st.to_protobuf(result->mutable_status());
    done->Run();
    delete this;
}

BufferControlBlock::BufferControlBlock(const TUniqueId& id, int buffer_size)
        : _fragment_id(id),
          _is_close(false),
          _is_cancelled(false),
          _buffer_bytes(0),
          _buffer_limit(buffer_size),
          _packet_num(0) {}

BufferControlBlock::~BufferControlBlock() {
    cancel();

    _batch_queue.clear();
    _buffer_bytes = 0;
}

Status BufferControlBlock::init() {
    return Status::OK();
}

Status BufferControlBlock::add_batch(TFetchDataResult* result, bool need_free) {
    if (_is_cancelled) {
        return Status::Cancelled("Cancelled BufferControlBlock::add_batch");
    }
    // serialize first
    ASSIGN_OR_RETURN(auto ser_res, _serialize_result(result))
    // should delete it outside in abnormal cases
    if (need_free) {
        delete result;
    }
    std::unique_lock<std::mutex> l(_lock);
    while ((_batch_queue.size() > _buffer_limit || _buffer_bytes > _max_memory_usage) && !_is_cancelled) {
        _data_removal.wait(l);
    }

    if (_is_cancelled) {
        return Status::Cancelled("Cancelled BufferControlBlock::add_batch");
    }

    _process_batch_without_lock(ser_res);
    return Status::OK();
}

StatusOr<std::unique_ptr<SerializeRes>> BufferControlBlock::_serialize_result(TFetchDataResult* result) {
    uint8_t* buf = nullptr;
    uint32_t len = 0;
    auto ser_res = std::make_unique<SerializeRes>();
    ser_res->row_size = result->result_batch.rows.size();
    ThriftSerializer ser(false, 4096);
    RETURN_IF_ERROR(ser.serialize(&result->result_batch, &len, &buf));
    ser_res->attachment.append(buf, len);
    return std::move(ser_res);
}

Status BufferControlBlock::add_batch(std::unique_ptr<TFetchDataResult>& result) {
    return add_batch(result.get(), false);
}

void BufferControlBlock::_process_batch_without_lock(std::unique_ptr<SerializeRes>& ser_res) {
    if (_waiting_rpc.empty()) {
        _buffer_bytes += ser_res->attachment.length();
        _batch_queue.push_back(std::move(ser_res));
        _data_arriaval.notify_one();
    } else {
        auto* ctx = _waiting_rpc.front();
        _waiting_rpc.pop_front();
        ctx->on_data(ser_res.get(), _packet_num);
        _packet_num++;
    }
}

StatusOr<bool> BufferControlBlock::try_add_batch(std::unique_ptr<TFetchDataResult>& result) {
    if (_is_cancelled) {
        return Status::Cancelled("Cancelled BufferControlBlock::add_batch");
    }
    std::unique_lock<std::mutex> l(_lock);
    if ((_batch_queue.size() > _buffer_limit || _buffer_bytes > _max_memory_usage) && !_is_cancelled) {
        return false;
    }
    l.unlock();
    ASSIGN_OR_RETURN(auto ser_res, _serialize_result(result.get()))
    l.lock();
    _process_batch_without_lock(ser_res);
    return true;
}

StatusOr<bool> BufferControlBlock::try_add_batch(std::vector<std::unique_ptr<TFetchDataResult>>& results) {
    if (_is_cancelled) {
        return Status::Cancelled("Cancelled BufferControlBlock::add_batch");
    }
    std::unique_lock<std::mutex> l(_lock);
    if ((_batch_queue.size() > _buffer_limit || _buffer_bytes > _max_memory_usage) && !_is_cancelled) {
        return false;
    }
    if (_is_cancelled) {
        return Status::Cancelled("Cancelled BufferControlBlock::add_batch");
    }
    l.unlock();
    // serialize first
    for (auto& result : results) {
        ASSIGN_OR_RETURN(auto ser_res, _serialize_result(result.get()))
        l.lock();
        _buffer_bytes += ser_res->attachment.length();
        _batch_queue.push_back(std::move(ser_res));
        _data_arriaval.notify_one();
        l.unlock();
    }

    l.lock();
    if (!_waiting_rpc.empty() && !_batch_queue.empty()) {
        std::unique_ptr<SerializeRes> ser = std::move(_batch_queue.front());
        _batch_queue.pop_front();
        _buffer_bytes -= ser->attachment.length();
        auto* ctx = _waiting_rpc.front();
        _waiting_rpc.pop_front();
        auto packet_num = _packet_num.load();
        _packet_num++;
        l.unlock();
        ctx->on_data(ser.get(), packet_num);
    }
    return true;
}

// seems no use?
Status BufferControlBlock::get_batch(TFetchDataResult* result) {
    std::unique_ptr<SerializeRes> ser = nullptr;
    {
        std::unique_lock<std::mutex> l(_lock);

        while (_batch_queue.empty() && !_is_close && !_is_cancelled) {
            _data_arriaval.wait(l);
        }
        // if Status has been set, return fail;
        RETURN_IF_ERROR(_status);
        // cancelled
        if (_is_cancelled) {
            return Status::Cancelled("Cancelled BufferControlBlock::get_batch");
        }

        if (_batch_queue.empty()) {
            if (_is_close) {
                // no result, normal end
                result->eos = true;
                result->__set_packet_num(_packet_num);
                _packet_num++;
                return Status::OK();
            } else {
                // can not get here
                return Status::InternalError("Internal error, can not Get here!");
            }
        }

        // get result
        ser = std::move(_batch_queue.front());
        _batch_queue.pop_front();
        _buffer_bytes -= ser->attachment.length();
        _data_removal.notify_one();
    }

    // as this function seems useless, so deserialize it.
    std::vector<uint8_t> continuous_mem(ser->attachment.size());
    // IOBuf is not continuous
    auto copied_size = ser->attachment.copy_to(continuous_mem.data(), ser->attachment.size(), 0);
    DCHECK(copied_size == ser->attachment.size());
    uint32_t len = continuous_mem.size();
    RETURN_IF_ERROR(deserialize_thrift_msg(continuous_mem.data(), &len, TProtocolType::BINARY, &result->result_batch));
    result->__set_packet_num(_packet_num);
    _packet_num++;

    return Status::OK();
}

void BufferControlBlock::get_batch(GetResultBatchCtx* ctx) {
    std::unique_lock<std::mutex> l(_lock);
    if (!_status.ok()) {
        ctx->on_failure(_status);
        return;
    }
    if (_is_cancelled) {
        ctx->on_failure(Status::Cancelled("Cancelled BufferControlBlock::get_batch"));
        return;
    }
    if (!_batch_queue.empty()) {
        std::unique_ptr<SerializeRes> ser = std::move(_batch_queue.front());
        _batch_queue.pop_front();
        _buffer_bytes -= ser->attachment.length();
        // _data_removal.notify_one();
        auto packet_num = _packet_num.load();
        ++_packet_num;
        l.unlock();
        ctx->on_data(ser.get(), packet_num);
        return;
    }
    if (_is_close) {
        ctx->on_close(_packet_num, _query_statistics.get());
        return;
    }
    // no ready data, push ctx to waiting list
    _waiting_rpc.push_back(ctx);
}

Status BufferControlBlock::close(Status exec_status) {
    std::unique_lock<std::mutex> l(_lock);
    _is_close = true;
    _status = std::move(exec_status);
    // notify blocked get thread
    _data_arriaval.notify_all();
    if (!_waiting_rpc.empty()) {
        if (_status.ok()) {
            for (auto& ctx : _waiting_rpc) {
                ctx->on_close(_packet_num, _query_statistics.get());
            }
        } else {
            for (auto& ctx : _waiting_rpc) {
                ctx->on_failure(_status);
            }
        }
        _waiting_rpc.clear();
    }
    return Status::OK();
}

void BufferControlBlock::cancel() {
    std::unique_lock<std::mutex> l(_lock);
    _is_cancelled = true;
    _data_removal.notify_all();
    _data_arriaval.notify_all();
    for (auto& ctx : _waiting_rpc) {
        ctx->on_failure(Status::Cancelled("Cancelled BufferControlBlock::cancel"));
    }
    _waiting_rpc.clear();
}

} // namespace starrocks
