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
//   https://github.com/apache/incubator-doris/blob/master/be/src/runtime/data_stream_mgr.cpp

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

#include "runtime/data_stream_mgr.h"

#include <iostream>
#include <utility>

#include "runtime/current_thread.h"
#include "runtime/data_stream_recvr.h"
#include "runtime/runtime_state.h"
#include "util/starrocks_metrics.h"

namespace starrocks {

DataStreamMgr::DataStreamMgr() {
    REGISTER_GAUGE_STARROCKS_METRIC(data_stream_receiver_count, [this]() { return _receiver_count.load(); });
    REGISTER_GAUGE_STARROCKS_METRIC(fragment_endpoint_count, [this]() { return _fragment_count.load(); });
}

inline uint32_t DataStreamMgr::get_bucket(const TUniqueId& fragment_instance_id) {
    uint32_t value = HashUtil::hash(&fragment_instance_id.lo, 8, 0);
    value = HashUtil::hash(&fragment_instance_id.hi, 8, value);
    return value % BUCKET_NUM;
}

std::shared_ptr<DataStreamRecvr> DataStreamMgr::create_recvr(
        RuntimeState* state, const RowDescriptor& row_desc, const TUniqueId& fragment_instance_id,
        PlanNodeId dest_node_id, int num_senders, int buffer_size, const std::shared_ptr<RuntimeProfile>& profile,
        bool is_merging, std::shared_ptr<QueryStatisticsRecvr> sub_plan_query_statistics_recvr, bool is_pipeline,
        int32_t degree_of_parallelism, bool keep_order) {
    DCHECK(profile != nullptr);
    VLOG_FILE << "creating receiver for fragment=" << fragment_instance_id << ", node=" << dest_node_id;
    PassThroughChunkBuffer* pass_through_chunk_buffer = get_pass_through_chunk_buffer(state->query_id());
    DCHECK(pass_through_chunk_buffer != nullptr);
    std::shared_ptr<DataStreamRecvr> recvr(
            new DataStreamRecvr(this, state, row_desc, fragment_instance_id, dest_node_id, num_senders, is_merging,
                                buffer_size, profile, std::move(sub_plan_query_statistics_recvr), is_pipeline,
                                degree_of_parallelism, keep_order, pass_through_chunk_buffer));

    uint32_t bucket = get_bucket(fragment_instance_id);
    auto& receiver_map = _receiver_map[bucket];
    std::lock_guard<Mutex> l(_lock[bucket]);
    auto iter = receiver_map.find(fragment_instance_id);
    if (iter == receiver_map.end()) {
        receiver_map.insert(std::make_pair(fragment_instance_id, std::make_shared<RecvrMap>()));
        iter = receiver_map.find(fragment_instance_id);
        _fragment_count += 1;
    }
    iter->second->insert(std::make_pair(dest_node_id, recvr));
    _receiver_count += 1;
    return recvr;
}

std::shared_ptr<DataStreamRecvr> DataStreamMgr::find_recvr(const TUniqueId& fragment_instance_id, PlanNodeId node_id) {
    VLOG_ROW << "looking up fragment_instance_id=" << fragment_instance_id << ", node=" << node_id;
    uint32_t bucket = get_bucket(fragment_instance_id);
    auto& receiver_map = _receiver_map[bucket];
    std::shared_lock l(_lock[bucket]);

    auto iter = receiver_map.find(fragment_instance_id);
    if (iter != receiver_map.end()) {
        auto sub_iter = iter->second->find(node_id);
        if (sub_iter != iter->second->end()) {
            return sub_iter->second;
        }
    }
    return {};
}

Status DataStreamMgr::transmit_data(const PTransmitDataParams* request, ::google::protobuf::Closure** done) {
    const PUniqueId& finst_id = request->finst_id();
    TUniqueId t_finst_id;
    t_finst_id.hi = finst_id.hi();
    t_finst_id.lo = finst_id.lo();
    std::shared_ptr<DataStreamRecvr> recvr = find_recvr(t_finst_id, request->node_id());
    if (recvr == nullptr) {
        // The receiver may remove itself from the receiver map via deregister_recvr()
        // at any time without considering the remaining number of senders.
        // As a consequence, find_recvr() may return an innocuous NULL if a thread
        // calling deregister_recvr() beat the thread calling find_recvr()
        // in acquiring _lock.
        // TODO: Rethink the lifecycle of DataStreamRecvr to distinguish
        // errors from receiver-initiated teardowns.
        return Status::OK();
    }

    // Request can only be used before calling recvr's add_batch or when request
    // is the last for the sender, because request maybe released after it's batch
    // is consumed by ExchangeNode.
    if (request->has_query_statistics()) {
        recvr->add_sub_plan_statistics(request->query_statistics(), request->sender_id());
    }

    bool eos = request->eos();
    if (request->has_row_batch()) {
        return Status::InternalError("Non-vectorized execute engine is not supported");
    }

    if (eos) {
        recvr->remove_sender(request->sender_id(), request->be_number());
    }
    return Status::OK();
}

Status DataStreamMgr::transmit_chunk(const PTransmitChunkParams& request, ::google::protobuf::Closure** done) {
    const PUniqueId& finst_id = request.finst_id();
    // TODO(zc): Use PUniqueId directly
    // We can use PUniqueId directly, because old version StarRocks has already use
    // BRPC to transmit data other than thrift.
    TUniqueId t_finst_id;
    t_finst_id.hi = finst_id.hi();
    t_finst_id.lo = finst_id.lo();
    SCOPED_SET_TRACE_INFO({}, {}, t_finst_id);
    std::shared_ptr<DataStreamRecvr> recvr = find_recvr(t_finst_id, request.node_id());
    if (recvr == nullptr) {
        // The receiver may remove itself from the receiver map via deregister_recvr()
        // at any time without considering the remaining number of senders.
        // As a consequence, find_recvr() may return an innocuous NULL if a thread
        // calling deregister_recvr() beat the thread calling find_recvr()
        // in acquiring _lock.
        // TODO: Rethink the lifecycle of DataStreamRecvr to distinguish
        // errors from receiver-initiated teardowns.
        return Status::OK();
    }

    // request can only be used before calling recvr's add_batch or when request
    // is the last for the sender, because request maybe released after it's batch
    // is consumed by ExchangeNode.
    if (request.has_query_statistics()) {
        recvr->add_sub_plan_statistics(request.query_statistics(), request.sender_id());
    }

    bool eos = request.eos();
    DeferOp op([&eos, &recvr, &request]() {
        if (eos) {
            recvr->remove_sender(request.sender_id(), request.be_number());
        }
    });
    if (request.chunks_size() > 0 || request.use_pass_through()) {
        RETURN_IF_ERROR(recvr->add_chunks(request, eos ? nullptr : done));
    }

    return Status::OK();
}

Status DataStreamMgr::deregister_recvr(const TUniqueId& fragment_instance_id, PlanNodeId node_id) {
    std::shared_ptr<DataStreamRecvr> target_recvr;
    VLOG_QUERY << "deregister_recvr(): fragment_instance_id=" << fragment_instance_id << ", node=" << node_id;
    uint32_t bucket = get_bucket(fragment_instance_id);
    auto& receiver_map = _receiver_map[bucket];
    {
        std::lock_guard<Mutex> l(_lock[bucket]);
        auto iter = receiver_map.find(fragment_instance_id);
        if (iter != receiver_map.end()) {
            auto sub_iter = iter->second->find(node_id);
            if (sub_iter != iter->second->end()) {
                target_recvr = sub_iter->second;
                iter->second->erase(sub_iter);
                _receiver_count -= 1;
                if (iter->second->empty()) {
                    _receiver_map[bucket].erase(iter);
                    _fragment_count -= 1;
                }
            }
        }
    }

    // Notify concurrent add_data() requests that the stream has been terminated.
    // cancel_stream maybe take a long time, so we handle it out of lock.
    if (target_recvr) {
        target_recvr->cancel_stream();
        return Status::OK();
    } else {
        std::stringstream err;
        err << "unknown row receiver id: fragment_instance_id=" << fragment_instance_id << " node_id=" << node_id;
        LOG(ERROR) << err.str();
        return Status::InternalError(err.str());
    }
}

void DataStreamMgr::cancel(const TUniqueId& fragment_instance_id) {
    VLOG_QUERY << "cancelling all streams for fragment=" << fragment_instance_id;
    std::vector<std::shared_ptr<DataStreamRecvr>> recvrs;
    uint32_t bucket = get_bucket(fragment_instance_id);
    auto& receiver_map = _receiver_map[bucket];
    {
        std::lock_guard<Mutex> l(_lock[bucket]);
        auto iter = receiver_map.find(fragment_instance_id);
        if (iter != receiver_map.end()) {
            // all of the value should collect
            for (auto& sub_iter : *iter->second) {
                recvrs.push_back(sub_iter.second);
            }
        }
    }

    // cancel_stream maybe take a long time, so we handle it out of lock.
    for (auto& it : recvrs) {
        it->cancel_stream();
    }
}

void DataStreamMgr::prepare_pass_through_chunk_buffer(const TUniqueId& query_id) {
    _pass_through_chunk_buffer_manager.open_fragment_instance(query_id);
}

void DataStreamMgr::destroy_pass_through_chunk_buffer(const TUniqueId& query_id) {
    _pass_through_chunk_buffer_manager.close_fragment_instance(query_id);
}

PassThroughChunkBuffer* DataStreamMgr::get_pass_through_chunk_buffer(const TUniqueId& query_id) {
    return _pass_through_chunk_buffer_manager.get(query_id);
}

} // namespace starrocks
