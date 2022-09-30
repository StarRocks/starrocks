// This file is made available under Elastic License 2.0.
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

<<<<<<< HEAD
#include "gen_cpp/InternalService_types.h"
#include "gen_cpp/types.pb.h" // PUniqueId
=======
#include "runtime/current_thread.h"
>>>>>>> 0c81be87a ([Enhancement] dump fragment instance id when be crash in DataStreamRecvr (#11866))
#include "runtime/data_stream_recvr.h"
#include "runtime/raw_value.h"
#include "runtime/runtime_state.h"
#include "util/starrocks_metrics.h"

namespace starrocks {

DataStreamMgr::DataStreamMgr() {
    REGISTER_GAUGE_STARROCKS_METRIC(data_stream_receiver_count, [this]() {
        std::lock_guard<std::mutex> l(_lock);
        return _receiver_map.size();
    });
    REGISTER_GAUGE_STARROCKS_METRIC(fragment_endpoint_count, [this]() {
        std::lock_guard<std::mutex> l(_lock);
        return _fragment_stream_set.size();
    });
}

inline uint32_t DataStreamMgr::get_hash_value(const TUniqueId& fragment_instance_id, PlanNodeId node_id) {
    uint32_t value = RawValue::get_hash_value(&fragment_instance_id.lo, TYPE_BIGINT, 0);
    value = RawValue::get_hash_value(&fragment_instance_id.hi, TYPE_BIGINT, value);
    value = RawValue::get_hash_value(&node_id, TYPE_INT, value);
    return value;
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
    uint32_t hash_value = get_hash_value(fragment_instance_id, dest_node_id);
    std::lock_guard<std::mutex> l(_lock);
    _fragment_stream_set.emplace(fragment_instance_id, dest_node_id);
    _receiver_map.insert(std::make_pair(hash_value, recvr));
    return recvr;
}

std::shared_ptr<DataStreamRecvr> DataStreamMgr::find_recvr(const TUniqueId& fragment_instance_id, PlanNodeId node_id,
                                                           bool acquire_lock) {
    VLOG_ROW << "looking up fragment_instance_id=" << fragment_instance_id << ", node=" << node_id;
    size_t hash_value = get_hash_value(fragment_instance_id, node_id);
    if (acquire_lock) {
        _lock.lock();
    }
    std::pair<StreamMap::iterator, StreamMap::iterator> range = _receiver_map.equal_range(hash_value);
    while (range.first != range.second) {
        std::shared_ptr<DataStreamRecvr> recvr = range.first->second;
        if (recvr->fragment_instance_id() == fragment_instance_id && recvr->dest_node_id() == node_id) {
            if (acquire_lock) {
                _lock.unlock();
            }
            return recvr;
        }
        ++range.first;
    }
    if (acquire_lock) {
        _lock.unlock();
    }
    return std::shared_ptr<DataStreamRecvr>();
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
    if (request.chunks_size() > 0 || request.use_pass_through()) {
        RETURN_IF_ERROR(recvr->add_chunks(request, eos ? nullptr : done));
    }
    if (eos) {
        recvr->remove_sender(request.sender_id(), request.be_number());
    }
    return Status::OK();
}

Status DataStreamMgr::deregister_recvr(const TUniqueId& fragment_instance_id, PlanNodeId node_id) {
    std::shared_ptr<DataStreamRecvr> target_recvr;
    VLOG_QUERY << "deregister_recvr(): fragment_instance_id=" << fragment_instance_id << ", node=" << node_id;
    size_t hash_value = get_hash_value(fragment_instance_id, node_id);
    {
        std::lock_guard<std::mutex> l(_lock);
        std::pair<StreamMap::iterator, StreamMap::iterator> range = _receiver_map.equal_range(hash_value);
        while (range.first != range.second) {
            const std::shared_ptr<DataStreamRecvr>& recvr = range.first->second;
            if (recvr->fragment_instance_id() == fragment_instance_id && recvr->dest_node_id() == node_id) {
                target_recvr = recvr;
                _fragment_stream_set.erase(std::make_pair(recvr->fragment_instance_id(), recvr->dest_node_id()));
                _receiver_map.erase(range.first);
                break;
            }
            ++range.first;
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
    {
        std::lock_guard<std::mutex> l(_lock);
        FragmentStreamSet::iterator i = _fragment_stream_set.lower_bound(std::make_pair(fragment_instance_id, 0));
        while (i != _fragment_stream_set.end() && i->first == fragment_instance_id) {
            std::shared_ptr<DataStreamRecvr> recvr = find_recvr(i->first, i->second, false);
            if (recvr == nullptr) {
                // keep going but at least log it
                std::stringstream err;
                err << "cancel(): missing in stream_map: fragment=" << i->first << " node=" << i->second;
                LOG(ERROR) << err.str();
            } else {
                recvrs.push_back(recvr);
            }
            ++i;
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
