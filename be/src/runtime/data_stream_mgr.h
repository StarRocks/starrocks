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
//   https://github.com/apache/incubator-doris/blob/master/be/src/runtime/data_stream_mgr.h

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

#pragma once

#include <bthread/mutex.h>

#include <list>
#include <mutex>
#include <set>
#include <shared_mutex>

#include "column/vectorized_fwd.h"
#include "common/compiler_util.h"
#include "common/object_pool.h"
#include "common/status.h"
#include "gen_cpp/Types_types.h" // for TUniqueId
#include "gen_cpp/doris_internal_service.pb.h"
#include "runtime/descriptors.h" // for PlanNodeId
#include "runtime/local_pass_through_buffer.h"
#include "runtime/mem_tracker.h"
#include "runtime/query_statistics.h"
#include "util/phmap/phmap.h"
#include "util/runtime_profile.h"

namespace google::protobuf {
class Closure;
} // namespace google::protobuf

namespace starrocks {

class DescriptorTbl;
class DataStreamRecvr;
class RuntimeState;
class PRowBatch;
class PUniqueId;
class PTransmitChunkParams;

// Singleton class which manages all incoming data streams at a backend node. It
// provides both producer and consumer functionality for each data stream.
// - starrocksBackend service threads use this to add incoming data to streams
//   in response to TransmitData rpcs (add_data()) or to signal end-of-stream conditions
//   (close_sender()).
// - Exchange nodes extract data from an incoming stream via a DataStreamRecvr,
//   which is created with create_recvr().
//
// DataStreamMgr also allows asynchronous cancellation of streams via cancel()
// which unblocks all DataStreamRecvr::GetBatch() calls that are made on behalf
// of the cancelled fragment id.
//
// TODO: The recv buffers used in DataStreamRecvr should count against
// per-query memory limits.

class DataStreamMgr {
public:
    DataStreamMgr();

    // Create a receiver for a specific fragment_instance_id/node_id destination;
    // If is_merging is true, the receiver maintains a separate queue of incoming row
    // batches for each sender and merges the sorted streams from each sender into a
    // single stream.
    // Ownership of the receiver is shared between this DataStream mgr instance and the
    // caller.
    std::shared_ptr<DataStreamRecvr> create_recvr(RuntimeState* state, const RowDescriptor& row_desc,
                                                  const TUniqueId& fragment_instance_id, PlanNodeId dest_node_id,
                                                  int num_senders, int buffer_size,
                                                  const std::shared_ptr<RuntimeProfile>& profile, bool is_merging,
                                                  std::shared_ptr<QueryStatisticsRecvr> sub_plan_query_statistics_recvr,
                                                  bool is_pipeline, int32_t degree_of_parallelism, bool keep_order);

    Status transmit_data(const PTransmitDataParams* request, ::google::protobuf::Closure** done);

    Status transmit_chunk(const PTransmitChunkParams& request, ::google::protobuf::Closure** done);
    // Closes all receivers registered for fragment_instance_id immediately.
    void cancel(const TUniqueId& fragment_instance_id);
    void clear();

    void prepare_pass_through_chunk_buffer(const TUniqueId& query_id);
    void destroy_pass_through_chunk_buffer(const TUniqueId& query_id);
    PassThroughChunkBuffer* get_pass_through_chunk_buffer(const TUniqueId& query_id);

private:
    friend class DataStreamRecvr;
    static const uint32_t BUCKET_NUM = 127;

    // protects all fields below
    typedef std::shared_mutex Mutex;
    Mutex _lock[BUCKET_NUM];

    // map from hash value of fragment instance id/node id pair to stream receivers;
    // Ownership of the stream revcr is shared between this instance and the caller of
    // create_recvr().
    typedef phmap::flat_hash_map<PlanNodeId, std::shared_ptr<DataStreamRecvr>> RecvrMap;
    typedef phmap::flat_hash_map<TUniqueId, std::shared_ptr<RecvrMap>> StreamMap;
    StreamMap _receiver_map[BUCKET_NUM];
    std::atomic<uint32_t> _fragment_count{0};
    std::atomic<uint32_t> _receiver_count{0};

    // Return the receiver for given fragment_instance_id/node_id,
    // or NULL if not found.
    std::shared_ptr<DataStreamRecvr> find_recvr(const TUniqueId& fragment_instance_id, PlanNodeId node_id);

    // Remove receiver block for fragment_instance_id/node_id from the map.
    Status deregister_recvr(const TUniqueId& fragment_instance_id, PlanNodeId node_id);

    inline uint32_t get_bucket(const TUniqueId& fragment_instance_id);

    PassThroughChunkBufferManager _pass_through_chunk_buffer_manager;
};

} // namespace starrocks
