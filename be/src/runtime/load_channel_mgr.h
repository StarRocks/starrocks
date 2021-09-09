// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/be/src/runtime/load_channel_mgr.h

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

#include <ctime>
#include <memory>
#include <mutex>
#include <thread>
#include <unordered_map>

#include "common/status.h"
#include "gen_cpp/InternalService_types.h"
#include "gen_cpp/Types_types.h"
#include "gen_cpp/internal_service.pb.h"
#include "runtime/tablets_channel.h"
#include "util/uid_util.h"

namespace starrocks {

class Cache;
class LoadChannel;

// LoadChannelMgr -> LoadChannel -> TabletsChannel -> DeltaWriter
// All dispatched load data for this backend is routed from this class
class LoadChannelMgr {
public:
    LoadChannelMgr();
    ~LoadChannelMgr();

    Status init(MemTracker* mem_tracker);

    // open a new load channel if not exist
    Status open(const PTabletWriterOpenRequest& request);

    Status add_batch(const PTabletWriterAddBatchRequest& request,
                     google::protobuf::RepeatedPtrField<PTabletInfo>* tablet_vec, int64_t* wait_lock_time_ns);

    Status add_chunk(const PTabletWriterAddChunkRequest& request,
                     google::protobuf::RepeatedPtrField<PTabletInfo>* tablet_vec, int64_t* wait_lock_time_ns);

    // cancel all tablet stream for 'load_id' load
    Status cancel(const PTabletWriterCancelRequest& request);

private:
    // check if the total load mem consumption exceeds limit.
    // If yes, it will pick several load channels to try to reduce memory consumption to limit.
    void _handle_mem_exceed_limit(const std::shared_ptr<LoadChannel>& data_channel);

    // find a load channel to reduce memory consumption
    // 1. select the load channel that consume this batch data if limit exceeded.
    // 2. select the largest limit exceeded load channel.
    // 3. select the largest consumption load channel.
    bool _find_candidate_load_channel(const std::shared_ptr<LoadChannel>& data_channel,
                                      std::shared_ptr<LoadChannel>* candidate_channel);

    Status _start_bg_worker();

    // lock protect the load channel map
    std::mutex _lock;
    // load id -> load channel
    std::unordered_map<UniqueId, std::shared_ptr<LoadChannel>> _load_channels;
    Cache* _lastest_success_channel = nullptr;

    // check the total load mem consumption of this Backend
    MemTracker* _mem_tracker = nullptr;

    // thread to clean timeout load channels
    std::thread _load_channels_clean_thread;
    Status _start_load_channels_clean();
    std::atomic<bool> _is_stopped;
};

} // namespace starrocks
