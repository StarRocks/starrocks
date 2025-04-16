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

#include <bthread/bthread.h>
#include <bthread/mutex.h>

#include <ctime>
#include <memory>
#include <mutex>
#include <thread>
#include <unordered_map>

#include "common/compiler_util.h"
#include "common/statusor.h"
#include "gen_cpp/InternalService_types.h"
#include "gen_cpp/Types_types.h"
#include "gen_cpp/internal_service.pb.h"
#include "runtime/load_channel.h"
#include "runtime/tablets_channel.h"
#include "util/blocking_queue.hpp"
#include "util/threadpool.h"
#include "util/uid_util.h"

namespace brpc {
class Controller;
}

namespace starrocks {

class Cache;

// LoadChannelMgr -> LoadChannel -> TabletsChannel -> DeltaWriter
// All dispatched load data for this backend is routed from this class
class LoadChannelMgr {
public:
    LoadChannelMgr();
    ~LoadChannelMgr();

    Status init(MemTracker* mem_tracker);

    void open(brpc::Controller* cntl, const PTabletWriterOpenRequest& request, PTabletWriterOpenResult* response,
              google::protobuf::Closure* done);

    void add_chunk(const PTabletWriterAddChunkRequest& request, PTabletWriterAddBatchResult* response);

    void add_chunks(const PTabletWriterAddChunksRequest& request, PTabletWriterAddBatchResult* response);

    void add_segment(brpc::Controller* cntl, const PTabletWriterAddSegmentRequest* request,
                     PTabletWriterAddSegmentResult* response, google::protobuf::Closure* done);

    void cancel(brpc::Controller* cntl, const PTabletWriterCancelRequest& request, PTabletWriterCancelResult* response,
                google::protobuf::Closure* done);

    void load_diagnose(brpc::Controller* cntl, const PLoadDiagnoseRequest* request, PLoadDiagnoseResult* response,
                       google::protobuf::Closure* done);

    std::shared_ptr<LoadChannel> remove_load_channel(const UniqueId& load_id);

    void abort_txn(int64_t txn_id);

    void close();

    ThreadPool* async_rpc_pool() { return _async_rpc_pool.get(); }

    std::shared_ptr<LoadChannel> TEST_get_load_channel(UniqueId load_id) {
        std::lock_guard l(_lock);
        auto it = _load_channels.find(load_id);
        return it != _load_channels.end() ? it->second : nullptr;
    }

private:
    friend class ChannelOpenTask;

    static void* load_channel_clean_bg_worker(void* arg);

    void _open(LoadChannelOpenContext open_context);
    Status _start_bg_worker();
    std::shared_ptr<LoadChannel> _find_load_channel(const UniqueId& load_id);
    std::shared_ptr<LoadChannel> _find_load_channel(int64_t txn_id);
    void _start_load_channels_clean();

    // lock protect the load channel map
    bthread::Mutex _lock;
    // load id -> load channel
    std::unordered_map<UniqueId, std::shared_ptr<LoadChannel>> _load_channels;

    // check the total load mem consumption of this Backend
    MemTracker* _mem_tracker;

    // thread to clean timeout load channels
    bthread_t _load_channels_clean_thread;

    // Thread pool used to handle rpc request asynchronously
    std::unique_ptr<ThreadPool> _async_rpc_pool;
};

} // namespace starrocks
