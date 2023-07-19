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
//   https://github.com/apache/incubator-doris/blob/master/be/src/runtime/load_channel.h

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

#include <memory>
#include <ostream>
#include <unordered_map>
#include <unordered_set>

#include "column/chunk.h"
#include "common/compiler_util.h"
#include "common/status.h"
#include "common/tracer.h"
#include "gen_cpp/InternalService_types.h"
#include "gen_cpp/Types_types.h"
#include "gen_cpp/internal_service.pb.h"
#include "gutil/macros.h"
#include "runtime/mem_tracker.h"
#include "serde/protobuf_serde.h"
#include "util/uid_util.h"

namespace brpc {
class Controller;
}

namespace starrocks {

class Cache;
class TabletsChannel;
class LoadChannel;
class LoadChannelMgr;
class OlapTableSchemaParam;
class TabletsChannelOpenTimeStat;

namespace lake {
class TabletManager;
}

class LoadChannelOpenTimeStat {
public:
    LoadChannelOpenTimeStat();

    int64_t get_start_time() { return _start_time_ns; }

    void set_start_time(int64_t start_time_ns) { _start_time_ns = start_time_ns; }

    int64_t get_lock_time() { return _lock_time_ns; }

    void set_lock_time(int64_t lock_time_ns) { _lock_time_ns = lock_time_ns; }

    TabletsChannelOpenTimeStat* getTabletsChannelStat() { return _tablets_channel_stat.get(); }

    int64_t get_end_time() { return _end_time_ns; }

    void set_end_time(int64_t end_time_ns) { _end_time_ns = end_time_ns; }

    int64_t get_total_time() { return _end_time_ns - _start_time_ns; }

    std::string to_string();

private:
    int64_t _start_time_ns;
    int64_t _lock_time_ns;
    // TODO not support LakeTabletsChannel currently
    std::shared_ptr<TabletsChannelOpenTimeStat> _tablets_channel_stat;
    int64_t _end_time_ns;
};

// A LoadChannel manages tablets channels for all indexes
// corresponding to a certain load job
class LoadChannel {
    using LakeTabletManager = lake::TabletManager;

public:
    LoadChannel(LoadChannelMgr* mgr, LakeTabletManager* lake_tablet_mgr, const UniqueId& load_id,
                const std::string& txn_trace_parent, int64_t timeout_s, std::unique_ptr<MemTracker> mem_tracker);

    ~LoadChannel();

    DISALLOW_COPY_AND_MOVE(LoadChannel);

    // Open a new load channel if it does not exist.
    // NOTE: This method may be called multiple times, and each time with a different |request|.
    void open(brpc::Controller* cntl, const PTabletWriterOpenRequest& request, PTabletWriterOpenResult* response,
              google::protobuf::Closure* done, LoadChannelOpenTimeStat* open_time_stat);

    void add_chunk(const PTabletWriterAddChunkRequest& request, PTabletWriterAddBatchResult* response);

    void add_chunks(const PTabletWriterAddChunksRequest& request, PTabletWriterAddBatchResult* response);

    void add_segment(brpc::Controller* cntl, const PTabletWriterAddSegmentRequest* request,
                     PTabletWriterAddSegmentResult* response, google::protobuf::Closure* done);

    void cancel();

    void abort();

    void abort(int64_t index_id, const std::vector<int64_t>& tablet_ids);

    time_t last_updated_time() const { return _last_updated_time.load(std::memory_order_relaxed); }

    const UniqueId& load_id() const { return _load_id; }

    int64_t timeout() const { return _timeout_s; }

    std::shared_ptr<TabletsChannel> get_tablets_channel(int64_t index_id);

    void remove_tablets_channel(int64_t index_id);

    MemTracker* mem_tracker() { return _mem_tracker.get(); }

    Span get_span() { return _span; }

private:
    void _add_chunk(Chunk* chunk, const PTabletWriterAddChunkRequest& request, PTabletWriterAddBatchResult* response);
    Status _build_chunk_meta(const ChunkPB& pb_chunk);
    Status _deserialize_chunk(const ChunkPB& pchunk, Chunk& chunk, faststring* uncompressed_buffer);

    LoadChannelMgr* _load_mgr;
    LakeTabletManager* _lake_tablet_mgr;
    UniqueId _load_id;
    int64_t _timeout_s;
    std::atomic<bool> _has_chunk_meta;
    mutable bthread::Mutex _chunk_meta_lock;
    serde::ProtobufChunkMeta _chunk_meta;
    std::shared_ptr<OlapTableSchemaParam> _schema;
    std::unique_ptr<RowDescriptor> _row_desc;

    std::unique_ptr<MemTracker> _mem_tracker;
    std::atomic<time_t> _last_updated_time;

    // lock protect the tablets channel map
    bthread::Mutex _lock;
    // index id -> tablets channel
    std::unordered_map<int64_t, std::shared_ptr<TabletsChannel>> _tablets_channels;

    Span _span;
    size_t _num_chunk{0};
    size_t _num_segment = 0;
};

inline std::ostream& operator<<(std::ostream& os, const LoadChannel& load_channel) {
    os << "LoadChannel(id=" << load_channel.load_id()
       << ", last_update_time=" << static_cast<uint64_t>(load_channel.last_updated_time()) << ")";
    return os;
}

} // namespace starrocks
