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

#pragma once

#include <butil/iobuf.h>

#include <atomic>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

#include "common/status.h"
#include "gen_cpp/lake_service.pb.h"

namespace starrocks {

class ExecEnv;

namespace lake {

class TabletManager;
class Segment;

// SegmentWarmupManager is responsible for warming up segments on peer CN nodes
// after compaction or memtable flush completes. It implements backpressure control
// to avoid overwhelming peer nodes by using push model with throttling.
class SegmentWarmupManager {
public:
    explicit SegmentWarmupManager(ExecEnv* env, TabletManager* tablet_mgr);
    ~SegmentWarmupManager();

    // Warm up segment blocks on peer CN nodes in the same warehouse (synchronous)
    // This method checks backpressure conditions before sending RPCs
    // Returns OK if warm up is initiated successfully or skipped due to backpressure
    // peer_nodes: list of peer node IP addresses/hostnames (provided by FE via compaction request)
    Status warm_up_segment(int64_t tablet_id, const std::string& segment_path, int64_t warehouse_id,
                           const std::vector<std::string>& peer_nodes);

    // Warm up segment blocks asynchronously (non-blocking)
    // Submits the warmup task to a thread pool and returns immediately
    // This is preferred for use in hot path to avoid blocking
    // peer_nodes: list of peer node IP addresses/hostnames (provided by FE via compaction request)
    void warm_up_segment_async(int64_t tablet_id, std::string segment_path, int64_t warehouse_id,
                               std::vector<std::string> peer_nodes);

    // Get current metrics
    int64_t pending_segment_count() const { return _pending_segment_count.load(std::memory_order_relaxed); }
    int64_t pending_memory_bytes() const { return _pending_memory_bytes.load(std::memory_order_relaxed); }
    int64_t total_warmup_requests() const { return _total_warmup_requests.load(std::memory_order_relaxed); }
    int64_t backpressure_skip_count() const { return _backpressure_skip_count.load(std::memory_order_relaxed); }

private:
    // Check if backpressure should be applied
    bool should_apply_backpressure() const;

    // Read segment blocks and send to peer nodes
    Status warmup_segment_blocks(int64_t tablet_id, const std::string& segment_path,
                                 const std::vector<std::string>& peer_nodes);

    // Send warm up RPC to a single peer node with attachment (port is automatically from config::brpc_port)
    Status send_warmup_rpc_to_peer(const std::string& host, const WarmUpSegmentRequest& request,
                                   const butil::IOBuf& attachment);

    ExecEnv* _env;
    TabletManager* _tablet_mgr;

    // Metrics for backpressure control
    std::atomic<int64_t> _pending_segment_count{0};
    std::atomic<int64_t> _pending_memory_bytes{0};

    // Statistics
    std::atomic<int64_t> _total_warmup_requests{0};
    std::atomic<int64_t> _backpressure_skip_count{0};
};

} // namespace lake
} // namespace starrocks
