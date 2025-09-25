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

#include "storage/lake/segment_warmup_manager.h"

#include <brpc/channel.h>
#include <brpc/controller.h>

#include "agent/agent_server.h"
#include "cache/block_cache/block_cache.h"
#include "common/config.h"
#include "common/logging.h"
#include "fs/fs.h"
#include "gen_cpp/lake_service.pb.h"
#include "gutil/strings/substitute.h"
#include "runtime/exec_env.h"
#include "service/brpc.h"
#include "storage/lake/filenames.h"
#include "storage/lake/tablet_manager.h"
#include "util/brpc_stub_cache.h"
#include "util/defer_op.h"
#include "util/threadpool.h"

namespace starrocks::lake {

SegmentWarmupManager::SegmentWarmupManager(ExecEnv* env, TabletManager* tablet_mgr)
        : _env(env), _tablet_mgr(tablet_mgr) {}

SegmentWarmupManager::~SegmentWarmupManager() = default;

bool SegmentWarmupManager::should_apply_backpressure() const {
    // Check if pending segments exceed threshold
    if (_pending_segment_count.load(std::memory_order_relaxed) >= config::lake_segment_warmup_max_pending_segments) {
        return true;
    }

    // Check if pending memory exceeds threshold
    if (_pending_memory_bytes.load(std::memory_order_relaxed) >= config::lake_segment_warmup_max_pending_memory_mb * 1024 * 1024) {
        return true;
    }

    return false;
}

Status SegmentWarmupManager::warm_up_segment(int64_t tablet_id, const std::string& segment_path,
                                              int64_t warehouse_id, const std::vector<std::string>& peer_nodes) {
    // Check if warmup is enabled
    if (!config::lake_enable_segment_warmup) {
        return Status::OK();
    }

    // Check backpressure
    if (should_apply_backpressure()) {
        _backpressure_skip_count.fetch_add(1, std::memory_order_relaxed);
        LOG(INFO) << "Skip segment warmup due to backpressure. tablet_id=" << tablet_id
                << " segment_path=" << segment_path << " pending_segments=" << _pending_segment_count.load()
                << " pending_memory_mb=" << (_pending_memory_bytes.load() / 1024 / 1024);
        return Status::OK();
    }

    _total_warmup_requests.fetch_add(1, std::memory_order_relaxed);

    // Check if peer nodes are provided from FE
    if (peer_nodes.empty()) {
        LOG(INFO) << "No peer nodes provided for warmup. tablet_id=" << tablet_id
                << " warehouse_id=" << warehouse_id;
        return Status::OK();
    }

    // Increment pending segment count
    _pending_segment_count.fetch_add(1, std::memory_order_relaxed);
    DeferOp defer([this]() { _pending_segment_count.fetch_sub(1, std::memory_order_relaxed); });

    // Warmup segment blocks
    Status st = warmup_segment_blocks(tablet_id, segment_path, peer_nodes);
    if (!st.ok()) {
        LOG(WARNING) << "Failed to warmup segment blocks. tablet_id=" << tablet_id << " segment_path=" << segment_path
                     << " error=" << st;
    }

    return st;
}

void SegmentWarmupManager::warm_up_segment_async(int64_t tablet_id, std::string segment_path,
                                                  int64_t warehouse_id, std::vector<std::string> peer_nodes) {
    // Check if warmup is enabled
    if (!config::lake_enable_segment_warmup) {
        return;
    }

    // Check backpressure - skip if over threshold
    if (should_apply_backpressure()) {
        _backpressure_skip_count.fetch_add(1, std::memory_order_relaxed);
        LOG(INFO) << "Skip async segment warmup due to backpressure. tablet_id=" << tablet_id
                << " segment_path=" << segment_path;
        return;
    }

    _total_warmup_requests.fetch_add(1, std::memory_order_relaxed);

    // Submit to thread pool for async execution
    // Use ExecEnv's lake_memtable_flush_executor or other suitable thread pool
    auto* env = _env;
    auto* tablet_mgr = _tablet_mgr;
    
    // Capture by value to ensure lifetime
    auto task = [env, tablet_mgr, tablet_id, segment_path = std::move(segment_path), 
                  warehouse_id, peer_nodes = std::move(peer_nodes), this]() {
        // Check if peer nodes are provided
        if (peer_nodes.empty()) {
            LOG(INFO) << "No peer nodes provided for async warmup. tablet_id=" << tablet_id
                    << " warehouse_id=" << warehouse_id;
            return;
        }

        Status st = warmup_segment_blocks(tablet_id, segment_path, peer_nodes);
        if (!st.ok()) {
            LOG(WARNING) << "Async warmup failed. tablet_id=" << tablet_id 
                         << " segment_path=" << segment_path << " error=" << st;
        }
    };

    // Use a thread pool to execute the warmup task
    // Try to get a suitable thread pool from ExecEnv
    ThreadPool* pool = nullptr;
    if (_env && _env->agent_server()) {
        // Use agent server's thread pool for background tasks
        pool = _env->agent_server()->get_thread_pool(TTaskType::MAKE_SNAPSHOT);
    }
    
    if (pool) {
        Status st = pool->submit_func(std::move(task));
        if (!st.ok()) {
            LOG(WARNING) << "Failed to submit async warmup task. tablet_id=" << tablet_id
                         << " segment_path=" << segment_path << " error=" << st;
        }
    } else {
        // Fallback: execute in current thread (not ideal but better than nothing)
        LOG(INFO) << "No thread pool available for async warmup, executing synchronously";
        task();
    }
}

Status SegmentWarmupManager::warmup_segment_blocks(int64_t tablet_id, const std::string& segment_path,
                                                    const std::vector<std::string>& peer_nodes) {
    // Get file system first
    ASSIGN_OR_RETURN(auto fs, FileSystem::CreateSharedFromString(segment_path));

    // Get block size from BlockCache
    auto block_cache = CacheEnv::GetInstance()->block_cache();
    if (block_cache == nullptr || !block_cache->available()) {
        LOG(WARNING) << "BlockCache is not available for warmup. tablet_id=" << tablet_id 
                     << " segment_path=" << segment_path;
        return Status::NotSupported("BlockCache is not available");
    }
    size_t block_size = block_cache->block_size();
    if (block_size == 0) {
        return Status::InternalError("Invalid block size");
    }

    // Open file with bundling interface (similar to segment.cpp)
    FileInfo file_info{.path = segment_path};
    RandomAccessFileOptions opts{.skip_fill_local_cache = false, .buffer_size = -1};
    
    ASSIGN_OR_RETURN(auto file, fs->new_random_access_file(opts, file_info));
    
    // Get file size from the RandomAccessFile object
	// todo reducet headobject
    ASSIGN_OR_RETURN(auto file_size, file->get_size());

    // Stream-based sending: read a batch → send → read next batch
    size_t offset = 0;
    int batch_num = 0;
    int64_t total_blocks = 0;
    int64_t total_bytes_sent = 0;
    Status final_status = Status::OK();

    std::vector<char> buffer(block_size);

    while (offset < file_size) {
        // Build one batch (up to max_blocks_per_request blocks)
        WarmUpSegmentRequest request;
        request.set_tablet_id(tablet_id);
        request.set_segment_path(segment_path);

        butil::IOBuf attachment;
        int64_t batch_bytes = 0;

        // Read blocks for this batch
        while (offset < file_size && request.blocks_size() < config::lake_segment_warmup_max_blocks_per_request) {
            size_t read_size = std::min(block_size, file_size - offset);

            // Read block from file
            Status st = file->read_at_fully(offset, buffer.data(), read_size);
            if (!st.ok()) {
                LOG(WARNING) << "Failed to read block at offset " << offset << " for segment " << segment_path
                             << " error=" << st;
                final_status.update(st);
                // Skip this block and continue with next
                offset += read_size;
                continue;
            }

            // Add block metadata to request (no data!)
            auto* block_data = request.add_blocks();
            block_data->set_cache_key(segment_path);
            block_data->set_offset(offset);
            block_data->set_size(read_size);

            // Append block data to attachment (zero-copy)
            attachment.append(buffer.data(), read_size);

            batch_bytes += read_size;
            offset += read_size;
        }

        if (request.blocks_size() == 0) {
            // No blocks in this batch (all failed to read), skip
            continue;
        }

        // Update pending memory for this batch
        _pending_memory_bytes.fetch_add(batch_bytes, std::memory_order_relaxed);
        DeferOp defer([this, batch_bytes]() { _pending_memory_bytes.fetch_sub(batch_bytes, std::memory_order_relaxed); });

        // Send this batch to all peer nodes
        LOG(INFO) << "Sending warmup batch. tablet_id=" << tablet_id << " segment_path=" << segment_path
                << " batch_num=" << batch_num << " block_count=" << request.blocks_size() 
                << " batch_bytes=" << batch_bytes << " progress=" << offset << "/" << file_size
                << " target_peer_nodes=" << peer_nodes.size();

        for (const auto& host : peer_nodes) {
            LOG(INFO) << "Warmup batch " << batch_num << " to peer_node=" << host 
                     << ":" << config::brpc_port
                     << ", tablet_id=" << tablet_id
                     << ", blocks=" << request.blocks_size()
                     << ", bytes=" << batch_bytes;
            
            Status st = send_warmup_rpc_to_peer(host, request, attachment);
            if (!st.ok()) {
                LOG(WARNING) << "Failed to send warmup RPC batch " << batch_num << " to peer_node=" << host 
                             << ":" << config::brpc_port 
                             << ", tablet_id=" << tablet_id
                             << ", error=" << st;
                final_status.update(st);
            } else {
                LOG(INFO) << "Successfully sent warmup batch " << batch_num << " to peer_node=" << host
                         << ":" << config::brpc_port
                         << ", tablet_id=" << tablet_id;
            }
        }

        total_blocks += request.blocks_size();
        total_bytes_sent += batch_bytes;
        batch_num++;
    }

    LOG(INFO) << "Completed warming up segment. tablet_id=" << tablet_id 
              << " segment_path=" << segment_path
              << " total_blocks=" << total_blocks 
              << " total_bytes=" << total_bytes_sent 
              << " batches=" << batch_num 
              << " target_peer_nodes=" << peer_nodes.size();

    return final_status;
}


Status SegmentWarmupManager::send_warmup_rpc_to_peer(const std::string& host,
                                                       const WarmUpSegmentRequest& request,
                                                       const butil::IOBuf& attachment) {
    // Create brpc channel (port is automatically from config::brpc_port)
    brpc::ChannelOptions options;
    options.timeout_ms = config::lake_segment_warmup_rpc_timeout_ms;
    options.max_retry = 3;
    
    brpc::Channel channel;
    std::string server_addr = strings::Substitute("$0:$1", host, config::brpc_port);
    if (channel.Init(server_addr.c_str(), &options) != 0) {
        return Status::InternalError(strings::Substitute("Failed to init channel to $0", server_addr));
    }

    // Create LakeService stub
    LakeService_Stub stub(&channel);

    // Send RPC with attachment for zero-copy data transmission
    brpc::Controller cntl;
    WarmUpSegmentResponse response;
    
    // Append attachment (block data) - this is zero-copy
    cntl.request_attachment().append(attachment);

    LOG(INFO) << "Sending warmup RPC to peer_node=" << host << ":" << config::brpc_port
            << " tablet_id=" << request.tablet_id()
            << " blocks=" << request.blocks_size() 
            << " attachment_size=" << cntl.request_attachment().size();

    stub.warm_up_segment(&cntl, &request, &response, nullptr);

    if (cntl.Failed()) {
        return Status::InternalError(
                strings::Substitute("Failed to send warmup RPC to $0:$1: $2", host, config::brpc_port, cntl.ErrorText()));
    }

    if (response.status().status_code() != 0) {
        return Status::InternalError(strings::Substitute("Warmup RPC failed on peer $0:$1: $2", host, config::brpc_port,
                                                          response.status().DebugString()));
    }

    LOG(INFO) << "Successfully sent warmup RPC to peer_node=" << host << ":" << config::brpc_port
            << " tablet_id=" << request.tablet_id()
            << " cached_blocks=" << response.cached_block_count();

    return Status::OK();
}

} // namespace starrocks::lake

