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
//   https://github.com/apache/incubator-doris/blob/master/be/src/runtime/result_buffer_mgr.cpp

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

#include "runtime/result_buffer_mgr.h"

#include <memory>

#include "gen_cpp/InternalService_types.h"
#include "runtime/buffer_control_block.h"
#include "util/misc.h"
#include "util/starrocks_metrics.h"
#include "util/thread.h"

namespace starrocks {

ResultBufferMgr::ResultBufferMgr() {
    // Each BufferControlBlock has a limited queue size of 1024, it's not needed to count the
    // actual size of all BufferControlBlock.
    REGISTER_GAUGE_STARROCKS_METRIC(result_buffer_block_count, [this]() {
        std::lock_guard<std::mutex> l(_lock);
        return _buffer_map.size();
    });
}

void ResultBufferMgr::stop() {
    _is_stop = true;
    _cancel_thread->join();
}

Status ResultBufferMgr::init() {
    _cancel_thread = std::make_unique<std::thread>(
            std::bind<void>(std::mem_fn(&ResultBufferMgr::cancel_thread), this)); // NOLINT
    Thread::set_thread_name(_cancel_thread->native_handle(), "res_buf_mgr");
    return Status::OK();
}

Status ResultBufferMgr::create_sender(const TUniqueId& query_id, int buffer_size,
                                      std::shared_ptr<BufferControlBlock>* sender) {
    *sender = find_control_block(query_id);
    if (*sender != nullptr) {
        LOG(WARNING) << "already have buffer control block for this instance " << query_id;
        return Status::OK();
    }

    std::shared_ptr<BufferControlBlock> control_block(new BufferControlBlock(query_id, buffer_size));
    {
        std::lock_guard<std::mutex> l(_lock);
        _buffer_map.insert(std::make_pair(query_id, control_block));
    }
    *sender = control_block;
    return Status::OK();
}

std::shared_ptr<BufferControlBlock> ResultBufferMgr::find_control_block(const TUniqueId& query_id) {
    // TODO(zhaochun): this lock can be bottleneck?
    std::lock_guard<std::mutex> l(_lock);
    auto iter = _buffer_map.find(query_id);

    if (_buffer_map.end() != iter) {
        return iter->second;
    }

    return {};
}

Status ResultBufferMgr::fetch_data(const TUniqueId& query_id, TFetchDataResult* result) {
    std::shared_ptr<BufferControlBlock> cb = find_control_block(query_id);

    if (nullptr == cb) {
        // the sender tear down its buffer block
        return Status::InternalError("no result for this query.");
    }

    return cb->get_batch(result);
}

void ResultBufferMgr::fetch_data(const PUniqueId& finst_id, GetResultBatchCtx* ctx) {
    TUniqueId tid;
    tid.__set_hi(finst_id.hi());
    tid.__set_lo(finst_id.lo());
    std::shared_ptr<BufferControlBlock> cb = find_control_block(tid);
    if (cb == nullptr) {
        LOG(WARNING) << "no result for this query, id=" << tid;
        ctx->on_failure(Status::InternalError("no result for this query"));
        return;
    }
    cb->get_batch(ctx);
}

Status ResultBufferMgr::cancel(const TUniqueId& query_id) {
    std::lock_guard<std::mutex> l(_lock);
    auto iter = _buffer_map.find(query_id);

    if (_buffer_map.end() != iter) {
        iter->second->cancel();
        _buffer_map.erase(iter);
    }

    return Status::OK();
}

Status ResultBufferMgr::cancel_at_time(time_t cancel_time, const TUniqueId& query_id) {
    std::lock_guard<std::mutex> l(_timeout_lock);
    auto iter = _timeout_map.find(cancel_time);

    if (_timeout_map.end() == iter) {
        _timeout_map.insert(std::pair<time_t, std::vector<TUniqueId> >(cancel_time, std::vector<TUniqueId>()));
        iter = _timeout_map.find(cancel_time);
    }

    iter->second.push_back(query_id);
    return Status::OK();
}

void ResultBufferMgr::cancel_thread() {
    LOG(INFO) << "result buffer manager cancel thread begin.";

    while (!_is_stop) {
        // get query
        std::vector<TUniqueId> query_to_cancel;
        time_t now_time = time(nullptr);
        {
            std::lock_guard<std::mutex> l(_timeout_lock);
            auto end = _timeout_map.upper_bound(now_time + 1);

            for (auto iter = _timeout_map.begin(); iter != end; ++iter) {
                for (auto& i : iter->second) {
                    query_to_cancel.push_back(i);
                }
            }

            _timeout_map.erase(_timeout_map.begin(), end);
        }

        // cancel query
        for (auto& i : query_to_cancel) {
            (void)cancel(i);
        }
        nap_sleep(1, [this] { return _is_stop; });
    }

    LOG(INFO) << "result buffer manager cancel thread finish.";
}

} // namespace starrocks
