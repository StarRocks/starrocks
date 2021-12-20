// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/be/src/runtime/load_channel.cpp

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

#include "runtime/load_channel.h"

#include "runtime/mem_tracker.h"
#include "runtime/tablets_channel.h"
#include "storage/lru_cache.h"

namespace starrocks {

LoadChannel::LoadChannel(const UniqueId& load_id, int64_t timeout_s, std::unique_ptr<MemTracker> mem_tracker)
        : _load_id(load_id),
          _timeout_s(timeout_s),
          _mem_tracker(std::move(mem_tracker)),
          _last_updated_time(time(nullptr)) {}

LoadChannel::~LoadChannel() {
    LOG(INFO) << "load channel mem peak usage=" << _mem_tracker->peak_consumption()
              << ", info=" << _mem_tracker->debug_string() << ", load_id=" << _load_id;
    _tablets_channels.clear();
}

Status LoadChannel::open(const PTabletWriterOpenRequest& params) {
    int64_t index_id = params.index_id();
    std::shared_ptr<TabletsChannel> channel;
    {
        std::lock_guard<std::mutex> l(_lock);
        auto it = _tablets_channels.find(index_id);
        if (it != _tablets_channels.end()) {
            channel = it->second;
        } else {
            // create a new tablets channel
            TabletsChannelKey key(params.id(), index_id);
            channel.reset(new TabletsChannel(key, _mem_tracker.get()));
            _tablets_channels.insert({index_id, channel});
        }
    }

    RETURN_IF_ERROR(channel->open(params));

    _opened = true;
    _last_updated_time.store(time(nullptr));
    return Status::OK();
}

Status LoadChannel::add_chunk(const PTabletWriterAddChunkRequest& request,
                              google::protobuf::RepeatedPtrField<PTabletInfo>* tablet_vec) {
    int64_t index_id = request.index_id();
    // 1. get tablets channel
    std::shared_ptr<TabletsChannel> channel;
    {
        std::lock_guard<std::mutex> l(_lock);
        auto it = _tablets_channels.find(index_id);
        if (it == _tablets_channels.end()) {
            if (_finished_channel_ids.find(index_id) != _finished_channel_ids.end()) {
                // this channel is already finished, just return OK
                return Status::OK();
            }
            std::stringstream ss;
            ss << "load channel " << _load_id << " add batch with unknown index id: " << index_id;
            return Status::InternalError(ss.str());
        }
        channel = it->second;
    }

    // 3. add batch to tablets channel
    if (request.has_chunk()) {
        RETURN_IF_ERROR(channel->add_chunk(request));
    }

    // 4. handle eos
    Status st;
    if (request.has_eos() && request.eos()) {
        bool finished = false;
        RETURN_IF_ERROR(channel->close(request.sender_id(), &finished, request.partition_ids(), tablet_vec));
        if (finished) {
            std::lock_guard<std::mutex> l(_lock);
            _tablets_channels.erase(index_id);
            _finished_channel_ids.emplace(index_id);
        }
    }
    _last_updated_time.store(time(nullptr));
    return st;
}

bool LoadChannel::is_finished() {
    if (!_opened) {
        return false;
    }
    std::lock_guard<std::mutex> l(_lock);
    return _tablets_channels.empty();
}

Status LoadChannel::cancel() {
    std::lock_guard<std::mutex> l(_lock);
    for (auto& it : _tablets_channels) {
        it.second->cancel();
    }
    return Status::OK();
}

} // namespace starrocks
