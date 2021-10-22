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

LoadChannel::LoadChannel(const UniqueId& load_id, int64_t mem_limit, int64_t timeout_s, MemTracker* mem_tracker)
        : _load_id(load_id), _timeout_s(timeout_s) {
    _mem_tracker = std::make_unique<MemTracker>(mem_limit, _load_id.to_string(), mem_tracker, true);
    // _last_updated_time should be set before being inserted to
    // _load_channels in load_channel_mgr, or it may be erased
    // immediately by gc thread.
    _last_updated_time.store(time(nullptr));
}

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

Status LoadChannel::add_batch(const PTabletWriterAddBatchRequest& request,
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

    // 2. check if mem consumption exceed limit
    _handle_mem_exceed_limit();

    // 3. add batch to tablets channel
    if (request.has_row_batch()) {
        RETURN_IF_ERROR(channel->add_batch(request));
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

    // 2. check if mem consumption exceed limit
    _handle_mem_exceed_limit();

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

void LoadChannel::_handle_mem_exceed_limit() {
    // lock so that only one thread can check mem limit
    std::lock_guard<std::mutex> l(_lock);

    if (!_mem_tracker->limit_exceeded()) {
        return;
    }
    LOG(INFO) << "Reducing memory of " << *this << " because its mem consumption=" << _mem_tracker->consumption()
              << " has exceeded limit=" << _mem_tracker->limit();

    int64_t exceeded_mem = _mem_tracker->consumption() - _mem_tracker->limit();
    std::vector<FlushTablet> flush_tablets;
    std::set<int64_t> flush_tablet_ids;
    int64_t tablet_mem_consumption;
    do {
        std::shared_ptr<TabletsChannel> tablets_channel;
        int64_t tablet_id = -1;
        _reduce_mem_usage_async_internal(flush_tablet_ids, &tablets_channel, &tablet_id, &tablet_mem_consumption);
        if (tablet_id != -1) {
            flush_tablets.emplace_back(this, tablets_channel.get(), tablet_id);
            flush_tablet_ids.insert(tablet_id);
            exceeded_mem -= tablet_mem_consumption;
            VLOG(3) << "Flush " << *this << ", tablet id=" << tablet_id
                    << ", mem consumption=" << tablet_mem_consumption;
        } else {
            break;
        }
    } while (exceeded_mem > 0);

    // wait flush finish
    for (const FlushTablet& flush_tablet : flush_tablets) {
        Status st = flush_tablet.tablets_channel->wait_mem_usage_reduced(flush_tablet.tablet_id);
        if (!st.ok()) {
            // wait may return failed, but no need to handle it here, just log.
            // tablet_vec will only contains success tablet, and then let FE judge it.
            LOG(WARNING) << "Fail to wait memory reduced. err=" << st.to_string();
        }
    }
    LOG(INFO) << "Reduce memory finish. " << *this << ", flush tablets num=" << flush_tablet_ids.size()
              << ", current mem consumption=" << _mem_tracker->consumption() << ", limit=" << _mem_tracker->limit();
}

void LoadChannel::reduce_mem_usage_async(const std::set<int64_t>& flush_tablet_ids,
                                         std::shared_ptr<TabletsChannel>* tablets_channel, int64_t* tablet_id,
                                         int64_t* tablet_mem_consumption) {
    // lock so that only one thread can check mem limit
    std::lock_guard<std::mutex> l(_lock);
    _reduce_mem_usage_async_internal(flush_tablet_ids, tablets_channel, tablet_id, tablet_mem_consumption);
}

void LoadChannel::_reduce_mem_usage_async_internal(const std::set<int64_t>& flush_tablet_ids,
                                                   std::shared_ptr<TabletsChannel>* tablets_channel, int64_t* tablet_id,
                                                   int64_t* tablet_mem_consumption) {
    if (_find_largest_consumption_channel(tablets_channel)) {
        Status st = (*tablets_channel)->reduce_mem_usage_async(flush_tablet_ids, tablet_id, tablet_mem_consumption);
        if (!st.ok()) {
            LOG(WARNING) << "Fail to reduce memory async. error=" << st.to_string();
        }
    } else {
        // should not happen, add log to observe
        LOG(WARNING) << "Fail to find suitable tablets channel when memory exceed. "
                     << "load_id=" << _load_id;
    }
}

// lock should be held when calling this method
bool LoadChannel::_find_largest_consumption_channel(std::shared_ptr<TabletsChannel>* channel) {
    int64_t max_consume = 0;
    for (auto& it : _tablets_channels) {
        if (it.second->mem_consumption() > max_consume) {
            max_consume = it.second->mem_consumption();
            *channel = it.second;
        }
    }
    return max_consume > 0;
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
