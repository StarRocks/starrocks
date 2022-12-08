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
//   https://github.com/apache/incubator-doris/blob/master/be/src/runtime/routine_load/data_consumer_pool.cpp

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

#include "runtime/routine_load/data_consumer_pool.h"

#include "common/config.h"
#include "data_consumer.h"
#include "runtime/routine_load/data_consumer_group.h"
#include "util/thread.h"

namespace starrocks {

Status DataConsumerPool::get_consumer(StreamLoadContext* ctx, std::shared_ptr<DataConsumer>* ret) {
    std::unique_lock<std::mutex> l(_lock);

    // check if there is an available consumer.
    // if has, return it, also remove it from the pool
    auto iter = std::begin(_pool);
    while (iter != std::end(_pool)) {
        if ((*iter)->match(ctx)) {
            VLOG(3) << "get an available data consumer from pool: " << (*iter)->id();
            (*iter)->reset();
            *ret = *iter;
            iter = _pool.erase(iter);
            return Status::OK();
        } else {
            ++iter;
        }
    }

    // no available consumer, create a new one
    std::shared_ptr<DataConsumer> consumer;
    switch (ctx->load_src_type) {
    case TLoadSourceType::KAFKA:
        consumer = std::make_shared<KafkaDataConsumer>(ctx);
        break;
    case TLoadSourceType::PULSAR:
        consumer = std::make_shared<PulsarDataConsumer>(ctx);
        break;
    default:
        std::stringstream ss;
        ss << "PAUSE: unknown routine load task type: " << ctx->load_type;
        return Status::InternalError(ss.str());
    }

    // init the consumer
    RETURN_IF_ERROR(consumer->init(ctx));

    VLOG(3) << "create new data consumer: " << consumer->id();
    *ret = consumer;
    return Status::OK();
}

Status DataConsumerPool::get_consumer_grp(StreamLoadContext* ctx, std::shared_ptr<DataConsumerGroup>* ret) {
    if (ctx->load_src_type != TLoadSourceType::KAFKA && ctx->load_src_type != TLoadSourceType::PULSAR) {
        return Status::InternalError("PAUSE: Currently only support consumer group for Kafka or Palsur data source");
    }

    if (ctx->load_src_type == TLoadSourceType::KAFKA) {
        DCHECK(ctx->kafka_info);

        // one data consumer group contains at least one data consumers.
        int max_consumer_num = config::max_consumer_num_per_group;
        size_t consumer_num = std::min((size_t)max_consumer_num, ctx->kafka_info->begin_offset.size());

        std::shared_ptr<KafkaDataConsumerGroup> grp = std::make_shared<KafkaDataConsumerGroup>(consumer_num);

        for (int i = 0; i < consumer_num; ++i) {
            std::shared_ptr<DataConsumer> consumer;
            RETURN_IF_ERROR(get_consumer(ctx, &consumer));
            grp->add_consumer(consumer);
        }

        LOG(INFO) << "get consumer group " << grp->grp_id() << " with " << consumer_num << " consumers";
        *ret = grp;
        return Status::OK();
    } else {
        DCHECK(ctx->pulsar_info);
        DCHECK_GE(ctx->pulsar_info->partitions.size(), 1);

        // Cumulative acknowledge is not supported for multiple topic subscribtion,
        // so one consumer can only subscribe one topic/partition
        int max_consumer_num = config::max_pulsar_consumer_num_per_group;
        if (max_consumer_num < ctx->pulsar_info->partitions.size()) {
            return Status::InternalError(
                    "PAUSE: Partition num is more than max consumer num in one data consumer group on some BEs, please "
                    "increase max_pulsar_consumer_num_per_group from BE side or just add more BEs");
        }
        size_t consumer_num = ctx->pulsar_info->partitions.size();

        std::shared_ptr<PulsarDataConsumerGroup> grp = std::make_shared<PulsarDataConsumerGroup>(consumer_num);

        for (int i = 0; i < consumer_num; ++i) {
            std::shared_ptr<DataConsumer> consumer;
            RETURN_IF_ERROR(get_consumer(ctx, &consumer));
            grp->add_consumer(consumer);
        }

        LOG(INFO) << "get consumer group " << grp->grp_id() << " with " << consumer_num << " consumers";
        *ret = grp;
        return Status::OK();
    }
}

void DataConsumerPool::return_consumer(const std::shared_ptr<DataConsumer>& consumer) {
    std::unique_lock<std::mutex> l(_lock);

    consumer->reset();

    if (_pool.size() == _max_pool_size) {
        VLOG(3) << "data consumer pool is full: " << _pool.size() << "-" << _max_pool_size
                << ", discard the returned consumer: " << consumer->id();
        return;
    }

    _pool.push_back(consumer);
    VLOG(3) << "return the data consumer: " << consumer->id() << ", current pool size: " << _pool.size();
}

void DataConsumerPool::return_consumers(DataConsumerGroup* grp) {
    for (const std::shared_ptr<DataConsumer>& consumer : grp->consumers()) {
        return_consumer(consumer);
    }
}

Status DataConsumerPool::start_bg_worker() {
    std::shared_ptr<bool> is_closed = _is_closed;

    _clean_idle_consumer_thread = std::thread([=] {
#ifdef GOOGLE_PROFILER
        ProfilerRegisterThread();
#endif

        uint32_t interval = 60;
        while (true) {
            if (*is_closed) {
                return;
            }

            _clean_idle_consumer_bg();
            sleep(interval);
        }
    });
    Thread::set_thread_name(_clean_idle_consumer_thread, "clean_idle_cm");
    _clean_idle_consumer_thread.detach();
    return Status::OK();
}

void DataConsumerPool::_clean_idle_consumer_bg() {
    const static int32_t max_idle_time_second = 600;

    std::unique_lock<std::mutex> l(_lock);
    time_t now = time(nullptr);

    if (*_is_closed) {
        return;
    }

    auto iter = std::begin(_pool);
    while (iter != std::end(_pool)) {
        if (difftime(now, (*iter)->last_visit_time()) >= max_idle_time_second) {
            LOG(INFO) << "remove data consumer " << (*iter)->id()
                      << ", since it last visit: " << (*iter)->last_visit_time() << ", now: " << now;
            iter = _pool.erase(iter);
        } else {
            ++iter;
        }
    }
}

} // end namespace starrocks
