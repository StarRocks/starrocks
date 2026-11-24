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

#include "data_workflows/load/routine_load/data_consumer_pool.h"

#include <algorithm>
#include <vector>

#include "common/config_ingest_fwd.h"
#include "common/thread/thread.h"
#include "common/util/misc.h"
#include "data_workflows/load/routine_load/data_consumer.h"
#include "data_workflows/load/routine_load/data_consumer_group.h"

namespace starrocks {

Status DataConsumerPool::get_consumer(StreamLoadContext* ctx, std::shared_ptr<DataConsumer>* ret) {
    std::unique_lock<std::mutex> l(_lock);

    // check if there is an available consumer.
    // if has, return it, also remove it from the pool
    auto iter = std::begin(_pool);
    while (iter != std::end(_pool)) {
        if ((*iter)->match(ctx)) {
            VLOG(3) << "get an available data consumer from pool: " << (*iter)->id();
            (void)(*iter)->reset();
            *ret = *iter;
            iter = _pool.erase(iter);
            return Status::OK();
        } else {
            ++iter;
        }
    }

    // A consumer kept apart for its commits is reused once its commits have completed and no commit uses it.
    for (auto committing = std::begin(_committing); committing != std::end(_committing); ++committing) {
        if (committing->use_count() == 1 && (*committing)->match(ctx) && !(*committing)->has_commit_in_flight()) {
            VLOG(3) << "get a data consumer whose commits have completed: " << (*committing)->id();
            (void)(*committing)->reset();
            *ret = std::move(*committing);
            _committing.erase(committing);
            return Status::OK();
        }
    }

    // no available consumer, create a new one
    std::shared_ptr<DataConsumer> consumer;
    switch (ctx->load_src_type) {
    case TLoadSourceType::KAFKA:
        consumer = std::make_shared<KafkaDataConsumer>(ctx);
        break;
    case TLoadSourceType::PULSAR:
#ifdef __APPLE__
        return Status::NotSupported("routine load pulsar is not supported on MacOS");
#else
        consumer = std::make_shared<PulsarDataConsumer>(ctx);
        break;
#endif
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

Status DataConsumerPool::get_consumer_for_commit(StreamLoadContext* ctx, std::shared_ptr<DataConsumer>* ret) {
    {
        std::unique_lock<std::mutex> l(_lock);
        // The commits of a group share a consumer kept apart for its commits: a group whose coordinator is slow keeps
        // one consumer then instead of one for each commit, and its commits reach the coordinator in order.
        for (const auto& consumer : _committing) {
            if (consumer->match(ctx)) {
                VLOG(3) << "share a data consumer kept apart for its commits: " << consumer->id();
                *ret = consumer;
                return Status::OK();
            }
        }
    }
    return get_consumer(ctx, ret);
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
#ifndef __APPLE__
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
#else
    }
    return Status::NotSupported("routine load pulsar is not supported on MacOS");
#endif
}

std::vector<std::shared_ptr<DataConsumer>> DataConsumerPool::return_consumer(
        const std::shared_ptr<DataConsumer>& consumer) {
    {
        std::unique_lock<std::mutex> l(_lock);
        // A consumer kept apart for its commits stays apart, and one whose commit is still on its way is kept apart
        // from now on: get_consumer() or the background worker pools them again once their commits have completed.
        // Asking a consumer about its commits frees the results of those that have completed. One kept apart is asked
        // too: the later commits of its group may keep using it, and get_consumer() and the background worker only
        // ask a consumer that no commit uses.
        const bool commit_in_flight = consumer->has_commit_in_flight();
        const bool kept_apart = std::find(_committing.begin(), _committing.end(), consumer) != _committing.end();
        if (kept_apart || commit_in_flight) {
            consumer->set_last_visit_time(time(nullptr));
            if (!kept_apart) {
                VLOG(3) << "keep the data consumer apart for its commit on its way: " << consumer->id();
                _committing.push_back(consumer);
            }
            return {};
        }
    }

    // The caller still owns the consumer, so reset it before taking the pool lock: a Kafka consumer drops the
    // messages fetched for its last assignment and a Pulsar consumer closes its subscription with the broker.
    (void)consumer->reset();

    // Transfer the evicted references to the caller, which decides when it is safe to wait for broker close.
    std::vector<std::shared_ptr<DataConsumer>> evicted;
    {
        std::unique_lock<std::mutex> l(_lock);

        consumer->set_last_visit_time(time(nullptr));

        // Evicting in a loop also shrinks the pool when its size is lowered at runtime.
        const auto max_pool_size =
                static_cast<size_t>(std::max<int32_t>(config::routine_load_kafka_consumer_pool_size, 0));
        while (!_pool.empty() && _pool.size() >= max_pool_size) {
            auto idlest = std::min_element(_pool.begin(), _pool.end(), [](const auto& lhs, const auto& rhs) {
                return lhs->last_visit_time() < rhs->last_visit_time();
            });
            VLOG(3) << "data consumer pool is full: " << _pool.size() << "-" << max_pool_size
                    << ", evict the data consumer idle for the longest: " << (*idlest)->id();
            evicted.emplace_back(std::move(*idlest));
            _pool.erase(idlest);
        }
        if (max_pool_size == 0) {
            VLOG(3) << "data consumer pool is disabled, discard the returned consumer: " << consumer->id();
            evicted.push_back(consumer);
        } else {
            _pool.push_back(consumer);
            VLOG(3) << "return the data consumer: " << consumer->id() << ", current pool size: " << _pool.size();
        }
    }
    return evicted;
}

std::vector<std::shared_ptr<DataConsumer>> DataConsumerPool::return_consumers(DataConsumerGroup* grp) {
    std::vector<std::shared_ptr<DataConsumer>> evicted;
    for (const std::shared_ptr<DataConsumer>& consumer : grp->consumers()) {
        for (auto& old_consumer : return_consumer(consumer)) {
            evicted.push_back(std::move(old_consumer));
        }
    }
    return evicted;
}

void DataConsumerPool::stop() {
    {
        std::unique_lock<std::mutex> l(_lock);
        _is_closed = true;
    }

    // Join without holding the lock: the worker takes it to clean the idle consumers.
    if (_clean_idle_consumer_thread.joinable()) {
        _clean_idle_consumer_thread.join();
    }
}

void DataConsumerPool::start_bg_worker() {
    _clean_idle_consumer_thread = std::thread([this] {
#ifdef GOOGLE_PROFILER
        ProfilerRegisterThread();
#endif

        uint32_t interval = 60;
        while (!_is_closed) {
            nap_sleep(interval, [this] { return _is_closed.load(); });
            _clean_idle_consumer_bg();
        }
    });
    Thread::set_thread_name(_clean_idle_consumer_thread, "clean_idle_cm");
}

void DataConsumerPool::_clean_idle_consumer_bg() {
    const static int32_t max_idle_time_second = 600;

    // Closing a consumer may talk to the brokers, so the idle ones are destroyed after the lock is released.
    std::vector<std::shared_ptr<DataConsumer>> idle_consumers;
    {
        std::unique_lock<std::mutex> l(_lock);
        time_t now = time(nullptr);

        if (_is_closed) {
            return;
        }

        auto iter = std::begin(_pool);
        while (iter != std::end(_pool)) {
            if (difftime(now, (*iter)->last_visit_time()) >= max_idle_time_second) {
                LOG(INFO) << "remove data consumer " << (*iter)->id()
                          << ", since it last visit: " << (*iter)->last_visit_time() << ", now: " << now;
                idle_consumers.emplace_back(std::move(*iter));
                iter = _pool.erase(iter);
            } else {
                ++iter;
            }
        }

        // A consumer kept apart for its commits and not in use goes back to the pool once its commits have completed,
        // or is closed when the pool has no room for it. Like a pooled one, it is closed once idle for too long,
        // whether its commits have completed or not.
        const auto max_pool_size =
                static_cast<size_t>(std::max<int32_t>(config::routine_load_kafka_consumer_pool_size, 0));
        auto committing = std::begin(_committing);
        while (committing != std::end(_committing)) {
            if (committing->use_count() > 1) {
                ++committing;
            } else if (difftime(now, (*committing)->last_visit_time()) >= max_idle_time_second) {
                LOG(INFO) << "remove data consumer " << (*committing)->id() << " kept apart for its commits"
                          << ", since it last visit: " << (*committing)->last_visit_time() << ", now: " << now;
                idle_consumers.emplace_back(std::move(*committing));
                committing = _committing.erase(committing);
            } else if (!(*committing)->has_commit_in_flight()) {
                if (_pool.size() < max_pool_size) {
                    _pool.push_back(std::move(*committing));
                } else {
                    idle_consumers.emplace_back(std::move(*committing));
                }
                committing = _committing.erase(committing);
            } else {
                ++committing;
            }
        }
    }
}

} // end namespace starrocks
