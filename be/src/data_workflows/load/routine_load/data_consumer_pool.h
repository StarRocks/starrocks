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
//   https://github.com/apache/incubator-doris/blob/master/be/src/runtime/routine_load/data_consumer_pool.h

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

#include <atomic>
#include <ctime>
#include <list>
#include <memory>
#include <mutex>
#include <thread>
#include <vector>

#include "data_workflows/load/routine_load/data_consumer.h"

namespace starrocks {

class DataConsumer;
class DataConsumerGroup;
class Status;

// DataConsumerPool saves all available data consumer
// to be reused. Its capacity is config::routine_load_kafka_consumer_pool_size.
//
// A consumer whose offset commit is still on its way would fetch nothing for a new assignment until the commit
// completes, and closing it would wait for the commit. The pool keeps such a consumer apart, outside its capacity,
// for the commits of its group only, and pools it again once its commits have completed.
class DataConsumerPool {
public:
    DataConsumerPool() = default;

    ~DataConsumerPool() { stop(); }

    void stop();

    // get a already initialized consumer from cache,
    // if not found in cache, create a new one.
    Status get_consumer(StreamLoadContext* ctx, std::shared_ptr<DataConsumer>* ret);

    // Gets a consumer to commit offsets with: one kept apart for its commits still on its way first, which the
    // commits of its group then share, otherwise like get_consumer(). The caller returns it with return_consumer().
    Status get_consumer_for_commit(StreamLoadContext* ctx, std::shared_ptr<DataConsumer>* ret);

    // get several consumers and put them into group
    Status get_consumer_grp(StreamLoadContext* ctx, std::shared_ptr<DataConsumerGroup>* ret);

    // Return the consumer to the pool; a full pool evicts the consumer idle for the longest to make room.
    // Task/RPC callers keep the evicted consumers alive until their transaction/callback or response is handled,
    // since closing them can wait for the brokers. A disabled pool also returns the unpooled incoming consumer.
    // A consumer whose commit is still on its way is kept apart instead, and nothing is evicted for it. Returning a
    // consumer, one kept apart too, frees the results of its commits that have completed.
    [[nodiscard]] std::vector<std::shared_ptr<DataConsumer>> return_consumer(
            const std::shared_ptr<DataConsumer>& consumer);
    // Return the group to the pool and transfer ownership of all evicted consumers to the caller.
    [[nodiscard]] std::vector<std::shared_ptr<DataConsumer>> return_consumers(DataConsumerGroup* grp);

    // start the background worker that closes the consumers idle for too long
    void start_bg_worker();

private:
    void _clean_idle_consumer_bg();

    std::mutex _lock;
    std::atomic<bool> _is_closed{false};
    std::list<std::shared_ptr<DataConsumer>> _pool;
    // The consumers kept apart for their commits still on their way. One that only this list holds is not in use: a
    // caller gets a consumer of the list only under _lock.
    std::list<std::shared_ptr<DataConsumer>> _committing;

    std::thread _clean_idle_consumer_thread;
};

} // end namespace starrocks
