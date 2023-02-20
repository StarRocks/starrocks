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
//   https://github.com/apache/incubator-doris/blob/master/be/src/runtime/routine_load/data_consumer_group.h

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

#include "runtime/routine_load/data_consumer.h"
#include "util/blocking_queue.hpp"
#include "util/priority_thread_pool.hpp"

namespace starrocks {

// data consumer group saves a group of data consumers.
// These data consumers share the same stream load pipe.
// This class is not thread safe.
class DataConsumerGroup {
public:
    typedef std::function<void(const Status&)> ConsumeFinishCallback;

    DataConsumerGroup(size_t sz) : _grp_id(UniqueId::gen_uid()), _thread_pool("data_consume", sz, 10) {}

    virtual ~DataConsumerGroup() { _consumers.clear(); }

    const UniqueId& grp_id() { return _grp_id; }

    const std::vector<std::shared_ptr<DataConsumer>>& consumers() { return _consumers; }

    void add_consumer(const std::shared_ptr<DataConsumer>& consumer) {
        consumer->set_grp(_grp_id);
        _consumers.push_back(consumer);
        ++_counter;
    }

    // start all consumers
    virtual Status start_all(StreamLoadContext* ctx) { return Status::OK(); }

protected:
    UniqueId _grp_id;
    std::vector<std::shared_ptr<DataConsumer>> _consumers;
    // thread pool to run each consumer in multi thread
    PriorityThreadPool _thread_pool;
    // mutex to protect counter.
    // the counter is init as the number of consumers.
    // once a consumer is done, decrease the counter.
    // when the counter becomes zero, shutdown the queue to finish
    std::mutex _mutex;
    int _counter{0};
};

// for kafka
class KafkaDataConsumerGroup : public DataConsumerGroup {
public:
    KafkaDataConsumerGroup(size_t sz) : DataConsumerGroup(sz), _queue(500) {}

    ~KafkaDataConsumerGroup() override;

    Status start_all(StreamLoadContext* ctx) override;
    // assign topic partitions to all consumers equally
    Status assign_topic_partitions(StreamLoadContext* ctx);

private:
    // start a single consumer
    void actual_consume(const std::shared_ptr<DataConsumer>& consumer, TimedBlockingQueue<RdKafka::Message*>* queue,
                        int64_t max_running_time_ms, const ConsumeFinishCallback& cb);

private:
    // blocking queue to receive msgs from all consumers
    TimedBlockingQueue<RdKafka::Message*> _queue;
};

// for pulsar
class PulsarDataConsumerGroup : public DataConsumerGroup {
public:
    PulsarDataConsumerGroup(size_t sz) : DataConsumerGroup(sz), _queue(500) {}

    ~PulsarDataConsumerGroup() override;

    Status start_all(StreamLoadContext* ctx) override;
    // assign topic partitions to all consumers equally
    Status assign_topic_partitions(StreamLoadContext* ctx);

private:
    // start a single consumer
    void actual_consume(const std::shared_ptr<DataConsumer>& consumer, TimedBlockingQueue<pulsar::Message*>* queue,
                        int64_t max_running_time_ms, const ConsumeFinishCallback& cb);

    void get_backlog_nums(StreamLoadContext* ctx);

private:
    // blocking queue to receive msgs from all consumers
    TimedBlockingQueue<pulsar::Message*> _queue;
};

} // end namespace starrocks
