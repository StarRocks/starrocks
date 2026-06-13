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

#include <cstdint>
#include <string>

#include "base/concurrency/blocking_queue.hpp"
#include "common/thread/priority_thread_pool.hpp"
#include "runtime/routine_load/data_consumer.h"

namespace starrocks {

// Recover a Pulsar message's partition index from its topic name. The Pulsar C++ Message API exposes
// no partition number at all -- only getTopicName() -- so the partition has to be parsed out of the
// per-partition name Pulsar gives the Nth partition of a partitioned topic: "<topic>-partition-N".
// (pulsar_topic() does not go through here; it is taken straight from the job's configured logical
// topic, so only the index is parsed below.)
//
// The "-partition-N" suffix on its own is ambiguous: Pulsar lets you create a standalone
// non-partitioned topic literally named "<x>-partition-<n>" (it only refuses the name when a
// partitioned topic <x> with more than n partitions already exists), and that topic's getTopicName()
// is byte-for-byte identical to partition n of a partitioned topic <x>. The job's configured logical
// topic is the only thing that tells the two apart, so we anchor on it: a trailing "-partition-N" is a
// real partition only when stripping it leaves exactly the configured topic.
//
// getTopicName() is the fully-qualified canonical name ("persistent://tenant/ns/<local-name>"), while
// the configured topic may be the short local form ("<local-name>"). Pulsar only prepends the
// scheme/tenant/namespace defaults and never rewrites the local name, so the configured topic is
// always a suffix of the canonical name aligned on a "/" -- hence the match accepts either an exact
// equality or a match at a leading "/" boundary.
//
// Returns the partition index, or -1 when message_topic is not a partition of logical_topic (the
// scanner renders -1 as SQL NULL).
int32_t parse_pulsar_partition_index(const std::string& logical_topic, const std::string& message_topic);

// Parse the Confluent wire-format schema id from a message: a 0x00 magic byte followed by a 4-byte
// big-endian schema id. Writes the id to *schema_id and returns true on success; returns false for
// anything not framed this way (raw Avro, fewer than 5 bytes, wrong magic), so the caller falls back
// to the normal append path. Used to detect schema-id boundaries within a routine-load batch.
bool parse_confluent_schema_id(const char* data, size_t size, int32_t* schema_id);

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
