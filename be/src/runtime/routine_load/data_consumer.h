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
//   https://github.com/apache/incubator-doris/blob/master/be/src/runtime/routine_load/data_consumer.h

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

#include <ctime>
#include <mutex>
#include <unordered_map>

#include "librdkafka/rdkafkacpp.h"
#include "pulsar/Client.h"
#include "runtime/stream_load/stream_load_context.h"
#include "util/blocking_queue.hpp"
#include "util/uid_util.h"

namespace starrocks {

class KafkaConsumerPipe;
class Status;
class StreamLoadPipe;

using PulsarConsumerPipe = KafkaConsumerPipe;

class DataConsumer {
public:
    DataConsumer() : _id(UniqueId::gen_uid()), _grp_id(UniqueId::gen_uid()) {}

    virtual ~DataConsumer() = default;

    // init the consumer with the given parameters
    virtual Status init(StreamLoadContext* ctx) = 0;
    // start consuming
    virtual Status consume(StreamLoadContext* ctx) = 0;
    // cancel the consuming process.
    // if the consumer is not initialized, or the consuming
    // process is already finished, call cancel() will
    // return ERROR
    virtual Status cancel(StreamLoadContext* ctx) = 0;
    // reset the data consumer before being reused
    virtual Status reset() = 0;
    // return true the if the consumer match the need
    virtual bool match(StreamLoadContext* ctx) = 0;

    const UniqueId& id() { return _id; }
    time_t last_visit_time() const { return _last_visit_time; }
    void set_grp(const UniqueId& grp_id) { _grp_id = grp_id; }

protected:
    UniqueId _id;
    UniqueId _grp_id;

    // lock to protect the following bools
    std::mutex _lock;
    bool _init{false};
    bool _cancelled{false};
    time_t _last_visit_time{0};
};

class KafkaEventCb : public RdKafka::EventCb {
public:
    void event_cb(RdKafka::Event& event) override {
        switch (event.type()) {
        case RdKafka::Event::EVENT_ERROR:
            LOG(INFO) << "kafka error: " << RdKafka::err2str(event.err()) << ", event: " << event.str();
            break;
        case RdKafka::Event::EVENT_STATS:
            LOG(INFO) << "kafka stats: " << event.str();
            break;

        case RdKafka::Event::EVENT_LOG:
            LOG(INFO) << "kafka log-" << event.severity() << "-" << event.fac().c_str() << ", event: " << event.str();
            break;

        case RdKafka::Event::EVENT_THROTTLE:
            LOG(INFO) << "kafka throttled: " << event.throttle_time() << "ms by " << event.broker_name() << " id "
                      << (int)event.broker_id();
            break;

        default:
            LOG(INFO) << "kafka event: " << event.type() << ", err: " << RdKafka::err2str(event.err())
                      << ", event: " << event.str();
            break;
        }
    }
};

class KafkaDataConsumer : public DataConsumer {
public:
    explicit KafkaDataConsumer(StreamLoadContext* ctx)
            : DataConsumer(), _brokers(ctx->kafka_info->brokers), _topic(ctx->kafka_info->topic) {}

    ~KafkaDataConsumer() override {
        VLOG(3) << "deconstruct consumer";
        if (_k_consumer) {
            _k_consumer->close();
            delete _k_consumer;
            _k_consumer = nullptr;
        }
    }

    Status init(StreamLoadContext* ctx) override;
    // TODO(cmy): currently do not implement single consumer start method, using group_consume
    Status consume(StreamLoadContext* ctx) override { return Status::OK(); }
    Status cancel(StreamLoadContext* ctx) override;
    // reassign partition topics
    Status reset() override;
    bool match(StreamLoadContext* ctx) override;
    // commit kafka offset
    Status commit(std::vector<RdKafka::TopicPartition*>& offset);

    Status assign_topic_partitions(const std::map<int32_t, int64_t>& begin_partition_offset, const std::string& topic,
                                   StreamLoadContext* ctx);

    // start the consumer and put msgs to queue
    Status group_consume(TimedBlockingQueue<RdKafka::Message*>* queue, int64_t max_running_time_ms);

    // get the partitions ids of the topic
    Status get_partition_meta(std::vector<int32_t>* partition_ids, int timeout);

    // get beginning or latest offset of partitions
    Status get_partition_offset(std::vector<int32_t>* partition_ids, std::vector<int64_t>* beginning_offsets,
                                std::vector<int64_t>* latest_offsets, int timeout);

private:
    std::string _brokers;
    std::string _topic;
    std::unordered_map<std::string, std::string> _custom_properties;

    size_t _non_eof_partition_count = 0;
    KafkaEventCb _k_event_cb;
    RdKafka::KafkaConsumer* _k_consumer = nullptr;
};

class PulsarDataConsumer : public DataConsumer {
public:
    PulsarDataConsumer(StreamLoadContext* ctx)
            : DataConsumer(),
              _service_url(ctx->pulsar_info->service_url),
              _topic(ctx->pulsar_info->topic),
              _subscription(ctx->pulsar_info->subscription) {}

    ~PulsarDataConsumer() override {
        VLOG(3) << "deconstruct pulsar client";
        if (_p_client) {
            _p_client->close();
            delete _p_client;
            _p_client = nullptr;
        }
    }

    enum InitialPosition { LATEST, EARLIEST };

    Status init(StreamLoadContext* ctx) override;
    Status assign_partition(const std::string& partition, StreamLoadContext* ctx, int64_t initial_position = -1);
    // TODO(cmy): currently do not implement single consumer start method, using group_consume
    Status consume(StreamLoadContext* ctx) override { return Status::OK(); }
    Status cancel(StreamLoadContext* ctx) override;
    // reassign partition topics
    Status reset() override;
    bool match(StreamLoadContext* ctx) override;
    // acknowledge pulsar message
    Status acknowledge_cumulative(pulsar::MessageId& message_id);

    // start the consumer and put msgs to queue
    Status group_consume(TimedBlockingQueue<pulsar::Message*>* queue, int64_t max_running_time_ms);

    // get the partitions of the topic
    Status get_topic_partition(std::vector<std::string>* partitions);

    // get backlog num of partition
    Status get_partition_backlog(int64_t* backlog);

    const std::string& get_partition();

private:
    std::string _service_url;
    std::string _topic;
    std::string _subscription;
    std::unordered_map<std::string, std::string> _custom_properties;

    pulsar::Client* _p_client = nullptr;
    pulsar::Consumer _p_consumer;
    std::shared_ptr<PulsarConsumerPipe> _p_consumer_pipe;
};

} // end namespace starrocks
