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
//   https://github.com/apache/incubator-doris/blob/master/be/src/runtime/routine_load/data_consumer.cpp

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

#include "runtime/routine_load/data_consumer.h"

#include <functional>
#include <string>
#include <vector>

#include "common/status.h"
#include "gutil/strings/split.h"
#include "runtime/small_file_mgr.h"
#include "service/backend_options.h"
#include "util/defer_op.h"
#include "util/stopwatch.hpp"
#include "util/uid_util.h"

namespace starrocks {

// init kafka consumer will only set common configs such as
// brokers, groupid
Status KafkaDataConsumer::init(StreamLoadContext* ctx) {
    std::unique_lock<std::mutex> l(_lock);
    if (_init) {
        // this consumer has already been initialized.
        return Status::OK();
    }

    RdKafka::Conf* conf = RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL);

    // conf has to be deleted finally
    auto conf_deleter = [conf]() { delete conf; };
    DeferOp delete_conf([conf_deleter] { return conf_deleter(); });

    std::string group_id;
    auto it = ctx->kafka_info->properties.find("group.id");
    if (it == ctx->kafka_info->properties.end()) {
        group_id = BackendOptions::get_localhost() + "_" + UniqueId::gen_uid().to_string();
    } else {
        group_id = it->second;
    }
    LOG(INFO) << "init kafka consumer with group id: " << group_id;

    std::string errstr;
    auto set_conf = [&conf, &errstr](const std::string& conf_key, const std::string& conf_val) {
        RdKafka::Conf::ConfResult res = conf->set(conf_key, conf_val, errstr);
        if (res == RdKafka::Conf::CONF_UNKNOWN) {
            // ignore unknown config
            return Status::OK();
        } else if (errstr.find("not supported") != std::string::npos) {
            // some java-only properties may be passed to here, and librdkafak will return 'xxx' not supported
            // ignore it
            return Status::OK();
        } else if (res != RdKafka::Conf::CONF_OK) {
            std::stringstream ss;
            ss << "PAUSE: failed to set '" << conf_key << "', value: '" << conf_val << "', err: " << errstr;
            LOG(WARNING) << ss.str();
            return Status::InternalError(ss.str());
        }
        VLOG(3) << "set " << conf_key << ": " << conf_val;
        return Status::OK();
    };

    RETURN_IF_ERROR(set_conf("metadata.broker.list", ctx->kafka_info->brokers));
    RETURN_IF_ERROR(set_conf("group.id", group_id));
    // For transaction producer, producer will append one control msg to the group of msgs,
    // but the control msg will not return to consumer,
    // so we can't to judge whether the consumption has been completed by offset comparison.
    // So we set enable.partition.eof=true,
    // if we find current partition is already reach end,
    // we will direct return instead of wait consume until timeout.
    // Another advantage of doing this is that for topics with little data,
    // there is no need to wait until the timeout and occupy a lot of consumer threads.
    RETURN_IF_ERROR(set_conf("enable.partition.eof", "true"));
    RETURN_IF_ERROR(set_conf("enable.auto.offset.store", "false"));
    // TODO: set it larger than 0 after we set rd_kafka_conf_set_stats_cb()
    RETURN_IF_ERROR(set_conf("statistics.interval.ms", "0"));
    RETURN_IF_ERROR(set_conf("auto.offset.reset", "error"));
    RETURN_IF_ERROR(set_conf("api.version.request", "true"));
    RETURN_IF_ERROR(set_conf("api.version.fallback.ms", "0"));
    if (config::dependency_librdkafka_debug_enable) {
        RETURN_IF_ERROR(set_conf("debug", config::dependency_librdkafka_debug));
    }

    for (auto& item : ctx->kafka_info->properties) {
        if (boost::algorithm::starts_with(item.second, "FILE:")) {
            // file property should has format: FILE:file_id:md5
            std::vector<std::string> parts = strings::Split(item.second, ":", strings::SkipWhitespace());
            if (parts.size() != 3) {
                return Status::InternalError("PAUSE: Invalid file property of kafka: " + item.second);
            }
            int64_t file_id = std::stol(parts[1]);
            std::string file_path;
            Status st = ctx->exec_env()->small_file_mgr()->get_file(file_id, parts[2], &file_path);
            if (!st.ok()) {
                std::stringstream ss;
                ss << "PAUSE: failed to get file for config: " << item.first << ", error: " << st.get_error_msg();
                return Status::InternalError(ss.str());
            }
            RETURN_IF_ERROR(set_conf(item.first, file_path));
        } else {
            RETURN_IF_ERROR(set_conf(item.first, item.second));
        }
        _custom_properties.emplace(item.first, item.second);
    }

    if (conf->set("event_cb", &_k_event_cb, errstr) != RdKafka::Conf::CONF_OK) {
        std::stringstream ss;
        ss << "PAUSE: failed to set 'event_cb'";
        LOG(WARNING) << ss.str();
        return Status::InternalError(ss.str());
    }

    // create consumer
    _k_consumer = RdKafka::KafkaConsumer::create(conf, errstr);
    if (!_k_consumer) {
        LOG(WARNING) << "PAUSE: failed to create kafka consumer: " << errstr;
        return Status::InternalError("PAUSE: failed to create kafka consumer: " + errstr);
    }

    VLOG(3) << "finished to init kafka consumer. " << ctx->brief();

    _init = true;
    return Status::OK();
}

Status KafkaDataConsumer::assign_topic_partitions(const std::map<int32_t, int64_t>& begin_partition_offset,
                                                  const std::string& topic, StreamLoadContext* ctx) {
    DCHECK(_k_consumer);
    // create TopicPartitions
    std::stringstream ss;
    std::vector<RdKafka::TopicPartition*> topic_partitions;
    for (auto& entry : begin_partition_offset) {
        RdKafka::TopicPartition* tp1 = RdKafka::TopicPartition::create(topic, entry.first, entry.second);
        topic_partitions.push_back(tp1);
        ss << "[" << entry.first << ": " << entry.second << "] ";
    }
    _non_eof_partition_count = topic_partitions.size();

    LOG(INFO) << "consumer: " << _id << ", grp: " << _grp_id << " assign topic partitions: " << topic << ", "
              << ss.str();

    // delete TopicPartition finally
    auto tp_deleter = [&topic_partitions]() {
        std::for_each(topic_partitions.begin(), topic_partitions.end(),
                      [](RdKafka::TopicPartition* tp1) { delete tp1; });
    };
    DeferOp delete_tp([tp_deleter] { return tp_deleter(); });

    // assign partition
    RdKafka::ErrorCode err = _k_consumer->assign(topic_partitions);
    if (err) {
        LOG(WARNING) << "failed to assign topic partitions: " << ctx->brief(true) << ", err: " << RdKafka::err2str(err);
        return Status::InternalError("failed to assign topic partitions");
    }

    return Status::OK();
}

Status KafkaDataConsumer::group_consume(TimedBlockingQueue<RdKafka::Message*>* queue, int64_t max_running_time_ms) {
    _last_visit_time = time(nullptr);
    int64_t left_time = max_running_time_ms;
    LOG(INFO) << "start kafka consumer: " << _id << ", grp: " << _grp_id << ", max running time(ms): " << left_time;

    int64_t received_rows = 0;
    int64_t put_rows = 0;
    Status st = Status::OK();
    MonotonicStopWatch consumer_watch;
    MonotonicStopWatch watch;
    watch.start();
    while (true) {
        {
            std::unique_lock<std::mutex> l(_lock);
            if (_cancelled) {
                break;
            }
        }

        if (left_time <= 0) {
            break;
        }

        bool done = false;
        // consume 1 message at a time
        consumer_watch.start();
        int64_t consume_timeout = std::min<int64_t>(left_time, config::routine_load_kafka_timeout_second * 1000);
        std::unique_ptr<RdKafka::Message> msg(_k_consumer->consume(consume_timeout /* timeout, ms */));
        consumer_watch.stop();
        switch (msg->err()) {
        case RdKafka::ERR_NO_ERROR:
            if (!queue->blocking_put(msg.get())) {
                // queue is shutdown
                done = true;
            } else {
                ++put_rows;
                msg.release(); // release the ownership, msg will be deleted after being processed
            }
            ++received_rows;
            break;
        case RdKafka::ERR__TIMED_OUT: {
            // leave the status as OK, because this may happened
            // if there is no data in kafka.
            std::stringstream ss;
            ss << msg->errstr() << ", timeout " << consume_timeout;
            LOG(INFO) << "kafka consume timeout: " << _id << " msg " << ss.str();
            break;
        }
        case RdKafka::ERR_OFFSET_OUT_OF_RANGE: {
            done = true;
            std::stringstream ss;
            ss << msg->errstr() << ", partition " << msg->partition() << " offset " << msg->offset() << " has no data";
            LOG(WARNING) << "kafka consume failed: " << _id << ", msg: " << ss.str();
            st = Status::InternalError(ss.str());
            break;
        }
        case RdKafka::ERR__PARTITION_EOF: {
            _non_eof_partition_count--;
            // For transaction producer, producer will append one control msg to the group of msgs,
            // but the control msg will not return to consumer,
            // so we use the offset of eof to compute the last offset.
            //
            // The last offset of partition = `offset of eof` - 1
            // The goal of put the EOF msg to queue is that:
            // we will calculate the last offset of the partition using offset of EOF msg
            if (!queue->blocking_put(msg.get())) {
                done = true;
            } else if (_non_eof_partition_count <= 0) {
                msg.release();
                done = true;
            } else {
                msg.release();
            }
            break;
        }
        default:
            LOG(WARNING) << "kafka consume failed: " << _id << ", msg: " << msg->errstr();
            done = true;
            st = Status::InternalError(msg->errstr());
            break;
        }

        left_time = max_running_time_ms - watch.elapsed_time() / 1000 / 1000;
        if (done) {
            break;
        }
    }

    LOG(INFO) << "kafka consume done: " << _id << ", grp: " << _grp_id << ". cancelled: " << _cancelled
              << ", left time(ms): " << left_time << ", total cost(ms): " << watch.elapsed_time() / 1000 / 1000
              << ", consume cost(ms): " << consumer_watch.elapsed_time() / 1000 / 1000
              << ", received rows: " << received_rows << ", put rows: " << put_rows;

    return st;
}

Status KafkaDataConsumer::get_partition_offset(std::vector<int32_t>* partition_ids,
                                               std::vector<int64_t>* beginning_offsets,
                                               std::vector<int64_t>* latest_offsets, int timeout) {
    _last_visit_time = time(nullptr);
    beginning_offsets->reserve(partition_ids->size());
    latest_offsets->reserve(partition_ids->size());
    MonotonicStopWatch watch;
    watch.start();
    for (auto p_id : *partition_ids) {
        int64_t beginning_offset;
        int64_t latest_offset;
        auto left_ms = timeout - watch.elapsed_time() / 1000 / 1000;
        if (left_ms <= 0) {
            return Status::TimedOut("get kafka partition offset timeout");
        }
        RdKafka::ErrorCode err =
                _k_consumer->query_watermark_offsets(_topic, p_id, &beginning_offset, &latest_offset, left_ms);
        if (err != RdKafka::ERR_NO_ERROR) {
            LOG(WARNING) << "failed to query watermark offset of topic: " << _topic << " partition: " << p_id
                         << ", err: " << RdKafka::err2str(err);
            return Status::InternalError("failed to query watermark offset, err: " + RdKafka::err2str(err));
        }
        beginning_offsets->push_back(beginning_offset);
        latest_offsets->push_back(latest_offset);
    }

    return Status::OK();
}

Status KafkaDataConsumer::get_partition_meta(std::vector<int32_t>* partition_ids, int timeout) {
    _last_visit_time = time(nullptr);
    // create topic conf
    RdKafka::Conf* tconf = RdKafka::Conf::create(RdKafka::Conf::CONF_TOPIC);
    auto conf_deleter = [tconf]() { delete tconf; };
    DeferOp delete_conf([conf_deleter] { return conf_deleter(); });

    // create topic
    std::string errstr;
    RdKafka::Topic* topic = RdKafka::Topic::create(_k_consumer, _topic, tconf, errstr);
    if (topic == nullptr) {
        std::stringstream ss;
        ss << "failed to create topic: " << errstr;
        LOG(WARNING) << ss.str();
        return Status::InternalError(ss.str());
    }
    auto topic_deleter = [topic]() { delete topic; };
    DeferOp delete_topic([topic_deleter] { return topic_deleter(); });

    // get topic metadata
    RdKafka::Metadata* metadata = nullptr;
    RdKafka::ErrorCode err = _k_consumer->metadata(true /* for this topic */, topic, &metadata, timeout);
    if (err != RdKafka::ERR_NO_ERROR) {
        std::stringstream ss;
        ss << "failed to get partition meta: " << RdKafka::err2str(err);
        LOG(WARNING) << ss.str();
        return Status::InternalError(ss.str());
    }
    auto meta_deleter = [metadata]() { delete metadata; };
    DeferOp delete_meta([meta_deleter] { return meta_deleter(); });

    // get partition ids
    RdKafka::Metadata::TopicMetadataIterator it;
    for (it = metadata->topics()->begin(); it != metadata->topics()->end(); ++it) {
        if ((*it)->topic() != _topic) {
            continue;
        }

        if ((*it)->err() != RdKafka::ERR_NO_ERROR) {
            std::stringstream ss;
            ss << "error: " << err2str((*it)->err());
            if ((*it)->err() == RdKafka::ERR_LEADER_NOT_AVAILABLE) {
                ss << ", try again";
            }
            LOG(WARNING) << ss.str();
            return Status::InternalError(ss.str());
        }

        RdKafka::TopicMetadata::PartitionMetadataIterator ip;
        for (ip = (*it)->partitions()->begin(); ip != (*it)->partitions()->end(); ++ip) {
            partition_ids->push_back((*ip)->id());
        }
    }

    if (partition_ids->empty()) {
        return Status::InternalError("no partition in this topic");
    }

    return Status::OK();
}

Status KafkaDataConsumer::cancel(StreamLoadContext* ctx) {
    std::unique_lock<std::mutex> l(_lock);
    if (!_init) {
        return Status::InternalError("consumer is not initialized");
    }

    _cancelled = true;
    LOG(INFO) << "kafka consumer cancelled. " << _id;
    return Status::OK();
}

void KafkaDataConsumer::reset() {
    std::unique_lock<std::mutex> l(_lock);
    _cancelled = false;
    _k_consumer->unassign();
    _non_eof_partition_count = 0;
}

Status KafkaDataConsumer::commit(std::vector<RdKafka::TopicPartition*>& offset) {
    RdKafka::ErrorCode err = _k_consumer->commitSync(offset);
    if (err != RdKafka::ERR_NO_ERROR) {
        std::stringstream ss;
        ss << "failed to commit kafka offset : " << RdKafka::err2str(err);
        return Status::InternalError(ss.str());
    }
    return Status::OK();
}

// if the kafka brokers and topic are same,
// we considered this consumer as matched, thus can be reused.
bool KafkaDataConsumer::match(StreamLoadContext* ctx) {
    if (ctx->load_src_type != TLoadSourceType::KAFKA) {
        return false;
    }
    if (_brokers != ctx->kafka_info->brokers || _topic != ctx->kafka_info->topic) {
        return false;
    }
    // check properties
    if (_custom_properties.size() != ctx->kafka_info->properties.size()) {
        return false;
    }
    for (auto& item : ctx->kafka_info->properties) {
        auto it = _custom_properties.find(item.first);
        if (it == _custom_properties.end() || it->second != item.second) {
            return false;
        }
    }
    return true;
}

// init pulsar consumer will only set common configs
Status PulsarDataConsumer::init(StreamLoadContext* ctx) {
    std::unique_lock<std::mutex> l(_lock);
    if (_init) {
        // this consumer has already been initialized.
        return Status::OK();
    }

    pulsar::ClientConfiguration config;
    for (auto& item : ctx->pulsar_info->properties) {
        if (item.first == "auth.token") {
            config.setAuth(pulsar::AuthToken::createWithToken(item.second));
        } else {
            LOG(WARNING) << "Config " << item.first << " not supported for now.";
        }

        _custom_properties.emplace(item.first, item.second);
    }

    _p_client = new pulsar::Client(_service_url, config);

    VLOG(3) << "finished to init pulsar consumer. " << ctx->brief();

    _init = true;
    return Status::OK();
}

Status PulsarDataConsumer::assign_partition(const std::string& partition, StreamLoadContext* ctx,
                                            int64_t initial_position) {
    DCHECK(_p_client);

    std::stringstream ss;
    ss << "consumer: " << _id << ", grp: " << _grp_id << " assign partition: " << _topic
       << ", subscription: " << _subscription << ", initial_position: " << initial_position;
    LOG(INFO) << ss.str();

    // do subscribe
    pulsar::Result result;
    result = _p_client->subscribe(partition, _subscription, _p_consumer);
    if (result != pulsar::ResultOk) {
        LOG(WARNING) << "PAUSE: failed to create pulsar consumer: " << ctx->brief(true) << ", err: " << result;
        return Status::InternalError("PAUSE: failed to create pulsar consumer: " +
                                     std::string(pulsar::strResult(result)));
    }

    if (initial_position == InitialPosition::LATEST || initial_position == InitialPosition::EARLIEST) {
        pulsar::InitialPosition p_initial_position = initial_position == InitialPosition::LATEST
                                                             ? pulsar::InitialPosition::InitialPositionLatest
                                                             : pulsar::InitialPosition::InitialPositionEarliest;
        result = _p_consumer.seek(p_initial_position);
        if (result != pulsar::ResultOk) {
            LOG(WARNING) << "PAUSE: failed to reset the subscription: " << ctx->brief(true) << ", err: " << result;
            return Status::InternalError("PAUSE: failed to reset the subscription: " +
                                         std::string(pulsar::strResult(result)));
        }
    }

    return Status::OK();
}

Status PulsarDataConsumer::group_consume(TimedBlockingQueue<pulsar::Message*>* queue, int64_t max_running_time_ms) {
    _last_visit_time = time(nullptr);
    int64_t left_time = max_running_time_ms;
    LOG(INFO) << "start pulsar consumer: " << _id << ", grp: " << _grp_id << ", max running time(ms): " << left_time;

    int64_t received_rows = 0;
    int64_t put_rows = 0;
    Status st = Status::OK();
    MonotonicStopWatch consumer_watch;
    MonotonicStopWatch watch;
    watch.start();
    while (true) {
        {
            std::unique_lock<std::mutex> l(_lock);
            if (_cancelled) {
                break;
            }
        }

        if (left_time <= 0) {
            break;
        }

        bool done = false;
        auto msg = std::make_unique<pulsar::Message>();
        // consume 1 message at a time
        consumer_watch.start();
        pulsar::Result res = _p_consumer.receive(*(msg.get()), 1000 /* timeout, ms */);
        consumer_watch.stop();
        switch (res) {
        case pulsar::ResultOk:
            if (!queue->blocking_put(msg.get())) {
                // queue is shutdown
                done = true;
            } else {
                ++put_rows;
                msg.release(); // release the ownership, msg will be deleted after being processed
            }
            ++received_rows;
            break;
        case pulsar::ResultTimeout:
            // leave the status as OK, because this may happened
            // if there is no data in pulsar.
            LOG(INFO) << "pulsar consumer"
                      << "[" << _id << "]"
                      << " consume timeout.";
            break;
        default:
            LOG(WARNING) << "pulsar consumer"
                         << "[" << _id << "]"
                         << " consume failed: "
                         << ", errmsg: " << res;
            done = true;
            st = Status::InternalError(pulsar::strResult(res));
            break;
        }

        left_time = max_running_time_ms - watch.elapsed_time() / 1000 / 1000;
        if (done) {
            break;
        }
    }

    LOG(INFO) << "pulsar consume done: " << _id << ", grp: " << _grp_id << ". cancelled: " << _cancelled
              << ", left time(ms): " << left_time << ", total cost(ms): " << watch.elapsed_time() / 1000 / 1000
              << ", consume cost(ms): " << consumer_watch.elapsed_time() / 1000 / 1000
              << ", received rows: " << received_rows << ", put rows: " << put_rows;

    return st;
}

const std::string& PulsarDataConsumer::get_partition() {
    _last_visit_time = time(nullptr);
    return _p_consumer.getTopic();
}

Status PulsarDataConsumer::get_partition_backlog(int64_t* backlog) {
    _last_visit_time = time(nullptr);
    pulsar::BrokerConsumerStats broker_consumer_stats;
    pulsar::Result result = _p_consumer.getBrokerConsumerStats(broker_consumer_stats);
    if (result != pulsar::ResultOk) {
        LOG(WARNING) << "Failed to get broker consumer stats: "
                     << ", err: " << result;
        return Status::InternalError("Failed to get broker consumer stats: " + std::string(pulsar::strResult(result)));
    }
    *backlog = broker_consumer_stats.getMsgBacklog();

    return Status::OK();
}

Status PulsarDataConsumer::get_topic_partition(std::vector<std::string>* partitions) {
    _last_visit_time = time(nullptr);
    pulsar::Result result = _p_client->getPartitionsForTopic(_topic, *partitions);
    if (result != pulsar::ResultOk) {
        LOG(WARNING) << "Failed to get partitions for topic: " << _topic << ", err: " << result;
        return Status::InternalError("Failed to get partitions for topic: " + std::string(pulsar::strResult(result)));
    }

    return Status::OK();
}

Status PulsarDataConsumer::cancel(StreamLoadContext* ctx) {
    std::unique_lock<std::mutex> l(_lock);
    if (!_init) {
        return Status::InternalError("consumer is not initialized");
    }

    _cancelled = true;
    LOG(INFO) << "pulsar consumer cancelled. " << _id;
    return Status::OK();
}

void PulsarDataConsumer::reset() {
    std::unique_lock<std::mutex> l(_lock);
    _cancelled = false;
    _p_consumer.close();
}

Status PulsarDataConsumer::acknowledge_cumulative(pulsar::MessageId& message_id) {
    pulsar::Result res = _p_consumer.acknowledgeCumulative(message_id);
    if (res != pulsar::ResultOk) {
        std::stringstream ss;
        ss << "failed to acknowledge pulsar message : " << res;
        return Status::InternalError(ss.str());
    }
    return Status::OK();
}

bool PulsarDataConsumer::match(StreamLoadContext* ctx) {
    if (ctx->load_src_type != TLoadSourceType::PULSAR) {
        return false;
    }
    if (_service_url != ctx->pulsar_info->service_url || _topic != ctx->pulsar_info->topic ||
        _subscription != ctx->pulsar_info->subscription) {
        return false;
    }
    // check properties
    if (_custom_properties.size() != ctx->pulsar_info->properties.size()) {
        return false;
    }
    for (auto& item : ctx->pulsar_info->properties) {
        auto it = _custom_properties.find(item.first);
        if (it == _custom_properties.end() || it->second != item.second) {
            return false;
        }
    }

    return true;
}

} // end namespace starrocks
