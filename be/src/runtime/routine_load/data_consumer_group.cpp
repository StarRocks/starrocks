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
//   https://github.com/apache/incubator-doris/blob/master/be/src/runtime/routine_load/data_consumer_group.cpp

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
#include "runtime/routine_load/data_consumer_group.h"

#include <limits>
#include <sstream>

#include "base/utility/defer_op.h"
#include "librdkafka/rdkafka.h"
#include "librdkafka/rdkafkacpp.h"
#include "runtime/routine_load/data_consumer.h"
#include "runtime/routine_load/kafka_consumer_pipe.h"
#include "runtime/stream_load/stream_load_context.h"

namespace starrocks {

int32_t parse_pulsar_partition_index(const std::string& logical_topic, const std::string& message_topic) {
    static const std::string kSuffix = "-partition-";
    auto pos = message_topic.rfind(kSuffix);
    if (pos == std::string::npos) {
        return -1;
    }
    const size_t digits_start = pos + kSuffix.size();
    if (digits_start >= message_topic.size()) {
        return -1;
    }
    // Accumulate in int64 and bail past INT32_MAX so a pathological suffix can't overflow (signed
    // overflow is UB). Real Pulsar partition indices are small, so this only guards malformed names.
    int64_t value = 0;
    for (size_t i = digits_start; i < message_topic.size(); ++i) {
        char c = message_topic[i];
        if (c < '0' || c > '9') {
            return -1;
        }
        value = value * 10 + (c - '0');
        if (value > std::numeric_limits<int32_t>::max()) {
            return -1;
        }
    }
    // The suffix is a real partition only if what precedes it is the configured logical topic; this is
    // what keeps a standalone topic literally named "<x>-partition-<n>" from being mistaken for a
    // partition. Accept either an exact match (configured topic already fully qualified) or a match at
    // the leading "/" boundary (configured topic given in short form against the canonical name).
    const std::string prefix = message_topic.substr(0, pos);
    if (prefix == logical_topic) {
        return static_cast<int32_t>(value);
    }
    if (prefix.size() > logical_topic.size() &&
        prefix.compare(prefix.size() - logical_topic.size(), logical_topic.size(), logical_topic) == 0 &&
        prefix[prefix.size() - logical_topic.size() - 1] == '/') {
        return static_cast<int32_t>(value);
    }
    return -1;
}

namespace {

// Render a Pulsar MessageId as a stable string for the __message_id__ column. operator<< is what the
// BE already uses to log MessageId, so the format is consistent across the codebase.
std::string pulsar_message_id_to_string(const pulsar::MessageId& id) {
    std::ostringstream oss;
    oss << id;
    return oss.str();
}

} // namespace

Status KafkaDataConsumerGroup::assign_topic_partitions(StreamLoadContext* ctx) {
    DCHECK(ctx->kafka_info);
    DCHECK(_consumers.size() >= 1);

    // divide partitions
    int consumer_size = _consumers.size();
    std::vector<std::map<int32_t, int64_t>> divide_parts(consumer_size);
    int i = 0;
    for (auto& kv : ctx->kafka_info->begin_offset) {
        int idx = i % consumer_size;
        divide_parts[idx].emplace(kv.first, kv.second);
        i++;
    }

    // assign partitions to consumers equally
    for (int i = 0; i < consumer_size; ++i) {
        RETURN_IF_ERROR(std::static_pointer_cast<KafkaDataConsumer>(_consumers[i])
                                ->assign_topic_partitions(divide_parts[i], ctx->kafka_info->topic, ctx));
    }

    return Status::OK();
}

KafkaDataConsumerGroup::~KafkaDataConsumerGroup() {
    // clean the msgs left in queue
    _queue.shutdown();
    while (true) {
        RdKafka::Message* msg;
        if (_queue.blocking_get(&msg)) {
            delete msg;
            msg = nullptr;
        } else {
            break;
        }
    }
    DCHECK(_queue.get_size() == 0);
}

Status KafkaDataConsumerGroup::start_all(StreamLoadContext* ctx) {
    Status result_st = Status::OK();
    // start all consumers
    for (auto& consumer : _consumers) {
        if (!_thread_pool.offer([this, consumer, capture0 = &_queue, capture1 = ctx->max_interval_s * 1000,
                                 capture2 = [this, &result_st](const Status& st) {
                                     std::unique_lock<std::mutex> lock(_mutex);
                                     _counter--;
                                     VLOG(2) << "group counter is: " << _counter << ", grp: " << _grp_id;
                                     if (_counter == 0) {
                                         _queue.shutdown();
                                         LOG(INFO)
                                                 << "all consumers are finished. shutdown queue. group id: " << _grp_id;
                                     }
                                     if (result_st.ok() && !st.ok()) {
                                         result_st = st;
                                     }
                                 }] { actual_consume(consumer, capture0, capture1, capture2); })) {
            LOG(WARNING) << "failed to submit data consumer: " << consumer->id() << ", group id: " << _grp_id;
            return Status::InternalError("failed to submit data consumer");
        } else {
            VLOG(2) << "submit a data consumer: " << consumer->id() << ", group id: " << _grp_id;
        }
    }

    // consuming from queue and put data to stream load pipe
    int64_t left_time = ctx->max_interval_s * 1000;
    int64_t received_rows = 0;
    int64_t left_bytes = ctx->max_batch_size;

    std::shared_ptr<KafkaConsumerPipe> kafka_pipe = std::static_pointer_cast<KafkaConsumerPipe>(ctx->body_sink);

    LOG(INFO) << "start consumer group: " << _grp_id << ". max time(ms): " << left_time
              << ", batch size: " << left_bytes << ". " << ctx->brief();

    // copy one
    std::map<int32_t, int64_t> cmt_offset = ctx->kafka_info->cmt_offset;
    std::map<int32_t, int64_t> cmt_offset_timestamp;

    // JSON/Avro: one message per buffer (with source metadata). CSV: rows separated by row_delimiter.
    const bool append_as_message =
            ctx->format == TFileFormatType::FORMAT_JSON || ctx->format == TFileFormatType::FORMAT_AVRO;
    char row_delimiter = '\n';
    if (!append_as_message) {
        auto& per_node_scan_ranges = ctx->put_result.params.params.per_node_scan_ranges;

        if (!per_node_scan_ranges.empty()) {
            DCHECK_GE(per_node_scan_ranges.begin()->second.size(), 1);

            auto& scan_range = per_node_scan_ranges.begin()->second[0].scan_range;
            auto& params = scan_range.broker_scan_range.params;
            row_delimiter = static_cast<char>(params.row_delimiter);
        }
    }
    // Attach the extended metadata (topic/timestamp/key/headers) only when the job's COLUMNS clause
    // references a metadata column.
    const bool need_meta = ctx->kafka_info->need_source_metadata;
    // Only copy the (potentially large) key/headers when the job references __key__ / __headers__.
    const bool need_key = ctx->kafka_info->need_message_key;
    const bool need_headers = ctx->kafka_info->need_message_headers;

    MonotonicStopWatch watch;
    watch.start();
    bool eos = false;
    while (true) {
        if (eos || left_time <= 0 || left_bytes <= 0) {
            LOG(INFO) << "consumer group done: " << _grp_id
                      << ". consume time(ms)=" << ctx->max_interval_s * 1000 - left_time
                      << ", received rows=" << received_rows << ", received bytes=" << ctx->max_batch_size - left_bytes
                      << ", eos: " << eos << ", left_time: " << left_time << ", left_bytes: " << left_bytes
                      << ", blocking get time(us): " << _queue.total_get_wait_time() / 1000
                      << ", blocking put time(us): " << _queue.total_put_wait_time() / 1000;

            // shutdown queue
            _queue.shutdown();
            // cancel all consumers
            for (auto& consumer : _consumers) {
                (void)consumer->cancel(ctx);
            }

            // waiting all threads finished
            _thread_pool.shutdown();
            _thread_pool.join();

            if (!result_st.ok()) {
                // some consumers encounter errors, cancel this task
                return result_st;
            }

            ctx->kafka_info->cmt_offset_timestamp = cmt_offset_timestamp;

            if (left_bytes == ctx->max_batch_size) {
                // nothing to be consumed, we have to cancel it, because
                // we do not allow finishing stream load pipe without data.
                //
                // But if the offset have already moved, such as the control msg,
                // we need to commit and tell fe to move offset to the newest offset, otherwise, fe will retry consume.
                for (auto& item : cmt_offset) {
                    if (item.second > ctx->kafka_info->cmt_offset[item.first]) {
                        RETURN_IF_ERROR(kafka_pipe->finish());
                        ctx->kafka_info->cmt_offset = std::move(cmt_offset);
                        ctx->receive_bytes = 0;
                        return Status::OK();
                    }
                }
                kafka_pipe->cancel(Status::Cancelled("Cancelled"));
                return Status::Cancelled("Cancelled");
            } else {
                DCHECK(left_bytes < ctx->max_batch_size);
                RETURN_IF_ERROR(kafka_pipe->finish());
                ctx->kafka_info->cmt_offset = std::move(cmt_offset);
                ctx->receive_bytes = ctx->max_batch_size - left_bytes;
                return Status::OK();
            }
        }

        RdKafka::Message* msg;
        bool res = _queue.blocking_get(&msg);
        if (res) {
            VLOG(3) << "get kafka message"
                    << ", partition: " << msg->partition() << ", offset: " << msg->offset() << ", len: " << msg->len();
            DeferOp msgDeleter([&] { delete msg; });

            if (msg->err() == RdKafka::ERR__PARTITION_EOF) {
                // For transaction producer, producer will append one control msg to the group of msgs,
                // but the control msg will not return to consumer,
                // so we use the offset of eof to compute the last offset.
                // The last offset of partition = `offset of eof` - 1
                //
                // if msg->offset == 0, don't record into cmt_offset,
                // because the fe will +1 and then consume the next msg.
                //
                // Our offset recorded in the kafka is the offset of last consumed msg,
                // but the standard usage is to record the last offset + 1.
                if (msg->offset() > 0) {
                    cmt_offset[msg->partition()] = msg->offset() - 1;
                    auto timestamp = msg->timestamp();
                    if (timestamp.type != RdKafka::MessageTimestamp::MSG_TIMESTAMP_NOT_AVAILABLE) {
                        cmt_offset_timestamp[msg->partition()] = msg->timestamp().timestamp;
                    }
                }
            } else {
                auto timestamp = msg->timestamp();
                Status st;
                if (append_as_message) {
                    StreamMessageMeta meta(ByteBufferMetaType::KAFKA);
                    // Always set: the scanner stamps partition/offset into error logs so rejected rows
                    // stay locatable even when the job selects no metadata column.
                    meta.set_partition(msg->partition());
                    meta.set_offset(msg->offset());
                    if (need_meta) {
                        meta.set_topic(ctx->kafka_info->topic);
                        // Leave the timestamp at its -1 sentinel when the broker reports none, so the
                        // column renders as NULL instead of a bogus epoch value.
                        if (timestamp.type != RdKafka::MessageTimestamp::MSG_TIMESTAMP_NOT_AVAILABLE) {
                            meta.set_timestamp(timestamp.timestamp);
                        }
                        if (need_key && msg->key_pointer() != nullptr) {
                            meta.set_key(std::string(static_cast<const char*>(msg->key_pointer()), msg->key_len()));
                        }
                        if (need_headers) {
                            // The Headers wrapper is owned by the message (its lifetime is the same as the
                            // Message, per RdKafka::Message::headers() docs) and is freed when `msg` is
                            // deleted below via msgDeleter. Do NOT delete it here, or it is a double free.
                            RdKafka::Headers* headers = msg->headers();
                            if (headers != nullptr) {
                                for (const auto& h : headers->get_all()) {
                                    meta.add_header(h.key(), h.value() != nullptr
                                                                     ? std::string(static_cast<const char*>(h.value()),
                                                                                   h.value_size())
                                                                     : std::string());
                                }
                            }
                        }
                    }
                    st = kafka_pipe->append_json(static_cast<const char*>(msg->payload()),
                                                 static_cast<size_t>(msg->len()), row_delimiter, &meta);
                } else {
                    st = kafka_pipe->append_with_row_delimiter(static_cast<const char*>(msg->payload()),
                                                               static_cast<size_t>(msg->len()), row_delimiter);
                }
                if (st.ok()) {
                    received_rows++;
                    left_bytes -= msg->len();
                    cmt_offset[msg->partition()] = msg->offset();

                    if (timestamp.type != RdKafka::MessageTimestamp::MSG_TIMESTAMP_NOT_AVAILABLE) {
                        cmt_offset_timestamp[msg->partition()] = timestamp.timestamp;
                    }
                    VLOG(3) << "consume partition[" << msg->partition() << " - " << msg->offset() << "]";
                } else {
                    // failed to append this msg, we must stop
                    LOG(WARNING) << "failed to append msg to pipe. grp: " << _grp_id << ", errmsg=" << st.message();
                    eos = true;
                    {
                        std::unique_lock<std::mutex> lock(_mutex);
                        if (result_st.ok()) {
                            result_st = st;
                        }
                    }
                }
            }
        } else {
            // queue is empty and shutdown
            eos = true;
        }

        left_time = ctx->max_interval_s * 1000 - watch.elapsed_time() / 1000 / 1000;
    }

    return Status::OK();
}

void KafkaDataConsumerGroup::actual_consume(const std::shared_ptr<DataConsumer>& consumer,
                                            TimedBlockingQueue<RdKafka::Message*>* queue, int64_t max_running_time_ms,
                                            const ConsumeFinishCallback& cb) {
    Status st = std::static_pointer_cast<KafkaDataConsumer>(consumer)->group_consume(queue, max_running_time_ms);
    cb(st);
}

Status PulsarDataConsumerGroup::assign_topic_partitions(StreamLoadContext* ctx) {
    DCHECK(ctx->pulsar_info);
    DCHECK(_consumers.size() >= 1);
    // Cumulative acknowledgement when consuming partitioned topics is not supported by pulsar
    DCHECK(_consumers.size() == ctx->pulsar_info->partitions.size());

    // assign partition to consumers
    int consumer_size = _consumers.size();
    for (int i = 0; i < consumer_size; ++i) {
        auto iter = ctx->pulsar_info->initial_positions.find(ctx->pulsar_info->partitions[i]);
        if (iter != ctx->pulsar_info->initial_positions.end()) {
            RETURN_IF_ERROR(std::static_pointer_cast<PulsarDataConsumer>(_consumers[i])
                                    ->assign_partition(ctx->pulsar_info->partitions[i], ctx, iter->second));
        } else {
            RETURN_IF_ERROR(std::static_pointer_cast<PulsarDataConsumer>(_consumers[i])
                                    ->assign_partition(ctx->pulsar_info->partitions[i], ctx));
        }
    }

    return Status::OK();
}

PulsarDataConsumerGroup::~PulsarDataConsumerGroup() {
    // clean the msgs left in queue
    _queue.shutdown();
    while (true) {
        pulsar::Message* msg;
        if (_queue.blocking_get(&msg)) {
            delete msg;
            msg = nullptr;
        } else {
            break;
        }
    }
    DCHECK(_queue.get_size() == 0);
}

Status PulsarDataConsumerGroup::start_all(StreamLoadContext* ctx) {
    Status result_st = Status::OK();
    // start all consumers
    for (auto& consumer : _consumers) {
        if (!_thread_pool.offer([this, consumer, capture0 = &_queue, capture1 = ctx->max_interval_s * 1000,
                                 capture2 = [this, &result_st](const Status& st) {
                                     std::unique_lock<std::mutex> lock(_mutex);
                                     _counter--;
                                     VLOG(2) << "group counter is: " << _counter << ", grp: " << _grp_id;
                                     if (_counter == 0) {
                                         _queue.shutdown();
                                         LOG(INFO)
                                                 << "all consumers are finished. shutdown queue. group id: " << _grp_id;
                                     }
                                     if (result_st.ok() && !st.ok()) {
                                         result_st = st;
                                     }
                                 }] { actual_consume(consumer, capture0, capture1, capture2); })) {
            LOG(WARNING) << "failed to submit data consumer: " << consumer->id() << ", group id: " << _grp_id;
            return Status::InternalError("failed to submit data consumer");
        } else {
            VLOG(2) << "submit a data consumer: " << consumer->id() << ", group id: " << _grp_id;
        }
    }

    // consuming from queue and put data to stream load pipe
    int64_t left_time = ctx->max_interval_s * 1000;
    int64_t received_rows = 0;
    int64_t left_bytes = ctx->max_batch_size;

    std::shared_ptr<PulsarConsumerPipe> pulsar_pipe = std::static_pointer_cast<PulsarConsumerPipe>(ctx->body_sink);

    LOG(INFO) << "start consumer group: " << _grp_id << ". max time(ms): " << left_time
              << ", batch size: " << left_bytes << ". " << ctx->brief();

    // copy one
    std::map<std::string, pulsar::MessageId> ack_offset = ctx->pulsar_info->ack_offset;

    // JSON: one message per buffer, carrying source metadata. CSV: rows separated by row_delimiter.
    // The Pulsar path only ever sees JSON or CSV: PulsarTaskInfo maps every non-json format (including
    // avro) to CSV_PLAIN, so the avro scanner never runs for Pulsar.
    const bool append_as_message = ctx->format == TFileFormatType::FORMAT_JSON;
    char row_delimiter = '\n';
    if (!append_as_message) {
        auto& per_node_scan_ranges = ctx->put_result.params.params.per_node_scan_ranges;

        if (!per_node_scan_ranges.empty()) {
            DCHECK_GE(per_node_scan_ranges.begin()->second.size(), 1);

            auto& scan_range = per_node_scan_ranges.begin()->second[0].scan_range;
            auto& params = scan_range.broker_scan_range.params;
            row_delimiter = static_cast<char>(params.row_delimiter);
        }
    }
    // Attach per-message source metadata only when the job references a metadata column; otherwise the
    // buffer stays metadata-free and the scanner reads every field from the payload.
    const bool need_meta = ctx->pulsar_info->need_source_metadata;
    // Only copy the (potentially large) partition key/properties when the job references __key__ / __headers__.
    const bool need_key = ctx->pulsar_info->need_message_key;
    const bool need_headers = ctx->pulsar_info->need_message_headers;

    MonotonicStopWatch watch;
    watch.start();
    bool eos = false;
    while (true) {
        if (eos || left_time <= 0 || left_bytes <= 0) {
            LOG(INFO) << "consumer group done: " << _grp_id
                      << ". consume time(ms)=" << ctx->max_interval_s * 1000 - left_time
                      << ", received rows=" << received_rows << ", received bytes=" << ctx->max_batch_size - left_bytes
                      << ", eos: " << eos << ", left_time: " << left_time << ", left_bytes: " << left_bytes
                      << ", blocking get time(us): " << _queue.total_get_wait_time() / 1000
                      << ", blocking put time(us): " << _queue.total_put_wait_time() / 1000;

            // shutdown queue
            _queue.shutdown();
            // cancel all consumers
            for (auto& consumer : _consumers) {
                (void)consumer->cancel(ctx);
            }

            // waiting all threads finished
            _thread_pool.shutdown();
            _thread_pool.join();

            if (!result_st.ok()) {
                // some consumers encounter errors, cancel this task
                return result_st;
            }

            if (left_bytes == ctx->max_batch_size) {
                // nothing to be consumed, we have to cancel it, because
                // we do not allow finishing stream load pipe without data
                pulsar_pipe->cancel(Status::Cancelled("Cancelled"));
                return Status::Cancelled("Cancelled");
            } else {
                DCHECK(left_bytes < ctx->max_batch_size);
                RETURN_IF_ERROR(pulsar_pipe->finish());
                ctx->pulsar_info->ack_offset = std::move(ack_offset);
                ctx->receive_bytes = ctx->max_batch_size - left_bytes;
                get_backlog_nums(ctx);
                return Status::OK();
            }
        }

        pulsar::Message* msg;
        bool res = _queue.blocking_get(&msg);
        if (res) {
            std::string partition = msg->getTopicName();
            pulsar::MessageId msg_id = msg->getMessageId();
            std::size_t len = msg->getLength();

            VLOG(3) << "get pulsar message"
                    << ", partition: " << partition << ", message id: " << msg_id << ", len: " << len;

            Status st;
            if (append_as_message) {
                StreamMessageMeta meta(ByteBufferMetaType::PULSAR);
                const StreamMessageMeta* meta_ptr = nullptr;
                if (need_meta) {
                    // pulsar_topic() exposes the configured logical topic; pulsar_partition() surfaces
                    // the index parsed out of the per-message "<topic>-partition-N" name. The full
                    // per-partition name (`partition`) stays the ack key.
                    meta.set_topic(ctx->pulsar_info->topic);
                    int32_t partition_index = parse_pulsar_partition_index(ctx->pulsar_info->topic, partition);
                    if (partition_index >= 0) {
                        meta.set_partition(partition_index);
                    }
                    meta.set_message_id(pulsar_message_id_to_string(msg_id));
                    meta.set_timestamp(static_cast<int64_t>(msg->getPublishTimestamp()));
                    // Pulsar uses 0 for an unset event time; map it to -1 so the column renders as NULL.
                    int64_t event_ts = static_cast<int64_t>(msg->getEventTimestamp());
                    meta.set_event_timestamp(event_ts > 0 ? event_ts : -1);
                    if (need_key && msg->hasPartitionKey()) {
                        meta.set_key(msg->getPartitionKey());
                    }
                    if (need_headers) {
                        for (const auto& kv : msg->getProperties()) {
                            meta.add_header(kv.first, kv.second);
                        }
                    }
                    meta_ptr = &meta;
                }
                st = pulsar_pipe->append_json(static_cast<const char*>(msg->getData()), static_cast<size_t>(len),
                                              row_delimiter, meta_ptr);
            } else {
                st = pulsar_pipe->append_with_row_delimiter(static_cast<const char*>(msg->getData()),
                                                            static_cast<size_t>(len), row_delimiter);
            }

            if (st.ok()) {
                received_rows++;
                left_bytes -= len;
                ack_offset[partition] = msg_id;
                VLOG(3) << "consume partition" << partition << " - " << msg_id;
            } else {
                // failed to append this msg, we must stop
                LOG(WARNING) << "failed to append msg to pipe. grp: " << _grp_id << ", errmsg=" << st.message();
                eos = true;
                {
                    std::unique_lock<std::mutex> lock(_mutex);
                    if (result_st.ok()) {
                        result_st = st;
                    }
                }
            }
            delete msg;
        } else {
            // queue is empty and shutdown
            eos = true;
        }

        left_time = ctx->max_interval_s * 1000 - watch.elapsed_time() / 1000 / 1000;
    }

    return Status::OK();
}

void PulsarDataConsumerGroup::actual_consume(const std::shared_ptr<DataConsumer>& consumer,
                                             TimedBlockingQueue<pulsar::Message*>* queue, int64_t max_running_time_ms,
                                             const ConsumeFinishCallback& cb) {
    Status st = std::static_pointer_cast<PulsarDataConsumer>(consumer)->group_consume(queue, max_running_time_ms);
    cb(st);
}

void PulsarDataConsumerGroup::get_backlog_nums(StreamLoadContext* ctx) {
    for (auto& consumer : _consumers) {
        // get backlog num
        int64_t backlog_num;
        Status st = std::static_pointer_cast<PulsarDataConsumer>(consumer)->get_partition_backlog(&backlog_num);
        if (!st.ok()) {
            LOG(WARNING) << st.message();
        } else {
            ctx->pulsar_info
                    ->partition_backlog[std::static_pointer_cast<PulsarDataConsumer>(consumer)->get_partition()] =
                    backlog_num;
        }
    }
}

} // namespace starrocks
