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
//   https://github.com/apache/incubator-doris/blob/master/be/src/runtime/routine_load/routine_load_task_executor.cpp

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

#include "orchestration/routine_load_task_executor.h"

#include <functional>
#include <memory>
#include <thread>
#include <utility>

#include "base/concurrency/stopwatch.hpp"
#include "base/uid_util.h"
#include "base/utility/defer_op.h"
#include "common/logging.h"
#include "common/status.h"
#include "compute_env/load/load_stream_mgr.h"
#include "compute_env/load/stream_load_context.h"
#include "data_workflows/load/routine_load/data_consumer_group.h"
#include "data_workflows/load/routine_load/kafka_consumer_pipe.h"
#include "data_workflows/load/stream_load/stream_load_executor.h"
#include "exec/exec_env.h"
#include "fmt/format.h"
#include "gen_cpp/PlanNodes_types.h"
#include "orchestration/stream_load_orchestrator.h"
#include "rapidjson/document.h"
#include "rapidjson/stringbuffer.h"
#include "rapidjson/writer.h"
#include "runtime/kafka_scan_range_report.h"
#include "runtime/runtime_metrics.h"

namespace starrocks::orchestration {

namespace {

void set_need_rollback(StreamLoadContext* ctx, StreamLoadExecutor* stream_load_executor) {
    ctx->set_need_rollback([stream_load_executor](StreamLoadContext* rollback_ctx) {
        return stream_load_executor->rollback_txn(rollback_ctx);
    });
}

} // namespace

// Build a JSON anchor that identifies the streaming source of a routine
// load fragment. Threaded into TQueryOptions.routine_load_source_info
// so BE's LoadPathStateHelper::append_rejected_record_to_file can populate
// _statistics_.rejected_records.source_info with the kafka topic/offsets
// instead of the hardcoded "stream-load-pipe" SequentialFile name.
std::string RoutineLoadTaskExecutor::kafka_source_info(const std::string& topic,
                                                       const std::map<int32_t, int64_t>& partition_begin_offset) {
    rapidjson::Document d;
    d.SetObject();
    auto& alloc = d.GetAllocator();
    d.AddMember("format", "kafka", alloc);
    d.AddMember("topic", rapidjson::Value(topic.c_str(), static_cast<rapidjson::SizeType>(topic.size()), alloc), alloc);
    rapidjson::Value parts(rapidjson::kArrayType);
    rapidjson::Value offsets(rapidjson::kArrayType);
    for (const auto& [partition, begin] : partition_begin_offset) {
        parts.PushBack(partition, alloc);
        offsets.PushBack(begin, alloc);
    }
    d.AddMember("partitions", parts, alloc);
    d.AddMember("begin_offsets", offsets, alloc);
    rapidjson::StringBuffer buf;
    rapidjson::Writer<rapidjson::StringBuffer> w(buf);
    d.Accept(w);
    return std::string(buf.GetString(), buf.GetSize());
}

Status RoutineLoadTaskExecutor::init(MetricRegistry* metrics) {
    DCHECK(_stream_load_orchestrator != nullptr);
    DCHECK(_stream_load_executor != nullptr);

    if (metrics != nullptr) {
        REGISTER_GAUGE_RUNTIME_METRIC(metrics, routine_load_task_count, [this]() {
            std::lock_guard<std::mutex> l(_lock);
            return _task_map.size() + _kafka_scan_range_task_count->load(std::memory_order_relaxed);
        })
    }

    auto st = ThreadPoolBuilder("routine_load")
                      .set_min_threads(0)
                      .set_max_threads(INT_MAX)
                      .set_max_queue_size(INT_MAX)
                      .build(&_thread_pool);
    RETURN_IF_ERROR(st);

    _data_consumer_pool.start_bg_worker();
    return Status::OK();
}

void RoutineLoadTaskExecutor::stop() {
    _data_consumer_pool.stop();

    if (_thread_pool) {
        _thread_pool->shutdown();
    }

    for (auto& it : _task_map) {
        auto ctx = it.second;
        if (ctx->unref()) {
            delete ctx;
        }
    }
    _task_map.clear();
}

Status RoutineLoadTaskExecutor::get_kafka_partition_meta(
        const PKafkaMetaProxyRequest& request, std::vector<int32_t>* partition_ids, int timeout_ms,
        std::string* group_id, std::vector<std::shared_ptr<DataConsumer>>* evicted_consumers) {
    DCHECK(request.has_kafka_info());

    // This context is meaningless, just for unifing the interface
    StreamLoadContext ctx(_exec_env->load_stream_mgr());
    ctx.load_type = TLoadType::ROUTINE_LOAD;
    ctx.load_src_type = TLoadSourceType::KAFKA;
    ctx.label = "NaN";

    // convert PKafkaInfo to TKafkaLoadInfo
    TKafkaLoadInfo t_info;
    t_info.brokers = request.kafka_info().brokers();
    t_info.topic = request.kafka_info().topic();
    std::map<std::string, std::string> properties;
    for (int i = 0; i < request.kafka_info().properties_size(); ++i) {
        const PStringPair& pair = request.kafka_info().properties(i);
        properties.emplace(pair.key(), pair.val());
    }
    t_info.__set_properties(properties);

    ctx.kafka_info = std::make_unique<KafkaLoadInfo>(t_info);

    std::shared_ptr<DataConsumer> consumer;
    RETURN_IF_ERROR(_data_consumer_pool.get_consumer(&ctx, &consumer));
    auto it = ctx.kafka_info->properties.find("group.id");
    if (it == ctx.kafka_info->properties.end()) {
        *group_id = "unknown";
    } else {
        *group_id = it->second;
    }

    Status st = std::static_pointer_cast<KafkaDataConsumer>(consumer)->get_partition_meta(partition_ids, timeout_ms);
    // if get partition meta failed, should not return consumer because the KafkaConsumer may be closed.
    if (st.ok()) {
        for (auto& evicted : _data_consumer_pool.return_consumer(consumer)) {
            evicted_consumers->push_back(std::move(evicted));
        }
    }
    return st;
}

Status RoutineLoadTaskExecutor::get_kafka_partition_offset(
        const PKafkaOffsetProxyRequest& request, std::vector<int64_t>* beginning_offsets,
        std::vector<int64_t>* latest_offsets, int timeout_ms, std::string* group_id,
        std::vector<std::shared_ptr<DataConsumer>>* evicted_consumers) {
    DCHECK(request.has_kafka_info());

    // This context is meaningless, just for unifing the interface
    StreamLoadContext ctx(_exec_env->load_stream_mgr());
    ctx.load_type = TLoadType::ROUTINE_LOAD;
    ctx.load_src_type = TLoadSourceType::KAFKA;
    ctx.label = "NaN";

    // convert PKafkaInfo to TKafkaLoadInfo
    TKafkaLoadInfo t_info;
    t_info.brokers = request.kafka_info().brokers();
    t_info.topic = request.kafka_info().topic();
    std::map<std::string, std::string> properties;
    for (int i = 0; i < request.kafka_info().properties_size(); ++i) {
        const PStringPair& pair = request.kafka_info().properties(i);
        properties.emplace(pair.key(), pair.val());
    }
    t_info.__set_properties(properties);

    ctx.kafka_info = std::make_unique<KafkaLoadInfo>(t_info);

    // convert pb repeated value to vector
    std::vector<int32_t> partition_ids;
    partition_ids.reserve(request.partition_ids().size());
    for (auto p_id : request.partition_ids()) {
        partition_ids.push_back(p_id);
    }

    std::shared_ptr<DataConsumer> consumer;
    RETURN_IF_ERROR(_data_consumer_pool.get_consumer(&ctx, &consumer));
    auto it = ctx.kafka_info->properties.find("group.id");
    if (it == ctx.kafka_info->properties.end()) {
        *group_id = "unknown";
    } else {
        *group_id = it->second;
    }
    MonotonicStopWatch watch;
    watch.start();
    Status st = std::static_pointer_cast<KafkaDataConsumer>(consumer)->get_partition_offset(
            &partition_ids, beginning_offsets, latest_offsets, timeout_ms);
    if (st.ok()) {
        for (auto& evicted : _data_consumer_pool.return_consumer(consumer)) {
            evicted_consumers->push_back(std::move(evicted));
        }
    }
    return st;
}

Status RoutineLoadTaskExecutor::commit_kafka_offsets(const PKafkaCommitOffsetProxyRequest& request, int timeout_ms,
                                                     std::string* group_id,
                                                     std::vector<std::shared_ptr<DataConsumer>>* evicted_consumers) {
    if (!request.has_kafka_info()) {
        return Status::InvalidArgument("kafka_info is not set in the kafka commit offset request");
    }
    if (request.partition_ids_size() != request.offsets_size()) {
        return Status::InvalidArgument(fmt::format("kafka commit offset request has {} partitions but {} offsets",
                                                   request.partition_ids_size(), request.offsets_size()));
    }

    // This context is meaningless, just for unifing the interface
    StreamLoadContext ctx(_exec_env->load_stream_mgr());
    ctx.load_type = TLoadType::ROUTINE_LOAD;
    ctx.load_src_type = TLoadSourceType::KAFKA;
    ctx.label = "NaN";

    // convert PKafkaInfo to TKafkaLoadInfo
    TKafkaLoadInfo t_info;
    t_info.brokers = request.kafka_info().brokers();
    t_info.topic = request.kafka_info().topic();
    std::map<std::string, std::string> properties;
    for (int i = 0; i < request.kafka_info().properties_size(); ++i) {
        const PStringPair& pair = request.kafka_info().properties(i);
        properties.emplace(pair.key(), pair.val());
    }
    t_info.__set_properties(properties);

    ctx.kafka_info = std::make_unique<KafkaLoadInfo>(t_info);

    auto it = ctx.kafka_info->properties.find("group.id");
    if (it == ctx.kafka_info->properties.end()) {
        *group_id = "unknown";
    } else {
        *group_id = it->second;
    }

    // The offsets are the last consumed ones, which KafkaDataConsumer::commit() advances by one.
    std::map<int32_t, int64_t> offsets;
    for (int i = 0; i < request.partition_ids_size(); ++i) {
        offsets[request.partition_ids(i)] = request.offsets(i);
    }
    if (offsets.empty()) {
        return Status::OK();
    }

    std::shared_ptr<DataConsumer> consumer;
    RETURN_IF_ERROR(_data_consumer_pool.get_consumer_for_commit(&ctx, &consumer));
    // The RPC thread that runs the commit is shared, so the commit waits for the broker at most the time the request
    // has left.
    auto* kafka_consumer = static_cast<KafkaDataConsumer*>(consumer.get());
    Status st = kafka_consumer->commit(ctx.kafka_info->topic, offsets, timeout_ms);
    // As after the commit of a classic task, the consumer goes back to the pool whatever the commit did. The pool keeps
    // it apart while a commit of it is still on its way, so that no round consumes with it and no RPC thread closes it.
    for (auto& evicted : _data_consumer_pool.return_consumer(consumer)) {
        evicted_consumers->push_back(std::move(evicted));
    }
    return st;
}

#ifndef __APPLE__
Status RoutineLoadTaskExecutor::get_pulsar_partition_meta(
        const PPulsarMetaProxyRequest& request, std::vector<std::string>* partitions,
        std::vector<std::shared_ptr<DataConsumer>>* evicted_consumers) {
    DCHECK(request.has_pulsar_info());

    // This context is meaningless, just for unifing the interface
    StreamLoadContext ctx(_exec_env->load_stream_mgr());
    ctx.load_type = TLoadType::ROUTINE_LOAD;
    ctx.load_src_type = TLoadSourceType::PULSAR;
    ctx.label = "NaN";

    // convert PPulsarInfo to TPulsarLoadInfo
    TPulsarLoadInfo t_info;
    t_info.service_url = request.pulsar_info().service_url();
    t_info.topic = request.pulsar_info().topic();
    t_info.subscription = request.pulsar_info().subscription();
    std::map<std::string, std::string> properties;
    for (int i = 0; i < request.pulsar_info().properties_size(); ++i) {
        const PStringPair& pair = request.pulsar_info().properties(i);
        properties.emplace(pair.key(), pair.val());
    }
    t_info.__set_properties(properties);

    ctx.pulsar_info = std::make_unique<PulsarLoadInfo>(t_info);

    std::shared_ptr<DataConsumer> consumer;
    RETURN_IF_ERROR(_data_consumer_pool.get_consumer(&ctx, &consumer));

    Status st = std::static_pointer_cast<PulsarDataConsumer>(consumer)->get_topic_partition(partitions);
    if (st.ok()) {
        for (auto& evicted : _data_consumer_pool.return_consumer(consumer)) {
            evicted_consumers->push_back(std::move(evicted));
        }
    }
    return st;
}
#endif

#ifndef __APPLE__
Status RoutineLoadTaskExecutor::get_pulsar_partition_backlog(
        const PPulsarBacklogProxyRequest& request, std::vector<int64_t>* backlog_num,
        std::vector<std::shared_ptr<DataConsumer>>* evicted_consumers) {
    DCHECK(request.has_pulsar_info());

    // This context is meaningless, just for unifing the interface
    StreamLoadContext ctx(_exec_env->load_stream_mgr());
    ctx.load_type = TLoadType::ROUTINE_LOAD;
    ctx.load_src_type = TLoadSourceType::PULSAR;
    ctx.label = "NaN";

    // convert PPulsarInfo to TPulsarLoadInfo
    TPulsarLoadInfo t_info;
    t_info.service_url = request.pulsar_info().service_url();
    t_info.topic = request.pulsar_info().topic();
    t_info.subscription = request.pulsar_info().subscription();
    std::map<std::string, std::string> properties;
    for (int i = 0; i < request.pulsar_info().properties_size(); ++i) {
        const PStringPair& pair = request.pulsar_info().properties(i);
        properties.emplace(pair.key(), pair.val());
    }
    t_info.__set_properties(properties);

    ctx.pulsar_info = std::make_unique<PulsarLoadInfo>(t_info);

    // convert pb repeated value to vector
    std::vector<std::string> partitions;
    partitions.reserve(request.partitions().size());
    for (const auto& p : request.partitions()) {
        partitions.push_back(p);
    }

    backlog_num->reserve(partitions.size());

    Status st;
    std::shared_ptr<DataConsumer> consumer;
    RETURN_IF_ERROR(_data_consumer_pool.get_consumer(&ctx, &consumer));
    for (const auto& p : partitions) {
        int64_t backlog = 0;
        RETURN_IF_ERROR(std::static_pointer_cast<PulsarDataConsumer>(consumer)->assign_partition(p, &ctx));
        st = std::static_pointer_cast<PulsarDataConsumer>(consumer)->get_partition_backlog(&backlog);
        std::static_pointer_cast<PulsarDataConsumer>(consumer).reset();
        backlog_num->push_back(backlog);
    }

    if (st.ok()) {
        for (auto& evicted : _data_consumer_pool.return_consumer(consumer)) {
            evicted_consumers->push_back(std::move(evicted));
        }
    }

    return Status::OK();
}
#endif

Status RoutineLoadTaskExecutor::submit_task(const TRoutineLoadTask& task) {
    std::unique_lock<std::mutex> l(_lock);
    if (_task_map.find(task.id) != _task_map.end()) {
        // already submitted
        LOG(INFO) << "routine load task " << UniqueId(task.id) << " has already been submitted";
        return Status::OK();
    }

    // create the context
    auto* ctx = new StreamLoadContext(_exec_env->load_stream_mgr());
    ctx->load_type = TLoadType::ROUTINE_LOAD;
    ctx->load_src_type = task.type;
    ctx->job_id = task.job_id;
    ctx->id = UniqueId(task.id);
    ctx->txn_id = task.txn_id;
    ctx->db = task.db;
    ctx->table = task.tbl;
    ctx->label = task.label;
    ctx->auth.auth_code = task.auth_code;

    if (task.__isset.max_interval_s) {
        ctx->max_interval_s = task.max_interval_s;
    }
    if (task.__isset.max_batch_rows) {
        ctx->max_batch_rows = task.max_batch_rows;
    }
    if (task.__isset.max_batch_size) {
        ctx->max_batch_size = task.max_batch_size;
    }

    // set execute plan params
    TStreamLoadPutResult put_result;
    TStatus tstatus;
    tstatus.status_code = TStatusCode::OK;
    put_result.status = tstatus;
    put_result.params = task.params;
    put_result.__isset.params = true;
    ctx->put_result = put_result;
    if (task.__isset.format) {
        ctx->format = task.format;
    }
    // the routine load task'txn has alreay began in FE.
    // so it need to rollback if encounter error.
    set_need_rollback(ctx, _stream_load_executor);
    if (task.__isset.max_filter_ratio) {
        ctx->max_filter_ratio = task.max_filter_ratio;
    } else {
        ctx->max_filter_ratio = 1.0;
    }

    // set source related params
    switch (task.type) {
    case TLoadSourceType::KAFKA:
        ctx->kafka_info = std::make_unique<KafkaLoadInfo>(task.kafka_load_info);
        // Make the kafka topic/partitions/offsets visible to the fragment's
        // RuntimeState so rejected_records.source_info points back at the
        // real source instead of the SequentialFile placeholder name.
        ctx->put_result.params.query_options.__set_routine_load_source_info(
                kafka_source_info(task.kafka_load_info.topic, task.kafka_load_info.partition_begin_offset));
        break;
#ifndef __APPLE__
    case TLoadSourceType::PULSAR:
        ctx->pulsar_info = std::make_unique<PulsarLoadInfo>(task.pulsar_load_info);
        break;
#endif
    default:
        LOG(WARNING) << "unknown load source type: " << task.type;
        delete ctx;
        return Status::InternalError("unknown load source type");
    }

    VLOG(2) << "receive a new routine load task: " << ctx->brief();
    // register the task
    ctx->ref();
    _task_map[ctx->id] = ctx;

    // offer the task to thread pool
    if (!_thread_pool
                 ->submit_func([this, ctx, capture0 = &_data_consumer_pool,
                                capture1 =
                                        [this](StreamLoadContext* ctx) {
                                            std::unique_lock<std::mutex> l(_lock);
                                            _task_map.erase(ctx->id);
                                            LOG(INFO) << "finished routine load task " << ctx->brief()
                                                      << ", status: " << ctx->status.message()
                                                      << ", current tasks num: " << _task_map.size();
                                            if (ctx->unref()) {
                                                delete ctx;
                                            }
                                        }] { exec_task(ctx, capture0, capture1); })
                 .ok()) {
        // failed to submit task, clear and return
        LOG(WARNING) << "failed to submit routine load task: " << ctx->brief();
        _task_map.erase(ctx->id);
        if (ctx->unref()) {
            delete ctx;
        }
        return Status::InternalError("failed to submit routine load task");
    } else {
        LOG(INFO) << "submit a new routine load task: " << ctx->brief() << ", current tasks num: " << _task_map.size();
        return Status::OK();
    }
}

void RoutineLoadTaskExecutor::exec_task(StreamLoadContext* ctx, DataConsumerPool* consumer_pool,
                                        const ExecFinishCallback& cb) {
#define HANDLE_ERROR(stmt, err_msg)                                       \
    do {                                                                  \
        Status _status_ = (stmt);                                         \
        if (UNLIKELY(!_status_.ok() && !_status_.is_publish_timeout())) { \
            err_handler(ctx, _status_, err_msg);                          \
            cb(ctx);                                                      \
            return;                                                       \
        }                                                                 \
    } while (false);

    LOG(INFO) << "begin to execute routine load task: " << ctx->brief();

    // create data consumer group
    std::shared_ptr<DataConsumerGroup> consumer_grp;
    HANDLE_ERROR(consumer_pool->get_consumer_grp(ctx, &consumer_grp), "failed to get consumers")

    // create and set pipe
    std::shared_ptr<StreamLoadPipe> pipe;
    switch (ctx->load_src_type) {
    case TLoadSourceType::KAFKA: {
        pipe = std::make_shared<KafkaConsumerPipe>();
        Status st = std::static_pointer_cast<KafkaDataConsumerGroup>(consumer_grp)->assign_topic_partitions(ctx);
        if (!st.ok()) {
            err_handler(ctx, st, st.message());
            cb(ctx);
            return;
        }
        break;
    }
#ifndef __APPLE__
    case TLoadSourceType::PULSAR: {
        pipe = std::make_shared<PulsarConsumerPipe>();
        Status st = std::static_pointer_cast<PulsarDataConsumerGroup>(consumer_grp)->assign_topic_partitions(ctx);
        if (!st.ok()) {
            err_handler(ctx, st, st.message());
            cb(ctx);
            return;
        }
        break;
    }
#endif
    default: {
        std::stringstream ss;
        ss << "unknown routine load task type: " << ctx->load_type;
        err_handler(ctx, Status::Cancelled("Cancelled"), ss.str());
        cb(ctx);
        return;
    }
    }
    ctx->body_sink = pipe;

    // must put pipe before executing plan fragment
    HANDLE_ERROR(_exec_env->load_stream_mgr()->put(ctx->id, pipe), "failed to add pipe")

    // execute plan fragment, async
    HANDLE_ERROR(_stream_load_orchestrator->execute_plan_fragment(ctx), "failed to execute plan fragment")

    // start to consume, this may block a while
    HANDLE_ERROR(consumer_grp->start_all(ctx), "consuming failed")

    // wait for all consumers finished
    HANDLE_ERROR(ctx->future.get(), "consume failed")

    ctx->load_cost_nanos = MonotonicNanos() - ctx->start_nanos;

    // return the consumer back to pool
    // call this before commit txn, in case the next task can come very fast
    // Keep evicted consumers alive through commit/rollback and the completion callback. Their close may wait for
    // another job's pending Kafka offset commit, which must not delay this task's transaction handling.
    auto evicted_consumers = consumer_pool->return_consumers(consumer_grp.get());

    // commit txn
    HANDLE_ERROR(_stream_load_executor->commit_txn(ctx), "commit failed")

    // commit messages
    switch (ctx->load_src_type) {
    case TLoadSourceType::KAFKA: {
        std::shared_ptr<DataConsumer> consumer;
        Status st = _data_consumer_pool.get_consumer(ctx, &consumer);
        if (!st.ok()) {
            // Kafka Offset Commit is idempotent, Failure should not block the normal process
            // So just print a warning
            LOG(WARNING) << st.message();
            break;
        }

        st = std::static_pointer_cast<KafkaDataConsumer>(consumer)->commit(ctx->kafka_info->topic,
                                                                           ctx->kafka_info->cmt_offset);
        if (!st.ok()) {
            // Kafka Offset Commit is idempotent, Failure should not block the normal process
            // So just print a warning
            LOG(WARNING) << st.message();
        }
        for (auto& evicted : _data_consumer_pool.return_consumer(consumer)) {
            evicted_consumers.push_back(std::move(evicted));
        }

    } break;
    case TLoadSourceType::PULSAR: {
#ifndef __APPLE__
        for (auto& kv : ctx->pulsar_info->ack_offset) {
            Status st;
            // get consumer
            std::shared_ptr<DataConsumer> consumer;
            st = _data_consumer_pool.get_consumer(ctx, &consumer);
            if (!st.ok()) {
                // Pulsar Offset Acknowledgement is idempotent, Failure should not block the normal process
                // So just print a warning
                LOG(WARNING) << st.message();
                break;
            }

            // assign partition for consumer
            st = std::static_pointer_cast<PulsarDataConsumer>(consumer)->assign_partition(kv.first, ctx);
            if (!st.ok()) {
                // Pulsar Offset Acknowledgement is idempotent, Failure should not block the normal process
                // So just print a warning
                LOG(WARNING) << st.message();
            }

            // do ack
            st = std::static_pointer_cast<PulsarDataConsumer>(consumer)->acknowledge_cumulative(kv.second);
            if (!st.ok()) {
                // Pulsar Offset Acknowledgement is idempotent, Failure should not block the normal process
                // So just print a warning
                LOG(WARNING) << st.message();
            }

            // return consumer
            for (auto& evicted : _data_consumer_pool.return_consumer(consumer)) {
                evicted_consumers.push_back(std::move(evicted));
            }
        }
#endif
    } break;
    default:
        return;
    }
    cb(ctx);
}

void RoutineLoadTaskExecutor::err_handler(StreamLoadContext* ctx, const Status& st, std::string_view err_msg) {
    LOG(WARNING) << err_msg << " " << ctx->brief();
    ctx->status = st;
    if (ctx->need_rollback()) {
        (void)_stream_load_executor->rollback_txn(ctx);
        ctx->clear_need_rollback();
    }
    if (ctx->body_sink != nullptr) {
        ctx->body_sink->cancel(st);
    }
}

StatusOr<StreamLoadContext*> RoutineLoadTaskExecutor::prepare_kafka_scan_range(const TBrokerScanRange& scan_range) {
    if (scan_range.ranges.size() != 1 || scan_range.ranges[0].file_type != TFileType::FILE_STREAM ||
        !scan_range.ranges[0].__isset.load_id) {
        return Status::InvalidArgument("a kafka scan range must have exactly one stream range with a load id");
    }
    const TBrokerRangeDesc& range = scan_range.ranges[0];
    const TKafkaScanRange& kafka_scan_range = scan_range.kafka_scan_range;
    if (kafka_scan_range.partition_begin_offset.empty()) {
        return Status::InvalidArgument(
                fmt::format("kafka scan range {} has no partition to consume", print_id(range.load_id)));
    }

    TKafkaLoadInfo kafka_load_info;
    kafka_load_info.brokers = kafka_scan_range.brokers;
    kafka_load_info.topic = kafka_scan_range.topic;
    kafka_load_info.partition_begin_offset = kafka_scan_range.partition_begin_offset;
    kafka_load_info.__set_properties(kafka_scan_range.properties);
    kafka_load_info.__set_need_source_metadata(kafka_scan_range.__isset.need_source_metadata &&
                                               kafka_scan_range.need_source_metadata);
    kafka_load_info.__set_need_message_key(kafka_scan_range.__isset.need_message_key &&
                                           kafka_scan_range.need_message_key);
    kafka_load_info.__set_need_message_headers(kafka_scan_range.__isset.need_message_headers &&
                                               kafka_scan_range.need_message_headers);

    // The pipe is read by a pipeline scan operator, which must not block on Kafka: reading it empty returns at once,
    // and the scan waits for it to become readable (StreamLoadPipe::notify_when_readable) instead of polling.
    auto pipe = std::make_shared<KafkaConsumerPipe>(true, 0, DEFAULT_STREAM_LOAD_PIPE_BUFFERED_BYTES,
                                                    DEFAULT_STREAM_LOAD_PIPE_CHUNK_SIZE);
    // Register the pipe before creating the context: a context removes its id from the manager when destroyed,
    // which must not unregister the pipe of whoever registered the same id first.
    LoadStreamMgr* load_stream_mgr = _exec_env->load_stream_mgr();
    Status st = load_stream_mgr->put(range.load_id, pipe);
    if (!st.ok()) {
        return Status::InternalError(fmt::format("failed to register the pipe of kafka scan range {}: {}",
                                                 print_id(range.load_id), st.message()));
    }

    auto* ctx = new StreamLoadContext(range.load_id, load_stream_mgr);
    ctx->ref();
    ctx->load_type = TLoadType::ROUTINE_LOAD;
    ctx->load_src_type = TLoadSourceType::KAFKA;
    ctx->job_id = kafka_scan_range.__isset.job_id ? kafka_scan_range.job_id : -1;
    // Only for logs: the FE owns the transaction of the round.
    const TBrokerScanRangeParams& params = scan_range.params;
    if (params.__isset.db_name) {
        ctx->db = params.db_name;
    }
    if (params.__isset.table_name) {
        ctx->table = params.table_name;
    }
    if (params.__isset.label) {
        ctx->label = params.label;
    }
    if (params.__isset.txn_id) {
        ctx->txn_id = params.txn_id;
    }
    ctx->format = range.format_type;
    ctx->kafka_info = std::make_unique<KafkaLoadInfo>(kafka_load_info);
    if (kafka_scan_range.__isset.max_consume_ms) {
        ctx->max_interval_ms = kafka_scan_range.max_consume_ms;
    }
    if (kafka_scan_range.__isset.max_consume_bytes && kafka_scan_range.max_consume_bytes > 0) {
        ctx->max_batch_size = kafka_scan_range.max_consume_bytes;
    }
    if (kafka_scan_range.__isset.idle_end_ms && kafka_scan_range.idle_end_ms >= 0) {
        ctx->kafka_idle_end_ms = kafka_scan_range.idle_end_ms;
    }
    ctx->kafka_row_delimiter = static_cast<char>(params.row_delimiter);
    ctx->kafka_scan_range_mode = true;
    ctx->body_sink = pipe;
    return ctx;
}

Status RoutineLoadTaskExecutor::start_kafka_scan_range(StreamLoadContext* ctx,
                                                       std::shared_ptr<KafkaScanRangeReport> report,
                                                       CancelFragmentCallback cancel_fragment) {
    ctx->kafka_consumed_callback = [report = std::move(report)](StreamLoadContext* consumed_ctx) {
        report->merge(consumed_ctx->kafka_info->cmt_offset, consumed_ctx->kafka_info->cmt_offset_timestamp,
                      consumed_ctx->receive_bytes, consumed_ctx->receive_rows, consumed_ctx->kafka_reached_end);
    };
    // The consumption holds its own reference since it may outlive the fragment. Owning it through the task also
    // drops it when a shutdown discards the task before it runs.
    ctx->ref();
    // The same reference owns its task count through success, failure and being discarded during shutdown. A rejected
    // submission drops the reference here, so every increment has exactly one decrement.
    auto task_count = _kafka_scan_range_task_count;
    task_count->fetch_add(1, std::memory_order_relaxed);
    std::shared_ptr<StreamLoadContext> ctx_ref(ctx, [task_count](StreamLoadContext* context) {
        StreamLoadContext::release(context);
        task_count->fetch_sub(1, std::memory_order_relaxed);
    });
    Status st = _thread_pool->submit_func([this, ctx_ref, cancel_fragment = std::move(cancel_fragment)] {
        consume_kafka_scan_range(ctx_ref.get(), cancel_fragment);
    });
    if (!st.ok()) {
        LOG(WARNING) << "failed to submit the consumption of kafka scan range: " << ctx->brief() << ", " << st;
        return st;
    }
    return Status::OK();
}

void RoutineLoadTaskExecutor::consume_kafka_scan_range(StreamLoadContext* ctx,
                                                       const CancelFragmentCallback& cancel_fragment) {
    auto pipe = std::static_pointer_cast<KafkaConsumerPipe>(ctx->body_sink);
    // The fragment cancels the pipe when it fails or is cancelled, also while the consumption waits for a thread:
    // nothing reads the pipe then, so the consumption takes no consumers and assigns no partitions for it.
    Status cancelled = pipe->cancel_status();
    if (!cancelled.ok()) {
        LOG(INFO) << "skip consuming kafka scan range of a cancelled pipe: " << ctx->brief() << ", " << cancelled;
        return;
    }
    LOG(INFO) << "begin to consume kafka scan range: " << ctx->brief(true);

    std::shared_ptr<DataConsumerGroup> consumer_grp;
    Status st = _data_consumer_pool.get_consumer_grp(ctx, &consumer_grp);
    if (st.ok()) {
        st = std::static_pointer_cast<KafkaDataConsumerGroup>(consumer_grp)->assign_topic_partitions(ctx);
    }
    if (st.ok()) {
        st = consumer_grp->start_all(ctx);
    }
    if (!st.ok()) {
        // A consumption that stopped because its pipe was cancelled has nothing to fail: the fragment failed, was
        // cancelled or is going away with its query, which the consumption does not call back into.
        cancelled = pipe->cancel_status();
        if (!cancelled.ok()) {
            LOG(INFO) << "kafka scan range stopped by its cancelled pipe: " << ctx->brief() << ", " << cancelled;
            return;
        }
        LOG(WARNING) << "failed to consume kafka scan range: " << ctx->brief() << ", error: " << st;
        // Fail the fragment before cancelling the pipe, so that it fails with this very error instead of what its
        // scanner makes of the cancelled pipe: the FE derives the pause reason of the job from the message.
        if (cancel_fragment) {
            cancel_fragment(st);
        }
        ctx->body_sink->cancel(st);
        return;
    }

    // A consumer that failed may be closed, so the consumers only go back to the pool after a success. The ones the
    // pool evicts close when this function returns: the pipe has finished by then, so waiting for their brokers
    // holds up no fragment or commit of the round.
    [[maybe_unused]] auto evicted_consumers = _data_consumer_pool.return_consumers(consumer_grp.get());
    LOG(INFO) << "finished consuming kafka scan range: " << ctx->brief() << ", received rows: " << ctx->receive_rows
              << ", received bytes: " << ctx->receive_bytes;
}

} // namespace starrocks::orchestration
