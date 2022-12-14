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

#include "runtime/routine_load/routine_load_task_executor.h"

#include <functional>
#include <memory>
#include <thread>

#include "common/status.h"
#include "runtime/routine_load/data_consumer_group.h"
#include "runtime/routine_load/kafka_consumer_pipe.h"
#include "runtime/stream_load/stream_load_context.h"
#include "util/defer_op.h"
#include "util/uid_util.h"

namespace starrocks {

Status RoutineLoadTaskExecutor::get_kafka_partition_meta(const PKafkaMetaProxyRequest& request,
                                                         std::vector<int32_t>* partition_ids, int timeout_ms) {
    DCHECK(request.has_kafka_info());

    // This context is meaningless, just for unifing the interface
    StreamLoadContext ctx(_exec_env);
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
    ctx.need_rollback = false;

    std::shared_ptr<DataConsumer> consumer;
    RETURN_IF_ERROR(_data_consumer_pool.get_consumer(&ctx, &consumer));

    Status st = std::static_pointer_cast<KafkaDataConsumer>(consumer)->get_partition_meta(partition_ids, timeout_ms);
    if (st.ok()) {
        _data_consumer_pool.return_consumer(consumer);
    }
    return st;
}

Status RoutineLoadTaskExecutor::get_kafka_partition_offset(const PKafkaOffsetProxyRequest& request,
                                                           std::vector<int64_t>* beginning_offsets,
                                                           std::vector<int64_t>* latest_offsets, int timeout_ms) {
    DCHECK(request.has_kafka_info());

    // This context is meaningless, just for unifing the interface
    StreamLoadContext ctx(_exec_env);
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
    ctx.need_rollback = false;

    // convert pb repeated value to vector
    std::vector<int32_t> partition_ids;
    partition_ids.reserve(request.partition_ids().size());
    for (auto p_id : request.partition_ids()) {
        partition_ids.push_back(p_id);
    }

    std::shared_ptr<DataConsumer> consumer;
    RETURN_IF_ERROR(_data_consumer_pool.get_consumer(&ctx, &consumer));

    Status st = std::static_pointer_cast<KafkaDataConsumer>(consumer)->get_partition_offset(
            &partition_ids, beginning_offsets, latest_offsets, timeout_ms);
    if (st.ok()) {
        _data_consumer_pool.return_consumer(consumer);
    }
    return st;
}

Status RoutineLoadTaskExecutor::get_pulsar_partition_meta(const PPulsarMetaProxyRequest& request,
                                                          std::vector<std::string>* partitions) {
    DCHECK(request.has_pulsar_info());

    // This context is meaningless, just for unifing the interface
    StreamLoadContext ctx(_exec_env);
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
    ctx.need_rollback = false;

    std::shared_ptr<DataConsumer> consumer;
    RETURN_IF_ERROR(_data_consumer_pool.get_consumer(&ctx, &consumer));

    Status st = std::static_pointer_cast<PulsarDataConsumer>(consumer)->get_topic_partition(partitions);
    if (st.ok()) {
        _data_consumer_pool.return_consumer(consumer);
    }
    return st;
}

Status RoutineLoadTaskExecutor::get_pulsar_partition_backlog(const PPulsarBacklogProxyRequest& request,
                                                             std::vector<int64_t>* backlog_num) {
    DCHECK(request.has_pulsar_info());

    // This context is meaningless, just for unifing the interface
    StreamLoadContext ctx(_exec_env);
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
    ctx.need_rollback = false;

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
        _data_consumer_pool.return_consumer(consumer);
    }

    return Status::OK();
}

Status RoutineLoadTaskExecutor::submit_task(const TRoutineLoadTask& task) {
    std::unique_lock<std::mutex> l(_lock);
    if (_task_map.find(task.id) != _task_map.end()) {
        // already submitted
        LOG(INFO) << "routine load task " << UniqueId(task.id) << " has already been submitted";
        return Status::OK();
    }

    if (_task_map.size() >= config::routine_load_thread_pool_size) {
        LOG(INFO) << "too many tasks in thread pool. reject task: " << UniqueId(task.id) << ", job id: " << task.job_id
                  << ", queue size: " << _thread_pool.get_queue_size() << ", current tasks num: " << _task_map.size();
        return Status::TooManyTasks(UniqueId(task.id).to_string());
    }

    // create the context
    auto* ctx = new StreamLoadContext(_exec_env);
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
    ctx->need_rollback = true;
    ctx->max_filter_ratio = 1.0;

    // set source related params
    switch (task.type) {
    case TLoadSourceType::KAFKA:
        ctx->kafka_info = std::make_unique<KafkaLoadInfo>(task.kafka_load_info);
        break;
    case TLoadSourceType::PULSAR:
        ctx->pulsar_info = std::make_unique<PulsarLoadInfo>(task.pulsar_load_info);
        break;
    default:
        LOG(WARNING) << "unknown load source type: " << task.type;
        delete ctx;
        return Status::InternalError("unknown load source type");
    }

    VLOG(1) << "receive a new routine load task: " << ctx->brief();
    // register the task
    ctx->ref();
    _task_map[ctx->id] = ctx;

    // offer the task to thread pool
    if (!_thread_pool.offer([this, ctx, capture0 = &_data_consumer_pool, capture1 = [this](StreamLoadContext* ctx) {
            std::unique_lock<std::mutex> l(_lock);
            _task_map.erase(ctx->id);
            LOG(INFO) << "finished routine load task " << ctx->brief() << ", status: " << ctx->status.get_error_msg()
                      << ", current tasks num: " << _task_map.size();
            if (ctx->unref()) {
                delete ctx;
            }
        }] { exec_task(ctx, capture0, capture1); })) {
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
#define HANDLE_ERROR(stmt, err_msg)                                                        \
    do {                                                                                   \
        Status _status_ = (stmt);                                                          \
        if (UNLIKELY(!_status_.ok() && _status_.code() != TStatusCode::PUBLISH_TIMEOUT)) { \
            err_handler(ctx, _status_, err_msg);                                           \
            cb(ctx);                                                                       \
            return;                                                                        \
        }                                                                                  \
    } while (false);

    LOG(INFO) << "begin to execute routine load task: " << ctx->brief();

    // create data consumer group
    std::shared_ptr<DataConsumerGroup> consumer_grp;
    HANDLE_ERROR(consumer_pool->get_consumer_grp(ctx, &consumer_grp), "failed to get consumers");

    // create and set pipe
    std::shared_ptr<StreamLoadPipe> pipe;
    switch (ctx->load_src_type) {
    case TLoadSourceType::KAFKA: {
        pipe = std::make_shared<KafkaConsumerPipe>();
        Status st = std::static_pointer_cast<KafkaDataConsumerGroup>(consumer_grp)->assign_topic_partitions(ctx);
        if (!st.ok()) {
            err_handler(ctx, st, st.get_error_msg());
            cb(ctx);
            return;
        }
        break;
    }
    case TLoadSourceType::PULSAR: {
        pipe = std::make_shared<PulsarConsumerPipe>();
        Status st = std::static_pointer_cast<PulsarDataConsumerGroup>(consumer_grp)->assign_topic_partitions(ctx);
        if (!st.ok()) {
            err_handler(ctx, st, st.get_error_msg());
            cb(ctx);
            return;
        }
        break;
    }
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
    HANDLE_ERROR(_exec_env->load_stream_mgr()->put(ctx->id, pipe), "failed to add pipe");

#ifndef BE_TEST
    // execute plan fragment, async
    HANDLE_ERROR(_exec_env->stream_load_executor()->execute_plan_fragment(ctx), "failed to execute plan fragment");
#else
    // only for test
    HANDLE_ERROR(_execute_plan_for_test(ctx), "test failed");
#endif

    // start to consume, this may block a while
    HANDLE_ERROR(consumer_grp->start_all(ctx), "consuming failed");

    // wait for all consumers finished
    HANDLE_ERROR(ctx->future.get(), "consume failed");

    ctx->load_cost_nanos = MonotonicNanos() - ctx->start_nanos;

    // return the consumer back to pool
    // call this before commit txn, in case the next task can come very fast
    consumer_pool->return_consumers(consumer_grp.get());

    // commit txn
    HANDLE_ERROR(_exec_env->stream_load_executor()->commit_txn(ctx), "commit failed");

    // commit messages
    switch (ctx->load_src_type) {
    case TLoadSourceType::KAFKA: {
        std::shared_ptr<DataConsumer> consumer;
        Status st = _data_consumer_pool.get_consumer(ctx, &consumer);
        if (!st.ok()) {
            // Kafka Offset Commit is idempotent, Failure should not block the normal process
            // So just print a warning
            LOG(WARNING) << st.get_error_msg();
            break;
        }

        std::vector<RdKafka::TopicPartition*> topic_partitions;
        for (auto& kv : ctx->kafka_info->cmt_offset) {
            RdKafka::TopicPartition* tp1 = RdKafka::TopicPartition::create(ctx->kafka_info->topic, kv.first, kv.second);
            topic_partitions.push_back(tp1);
        }

        st = std::static_pointer_cast<KafkaDataConsumer>(consumer)->commit(topic_partitions);
        if (!st.ok()) {
            // Kafka Offset Commit is idempotent, Failure should not block the normal process
            // So just print a warning
            LOG(WARNING) << st.get_error_msg();
        }
        _data_consumer_pool.return_consumer(consumer);

        // delete TopicPartition finally
        auto tp_deleter = [&topic_partitions]() {
            std::for_each(topic_partitions.begin(), topic_partitions.end(),
                          [](RdKafka::TopicPartition* tp1) { delete tp1; });
        };
        DeferOp delete_tp([tp_deleter] { return tp_deleter(); });
    } break;
    case TLoadSourceType::PULSAR: {
        for (auto& kv : ctx->pulsar_info->ack_offset) {
            Status st;
            // get consumer
            std::shared_ptr<DataConsumer> consumer;
            st = _data_consumer_pool.get_consumer(ctx, &consumer);
            if (!st.ok()) {
                // Pulsar Offset Acknowledgement is idempotent, Failure should not block the normal process
                // So just print a warning
                LOG(WARNING) << st.get_error_msg();
                break;
            }

            // assign partition for consumer
            st = std::static_pointer_cast<PulsarDataConsumer>(consumer)->assign_partition(kv.first, ctx);
            if (!st.ok()) {
                // Pulsar Offset Acknowledgement is idempotent, Failure should not block the normal process
                // So just print a warning
                LOG(WARNING) << st.get_error_msg();
            }

            // do ack
            st = std::static_pointer_cast<PulsarDataConsumer>(consumer)->acknowledge_cumulative(kv.second);
            if (!st.ok()) {
                // Pulsar Offset Acknowledgement is idempotent, Failure should not block the normal process
                // So just print a warning
                LOG(WARNING) << st.get_error_msg();
            }

            // return consumer
            _data_consumer_pool.return_consumer(consumer);
        }
    } break;
    default:
        return;
    }
    cb(ctx);
}

void RoutineLoadTaskExecutor::err_handler(StreamLoadContext* ctx, const Status& st, const std::string& err_msg) {
    LOG(WARNING) << err_msg;
    ctx->status = st;
    if (ctx->need_rollback) {
        _exec_env->stream_load_executor()->rollback_txn(ctx);
        ctx->need_rollback = false;
    }
    if (ctx->body_sink != nullptr) {
        ctx->body_sink->cancel(st);
    }
}

// for test only
Status RoutineLoadTaskExecutor::_execute_plan_for_test(StreamLoadContext* ctx) {
    ctx->ref();
    auto mock_consumer = [this, ctx]() {
        std::shared_ptr<StreamLoadPipe> pipe = _exec_env->load_stream_mgr()->get(ctx->id);
        bool eof = false;
        std::stringstream ss;
        while (true) {
            char one;
            size_t len = 1;
            Status st = pipe->read((uint8_t*)&one, &len, &eof);
            if (!st.ok()) {
                LOG(WARNING) << "read failed";
                ctx->promise.set_value(st);
                break;
            }

            if (eof) {
                ctx->promise.set_value(Status::OK());
                break;
            }

            if (one == '\n') {
                LOG(INFO) << "get line: " << ss.str();
                ss.str("");
                ctx->number_loaded_rows++;
            } else {
                ss << one;
            }
        }
        if (ctx->unref()) {
            delete ctx;
        }
    };

    std::thread t1(mock_consumer);
    t1.detach();
    return Status::OK();
}

} // namespace starrocks
