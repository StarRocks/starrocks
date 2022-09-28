// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.
#include "runtime/routine_load/routine_load_executor.h"

#include <functional>
#include <memory>
#include <thread>

#include "common/status.h"
#include "runtime/routine_load/data_consumer_group.h"
#include "runtime/routine_load/kafka_consumer_pipe.h"
#include "runtime/stream_load/stream_load_context.h"

namespace starrocks {

Status RoutineLoadExecutor::submit_task(StreamLoadContext* ctx) {
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

void RoutineLoadExecutor::exec_task(StreamLoadContext* ctx, DataConsumerPool* consumer_pool,
                                        const ExecFinishCallback& cb) {
#define HANDLE_ERROR(stmt, err_msg)                                                        \
    do {                                                                                   \
        Status _status = (stmt);                                                           \
        std::string _err_msg = err_msg;                                                    \
        if (UNLIKELY(!_status.ok() && _status.code() != TStatusCode::PUBLISH_TIMEOUT)) { \
            LOG(WARNING) << "status: " << _status << " msg: " << _err_msg;                 \
            cb(ctx);                                                                       \
            return;                                                                        \
        }                                                                                  \
    } while (false);

    LOG(INFO) << "begin to execute routine load: " << ctx->brief();

    // create data consumer group
    std::shared_ptr<DataConsumerGroup> consumer_grp;
    HANDLE_ERROR(consumer_pool->get_consumer_grp(ctx, &consumer_grp), "failed to get consumers");

    // create and set pipe
    std::shared_ptr<StreamLoadPipe> pipe;
    switch (ctx->load_src_type) {
    case TLoadSourceType::KAFKA: {
        pipe = std::make_shared<KafkaConsumerPipe>();
        HANDLE_ERROR(std::static_pointer_cast<KafkaDataConsumerGroup>(consumer_grp)->assign_topic_partitions(ctx), "assign topic partition failed");
        break;
    }
    default: {
        LOG(WARNING) << "unknown routine load task type: " << ctx->load_type;
        cb(ctx);
        return;
    }
    }

    // start to consume, this may block a while
    HANDLE_ERROR(consumer_grp->start_all(ctx), "consuming failed");

    // wait for all consumers finished
    HANDLE_ERROR(ctx->future.get(), "consume failed");

    ctx->load_cost_nanos = MonotonicNanos() - ctx->start_nanos;

    // return the consumer back to pool
    // call this before commit txn, in case the next task can come very fast
    consumer_pool->return_consumers(consumer_grp.get());
}

Status RoutineLoadExecutor::commit_offset(StreamLoadContext* ctx) {
    std::shared_ptr<DataConsumer> consumer;
    RETURN_IF_ERROR(_data_consumer_pool.get_consumer(ctx, &consumer));

    std::vector<RdKafka::TopicPartition*> topic_partitions;
    for (auto& kv : ctx->kafka_info->cmt_offset) {
        RdKafka::TopicPartition* tp1 = RdKafka::TopicPartition::create(ctx->kafka_info->topic, kv.first, kv.second);
        topic_partitions.push_back(tp1);
    }

    RETURN_IF_ERROR(std::static_pointer_cast<KafkaDataConsumer>(consumer)->commit(topic_partitions));
}

} // namespace starrocks
