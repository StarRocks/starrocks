// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.
#include "runtime/routine_load/routine_load_executor.h"

#include <functional>
#include <memory>
#include <thread>

#include "common/status.h"
#include "runtime/routine_load/data_consumer_group.h"
#include "runtime/routine_load/kafka_consumer_pipe.h"
#include "runtime/stream_load/stream_load_context.h"
#include "util/defer_op.h"

namespace starrocks {

StatusOr<StreamLoadContext*> RoutineLoadExecutor::submit_task(const TRoutineLoadTaskV2& task) {
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
    ctx->need_rollback = false;

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
    put_result.__isset.params = false;
    ctx->put_result = put_result;
    if (task.__isset.format) {
        ctx->format = task.format;
    }
    // the routine load task'txn has alreay began in FE.
    // so it need to rollback if encounter error.
    ctx->max_filter_ratio = 1.0;

    // set source related params
    switch (task.type) {
    case TLoadSourceType::KAFKA:
        ctx->kafka_info = std::make_unique<KafkaLoadInfo>(task.kafka_load_info);
        break;
    default:
        LOG(WARNING) << "unknown load source type: " << task.type;
        delete ctx;
        return Status::InternalError("unknown load source type");
    }

    VLOG(1) << "receive a new routine load task: " << ctx->brief();
    // register the task
    ctx->ref();
    // here we add ref twice to avoid group consumer return first before the plan fragment executor finish,
    // at that time, load_stream_mgr will remove pipe and file scan node can not find consume data from pipe
    ctx->ref();
    _task_map[ctx->id] = ctx;

    // Todo: add callback
    // must put pipe before executing plan fragment
    std::shared_ptr<StreamLoadPipe> pipe;
    Status st = Status::OK();
    std::string error_msg;
    switch (ctx->load_src_type) {
    case TLoadSourceType::KAFKA: {
        pipe = std::make_shared<KafkaConsumerPipe>();
        ctx->body_sink = pipe;
        break;
    }
    default: {
        st = Status::InternalError("unknown routine load task type");
        error_msg = "unknown routine load task type: " + ctx->load_src_type;
    }
    }

    if (st.ok()) {
        st = _exec_env->load_stream_mgr()->put(ctx->id, pipe);
        if (!st.ok()) {
            error_msg = "failed to add pipe";
        }
    }

    if (st.ok()) {
        if (!_thread_pool.offer([this, ctx, capture0 = &_data_consumer_pool, capture1 = [this](StreamLoadContext* ctx) {
                std::unique_lock<std::mutex> l(_lock);
                _task_map.erase(ctx->id);
                LOG(INFO) << "finished routine load task " << ctx->brief()
                          << ", status: " << ctx->status.get_error_msg() << ", current tasks num: " << _task_map.size();
                if (ctx->unref()) {
                    delete ctx;
                }
            }] { exec_task(ctx, capture0, capture1); })) {
            st = Status::InternalError("failed to submit routine load task");
            error_msg = "failed to submit routine load task:";
        }
    }

    if (st.ok()) {
        LOG(INFO) << "submit a new routine load task: " << ctx->brief() << ", current tasks num: " << _task_map.size();
        return ctx;
    } else {
        LOG(WARNING) << "failed to submit routine load task, msg: " << error_msg << "task : " << ctx->brief();
        if (ctx->body_sink) {
            ctx->body_sink->cancel(st);
        }
        if (ctx->unref()) {
            delete ctx;
        }
        if (ctx->unref()) {
            delete ctx;
        }
        _task_map.erase(ctx->id);
        return st;
    }
}

void RoutineLoadExecutor::exec_task(StreamLoadContext* ctx, DataConsumerPool* consumer_pool,
                                    const ExecFinishCallback& cb) {
#define HANDLE_ERROR(stmt, err_msg)                                                      \
    do {                                                                                 \
        Status _status = (stmt);                                                         \
        std::string _err_msg = err_msg;                                                  \
        if (UNLIKELY(!_status.ok() && _status.code() != TStatusCode::PUBLISH_TIMEOUT)) { \
            LOG(WARNING) << "status: " << _status << " msg: " << _err_msg;               \
            cb(ctx);                                                                     \
            return;                                                                      \
        }                                                                                \
    } while (false);

    LOG(INFO) << "begin to execute routine load: " << ctx->brief();

    // create data consumer group
    std::shared_ptr<DataConsumerGroup> consumer_grp;
    HANDLE_ERROR(consumer_pool->get_consumer_grp(ctx, &consumer_grp), "failed to get consumers");
    DeferOp return_consumers([&consumer_pool, &consumer_grp] { consumer_pool->return_consumers(consumer_grp.get()); });

    switch (ctx->load_src_type) {
    case TLoadSourceType::KAFKA: {
        Status st = std::static_pointer_cast<KafkaDataConsumerGroup>(consumer_grp)->assign_topic_partitions(ctx);
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

    // start to consume, this may block a while
    HANDLE_ERROR(consumer_grp->start_all(ctx), "consuming failed");

    ctx->load_cost_nanos = MonotonicNanos() - ctx->start_nanos;
}

Status RoutineLoadExecutor::commit_offset(const TRoutineLoadCommitOffsetInfo& commit_info) {
    Status st = Status::OK();
    std::string err_msg;
    auto* ctx = new StreamLoadContext(_exec_env);
    DeferOp delete_ctx([&ctx] { delete ctx; });
    ctx->load_type = TLoadType::ROUTINE_LOAD;
    ctx->load_src_type = commit_info.type;
    ctx->job_id = commit_info.job_id;
    ctx->id = UniqueId(commit_info.id);
    ctx->txn_id = commit_info.txn_id;
    ctx->db = commit_info.db;
    ctx->table = commit_info.tbl;
    ctx->label = commit_info.label;
    ctx->auth.auth_code = commit_info.auth_code;

    switch (commit_info.type) {
    case TLoadSourceType::KAFKA:
        ctx->kafka_info = std::make_unique<KafkaLoadInfo>(commit_info.kafka_load_info);
        break;
    default:
        err_msg = "unknown load source type: " + commit_info.type;
        st = Status::InternalError(err_msg);
        LOG(WARNING) << err_msg;
        return st;
    }

    switch (commit_info.type) {
    case TLoadSourceType::KAFKA: {
        std::shared_ptr<DataConsumer> consumer;
        st = _data_consumer_pool.get_consumer(ctx, &consumer);
        if (!st.ok()) {
            // Kafka Offset Commit is idempotent, Failure should not block the normal process
            // So just print a warning
            LOG(WARNING) << st.get_error_msg();
            break;
        }
        DeferOp return_consumer([this, &consumer] { this->_data_consumer_pool.return_consumer(consumer); });

        std::vector<RdKafka::TopicPartition*> topic_partitions;
        for (auto& kv : ctx->kafka_info->cmt_offset) {
            RdKafka::TopicPartition* tp1 = RdKafka::TopicPartition::create(ctx->kafka_info->topic, kv.first, kv.second);
            topic_partitions.push_back(tp1);
        }
        // delete TopicPartition finally
        auto tp_deleter = [&topic_partitions]() {
            std::for_each(topic_partitions.begin(), topic_partitions.end(),
                          [](RdKafka::TopicPartition* tp1) { delete tp1; });
        };
        DeferOp delete_tp([tp_deleter] { return tp_deleter(); });

        st = std::static_pointer_cast<KafkaDataConsumer>(consumer)->commit(topic_partitions);
        if (!st.ok()) {
            // Kafka Offset Commit is idempotent, Failure should not block the normal process
            // So just print a warning
            LOG(WARNING) << st.get_error_msg();
        }
    } break;
    default:
        err_msg = "unknown load source type: " + commit_info.type;
        st = Status::InternalError(err_msg);
        LOG(WARNING) << err_msg;
        return st;
    }
    return st;
}

void RoutineLoadExecutor::err_handler(StreamLoadContext* ctx, const Status& st, const std::string& err_msg) {
    LOG(WARNING) << err_msg;
    ctx->status = st;
    if (ctx->body_sink != nullptr) {
        ctx->body_sink->cancel(st);
    }
}

} // namespace starrocks
