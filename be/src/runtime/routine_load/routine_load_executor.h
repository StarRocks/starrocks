// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.
#pragma once

#include <functional>
#include <map>
#include <mutex>

#include "runtime/routine_load/data_consumer_pool.h"
#include "util/priority_thread_pool.hpp"
#include "util/starrocks_metrics.h"

namespace starrocks {

class ExecEnv;
class Status;
class StreamLoadContext;
class TRoutineLoadTaskV2;

class RoutineLoadExecutor {
public:
    typedef std::function<void(StreamLoadContext*)> ExecFinishCallback;

    RoutineLoadExecutor(ExecEnv* exec_env)
            : _exec_env(exec_env),
              _thread_pool("routine_load_executor", config::routine_load_thread_pool_size,
                           config::routine_load_thread_pool_size),
              _data_consumer_pool(10) {
        REGISTER_GAUGE_STARROCKS_METRIC(routine_load_task_count, [this]() {
            std::lock_guard<std::mutex> l(_lock);
            return _task_map.size();
        });

        _data_consumer_pool.start_bg_worker();
    }

    ~RoutineLoadExecutor() {
        _thread_pool.shutdown();
        _thread_pool.join();

        for (auto& it : _task_map) {
            auto ctx = it.second;
            if (ctx->unref()) {
                delete ctx;
            }
        }
        _task_map.clear();
    }

    StatusOr<StreamLoadContext*> submit_task(const TRoutineLoadTaskV2& task);

    Status commit_offset(const TRoutineLoadCommitOffsetInfo& commit_info);

private:
    // execute the task
    void exec_task(StreamLoadContext* ctx, DataConsumerPool* pool, const ExecFinishCallback& cb);

    void err_handler(StreamLoadContext* ctx, const Status& st, const std::string& err_msg);

private:
    ExecEnv* _exec_env;
    PriorityThreadPool _thread_pool;
    DataConsumerPool _data_consumer_pool;

    std::mutex _lock;
    // task id -> load context
    std::unordered_map<UniqueId, StreamLoadContext*> _task_map;
};

} // namespace starrocks
