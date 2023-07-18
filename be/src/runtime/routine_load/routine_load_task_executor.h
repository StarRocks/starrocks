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
//   https://github.com/apache/incubator-doris/blob/master/be/src/runtime/routine_load/routine_load_task_executor.h

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

#include <climits>
#include <functional>
#include <map>
#include <mutex>

#include "gen_cpp/internal_service.pb.h"
#include "runtime/routine_load/data_consumer_pool.h"
#include "util/starrocks_metrics.h"
#include "util/threadpool.h"
#include "util/uid_util.h"

namespace starrocks {

class ExecEnv;
class Status;
class StreamLoadContext;
class TRoutineLoadTask;

// A routine load task executor will receive routine load
// tasks from FE, put it to a fixed thread pool.
// The thread pool will process each task and report the result
// to FE finally.
class RoutineLoadTaskExecutor {
public:
    typedef std::function<void(StreamLoadContext*)> ExecFinishCallback;

    RoutineLoadTaskExecutor(ExecEnv* exec_env) : _exec_env(exec_env), _data_consumer_pool(10) {
        REGISTER_GAUGE_STARROCKS_METRIC(routine_load_task_count, [this]() {
            std::lock_guard<std::mutex> l(_lock);
            return _task_map.size();
        });
        auto st = ThreadPoolBuilder("routine_load")
                          .set_min_threads(0)
                          .set_max_threads(INT_MAX)
                          .set_max_queue_size(INT_MAX)
                          .build(&_thread_pool);
        DCHECK(st.ok());
        WARN_IF_ERROR(_data_consumer_pool.start_bg_worker(), "Failed to start data consumer pool");
    }

    ~RoutineLoadTaskExecutor() noexcept {
        // _thread_pool.shutdown();
        // _thread_pool.join();

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

    // submit a routine load task
    Status submit_task(const TRoutineLoadTask& task);

    Status get_kafka_partition_meta(const PKafkaMetaProxyRequest& request, std::vector<int32_t>* partition_ids,
                                    int timeout, std::string* group_id);

    Status get_kafka_partition_offset(const PKafkaOffsetProxyRequest& request, std::vector<int64_t>* beginning_offsets,
                                      std::vector<int64_t>* latest_offsets, int timeout, std::string* group_id);

    Status get_pulsar_partition_meta(const PPulsarMetaProxyRequest& request, std::vector<std::string>* partitions);

    Status get_pulsar_partition_backlog(const PPulsarBacklogProxyRequest& request, std::vector<int64_t>* backlog_num);

private:
    // execute the task
    void exec_task(StreamLoadContext* ctx, DataConsumerPool* pool, const ExecFinishCallback& cb);

    void err_handler(StreamLoadContext* ctx, const Status& st, const std::string& err_msg);

    // for test only
    Status _execute_plan_for_test(StreamLoadContext* ctx);

private:
    ExecEnv* _exec_env;
    std::unique_ptr<ThreadPool> _thread_pool;
    DataConsumerPool _data_consumer_pool;

    std::mutex _lock;
    // task id -> load context
    std::unordered_map<UniqueId, StreamLoadContext*> _task_map;
};

} // namespace starrocks
