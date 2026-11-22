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

#include <atomic>
#include <climits>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

#include "base/uid_util.h"
#include "common/statusor.h"
#include "common/thread/threadpool.h"
#include "data_workflows/load/routine_load/data_consumer_pool.h"
#include "gen_cpp/internal_service.pb.h"

namespace starrocks {

class ExecEnv;
class KafkaScanRangeReport;
class MetricRegistry;
class Status;
class StreamLoadContext;
class StreamLoadExecutor;
class TBrokerScanRange;
class TRoutineLoadTask;

namespace orchestration {

class StreamLoadOrchestrator;

// A routine load task executor will receive routine load
// tasks from FE, put it to a fixed thread pool.
// The thread pool will process each task and report the result
// to FE finally.
class RoutineLoadTaskExecutor {
public:
    typedef std::function<void(StreamLoadContext*)> ExecFinishCallback;
    // Fails the fragment that reads a Kafka scan range with the error that stopped its consumption.
    using CancelFragmentCallback = std::function<void(const Status&)>;

    RoutineLoadTaskExecutor(ExecEnv* exec_env, StreamLoadOrchestrator* stream_load_orchestrator,
                            StreamLoadExecutor* stream_load_executor)
            : _exec_env(exec_env),
              _stream_load_orchestrator(stream_load_orchestrator),
              _stream_load_executor(stream_load_executor) {}

    ~RoutineLoadTaskExecutor() noexcept = default;

    Status init(MetricRegistry* metrics = nullptr);
    void stop();

    // submit a routine load task
    Status submit_task(const TRoutineLoadTask& task);

    // Append pool evictions to the caller's container so an RPC can send its response before broker close.
    Status get_kafka_partition_meta(const PKafkaMetaProxyRequest& request, std::vector<int32_t>* partition_ids,
                                    int timeout, std::string* group_id,
                                    std::vector<std::shared_ptr<DataConsumer>>* evicted_consumers);

    Status get_kafka_partition_offset(const PKafkaOffsetProxyRequest& request, std::vector<int64_t>* beginning_offsets,
                                      std::vector<int64_t>* latest_offsets, int timeout, std::string* group_id,
                                      std::vector<std::shared_ptr<DataConsumer>>* evicted_consumers);

    // Commits the consumer group offsets of a routine load round on behalf of the FE, waiting at most timeout_ms for
    // the broker. A commit that does not finish in time stays on its way, and the pool keeps its consumer for the
    // later commits of the group until its commits complete. Like the metadata requests, it appends the consumers that
    // the pool evicts to evicted_consumers, so that the RPC responds before they close.
    Status commit_kafka_offsets(const PKafkaCommitOffsetProxyRequest& request, int timeout_ms, std::string* group_id,
                                std::vector<std::shared_ptr<DataConsumer>>* evicted_consumers);

    // A routine load round in the coordinated mode is a load fragment whose file scan reads one stream load pipe per
    // Kafka scan range (TBrokerScanRange.kafka_scan_range), which this executor fills by consuming the partitions
    // of the scan range. The FE commits the round's transaction from the final exec status reports, so nothing
    // here talks to the FE.
    //
    // Registers the pipe of a Kafka scan range under the load id of its range, for the file scanner to open. The
    // returned context carries one reference owned by the caller. Consuming only begins with
    // start_kafka_scan_range(), so the caller can hand the pipe over to the fragment first.
    StatusOr<StreamLoadContext*> prepare_kafka_scan_range(const TBrokerScanRange& scan_range);

    // Consumes the partitions of a prepared Kafka scan range into its pipe in the background. When the consumption
    // ends normally, what it consumed is merged into report before the pipe finishes, so it is in by the time the
    // scanner can reach the end of the pipe. When it fails, cancel_fragment is called with the error before the
    // pipe is cancelled.
    Status start_kafka_scan_range(StreamLoadContext* ctx, std::shared_ptr<KafkaScanRangeReport> report,
                                  CancelFragmentCallback cancel_fragment);

    // The JSON that names the Kafka topic of a routine load and the partitions it consumes, each with the offset it
    // consumes from, as the source of its rejected records (TQueryOptions.routine_load_source_info).
    static std::string kafka_source_info(const std::string& topic,
                                         const std::map<int32_t, int64_t>& partition_begin_offset);

    Status get_pulsar_partition_meta(const PPulsarMetaProxyRequest& request, std::vector<std::string>* partitions,
                                     std::vector<std::shared_ptr<DataConsumer>>* evicted_consumers);

    Status get_pulsar_partition_backlog(const PPulsarBacklogProxyRequest& request, std::vector<int64_t>* backlog_num,
                                        std::vector<std::shared_ptr<DataConsumer>>* evicted_consumers);

private:
    // execute the task
    void exec_task(StreamLoadContext* ctx, DataConsumerPool* pool, const ExecFinishCallback& cb);

    void err_handler(StreamLoadContext* ctx, const Status& st, std::string_view err_msg);

    void consume_kafka_scan_range(StreamLoadContext* ctx, const CancelFragmentCallback& cancel_fragment);

    ExecEnv* _exec_env;
    StreamLoadOrchestrator* _stream_load_orchestrator;
    StreamLoadExecutor* _stream_load_executor;
    std::unique_ptr<ThreadPool> _thread_pool;
    DataConsumerPool _data_consumer_pool;

    std::mutex _lock;
    // task id -> load context
    std::unordered_map<UniqueId, StreamLoadContext*> _task_map;
    // A queued or running Kafka scan range is a routine load task too. Shared with its submitted closure so cleanup
    // remains valid when shutdown discards the closure instead of running it.
    std::shared_ptr<std::atomic<size_t>> _kafka_scan_range_task_count = std::make_shared<std::atomic<size_t>>(0);
};

} // namespace orchestration
} // namespace starrocks
