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
//   https://github.com/apache/incubator-doris/blob/master/be/src/runtime/stream_load/stream_load_context.h

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

#include <rapidjson/prettywriter.h>

#include <cstdint>
#include <future>
#include <vector>

#include "common/status.h"
#include "common/utils.h"
#include "gen_cpp/BackendService_types.h"
#include "gen_cpp/FrontendService_types.h"
#include "pulsar/Client.h"
#include "runtime/exec_env.h"
#include "runtime/stream_load/load_stream_mgr.h"
#include "runtime/stream_load/stream_load_executor.h"
#include "service/backend_options.h"
#include "util/concurrent_limiter.h"
#include "util/string_util.h"
#include "util/time.h"
#include "util/uid_util.h"

namespace starrocks {

class RuntimeProfile;

// kafka related info
class KafkaLoadInfo {
public:
    explicit KafkaLoadInfo(const TKafkaLoadInfo& t_info)
            : brokers(t_info.brokers),
              topic(t_info.topic),
              confluent_schema_registry_url(t_info.confluent_schema_registry_url),
              begin_offset(t_info.partition_begin_offset),
              properties(t_info.properties) {
        // The offset(begin_offset) sent from FE is the starting offset,
        // and the offset(cmt_offset) reported by BE to FE is the consumed offset,
        // so we need to minus 1 here.
        for (auto& p : t_info.partition_begin_offset) {
            cmt_offset[p.first] = p.second - 1;
        }
    }

    void reset_offset() {
        // reset the commit offset
        for (auto& p : begin_offset) {
            cmt_offset[p.first] = p.second - 1;
        }
    }

public:
    std::string brokers;
    std::string topic;
    std::string confluent_schema_registry_url;

    // partition -> begin offset, inclusive.
    std::map<int32_t, int64_t> begin_offset;
    // partiton -> commit offset, inclusive.
    std::map<int32_t, int64_t> cmt_offset;
    //custom kafka property key -> value
    std::map<std::string, std::string> properties;
};

// pulsar related info
class PulsarLoadInfo {
public:
    explicit PulsarLoadInfo(const TPulsarLoadInfo& t_info)
            : service_url(t_info.service_url),
              topic(t_info.topic),
              subscription(t_info.subscription),
              partitions(t_info.partitions),
              properties(t_info.properties) {
        if (t_info.__isset.initial_positions) {
            initial_positions = t_info.initial_positions;
        }
    }

    void clear_backlog() {
        // clear the backlog
        partition_backlog.clear();
    }

public:
    std::string service_url;
    std::string topic;
    std::string subscription;
    std::vector<std::string> partitions;
    std::map<std::string, int64_t> initial_positions;

    // partition -> acknowledge offset, inclusive.
    std::map<std::string, pulsar::MessageId> ack_offset;
    // partition -> backlog num, inclusive.
    std::map<std::string, int64_t> partition_backlog;

    // custom kafka property key -> value
    std::map<std::string, std::string> properties;
};

class MessageBodySink;

const std::string TXN_BEGIN = "begin";
const std::string TXN_COMMIT = "commit";
const std::string TXN_PREPARE = "prepare";
const std::string TXN_ROLLBACK = "rollback";
const std::string TXN_LOAD = "load";
const std::string TXN_LIST = "list";

class StreamLoadContext {
public:
    explicit StreamLoadContext(ExecEnv* exec_env) : id(UniqueId::gen_uid()), _exec_env(exec_env), _refs(0) {
        start_nanos = MonotonicNanos();
    }

    explicit StreamLoadContext(ExecEnv* exec_env, UniqueId id) : id(id), _exec_env(exec_env), _refs(0) {
        start_nanos = MonotonicNanos();
    }

    ~StreamLoadContext() noexcept {
        if (need_rollback) {
            _exec_env->stream_load_executor()->rollback_txn(this);
            need_rollback = false;
        }

        _exec_env->load_stream_mgr()->remove(id);
    }

    std::string to_json() const;

    std::string to_resp_json(const std::string& txn_op, const Status& st) const;

    // return the brief info of this context.
    // also print the load source info if detail is set to true
    std::string brief(bool detail = false) const;

    void ref() { _refs.fetch_add(1); }
    // If unref() returns true, this object should be delete
    bool unref() { return _refs.fetch_sub(1) == 1; }

    bool check_and_set_http_limiter(ConcurrentLimiter* limiter);

public:
    // 1) Before the stream load receiving thread exits, Fragment may have been destructed.
    // At this time, mem_tracker may have been destructed,
    // so add shared_ptr here to prevent this from happening.
    //
    // 2) query_mem_tracker is the parent of instance_mem_tracker
    // runtime_profile will be used by [consumption] of mem_tracker to record peak memory
    std::shared_ptr<RuntimeProfile> runtime_profile;
    std::shared_ptr<MemTracker> query_mem_tracker;
    std::shared_ptr<MemTracker> instance_mem_tracker;
    // load type, eg: ROUTINE LOAD/MANUAL LOAD
    TLoadType::type load_type;
    // load data source: eg: KAFKA/PULSAR/RAW
    TLoadSourceType::type load_src_type;

    // the job this stream load task belongs to,
    // set to -1 if there is no job
    int64_t job_id = -1;

    // id for each load
    UniqueId id;

    std::string db;
    std::string table;
    std::string label;
    // optional
    double max_filter_ratio = 0.0;
    int32_t timeout_second = -1;
    AuthInfo auth;

    // the following members control the max progress of a consuming
    // process. if any of them reach, the consuming will finish.
    int64_t max_interval_s = 5;
    int64_t max_batch_rows = 100000;
    int64_t max_batch_size = 100 * 1024 * 1024; // 100MB

    // only used to check if we receive whole body
    size_t body_bytes = 0;
    size_t receive_bytes = 0;
    size_t total_receive_bytes = 0;

    // when use_streaming is true, we use stream_pipe to send source data,
    // otherwise we save source data to file first, then process it.
    bool use_streaming = false;
    TFileFormatType::type format = TFileFormatType::FORMAT_CSV_PLAIN;

    TStreamLoadPutResult put_result;

    int64_t number_total_rows = 0;
    int64_t number_loaded_rows = 0;
    int64_t number_filtered_rows = 0;
    int64_t number_unselected_rows = 0;
    int64_t loaded_bytes = 0;
    int64_t start_nanos = 0;
    int64_t start_write_data_nanos = 0;
    int64_t load_cost_nanos = 0;
    int64_t begin_txn_cost_nanos = 0;
    int64_t stream_load_put_cost_nanos = 0;
    int64_t commit_and_publish_txn_cost_nanos = 0;
    int64_t total_received_data_cost_nanos = 0;
    int64_t received_data_cost_nanos = 0;
    int64_t write_data_cost_nanos = 0;
    int64_t begin_txn_ts = 0;
    int64_t last_active_ts = 0;

    std::string error_url;
    // if label already be used, set existing job's status here
    // should be RUNNING or FINISHED
    std::string existing_job_status;

    std::unique_ptr<KafkaLoadInfo> kafka_info;
    std::unique_ptr<PulsarLoadInfo> pulsar_info;

    std::vector<TTabletCommitInfo> commit_infos;
    std::vector<TTabletFailInfo> fail_infos;

    std::mutex lock;

    std::shared_ptr<MessageBodySink> body_sink;
    bool need_rollback = false;
    int64_t txn_id = -1;

    std::promise<Status> promise;
    std::future<Status> future = promise.get_future();

    Status status;

    int32_t idle_timeout_sec = -1;
    int channel_id = -1;

    // buffer for reading data from ev_buffer
    static constexpr size_t kDefaultBufferSize = 64 * 1024;
    // max buffer size for JSON format is 4GB.
    static constexpr int64_t kJSONMaxBufferSize = 4294967296;
    ByteBufferPtr buffer = nullptr;

    TStreamLoadPutRequest request;

    std::unique_ptr<ConcurrentLimiterGuard> _http_limiter_guard;

public:
    bool is_channel_stream_load_context() { return channel_id != -1; }
    ExecEnv* exec_env() { return _exec_env; }

private:
    ExecEnv* _exec_env;
    std::atomic<int> _refs;
};

} // namespace starrocks
