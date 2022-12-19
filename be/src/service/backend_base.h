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
//   https://github.com/apache/incubator-doris/blob/master/be/src/service/backend_service.h

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

#include <ctime>
#include <map>
#include <memory>

#include "common/status.h"
#include "gen_cpp/BackendService.h"
#include "gen_cpp/MVMaintenance_types.h"

namespace starrocks {

class ExecEnv;
class ThriftServer;
class TAgentResult;
class TAgentTaskRequest;
class TAgentPublishRequest;
class TMiniLoadEtlTaskRequest;
class TMiniLoadEtlStatusResult;
class TMiniLoadEtlStatusRequest;
class TDeleteEtlFilesRequest;
class TExecPlanFragmentParams;
class TExecPlanFragmentResult;
class TCancelPlanFragmentResult;
class TTransmitDataResult;
class TExportTaskRequest;
class TExportStatusResult;

// This class just forward rpc requests to actual handlers, used
// to bind multiple services on single port.
class BackendServiceBase : public BackendServiceIf {
public:
    explicit BackendServiceBase(ExecEnv* exec_env);

    ~BackendServiceBase() override = default;

    // NOTE: now we do not support multiple backend in one process
    template <class Service>
    static std::unique_ptr<ThriftServer> create(ExecEnv* exec_env, int port);

    // Agent service
    void submit_tasks(TAgentResult& return_value, const std::vector<TAgentTaskRequest>& tasks) override {
        Status ret_st = Status::ServiceUnavailable("Not supported");
        ret_st.to_thrift(&return_value.status);
    }

    void make_snapshot(TAgentResult& return_value, const TSnapshotRequest& snapshot_request) override {
        Status ret_st = Status::ServiceUnavailable("Not supported");
        ret_st.to_thrift(&return_value.status);
    }

    void release_snapshot(TAgentResult& return_value, const std::string& snapshot_path) override {
        Status ret_st = Status::ServiceUnavailable("Not supported");
        ret_st.to_thrift(&return_value.status);
    }

    void publish_cluster_state(TAgentResult& result, const TAgentPublishRequest& request) override {
        Status ret_st = Status::ServiceUnavailable("Not supported");
        ret_st.to_thrift(&result.status);
    }

    void submit_etl_task(TAgentResult& result, const TMiniLoadEtlTaskRequest& request) override {}

    void get_etl_status(TMiniLoadEtlStatusResult& result, const TMiniLoadEtlStatusRequest& request) override {}

    void delete_etl_files(TAgentResult& result, const TDeleteEtlFilesRequest& request) override {}

    // StarrocksServer service
    void exec_plan_fragment(TExecPlanFragmentResult& return_val, const TExecPlanFragmentParams& params) override;

    void cancel_plan_fragment(TCancelPlanFragmentResult& return_val, const TCancelPlanFragmentParams& params) override;

    void transmit_data(TTransmitDataResult& return_val, const TTransmitDataParams& params) override;

    void fetch_data(TFetchDataResult& return_val, const TFetchDataParams& params) override;

    void submit_export_task(TStatus& t_status, const TExportTaskRequest& request) override{};

    void get_export_status(TExportStatusResult& result, const TUniqueId& task_id) override{};

    void erase_export_task(TStatus& t_status, const TUniqueId& task_id) override{};

    void get_tablet_stat(TTabletStatResult& result) override{};

    void submit_routine_load_task(TStatus& t_status, const std::vector<TRoutineLoadTask>& tasks) override;

    void finish_stream_load_channel(TStatus& t_status, const TStreamLoadChannel& stream_load_channel) override;

    // used for external service, open means start the scan procedure
    void open_scanner(TScanOpenResult& result_, const TScanOpenParams& params) override;

    // used for external service, external use getNext to fetch data batch after batch until eos = true
    void get_next(TScanBatchResult& result_, const TScanNextBatchParams& params) override;

    // used for external service, close some context and release resource related with this context
    void close_scanner(TScanCloseResult& result_, const TScanCloseParams& params) override;

private:
    Status start_plan_fragment_execution(const TExecPlanFragmentParams& exec_params);
    ExecEnv* _exec_env;
};

} // namespace starrocks
