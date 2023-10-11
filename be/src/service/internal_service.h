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
//   https://github.com/apache/incubator-doris/blob/master/be/src/service/internal_service.h

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

#include <bthread/condition_variable.h>
#include <bthread/mutex.h>

#include "common/compiler_util.h"
#include "common/status.h"
#include "exec/pipeline/pipeline_fwd.h"
#include "gen_cpp/MVMaintenance_types.h"
#include "gen_cpp/doris_internal_service.pb.h"
#include "gen_cpp/internal_service.pb.h"
#include "util/countdown_latch.h"
#include "util/priority_thread_pool.hpp"

namespace brpc {
class Controller;
}

namespace starrocks {

class TExecPlanFragmentParams;
class TMVCommitEpochTask;
class ExecEnv;

template <typename T>
class PInternalServiceImplBase : public T {
public:
    PInternalServiceImplBase(ExecEnv* exec_env);
    ~PInternalServiceImplBase() override;

    void transmit_data(::google::protobuf::RpcController* controller, const ::starrocks::PTransmitDataParams* request,
                       ::starrocks::PTransmitDataResult* response, ::google::protobuf::Closure* done) override;

    void transmit_chunk(::google::protobuf::RpcController* controller, const ::starrocks::PTransmitChunkParams* request,
                        ::starrocks::PTransmitChunkResult* response, ::google::protobuf::Closure* done) override;

    void transmit_chunk_via_http(::google::protobuf::RpcController* controller,
                                 const ::starrocks::PHttpRequest* request, ::starrocks::PTransmitChunkResult* response,
                                 ::google::protobuf::Closure* done) override;

    void transmit_runtime_filter(::google::protobuf::RpcController* controller,
                                 const ::starrocks::PTransmitRuntimeFilterParams* request,
                                 ::starrocks::PTransmitRuntimeFilterResult* response,
                                 ::google::protobuf::Closure* done) override;

    void exec_plan_fragment(google::protobuf::RpcController* controller, const PExecPlanFragmentRequest* request,
                            PExecPlanFragmentResult* result, google::protobuf::Closure* done) override;

    void exec_batch_plan_fragments(google::protobuf::RpcController* controller,
                                   const PExecBatchPlanFragmentsRequest* request, PExecBatchPlanFragmentsResult* result,
                                   google::protobuf::Closure* done) override;

    void cancel_plan_fragment(google::protobuf::RpcController* controller, const PCancelPlanFragmentRequest* request,
                              PCancelPlanFragmentResult* result, google::protobuf::Closure* done) override;

    void fetch_data(google::protobuf::RpcController* controller, const PFetchDataRequest* request,
                    PFetchDataResult* result, google::protobuf::Closure* done) override;

    void tablet_writer_open(google::protobuf::RpcController* controller, const PTabletWriterOpenRequest* request,
                            PTabletWriterOpenResult* response, google::protobuf::Closure* done) override;

    void tablet_writer_add_batch(google::protobuf::RpcController* controller,
                                 const PTabletWriterAddBatchRequest* request, PTabletWriterAddBatchResult* response,
                                 google::protobuf::Closure* done) override;

    void tablet_writer_add_chunk(google::protobuf::RpcController* controller,
                                 const PTabletWriterAddChunkRequest* request, PTabletWriterAddBatchResult* response,
                                 google::protobuf::Closure* done) override;

    void tablet_writer_add_chunks(google::protobuf::RpcController* controller,
                                  const PTabletWriterAddChunksRequest* request, PTabletWriterAddBatchResult* response,
                                  google::protobuf::Closure* done) override;

    void tablet_writer_add_segment(google::protobuf::RpcController* controller,
                                   const PTabletWriterAddSegmentRequest* request,
                                   PTabletWriterAddSegmentResult* response, google::protobuf::Closure* done) override;

    void tablet_writer_cancel(google::protobuf::RpcController* controller, const PTabletWriterCancelRequest* request,
                              PTabletWriterCancelResult* response, google::protobuf::Closure* done) override;

    void trigger_profile_report(google::protobuf::RpcController* controller,
                                const PTriggerProfileReportRequest* request, PTriggerProfileReportResult* result,
                                google::protobuf::Closure* done) override;

    void collect_query_statistics(google::protobuf::RpcController* controller,
                                  const PCollectQueryStatisticsRequest* request, PCollectQueryStatisticsResult* result,
                                  google::protobuf::Closure* done) override;

    void get_info(google::protobuf::RpcController* controller, const PProxyRequest* request, PProxyResult* response,
                  google::protobuf::Closure* done) override;

    void get_pulsar_info(google::protobuf::RpcController* controller, const PPulsarProxyRequest* request,
                         PPulsarProxyResult* response, google::protobuf::Closure* done) override;

    void get_file_schema(google::protobuf::RpcController* controller, const PGetFileSchemaRequest* request,
                         PGetFileSchemaResult* response, google::protobuf::Closure* done) override;

    void submit_mv_maintenance_task(google::protobuf::RpcController* controller,
                                    const PMVMaintenanceTaskRequest* request, PMVMaintenanceTaskResult* response,
                                    google::protobuf::Closure* done) override;

    void local_tablet_reader_open(google::protobuf::RpcController* controller, const PTabletReaderOpenRequest* request,
                                  PTabletReaderOpenResult* response, google::protobuf::Closure* done) override;

    void local_tablet_reader_close(google::protobuf::RpcController* controller,
                                   const PTabletReaderCloseRequest* request, PTabletReaderCloseResult* response,
                                   google::protobuf::Closure* done) override;

    void local_tablet_reader_multi_get(google::protobuf::RpcController* controller,
                                       const PTabletReaderMultiGetRequest* request,
                                       PTabletReaderMultiGetResult* response, google::protobuf::Closure* done) override;

    void local_tablet_reader_scan_open(google::protobuf::RpcController* controller,
                                       const PTabletReaderScanOpenRequest* request,
                                       PTabletReaderScanOpenResult* response, google::protobuf::Closure* done) override;

    void local_tablet_reader_scan_get_next(google::protobuf::RpcController* controller,
                                           const PTabletReaderScanGetNextRequest* request,
                                           PTabletReaderScanGetNextResult* response,
                                           google::protobuf::Closure* done) override;

    void execute_command(google::protobuf::RpcController* controller, const ExecuteCommandRequestPB* request,
                         ExecuteCommandResultPB* response, google::protobuf::Closure* done) override;

    void update_fail_point_status(google::protobuf::RpcController* controller,
                                  const PUpdateFailPointStatusRequest* request,
                                  PUpdateFailPointStatusResponse* response, google::protobuf::Closure* done) override;

    void list_fail_point(google::protobuf::RpcController* controller, const PListFailPointRequest* request,
                         PListFailPointResponse* response, google::protobuf::Closure* done) override;

    void exec_short_circuit(google::protobuf::RpcController* controller, const PExecShortCircuitRequest* request,
                            PExecShortCircuitResult* response, google::protobuf::Closure* done) override;

private:
    void _transmit_chunk(::google::protobuf::RpcController* controller,
                         const ::starrocks::PTransmitChunkParams* request, ::starrocks::PTransmitChunkResult* response,
                         ::google::protobuf::Closure* done);

    void _transmit_runtime_filter(::google::protobuf::RpcController* controller,
                                  const ::starrocks::PTransmitRuntimeFilterParams* request,
                                  ::starrocks::PTransmitRuntimeFilterResult* response,
                                  ::google::protobuf::Closure* done);

    void _exec_plan_fragment(google::protobuf::RpcController* controller, const PExecPlanFragmentRequest* request,
                             PExecPlanFragmentResult* result, google::protobuf::Closure* done);

    void _exec_batch_plan_fragments(google::protobuf::RpcController* controller,
                                    const PExecBatchPlanFragmentsRequest* request,
                                    PExecBatchPlanFragmentsResult* result, google::protobuf::Closure* done);

    void _cancel_plan_fragment(google::protobuf::RpcController* controller, const PCancelPlanFragmentRequest* request,
                               PCancelPlanFragmentResult* result, google::protobuf::Closure* done);

    void _fetch_data(google::protobuf::RpcController* controller, const PFetchDataRequest* request,
                     PFetchDataResult* result, google::protobuf::Closure* done);

    void _get_info_impl(const PProxyRequest* request, PProxyResult* response, google::protobuf::Closure* done,
                        int timeout_ms);

    void _get_pulsar_info_impl(const PPulsarProxyRequest* request, PPulsarProxyResult* response,
                               google::protobuf::Closure* done, int timeout_ms);

    void _get_file_schema(google::protobuf::RpcController* controller, const PGetFileSchemaRequest* request,
                          PGetFileSchemaResult* response, google::protobuf::Closure* done);

    Status _exec_plan_fragment(brpc::Controller* cntl, const PExecPlanFragmentRequest* request);
    Status _exec_plan_fragment_by_pipeline(const TExecPlanFragmentParams& t_common_request,
                                           const TExecPlanFragmentParams& t_unique_request);
    Status _exec_plan_fragment_by_non_pipeline(const TExecPlanFragmentParams& t_request);

    // MV Maintenance task
    Status _submit_mv_maintenance_task(brpc::Controller* cntl);
    Status _mv_start_maintenance(const TMVMaintenanceTasks& task);
    Status _mv_start_epoch(const pipeline::QueryContextPtr& query_ctx, const TMVMaintenanceTasks& task);
    Status _mv_commit_epoch(const pipeline::QueryContextPtr& query_ctx, const TMVMaintenanceTasks& task);
    Status _mv_abort_epoch(const pipeline::QueryContextPtr& query_ctx, const TMVMaintenanceTasks& task);

    // short circuit
    Status _exec_short_circuit(brpc::Controller* cntl, const PExecShortCircuitRequest* request,
                               PExecShortCircuitResult* response);

protected:
    ExecEnv* _exec_env;
};

} // namespace starrocks
