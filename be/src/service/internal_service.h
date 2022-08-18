// This file is made available under Elastic License 2.0.
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

#include "common/compiler_util.h"

DIAGNOSTIC_PUSH
DIAGNOSTIC_IGNORE("-Wclass-memaccess")
#include <bthread/condition_variable.h>
#include <bthread/mutex.h>
DIAGNOSTIC_POP

#include "common/status.h"
#include "gen_cpp/doris_internal_service.pb.h"
#include "gen_cpp/internal_service.pb.h"
#include "util/countdown_latch.h"
#include "util/priority_thread_pool.hpp"

namespace brpc {
class Controller;
}

namespace starrocks {

class TExecPlanFragmentParams;
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

    void get_info(google::protobuf::RpcController* controller, const PProxyRequest* request, PProxyResult* response,
                  google::protobuf::Closure* done) override;

private:
    void _get_info_impl(const PProxyRequest* request, PProxyResult* response,
                        GenericCountDownLatch<bthread::Mutex, bthread::ConditionVariable>* latch, int timeout_ms);

    Status _exec_plan_fragment(brpc::Controller* cntl);
    Status _exec_batch_plan_fragments(brpc::Controller* cntl);
    Status _exec_plan_fragment_by_pipeline(const TExecPlanFragmentParams& t_common_request,
                                           const TExecPlanFragmentParams& t_unique_request);
    Status _exec_plan_fragment_by_non_pipeline(const TExecPlanFragmentParams& t_request);

protected:
    ExecEnv* _exec_env;

    // The BRPC call is executed by bthread.
    // If the bthread is blocked by pthread primitive, the current bthread cannot release the bind pthread and cannot be yield.
    // In this way, the available pthread become less and the scheduling of bthread would be influenced.
    // So, we should execute the function that may use pthread block primitive in a specific thread pool.
    // More detail: https://github.com/apache/incubator-brpc/blob/master/docs/cn/bthread.md

    // Thread pool for executing task  asynchronously in BRPC call.
    PriorityThreadPool _async_thread_pool;
};

} // namespace starrocks
