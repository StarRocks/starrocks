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
//   https://github.com/apache/incubator-doris/blob/master/be/src/service/internal_service.cpp

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

#include "service/internal_service.h"

#include <fmt/format.h>

#include <atomic>
#include <memory>
#include <shared_mutex>
#include <sstream>
#include <utility>

#include "agent/agent_server.h"
#include "agent/publish_version.h"
#include "agent/task_worker_pool.h"
#include "brpc/errno.pb.h"
#include "column/stream_chunk.h"
#include "common/closure_guard.h"
#include "common/config.h"
#include "common/status.h"
#include "exec/orc_scanner.h"
#include "exec/parquet_scanner.h"
#include "exec/pipeline/fragment_context.h"
#include "exec/pipeline/fragment_executor.h"
#include "exec/pipeline/pipeline_driver_executor.h"
#include "exec/pipeline/query_context.h"
#include "exec/pipeline/stream_epoch_manager.h"
#include "gen_cpp/AgentService_types.h"
#include "gen_cpp/BackendService.h"
#include "gen_cpp/InternalService_types.h"
#include "gen_cpp/MVMaintenance_types.h"
#include "gen_cpp/PlanNodes_types.h"
#include "gutil/strings/substitute.h"
#include "runtime/buffer_control_block.h"
#include "runtime/command_executor.h"
#include "runtime/data_stream_mgr.h"
#include "runtime/descriptors.h"
#include "runtime/exec_env.h"
#include "runtime/fragment_mgr.h"
#include "runtime/load_channel_mgr.h"
#include "runtime/result_buffer_mgr.h"
#include "runtime/routine_load/routine_load_task_executor.h"
#include "runtime/runtime_filter_worker.h"
#include "runtime/types.h"
#include "service/brpc.h"
#include "storage/storage_engine.h"
#include "storage/txn_manager.h"
#include "util/failpoint/fail_point.h"
#include "util/stopwatch.hpp"
#include "util/thrift_util.h"
#include "util/time.h"
#include "util/uid_util.h"

namespace starrocks {

extern std::atomic<bool> k_starrocks_exit;
extern std::atomic<bool> k_starrocks_exit_quick;

using PromiseStatus = std::promise<Status>;
using PromiseStatusSharedPtr = std::shared_ptr<PromiseStatus>;

template <typename T>
PInternalServiceImplBase<T>::PInternalServiceImplBase(ExecEnv* exec_env) : _exec_env(exec_env) {}

template <typename T>
PInternalServiceImplBase<T>::~PInternalServiceImplBase() = default;

template <typename T>
void PInternalServiceImplBase<T>::transmit_data(google::protobuf::RpcController* cntl_base,
                                                const PTransmitDataParams* request, PTransmitDataResult* response,
                                                google::protobuf::Closure* done) {
    Status st = Status::InternalError("transmit_data is only used for non-vectorized engine, is not supported now");
    LOG(ERROR) << "transmit_data failed: " << st.to_string();
    if (done != nullptr) {
        st.to_protobuf(response->mutable_status());
        done->Run();
    }
}

template <typename T>
void PInternalServiceImplBase<T>::transmit_chunk(google::protobuf::RpcController* cntl_base,
                                                 const PTransmitChunkParams* request, PTransmitChunkResult* response,
                                                 google::protobuf::Closure* done) {
    auto task = [=]() { this->_transmit_chunk(cntl_base, request, response, done); };
    if (!_exec_env->query_rpc_pool()->try_offer(std::move(task))) {
        ClosureGuard closure_guard(done);
        Status::ServiceUnavailable("submit transmit_chunk task failed").to_protobuf(response->mutable_status());
    }
}

template <typename T>
void PInternalServiceImplBase<T>::_transmit_chunk(google::protobuf::RpcController* cntl_base,
                                                  const PTransmitChunkParams* request, PTransmitChunkResult* response,
                                                  google::protobuf::Closure* done) {
    class WrapClosure : public google::protobuf::Closure {
    public:
        WrapClosure(google::protobuf::Closure* done, PTransmitChunkResult* response)
                : _done(done), _response(response) {}
        ~WrapClosure() override = default;
        void Run() override {
            std::unique_ptr<WrapClosure> self_guard(this);
            const auto response_timestamp = MonotonicNanos();
            _response->set_receiver_post_process_time(response_timestamp - _receive_timestamp);
            if (_done != nullptr) {
                _done->Run();
            }
        }

    private:
        google::protobuf::Closure* _done;
        PTransmitChunkResult* _response;
        const int64_t _receive_timestamp = MonotonicNanos();
    };
    google::protobuf::Closure* wrapped_done = new WrapClosure(done, response);

    auto begin_ts = MonotonicNanos();
    std::string transmit_info = "";
    auto gen_transmit_info = [&transmit_info, &request]() {
        transmit_info = "transmit data: " + std::to_string((uint64_t)(request)) +
                        " fragment_instance_id=" + print_id(request->finst_id()) +
                        " node=" + std::to_string(request->node_id());
    };
    if (VLOG_ROW_IS_ON) {
        gen_transmit_info();
    }
    VLOG_ROW << transmit_info << " begin";
    // NOTE: we should give a default value to response to avoid concurrent risk
    // If we don't give response here, stream manager will call done->Run before
    // transmit_data(), which will cause a dirty memory access.
    auto* cntl = static_cast<brpc::Controller*>(cntl_base);
    auto* req = const_cast<PTransmitChunkParams*>(request);
    Status st;
    st.to_protobuf(response->mutable_status());
    DeferOp defer([&]() {
        if (!st.ok()) {
            gen_transmit_info();
            LOG(WARNING) << "failed to " << transmit_info;
        }
        if (wrapped_done != nullptr) {
            // NOTE: only when done is not null, we can set response status
            st.to_protobuf(response->mutable_status());
            wrapped_done->Run();
        }
        VLOG_ROW << transmit_info << " cost time = " << MonotonicNanos() - begin_ts;
    });
    if (cntl->request_attachment().size() > 0) {
        butil::IOBuf& io_buf = cntl->request_attachment();
        for (size_t i = 0; i < req->chunks().size(); ++i) {
            auto chunk = req->mutable_chunks(i);
            if (UNLIKELY(io_buf.size() < chunk->data_size())) {
                auto msg = fmt::format("iobuf's size {} < {}", io_buf.size(), chunk->data_size());
                LOG(WARNING) << msg;
                st = Status::InternalError(msg);
                return;
            }
            // also with copying due to the discontinuous memory in chunk
            auto size = io_buf.cutn(chunk->mutable_data(), chunk->data_size());
            if (UNLIKELY(size != chunk->data_size())) {
                auto msg = fmt::format("iobuf read {} != expected {}.", size, chunk->data_size());
                LOG(WARNING) << msg;
                st = Status::InternalError(msg);
                return;
            }
        }
    }

    st = _exec_env->stream_mgr()->transmit_chunk(*request, &wrapped_done);
}

template <typename T>
void PInternalServiceImplBase<T>::transmit_chunk_via_http(google::protobuf::RpcController* cntl_base,
                                                          const PHttpRequest* request, PTransmitChunkResult* response,
                                                          google::protobuf::Closure* done) {
    auto task = [=]() {
        auto params = std::make_shared<PTransmitChunkParams>();
        auto get_params = [&]() -> Status {
            auto* cntl = static_cast<brpc::Controller*>(cntl_base);
            butil::IOBuf& iobuf = cntl->request_attachment();
            // deserialize PTransmitChunkParams
            size_t params_size = 0;
            iobuf.cutn(&params_size, sizeof(params_size));
            butil::IOBuf params_from;
            iobuf.cutn(&params_from, params_size);
            butil::IOBufAsZeroCopyInputStream wrapper(params_from);
            params->ParseFromZeroCopyStream(&wrapper);
            // the left size is from chunks' data
            size_t attachment_size = 0;
            iobuf.cutn(&attachment_size, sizeof(attachment_size));
            if (attachment_size != iobuf.size()) {
                Status st = Status::InternalError(
                        fmt::format("{} != {} during deserialization via http", attachment_size, iobuf.size()));
                return st;
            }
            return Status::OK();
        };
        // may throw std::bad_alloc exception.
        Status st = get_params();
        if (!st.ok()) {
            st.to_protobuf(response->mutable_status());
            done->Run();
            LOG(WARNING) << "transmit_data via http rpc failed, message=" << st.get_error_msg();
            return;
        }
        this->_transmit_chunk(cntl_base, params.get(), response, done);
    };
    if (!_exec_env->query_rpc_pool()->try_offer(std::move(task))) {
        ClosureGuard closure_guard(done);
        Status::ServiceUnavailable("submit transmit_chunk_via_http task failed")
                .to_protobuf(response->mutable_status());
    }
}

template <typename T>
void PInternalServiceImplBase<T>::transmit_runtime_filter(google::protobuf::RpcController* cntl_base,
                                                          const PTransmitRuntimeFilterParams* request,
                                                          PTransmitRuntimeFilterResult* response,
                                                          google::protobuf::Closure* done) {
    auto task = [=]() { this->_transmit_runtime_filter(cntl_base, request, response, done); };
    if (!_exec_env->query_rpc_pool()->try_offer(std::move(task))) {
        ClosureGuard closure_guard(done);
        Status::ServiceUnavailable("submit transmit_runtime_filter task failed")
                .to_protobuf(response->mutable_status());
    }
}
template <typename T>
void PInternalServiceImplBase<T>::_transmit_runtime_filter(google::protobuf::RpcController* cntl_base,
                                                           const PTransmitRuntimeFilterParams* request,
                                                           PTransmitRuntimeFilterResult* response,
                                                           google::protobuf::Closure* done) {
    VLOG_FILE << "transmit runtime filter: fragment_instance_id = " << print_id(request->finst_id())
              << " query_id = " << print_id(request->query_id()) << ", is_partial = " << request->is_partial()
              << ", filter_id = " << request->filter_id() << ", is_pipeline = " << request->is_pipeline();
    ClosureGuard closure_guard(done);
    _exec_env->runtime_filter_worker()->receive_runtime_filter(*request);
    Status st;
    st.to_protobuf(response->mutable_status());
}

template <typename T>
void PInternalServiceImplBase<T>::tablet_writer_open(google::protobuf::RpcController* cntl_base,
                                                     const PTabletWriterOpenRequest* request,
                                                     PTabletWriterOpenResult* response,
                                                     google::protobuf::Closure* done) {
    ClosureGuard closure_guard(done);
    response->mutable_status()->set_status_code(TStatusCode::NOT_IMPLEMENTED_ERROR);
}

template <typename T>
void PInternalServiceImplBase<T>::exec_plan_fragment(google::protobuf::RpcController* cntl_base,
                                                     const PExecPlanFragmentRequest* request,
                                                     PExecPlanFragmentResult* response,
                                                     google::protobuf::Closure* done) {
    auto task = [=]() { this->_exec_plan_fragment(cntl_base, request, response, done); };
    if (!_exec_env->query_rpc_pool()->try_offer(std::move(task))) {
        ClosureGuard closure_guard(done);
        Status::ServiceUnavailable("submit exec_plan_fragment task failed").to_protobuf(response->mutable_status());
    }
}

template <typename T>
void PInternalServiceImplBase<T>::_exec_plan_fragment(google::protobuf::RpcController* cntl_base,
                                                      const PExecPlanFragmentRequest* request,
                                                      PExecPlanFragmentResult* response,
                                                      google::protobuf::Closure* done) {
    ClosureGuard closure_guard(done);
    auto* cntl = static_cast<brpc::Controller*>(cntl_base);
    if (k_starrocks_exit.load(std::memory_order_relaxed) || k_starrocks_exit_quick.load(std::memory_order_relaxed)) {
        cntl->SetFailed(brpc::EINTERNAL, "BE is shutting down");
        LOG(WARNING) << "reject exec plan fragment because of exit";
        return;
    }

    auto st = _exec_plan_fragment(cntl);
    if (!st.ok()) {
        LOG(WARNING) << "exec plan fragment failed, errmsg=" << st.get_error_msg();
    }
    st.to_protobuf(response->mutable_status());
}

template <typename T>
void PInternalServiceImplBase<T>::exec_batch_plan_fragments(google::protobuf::RpcController* cntl_base,
                                                            const PExecBatchPlanFragmentsRequest* request,
                                                            PExecBatchPlanFragmentsResult* response,
                                                            google::protobuf::Closure* done) {
    auto task = [=]() { this->_exec_batch_plan_fragments(cntl_base, request, response, done); };
    if (!_exec_env->pipeline_prepare_pool()->try_offer(std::move(task))) {
        ClosureGuard closure_guard(done);
        Status::ServiceUnavailable("submit exec_batch_plan_fragments failed").to_protobuf(response->mutable_status());
    }
}

template <typename T>
void PInternalServiceImplBase<T>::_exec_batch_plan_fragments(google::protobuf::RpcController* cntl_base,
                                                             const PExecBatchPlanFragmentsRequest* request,
                                                             PExecBatchPlanFragmentsResult* response,
                                                             google::protobuf::Closure* done) {
    ClosureGuard closure_guard(done);
    auto* cntl = static_cast<brpc::Controller*>(cntl_base);
    auto ser_request = cntl->request_attachment().to_string();
    std::shared_ptr<TExecBatchPlanFragmentsParams> t_batch_requests = std::make_shared<TExecBatchPlanFragmentsParams>();
    {
        const auto* buf = (const uint8_t*)ser_request.data();
        uint32_t len = ser_request.size();
        if (Status status = deserialize_thrift_msg(buf, &len, TProtocolType::BINARY, t_batch_requests.get());
            !status.ok()) {
            status.to_protobuf(response->mutable_status());
            return;
        }
    }

    auto& common_request = t_batch_requests->common_param;
    auto& unique_requests = t_batch_requests->unique_param_per_instance;

    if (unique_requests.empty()) {
        Status::OK().to_protobuf(response->mutable_status());
        return;
    }

    Status status = _exec_plan_fragment_by_pipeline(common_request, unique_requests[0]);
    status.to_protobuf(response->mutable_status());
}

template <typename T>
void PInternalServiceImplBase<T>::tablet_writer_add_batch(google::protobuf::RpcController* controller,
                                                          const PTabletWriterAddBatchRequest* request,
                                                          PTabletWriterAddBatchResult* response,
                                                          google::protobuf::Closure* done) {
    ClosureGuard closure_guard(done);
    response->mutable_status()->set_status_code(TStatusCode::NOT_IMPLEMENTED_ERROR);
}

template <typename T>
void PInternalServiceImplBase<T>::tablet_writer_add_chunk(google::protobuf::RpcController* cntl_base,
                                                          const PTabletWriterAddChunkRequest* request,
                                                          PTabletWriterAddBatchResult* response,
                                                          google::protobuf::Closure* done) {
    ClosureGuard closure_guard(done);
    response->mutable_status()->set_status_code(TStatusCode::NOT_IMPLEMENTED_ERROR);
}

template <typename T>
void PInternalServiceImplBase<T>::tablet_writer_add_chunks(google::protobuf::RpcController* cntl_base,
                                                           const PTabletWriterAddChunksRequest* request,
                                                           PTabletWriterAddBatchResult* response,
                                                           google::protobuf::Closure* done) {
    ClosureGuard closure_guard(done);
    response->mutable_status()->set_status_code(TStatusCode::NOT_IMPLEMENTED_ERROR);
}

template <typename T>
void PInternalServiceImplBase<T>::tablet_writer_add_segment(google::protobuf::RpcController* controller,
                                                            const PTabletWriterAddSegmentRequest* request,
                                                            PTabletWriterAddSegmentResult* response,
                                                            google::protobuf::Closure* done) {
    ClosureGuard closure_guard(done);
    response->mutable_status()->set_status_code(TStatusCode::NOT_IMPLEMENTED_ERROR);
}

template <typename T>
void PInternalServiceImplBase<T>::tablet_writer_cancel(google::protobuf::RpcController* cntl_base,
                                                       const PTabletWriterCancelRequest* request,
                                                       PTabletWriterCancelResult* response,
                                                       google::protobuf::Closure* done) {}

template <typename T>
Status PInternalServiceImplBase<T>::_exec_plan_fragment(brpc::Controller* cntl) {
    auto ser_request = cntl->request_attachment().to_string();
    TExecPlanFragmentParams t_request;
    {
        const auto* buf = (const uint8_t*)ser_request.data();
        uint32_t len = ser_request.size();
        RETURN_IF_ERROR(deserialize_thrift_msg(buf, &len, TProtocolType::BINARY, &t_request));
    }
    bool is_pipeline = t_request.__isset.is_pipeline && t_request.is_pipeline;
    LOG(INFO) << "exec plan fragment, fragment_instance_id=" << print_id(t_request.params.fragment_instance_id)
              << ", coord=" << t_request.coord << ", backend=" << t_request.backend_num
              << ", is_pipeline=" << is_pipeline << ", chunk_size=" << t_request.query_options.batch_size;
    if (is_pipeline) {
        return _exec_plan_fragment_by_pipeline(t_request, t_request);
    } else {
        return _exec_plan_fragment_by_non_pipeline(t_request);
    }
}

template <typename T>
Status PInternalServiceImplBase<T>::_exec_plan_fragment_by_pipeline(const TExecPlanFragmentParams& t_common_param,
                                                                    const TExecPlanFragmentParams& t_unique_request) {
    pipeline::FragmentExecutor fragment_executor;
    auto status = fragment_executor.prepare(_exec_env, t_common_param, t_unique_request);
    if (status.ok()) {
        return fragment_executor.execute(_exec_env);
    } else {
        return status.is_duplicate_rpc_invocation() ? Status::OK() : status;
    }
}

template <typename T>
Status PInternalServiceImplBase<T>::_exec_plan_fragment_by_non_pipeline(const TExecPlanFragmentParams& t_request) {
    return _exec_env->fragment_mgr()->exec_plan_fragment(t_request);
}

inline std::string cancel_reason_to_string(::starrocks::PPlanFragmentCancelReason reason) {
    switch (reason) {
    case LIMIT_REACH:
        return "LimitReach";
    case USER_CANCEL:
        return "UserCancel";
    case INTERNAL_ERROR:
        return "InternalError";
    case TIMEOUT:
        return "TimeOut";
    default:
        return "UnknownReason";
    }
}

template <typename T>
void PInternalServiceImplBase<T>::cancel_plan_fragment(google::protobuf::RpcController* cntl_base,
                                                       const PCancelPlanFragmentRequest* request,
                                                       PCancelPlanFragmentResult* result,
                                                       google::protobuf::Closure* done) {
    auto task = [=]() { this->_cancel_plan_fragment(cntl_base, request, result, done); };
    if (!_exec_env->query_rpc_pool()->try_offer(std::move(task))) {
        ClosureGuard closure_guard(done);
        Status::ServiceUnavailable("submit cancel_plan_fragment task failed").to_protobuf(result->mutable_status());
    }
}

template <typename T>
void PInternalServiceImplBase<T>::_cancel_plan_fragment(google::protobuf::RpcController* cntl_base,
                                                        const PCancelPlanFragmentRequest* request,
                                                        PCancelPlanFragmentResult* result,
                                                        google::protobuf::Closure* done) {
    ClosureGuard closure_guard(done);
    TUniqueId tid;
    tid.__set_hi(request->finst_id().hi());
    tid.__set_lo(request->finst_id().lo());

    Status st;
    auto reason_string =
            request->has_cancel_reason() ? cancel_reason_to_string(request->cancel_reason()) : "UnknownReason";
    LOG(INFO) << "cancel fragment, fragment_instance_id=" << print_id(tid) << ", reason: " << reason_string;

    if (request->has_is_pipeline() && request->is_pipeline()) {
        TUniqueId query_id;
        if (!request->has_query_id()) {
            LOG(WARNING) << "cancel_plan_fragment must provide query_id in request, upgrade FE";
            st = Status::NotSupported("cancel_plan_fragment must provide query_id in request, upgrade FE");
            st.to_protobuf(result->mutable_status());
            return;
        }
        query_id.__set_hi(request->query_id().hi());
        query_id.__set_lo(request->query_id().lo());
        auto&& query_ctx = _exec_env->query_context_mgr()->get(query_id);
        if (!query_ctx) {
            LOG(INFO) << strings::Substitute("QueryContext already destroyed: query_id=$0, fragment_instance_id=$1",
                                             print_id(query_id), print_id(tid));
            st.to_protobuf(result->mutable_status());
            return;
        }
        auto&& fragment_ctx = query_ctx->fragment_mgr()->get(tid);
        if (!fragment_ctx) {
            LOG(INFO) << strings::Substitute("FragmentContext already destroyed: query_id=$0, fragment_instance_id=$1",
                                             print_id(query_id), print_id(tid));
        } else {
            fragment_ctx->cancel(Status::Cancelled(reason_string));
        }
    } else {
        if (request->has_cancel_reason()) {
            st = _exec_env->fragment_mgr()->cancel(tid, request->cancel_reason());
        } else {
            LOG(INFO) << "cancel fragment, fragment_instance_id=" << print_id(tid);
            st = _exec_env->fragment_mgr()->cancel(tid);
        }
        if (!st.ok()) {
            LOG(WARNING) << "cancel plan fragment failed, errmsg=" << st.get_error_msg();
        }
    }
    st.to_protobuf(result->mutable_status());
}

template <typename T>
void PInternalServiceImplBase<T>::fetch_data(google::protobuf::RpcController* cntl_base,
                                             const PFetchDataRequest* request, PFetchDataResult* result,
                                             google::protobuf::Closure* done) {
    auto task = [=]() { this->_fetch_data(cntl_base, request, result, done); };
    if (!_exec_env->query_rpc_pool()->try_offer(std::move(task))) {
        ClosureGuard closure_guard(done);
        Status::ServiceUnavailable("submit fetch_data task failed").to_protobuf(result->mutable_status());
    }
}

template <typename T>
void PInternalServiceImplBase<T>::_fetch_data(google::protobuf::RpcController* cntl_base,
                                              const PFetchDataRequest* request, PFetchDataResult* result,
                                              google::protobuf::Closure* done) {
    auto* cntl = static_cast<brpc::Controller*>(cntl_base);
    auto* ctx = new GetResultBatchCtx(cntl, result, done);
    _exec_env->result_mgr()->fetch_data(request->finst_id(), ctx);
}

template <typename T>
void PInternalServiceImplBase<T>::trigger_profile_report(google::protobuf::RpcController* controller,
                                                         const PTriggerProfileReportRequest* request,
                                                         PTriggerProfileReportResult* result,
                                                         google::protobuf::Closure* done) {
    ClosureGuard closure_guard(done);
    result->mutable_status()->set_status_code(TStatusCode::OK);

    TUniqueId query_id;
    DCHECK(request->has_query_id());
    query_id.__set_hi(request->query_id().hi());
    query_id.__set_lo(request->query_id().lo());

    auto&& query_ctx = _exec_env->query_context_mgr()->get(query_id);
    if (query_ctx == nullptr) {
        LOG(WARNING) << "query context is null, query_id=" << print_id(query_id);
        result->mutable_status()->set_status_code(TStatusCode::NOT_FOUND);
        return;
    }

    for (size_t i = 0; i < request->instance_ids_size(); i++) {
        TUniqueId instance_id;
        instance_id.__set_hi(request->instance_ids(i).hi());
        instance_id.__set_lo(request->instance_ids(i).lo());

        auto&& fragment_ctx = query_ctx->fragment_mgr()->get(instance_id);
        if (fragment_ctx == nullptr) {
            LOG(WARNING) << "fragment context is null, query_id=" << print_id(query_id)
                         << ", instance_id=" << print_id(instance_id);
            result->mutable_status()->set_status_code(TStatusCode::NOT_FOUND);
            return;
        }
        pipeline::DriverExecutor* driver_executor = _exec_env->wg_driver_executor();
        driver_executor->report_exec_state(query_ctx.get(), fragment_ctx.get(), Status::OK(), false, true);
    }
}

template <typename T>
void PInternalServiceImplBase<T>::collect_query_statistics(google::protobuf::RpcController* controller,
                                                           const PCollectQueryStatisticsRequest* request,
                                                           PCollectQueryStatisticsResult* result,
                                                           google::protobuf::Closure* done) {
    ClosureGuard closure_guard(done);
    _exec_env->query_context_mgr()->collect_query_statistics(request, result);
}

template <typename T>
void PInternalServiceImplBase<T>::get_info(google::protobuf::RpcController* controller, const PProxyRequest* request,
                                           PProxyResult* response, google::protobuf::Closure* done) {
    int timeout_ms =
            request->has_timeout() ? request->timeout() * 1000 : config::routine_load_kafka_timeout_second * 1000;

    auto task = [this, request, response, done, timeout_ms]() {
        this->_get_info_impl(request, response, done, timeout_ms);
    };

    auto st = _exec_env->load_rpc_pool()->submit_func(std::move(task));
    if (!st.ok()) {
        LOG(WARNING) << "get kafka info: " << st << " ,timeout: " << timeout_ms
                     << ", thread pool size: " << _exec_env->load_rpc_pool()->num_threads();
        ClosureGuard closure_guard(done);
        Status::ServiceUnavailable(
                fmt::format("too busy to get kafka info, please check the kafka broker status, timeout ms: {}",
                            timeout_ms))
                .to_protobuf(response->mutable_status());
    }
}

template <typename T>
void PInternalServiceImplBase<T>::_get_info_impl(const PProxyRequest* request, PProxyResult* response,
                                                 google::protobuf::Closure* done, int timeout_ms) {
    ClosureGuard closure_guard(done);

    if (timeout_ms <= 0) {
        Status::TimedOut("get kafka info timeout").to_protobuf(response->mutable_status());
        return;
    }
    Status st = Status::OK();
    std::string group_id;
    MonotonicStopWatch watch;
    watch.start();
    if (request->has_kafka_meta_request()) {
        std::vector<int32_t> partition_ids;
        st = _exec_env->routine_load_task_executor()->get_kafka_partition_meta(request->kafka_meta_request(),
                                                                               &partition_ids, timeout_ms, &group_id);
        if (st.ok()) {
            PKafkaMetaProxyResult* kafka_result = response->mutable_kafka_meta_result();
            for (int32_t id : partition_ids) {
                kafka_result->add_partition_ids(id);
            }
        }
    } else if (request->has_kafka_offset_request()) {
        std::vector<int64_t> beginning_offsets;
        std::vector<int64_t> latest_offsets;
        st = _exec_env->routine_load_task_executor()->get_kafka_partition_offset(
                request->kafka_offset_request(), &beginning_offsets, &latest_offsets, timeout_ms, &group_id);
        if (st.ok()) {
            auto result = response->mutable_kafka_offset_result();
            for (int i = 0; i < beginning_offsets.size(); i++) {
                result->add_partition_ids(request->kafka_offset_request().partition_ids(i));
                result->add_beginning_offsets(beginning_offsets[i]);
                result->add_latest_offsets(latest_offsets[i]);
            }
        }
    } else if (request->has_kafka_offset_batch_request()) {
        for (const auto& offset_req : request->kafka_offset_batch_request().requests()) {
            std::vector<int64_t> beginning_offsets;
            std::vector<int64_t> latest_offsets;

            auto left_ms = timeout_ms - watch.elapsed_time() / 1000 / 1000;
            if (left_ms <= 0) {
                st = Status::TimedOut("get kafka offset batch timeout");
                break;
            }

            st = _exec_env->routine_load_task_executor()->get_kafka_partition_offset(
                    offset_req, &beginning_offsets, &latest_offsets, left_ms, &group_id);
            auto offset_result = response->mutable_kafka_offset_batch_result()->add_results();
            if (st.ok()) {
                for (int i = 0; i < beginning_offsets.size(); i++) {
                    offset_result->add_partition_ids(offset_req.partition_ids(i));
                    offset_result->add_beginning_offsets(beginning_offsets[i]);
                    offset_result->add_latest_offsets(latest_offsets[i]);
                }
            } else {
                response->clear_kafka_offset_batch_result();
                break;
            }
        }
    }
    st.to_protobuf(response->mutable_status());
    if (!st.ok()) {
        LOG(WARNING) << "group id " << group_id << " get kafka info timeout. used time(ms) "
                     << watch.elapsed_time() / 1000 / 1000 << ". error: " << st.to_string();
    }
}

template <typename T>
void PInternalServiceImplBase<T>::get_pulsar_info(google::protobuf::RpcController* controller,
                                                  const PPulsarProxyRequest* request, PPulsarProxyResult* response,
                                                  google::protobuf::Closure* done) {
    int timeout_ms =
            request->has_timeout() ? request->timeout() * 1000 : config::routine_load_pulsar_timeout_second * 1000;

    auto task = [this, request, response, done, timeout_ms]() {
        this->_get_pulsar_info_impl(request, response, done, timeout_ms);
    };

    auto st = _exec_env->load_rpc_pool()->submit_func(std::move(task));
    if (!st.ok()) {
        LOG(WARNING) << "get pulsar info: " << st << " ,timeout: " << timeout_ms
                     << ", thread pool size: " << _exec_env->load_rpc_pool()->num_threads();
        ClosureGuard closure_guard(done);
        Status::ServiceUnavailable(fmt::format("too busy to get pulsar info, please check the pulsar status, "
                                               "timeout ms: {}",
                                               timeout_ms))
                .to_protobuf(response->mutable_status());
    }
}

template <typename T>
void PInternalServiceImplBase<T>::_get_pulsar_info_impl(const PPulsarProxyRequest* request,
                                                        PPulsarProxyResult* response, google::protobuf::Closure* done,
                                                        int timeout_ms) {
    ClosureGuard closure_guard(done);

    if (timeout_ms <= 0) {
        Status::TimedOut("get pulsar info timeout").to_protobuf(response->mutable_status());
        return;
    }

    if (request->has_pulsar_meta_request()) {
        std::vector<std::string> partitions;
        Status st = _exec_env->routine_load_task_executor()->get_pulsar_partition_meta(request->pulsar_meta_request(),
                                                                                       &partitions);
        if (st.ok()) {
            PPulsarMetaProxyResult* pulsar_result = response->mutable_pulsar_meta_result();
            for (const std::string& p : partitions) {
                pulsar_result->add_partitions(p);
            }
        }
        st.to_protobuf(response->mutable_status());
        return;
    }
    if (request->has_pulsar_backlog_request()) {
        std::vector<int64_t> backlog_nums;
        Status st = _exec_env->routine_load_task_executor()->get_pulsar_partition_backlog(
                request->pulsar_backlog_request(), &backlog_nums);
        if (st.ok()) {
            auto result = response->mutable_pulsar_backlog_result();
            for (int i = 0; i < backlog_nums.size(); i++) {
                result->add_partitions(request->pulsar_backlog_request().partitions(i));
                result->add_backlog_nums(backlog_nums[i]);
            }
        }
        st.to_protobuf(response->mutable_status());
        return;
    }
    if (request->has_pulsar_backlog_batch_request()) {
        for (const auto& backlog_req : request->pulsar_backlog_batch_request().requests()) {
            std::vector<int64_t> backlog_nums;
            Status st =
                    _exec_env->routine_load_task_executor()->get_pulsar_partition_backlog(backlog_req, &backlog_nums);
            auto backlog_result = response->mutable_pulsar_backlog_batch_result()->add_results();
            if (st.ok()) {
                for (int i = 0; i < backlog_nums.size(); i++) {
                    backlog_result->add_partitions(backlog_req.partitions(i));
                    backlog_result->add_backlog_nums(backlog_nums[i]);
                }
            } else {
                response->clear_pulsar_backlog_batch_result();
                st.to_protobuf(response->mutable_status());
                return;
            }
        }
    }
    Status::OK().to_protobuf(response->mutable_status());
}

template <typename T>
void PInternalServiceImplBase<T>::get_file_schema(google::protobuf::RpcController* controller,
                                                  const PGetFileSchemaRequest* request, PGetFileSchemaResult* response,
                                                  google::protobuf::Closure* done) {
    auto task = [=]() { this->_get_file_schema(controller, request, response, done); };

    auto st = _exec_env->load_rpc_pool()->submit_func(std::move(task));
    if (!st.ok()) {
        LOG(WARNING) << "get file schema: " << st
                     << ", thread pool size: " << _exec_env->load_rpc_pool()->num_threads();
        ClosureGuard closure_guard(done);
        Status::ServiceUnavailable("too busy to get file schema").to_protobuf(response->mutable_status());
    }
}

template <typename T>
void PInternalServiceImplBase<T>::_get_file_schema(google::protobuf::RpcController* controller,
                                                   const PGetFileSchemaRequest* request, PGetFileSchemaResult* response,
                                                   google::protobuf::Closure* done) {
    ClosureGuard closure_guard(done);
    TGetFileSchemaRequest t_request;

    auto st = Status::OK();
    DeferOp defer1([&st, &response] { st.to_protobuf(response->mutable_status()); });

    {
        auto* cntl = static_cast<brpc::Controller*>(controller);
        auto ser_request = cntl->request_attachment().to_string();
        const auto* buf = (const uint8_t*)ser_request.data();
        uint32_t len = ser_request.size();
        st = deserialize_thrift_msg(buf, &len, TProtocolType::BINARY, &t_request);
        if (!st.ok()) {
            LOG(WARNING) << "deserialize thrift message error: " << st;
            return;
        }
    }
    const auto& scan_range = t_request.scan_range.broker_scan_range;
    if (scan_range.ranges.empty()) {
        st = Status::InvalidArgument("No file to scan. Please check the specified path.");
        return;
    }

    std::unique_ptr<FileScanner> p_scanner;
    auto tp = scan_range.ranges[0].format_type;
    {
        RuntimeState state{};
        RuntimeProfile profile{"dummy_profile", false};
        ScannerCounter counter{};
        switch (tp) {
        case TFileFormatType::FORMAT_PARQUET:
            p_scanner = std::make_unique<ParquetScanner>(&state, &profile, scan_range, &counter, true);
            break;

        case TFileFormatType::FORMAT_ORC:
            p_scanner = std::make_unique<ORCScanner>(&state, &profile, scan_range, &counter, true);
            break;

        default:
            auto err_msg = fmt::format("get file schema failed, format: {} not supported", to_string(tp));
            LOG(WARNING) << err_msg;
            st = Status::InvalidArgument(err_msg);
            return;
        }
    }

    st = p_scanner->open();
    if (!st.ok()) {
        LOG(WARNING) << "open file scanner failed: " << st;
        return;
    }

    DeferOp defer2([&p_scanner] { p_scanner->close(); });

    std::vector<SlotDescriptor> schema;
    st = p_scanner->get_schema(&schema);
    if (!st.ok()) {
        LOG(WARNING) << "get schema failed: " << st;
        return;
    }

    for (const auto& slot : schema) {
        slot.to_protobuf(response->add_schema());
    }
    return;
}

template <typename T>
void PInternalServiceImplBase<T>::submit_mv_maintenance_task(google::protobuf::RpcController* controller,
                                                             const PMVMaintenanceTaskRequest* request,
                                                             PMVMaintenanceTaskResult* response,
                                                             google::protobuf::Closure* done) {
    ClosureGuard closure_guard(done);
    auto* cntl = static_cast<brpc::Controller*>(controller);
    Status st = _submit_mv_maintenance_task(cntl);
    if (!st.ok()) {
        LOG(WARNING) << "submit mv maintenance task failed, errmsg=" << st.get_error_msg();
    }
    st.to_protobuf(response->mutable_status());
    return;
}

template <typename T>
Status PInternalServiceImplBase<T>::_submit_mv_maintenance_task(brpc::Controller* cntl) {
    auto ser_request = cntl->request_attachment().to_string();
    TMVMaintenanceTasks t_request;
    {
        const auto* buf = (const uint8_t*)ser_request.data();
        uint32_t len = ser_request.size();
        RETURN_IF_ERROR(deserialize_thrift_msg(buf, &len, TProtocolType::BINARY, &t_request));
    }
    LOG(INFO) << "[MV] mv maintenance task, query_id=" << t_request.query_id << ", mv_task_type:" << t_request.task_type
              << ", db_name=" << t_request.db_name << ", mv_name=" << t_request.mv_name
              << ", job_id=" << t_request.job_id << ", task_id=" << t_request.task_id
              << ", signature=" << t_request.signature;
    VLOG(2) << "[MV] mv maintenance task, plan=" << apache::thrift::ThriftDebugString(t_request);

    auto mv_task_type = t_request.task_type;
    const TUniqueId& query_id = t_request.query_id;

    // Check the existence of job
    auto query_ctx = _exec_env->query_context_mgr()->get(query_id);
    if (mv_task_type != MVTaskType::START_MAINTENANCE && !query_ctx) {
        std::string msg = fmt::format("execute maintenance task failed, query id not found:", print_id(query_id));
        LOG(WARNING) << msg;
        return Status::InternalError(msg);
    }

    switch (mv_task_type) {
    case MVTaskType::START_MAINTENANCE: {
        if (query_ctx) {
            std::string msg = fmt::format("MV Job already existed: {}", print_id(query_id));
            LOG(WARNING) << msg;
            return Status::InternalError(msg);
        }
        RETURN_IF_ERROR(_mv_start_maintenance(t_request));
        break;
    }
    case MVTaskType::START_EPOCH: {
        RETURN_IF_ERROR(_mv_start_epoch(query_ctx, t_request));
        break;
    }
    case MVTaskType::COMMIT_EPOCH: {
        RETURN_IF_ERROR(_mv_commit_epoch(query_ctx, t_request));

        auto& commit_epoch = t_request.commit_epoch;
        auto& version_info = commit_epoch.partition_version_infos;
        if (VLOG_ROW_IS_ON) {
            std::stringstream version_str;
            version_str << " version_info=[";
            for (auto& part : version_info) {
                version_str << part;
            }
            version_str << "]";
            VLOG(2) << "MV commit_epoch: epoch=" << commit_epoch.epoch << version_str.str();
        }

        break;
    }
    // TODO(murphy)
    // case MVTaskType: {
    //     break;
    // }
    case MVTaskType::STOP_MAINTENANCE: {
        // Find the fragment context for the specific MV job
        TUniqueId query_id;
        auto&& existing_query_ctx = _exec_env->query_context_mgr()->get(query_id);
        if (!existing_query_ctx) {
            return Status::InternalError(fmt::format("MV Job has been cancelled: {}.", print_id(query_id)));
        }
        auto stream_epoch_manager = existing_query_ctx->stream_epoch_manager();
        RETURN_IF_ERROR(stream_epoch_manager->set_finished(_exec_env, existing_query_ctx.get()));
        break;
    }
    default:
        return Status::NotSupported(fmt::format("Unsupported MVTaskType: {}", mv_task_type));
    }
    return Status::OK();
}

template <typename T>
Status PInternalServiceImplBase<T>::_mv_start_maintenance(const TMVMaintenanceTasks& task) {
    RETURN_IF(!task.__isset.start_maintenance, Status::InternalError("must be start_maintenance task"));
    auto& start_maintenance = task.start_maintenance;
    auto& fragments = start_maintenance.fragments;
    for (const auto& fragment : fragments) {
        pipeline::FragmentExecutor fragment_executor;
        RETURN_IF_ERROR(fragment_executor.prepare(_exec_env, fragment, fragment));
        RETURN_IF_ERROR(fragment_executor.execute(_exec_env));
    }

    // Prepare EpochManager
    const TUniqueId& query_id = task.query_id;
    auto&& existing_query_ctx = _exec_env->query_context_mgr()->get(query_id);
    if (!existing_query_ctx) {
        LOG(WARNING) << "start maintenance failed, query id not found:" << print_id(query_id);
        return Status::InternalError(fmt::format("MV Job has not been prepared: {}.", print_id(query_id)));
    }
    std::vector<pipeline::FragmentContext*> fragment_ctxs;
    for (auto& fragment : fragments) {
        auto fragment_instance_id = fragment.params.fragment_instance_id;
        auto&& fragment_ctx = existing_query_ctx->fragment_mgr()->get(fragment_instance_id);
        if (!fragment_ctx) {
            LOG(WARNING) << "start_epoch maintenance failed, fragment instance id not found:"
                         << print_id(fragment_instance_id);
            return Status::InternalError(
                    fmt::format("MV Job fragment_instance_id has been cancelled: {}.", print_id(fragment_instance_id)));
        }
        fragment_ctxs.push_back(fragment_ctx.get());
    }
    auto stream_epoch_manager = existing_query_ctx->stream_epoch_manager();
    DCHECK(stream_epoch_manager);
    auto maintenance_task = MVMaintenanceTaskInfo::from_maintenance_task(task);
    RETURN_IF_ERROR(stream_epoch_manager->prepare(maintenance_task, fragment_ctxs));
    return Status::OK();
}

template <typename T>
Status PInternalServiceImplBase<T>::_mv_start_epoch(const pipeline::QueryContextPtr& query_ctx,
                                                    const TMVMaintenanceTasks& task) {
    RETURN_IF(!task.__isset.start_epoch, Status::InternalError("must be start_epoch task"));
    auto& start_epoch_task = task.start_epoch;
    auto stream_epoch_manager = query_ctx->stream_epoch_manager();
    EpochInfo epoch_info = EpochInfo::from_start_epoch_task(start_epoch_task);
    pipeline::ScanRangeInfo scan_info = pipeline::ScanRangeInfo::from_start_epoch_start(start_epoch_task);

    std::vector<pipeline::FragmentContext*> fragment_ctxs;
    for (auto& [fragment_instance_id, node_to_scan_ranges] : start_epoch_task.per_node_scan_ranges) {
        // Find the fragment_ctx by fragment_instance_id;
        auto&& fragment_ctx = query_ctx->fragment_mgr()->get(fragment_instance_id);
        if (!fragment_ctx) {
            LOG(WARNING) << "start_epoch maintenance failed, fragment instance id not found:"
                         << print_id(fragment_instance_id);
            return Status::InternalError(
                    fmt::format("MV Job fragment_instance_id has been cancelled: {}.", print_id(fragment_instance_id)));
        }
        fragment_ctxs.push_back(fragment_ctx.get());
    }

    // Update state in the runtime state.
    return stream_epoch_manager->start_epoch(_exec_env, query_ctx.get(), fragment_ctxs, epoch_info, scan_info);
}

template <typename T>
Status PInternalServiceImplBase<T>::_mv_abort_epoch(const pipeline::QueryContextPtr& query_ctx,
                                                    const TMVMaintenanceTasks& task) {
    return Status::NotSupported("TODO");
}

template <typename T>
Status PInternalServiceImplBase<T>::_mv_commit_epoch(const pipeline::QueryContextPtr& query_ctx,
                                                     const TMVMaintenanceTasks& task) {
    RETURN_IF(!task.__isset.commit_epoch, Status::InternalError("must be commit_epoch task"));
    auto& commit_epoch_task = task.commit_epoch;
    auto* agent_server = ExecEnv::GetInstance()->agent_server();
    auto token =
            agent_server->get_thread_pool(TTaskType::PUBLISH_VERSION)->new_token(ThreadPool::ExecutionMode::CONCURRENT);

    std::unordered_set<DataDir*> affected_dirs;
    TFinishTaskRequest finish_task_request;
    finish_task_request.__set_backend(BackendOptions::get_localBackend());
    finish_task_request.__set_report_version(curr_report_version());
    TPublishVersionRequest publish_version_req;
    publish_version_req.partition_version_infos = commit_epoch_task.partition_version_infos;
    publish_version_req.transaction_id = commit_epoch_task.transaction_id;

    run_publish_version_task(token.get(), publish_version_req, finish_task_request, affected_dirs, 0);
    StorageEngine::instance()->txn_manager()->flush_dirs(affected_dirs);
    return Status::OK();
}

template <typename T>
void PInternalServiceImplBase<T>::local_tablet_reader_open(google::protobuf::RpcController* controller,
                                                           const PTabletReaderOpenRequest* request,
                                                           PTabletReaderOpenResult* response,
                                                           google::protobuf::Closure* done) {
    ClosureGuard closure_guard(done);
    response->mutable_status()->set_status_code(TStatusCode::NOT_IMPLEMENTED_ERROR);
}

template <typename T>
void PInternalServiceImplBase<T>::local_tablet_reader_close(google::protobuf::RpcController* controller,
                                                            const PTabletReaderCloseRequest* request,
                                                            PTabletReaderCloseResult* response,
                                                            google::protobuf::Closure* done) {
    ClosureGuard closure_guard(done);
    response->mutable_status()->set_status_code(TStatusCode::NOT_IMPLEMENTED_ERROR);
}

template <typename T>
void PInternalServiceImplBase<T>::local_tablet_reader_multi_get(google::protobuf::RpcController* controller,
                                                                const PTabletReaderMultiGetRequest* request,
                                                                PTabletReaderMultiGetResult* response,
                                                                google::protobuf::Closure* done) {
    ClosureGuard closure_guard(done);
    response->mutable_status()->set_status_code(TStatusCode::NOT_IMPLEMENTED_ERROR);
}

template <typename T>
void PInternalServiceImplBase<T>::local_tablet_reader_scan_open(google::protobuf::RpcController* controller,
                                                                const PTabletReaderScanOpenRequest* request,
                                                                PTabletReaderScanOpenResult* response,
                                                                google::protobuf::Closure* done) {
    ClosureGuard closure_guard(done);
    response->mutable_status()->set_status_code(TStatusCode::NOT_IMPLEMENTED_ERROR);
}

template <typename T>
void PInternalServiceImplBase<T>::local_tablet_reader_scan_get_next(google::protobuf::RpcController* controller,
                                                                    const PTabletReaderScanGetNextRequest* request,
                                                                    PTabletReaderScanGetNextResult* response,
                                                                    google::protobuf::Closure* done) {
    ClosureGuard closure_guard(done);
    response->mutable_status()->set_status_code(TStatusCode::NOT_IMPLEMENTED_ERROR);
}

template <typename T>
void PInternalServiceImplBase<T>::execute_command(google::protobuf::RpcController* controller,
                                                  const ExecuteCommandRequestPB* request,
                                                  ExecuteCommandResultPB* response, google::protobuf::Closure* done) {
    ClosureGuard closure_guard(done);
    Status st = starrocks::execute_command(request->command(), request->params(), response->mutable_result());
    if (!st.ok()) {
        LOG(WARNING) << "execute_command failed, errmsg=" << st.to_string();
    }
    st.to_protobuf(response->mutable_status());
}

template <typename T>
void PInternalServiceImplBase<T>::update_fail_point_status(google::protobuf::RpcController* controller,
                                                           const PUpdateFailPointStatusRequest* request,
                                                           PUpdateFailPointStatusResponse* response,
                                                           google::protobuf::Closure* done) {
    ClosureGuard closure_guard(done);
#ifdef FIU_ENABLE
    const auto name = request->fail_point_name();
    auto fp = starrocks::failpoint::FailPointRegistry::GetInstance()->get(name);
    if (fp == nullptr) {
        Status::InvalidArgument(fmt::format("FailPoint {} is not existed.", name))
                .to_protobuf(response->mutable_status());
        return;
    }
    fp->setMode(request->trigger_mode());
    Status::OK().to_protobuf(response->mutable_status());
#else
    Status::NotSupported("FailPoint is not supported, need re-compile BE with ENABLE_FAULT_INJECTION")
            .to_protobuf(response->mutable_status());
#endif
}

template <typename T>
void PInternalServiceImplBase<T>::list_fail_point(google::protobuf::RpcController* controller,
                                                  const PListFailPointRequest* request,
                                                  PListFailPointResponse* response, google::protobuf::Closure* done) {
    ClosureGuard closure_guard(done);
#ifdef FIU_ENABLE
    starrocks::failpoint::FailPointRegistry::GetInstance()->iterate([&](starrocks::failpoint::FailPoint* fp) {
        auto fp_info = response->add_fail_points();
        *fp_info = fp->to_pb();
    });
#endif
    Status::OK().to_protobuf(response->mutable_status());
}

template class PInternalServiceImplBase<PInternalService>;
template class PInternalServiceImplBase<doris::PBackendService>;

} // namespace starrocks
