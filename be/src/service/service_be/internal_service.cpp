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

#include "internal_service.h"

#include "common/closure_guard.h"
#include "common/config.h"
#include "exec/pipeline/fragment_context.h"
#include "gen_cpp/BackendService.h"
#include "gutil/strings/substitute.h"
#include "runtime/buffer_control_block.h"
#include "runtime/data_stream_mgr.h"
#include "runtime/fragment_mgr.h"
#include "runtime/load_channel_mgr.h"
#include "runtime/result_buffer_mgr.h"
#include "runtime/routine_load/routine_load_task_executor.h"
#include "runtime/runtime_filter_worker.h"
#include "service/brpc.h"
#include "util/uid_util.h"

namespace starrocks {

template <typename T>
void BackendInternalServiceImpl<T>::tablet_writer_open(google::protobuf::RpcController* cntl_base,
                                                       const PTabletWriterOpenRequest* request,
                                                       PTabletWriterOpenResult* response,
                                                       google::protobuf::Closure* done) {
    VLOG_RPC << "tablet writer open, id=" << print_id(request->id()) << ", index_id=" << request->index_id()
             << ", txn_id: " << request->txn_id();
    PInternalServiceImplBase<T>::_exec_env->load_channel_mgr()->open(static_cast<brpc::Controller*>(cntl_base),
                                                                     *request, response, done);
}

template <typename T>
void BackendInternalServiceImpl<T>::tablet_writer_add_batch(google::protobuf::RpcController* controller,
                                                            const PTabletWriterAddBatchRequest* request,
                                                            PTabletWriterAddBatchResult* response,
                                                            google::protobuf::Closure* done) {
    ClosureGuard closure_guard(done);
    response->mutable_status()->set_status_code(TStatusCode::NOT_IMPLEMENTED_ERROR);
}

template <typename T>
void BackendInternalServiceImpl<T>::tablet_writer_add_chunk(google::protobuf::RpcController* cntl_base,
                                                            const PTabletWriterAddChunkRequest* request,
                                                            PTabletWriterAddBatchResult* response,
                                                            google::protobuf::Closure* done) {
    ClosureGuard closure_guard(done);
    VLOG_RPC << "tablet writer add chunk, id=" << print_id(request->id()) << ", txn_id: " << request->txn_id()
             << ", index_id=" << request->index_id() << ", sender_id=" << request->sender_id()
             << ", eos=" << request->eos();
    PInternalServiceImplBase<T>::_exec_env->load_channel_mgr()->add_chunk(*request, response);
}

template <typename T>
void BackendInternalServiceImpl<T>::tablet_writer_add_chunks(google::protobuf::RpcController* cntl_base,
                                                             const PTabletWriterAddChunksRequest* request,
                                                             PTabletWriterAddBatchResult* response,
                                                             google::protobuf::Closure* done) {
    ClosureGuard closure_guard(done);
    PInternalServiceImplBase<T>::_exec_env->load_channel_mgr()->add_chunks(*request, response);
}

template <typename T>
void BackendInternalServiceImpl<T>::tablet_writer_add_segment(google::protobuf::RpcController* controller,
                                                              const PTabletWriterAddSegmentRequest* request,
                                                              PTabletWriterAddSegmentResult* response,
                                                              google::protobuf::Closure* done) {
    VLOG_RPC << "tablet writer add segment, id=" << print_id(request->id()) << ", txn_id: " << request->txn_id()
             << ", index_id=" << request->index_id() << ", tablet_id=" << request->tablet_id()
             << ", eos=" << request->eos();
    PInternalServiceImplBase<T>::_exec_env->load_channel_mgr()->add_segment(static_cast<brpc::Controller*>(controller),
                                                                            request, response, done);
}

template <typename T>
void BackendInternalServiceImpl<T>::tablet_writer_cancel(google::protobuf::RpcController* cntl_base,
                                                         const PTabletWriterCancelRequest* request,
                                                         PTabletWriterCancelResult* response,
                                                         google::protobuf::Closure* done) {
    VLOG_RPC << "tablet writer cancel, id=" << print_id(request->id()) << ", txn_id: " << request->txn_id()
             << ", index_id=" << request->index_id() << ", sender_id=" << request->sender_id()
             << ", tablet_id=" << request->tablet_id();
    PInternalServiceImplBase<T>::_exec_env->load_channel_mgr()->cancel(static_cast<brpc::Controller*>(cntl_base),
                                                                       *request, response, done);
}

template <typename T>
void BackendInternalServiceImpl<T>::local_tablet_reader_open(google::protobuf::RpcController* controller,
                                                             const PTabletReaderOpenRequest* request,
                                                             PTabletReaderOpenResult* response,
                                                             google::protobuf::Closure* done) {}

template <typename T>
void BackendInternalServiceImpl<T>::local_tablet_reader_close(google::protobuf::RpcController* controller,
                                                              const PTabletReaderCloseRequest* request,
                                                              PTabletReaderCloseResult* response,
                                                              google::protobuf::Closure* done) {}

template <typename T>
void BackendInternalServiceImpl<T>::local_tablet_reader_multi_get(google::protobuf::RpcController* controller,
                                                                  const PTabletReaderMultiGetRequest* request,
                                                                  PTabletReaderMultiGetResult* response,
                                                                  google::protobuf::Closure* done) {}

template <typename T>
void BackendInternalServiceImpl<T>::local_tablet_reader_scan_open(google::protobuf::RpcController* controller,
                                                                  const PTabletReaderScanOpenRequest* request,
                                                                  PTabletReaderScanOpenResult* response,
                                                                  google::protobuf::Closure* done) {}

template <typename T>
void BackendInternalServiceImpl<T>::local_tablet_reader_scan_get_next(google::protobuf::RpcController* controller,
                                                                      const PTabletReaderScanGetNextRequest* request,
                                                                      PTabletReaderScanGetNextResult* response,
                                                                      google::protobuf::Closure* done) {}

template class BackendInternalServiceImpl<PInternalService>;
template class BackendInternalServiceImpl<doris::PBackendService>;
} // namespace starrocks
